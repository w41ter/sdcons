// Copyright 2021 The sdcons Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info};
use sdcons::constant;
use sdcons::types::{
    ChangeConfig, ConfigStage, Entry, EntryMeta, HardState, LogIndex, MemberState, SnapshotDesc,
};
use sdcons::{PostReady, WriteTask};
use serde::{Deserialize, Serialize};

const GLOBAL_REGION: u8 = 0x0;
const META_REGION: u8 = 0x1;
const LOG_REGION: u8 = 0x2;
const LOG_END_REGION: u8 = 0x3;
const END_REGION: u8 = LOG_END_REGION + 0x1;

const CHANNEL_BEGIN_REGION: u8 = 0x1;
const CHANNEL_END_REGION: u8 = 0x2;

const META_COMMITTED_STATS: &'static [u8] = b"committed-stats";
const META_HARD_STATES: &'static [u8] = b"hard-states";
const META_MEMBER_DESC: &'static [u8] = b"member-descs";
const META_INITIAL_STATES: &'static [u8] = b"initial-states";

#[derive(Debug, Clone, Deserialize, Serialize)]
struct InitialState {
    pub id: u64,
    pub term: u64,
}

impl From<&EntryMeta> for InitialState {
    fn from(meta: &EntryMeta) -> InitialState {
        InitialState {
            id: meta.id,
            term: meta.term,
        }
    }
}

fn to_io_error(e: rocksdb::Error) -> Error {
    Error::new(ErrorKind::Other, e)
}

fn write_u64(value: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(value).unwrap();
    w
}

fn serialize(id: u64, region: u8, name: &[u8]) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(region).unwrap();
    w.extend(name);
    w
}

fn make_log_key(id: u64, channel_id: u64, log_id: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(LOG_REGION).unwrap();
    w.write_u64::<BigEndian>(channel_id).unwrap();
    w.write_u8(CHANNEL_BEGIN_REGION).unwrap();
    w.write_u64::<BigEndian>(log_id).unwrap();
    w
}

fn make_channel_lower_bound(id: u64, channel_id: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(LOG_REGION).unwrap();
    w.write_u64::<BigEndian>(channel_id).unwrap();
    w.write_u8(CHANNEL_BEGIN_REGION).unwrap();
    w
}

fn make_channel_upper_bound(id: u64, channel_id: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(LOG_REGION).unwrap();
    w.write_u64::<BigEndian>(channel_id).unwrap();
    w.write_u8(CHANNEL_END_REGION).unwrap();
    w
}

fn make_log_prefix(id: u64, channel_id: u64) -> Vec<u8> {
    let mut w = vec![];

    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(LOG_REGION).unwrap();
    w.write_u64::<BigEndian>(channel_id).unwrap();
    w
}

fn make_meta_key(id: u64, name: &[u8]) -> Vec<u8> {
    serialize(id, META_REGION, name)
}

fn make_exists_key(id: u64) -> Vec<u8> {
    serialize(id, META_REGION, b"exists")
}

fn make_lower_bound(id: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(GLOBAL_REGION).unwrap();
    w
}

fn make_upper_bound(id: u64) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<BigEndian>(id).unwrap();
    w.write_u8(END_REGION).unwrap();
    w
}

fn touch_exists_mark(wb: &mut rocksdb::WriteBatch, id: u64) {
    let value: Vec<u8> = vec![];
    wb.put(make_exists_key(id), value.as_slice());
}

fn extract_log_index(key: &[u8]) -> Option<(u64, u64, u64)> {
    let key_size = 3 * std::mem::size_of::<u64>() + 2 * std::mem::size_of::<u8>();
    if key.len() != key_size {
        None
    } else {
        let mut c = Cursor::new(key);
        let id = c.read_u64::<BigEndian>().unwrap();
        if c.read_u8().unwrap() != LOG_REGION {
            return None;
        }
        let channel_id = c.read_u64::<BigEndian>().unwrap();
        if c.read_u8().unwrap() != CHANNEL_BEGIN_REGION {
            return None;
        }
        let index_id = c.read_u64::<BigEndian>().unwrap();
        Some((id, channel_id, index_id))
    }
}

struct MetaAdaptor {
    id: u64,

    hard_states_updated: bool,
    hard_states: HashMap<u64, HardState>,

    member_states_updated: bool,
    member_states: HashMap<u64, MemberState>,

    committed_stats_updated: bool,
    committed_stats: HashMap<u64, u64>,

    initial_states_updated: bool,
    initial_states: HashMap<u64, InitialState>,
}

impl MetaAdaptor {
    fn save_update(&mut self, task: &WriteTask) {
        if let Some(hard_states) = &task.hard_states {
            debug!(
                "node {} stable hard states from {:?} to {:?}",
                self.id, self.hard_states, hard_states
            );
            self.hard_states_updated = true;
            self.hard_states = hard_states.clone();
        }
        if let Some(member_states) = &task.member_states {
            debug!(
                "node {} stable member states from {:?} to {:?}",
                self.id, self.member_states, member_states
            );
            self.member_states_updated = true;
            self.member_states = member_states.clone();
        }

        self.committed_stats_updated = true;
        self.committed_stats = task.committed_stats.clone();
    }

    fn partial_serialize(&self, wb: &mut rocksdb::WriteBatch) {
        if self.hard_states_updated {
            wb.put(
                make_meta_key(self.id, META_HARD_STATES),
                serde_json::to_vec(&self.hard_states).unwrap().as_slice(),
            );
        }

        if self.committed_stats_updated {
            wb.put(
                make_meta_key(self.id, META_COMMITTED_STATS),
                serde_json::to_vec(&self.committed_stats)
                    .unwrap()
                    .as_slice(),
            );
        }
        if self.initial_states_updated {
            wb.put(
                make_meta_key(self.id, META_INITIAL_STATES),
                serde_json::to_vec(&self.initial_states).unwrap().as_slice(),
            );
        }

        if self.member_states_updated {
            wb.put(
                make_meta_key(self.id, META_MEMBER_DESC),
                serde_json::to_vec(&self.member_states).unwrap().as_slice(),
            );
        }
    }
}

macro_rules! parse_hash_meta {
    ( $db:expr, $id:expr, $meta_name:expr ) => {{
        match $db
            .get(make_meta_key($id, $meta_name))
            .map_err(to_io_error)?
        {
            Some(v) => serde_json::from_slice(&v).expect("serde"),
            None => HashMap::new(),
        }
    }};
}

#[derive(Debug)]
struct MetaInfos {
    id: u64,
    hard_states: HashMap<u64, HardState>,
    member_states: HashMap<u64, MemberState>,
    committed_stats: HashMap<u64, u64>,
    initial_states: HashMap<u64, InitialState>,
}

impl MetaInfos {
    fn new(id: u64) -> MetaInfos {
        MetaInfos {
            id,
            hard_states: HashMap::new(),
            member_states: HashMap::new(),
            committed_stats: HashMap::new(),
            initial_states: HashMap::new(),
        }
    }

    fn deserialize(db: &rocksdb::DB, id: u64) -> Result<MetaInfos, Error> {
        let hard_states = parse_hash_meta!(db, id, META_HARD_STATES);
        let member_states = parse_hash_meta!(db, id, META_MEMBER_DESC);
        let committed_stats = parse_hash_meta!(db, id, META_COMMITTED_STATS);
        let initial_states = parse_hash_meta!(db, id, META_INITIAL_STATES);

        Ok(MetaInfos {
            id,
            hard_states,
            member_states,
            committed_stats,
            initial_states,
        })
    }

    fn new_adaptor(&self) -> MetaAdaptor {
        MetaAdaptor {
            id: self.id,
            hard_states_updated: false,
            hard_states: self.hard_states.clone(),
            committed_stats_updated: false,
            committed_stats: self.committed_stats.clone(),
            initial_states_updated: false,
            initial_states: self.initial_states.clone(),
            member_states_updated: false,
            member_states: self.member_states.clone(),
        }
    }

    fn apply(&mut self, adaptor: &mut MetaAdaptor) {
        if adaptor.hard_states_updated {
            std::mem::swap(&mut self.hard_states, &mut adaptor.hard_states);
        }

        if adaptor.committed_stats_updated {
            std::mem::swap(&mut self.committed_stats, &mut adaptor.committed_stats);
        }

        if adaptor.initial_states_updated {
            std::mem::swap(&mut self.initial_states, &mut adaptor.initial_states);
        }

        if adaptor.member_states_updated {
            std::mem::swap(&mut self.member_states, &mut adaptor.member_states);
        }
    }

    fn serialize(&self, wb: &mut rocksdb::WriteBatch) {
        wb.put(
            make_meta_key(self.id, META_COMMITTED_STATS),
            serde_json::to_vec(&self.committed_stats)
                .unwrap()
                .as_slice(),
        );
        wb.put(
            make_meta_key(self.id, META_INITIAL_STATES),
            serde_json::to_vec(&self.initial_states).unwrap().as_slice(),
        );
        wb.put(
            make_meta_key(self.id, META_MEMBER_DESC),
            serde_json::to_vec(&self.member_states).unwrap().as_slice(),
        );
        wb.put(
            make_meta_key(self.id, META_HARD_STATES),
            serde_json::to_vec(&self.hard_states).unwrap().as_slice(),
        );
    }
}

#[derive(Debug)]
pub struct LogStorage {
    id: u64,
    cached_ranges: HashMap<u64, (u64, u64)>,
    meta_infos: MetaInfos,
    db: Arc<rocksdb::DB>,
}

impl LogStorage {
    fn maybe_delete_range(&self, wb: &mut rocksdb::WriteBatch, channel_id: u64, first_id: u64) {
        let last_stabled_id = self.channel_flushed_entry_id(channel_id);
        if last_stabled_id + 1u64 != first_id {
            assert_eq!(first_id <= last_stabled_id, true);

            wb.delete_range(
                make_log_key(self.id, channel_id, first_id),
                make_log_key(self.id, channel_id, last_stabled_id + 1u64),
            );
        }
    }

    fn stable_indexes(&mut self, wb: &mut rocksdb::WriteBatch, task: &WriteTask) -> (u64, u64) {
        assert_eq!(task.unstable_indexes.is_empty(), false);
        let first_id = task.unstable_indexes.first().unwrap().index_id;
        self.maybe_delete_range(wb, constant::INDEX_CHANNEL_ID, first_id);

        task.unstable_indexes.iter().for_each(|index| {
            let key = make_log_key(self.id, constant::INDEX_CHANNEL_ID, index.index_id);
            wb.put(key, serde_json::to_vec(&index).expect("serde"));
        });

        let last_id = task.unstable_indexes.last().unwrap().index_id;
        self.save_channel_next_id(constant::INDEX_CHANNEL_ID, last_id);
        (first_id, last_id)
    }

    fn save_channel_next_id(&mut self, channel_id: u64, last_id: u64) {
        let next_id = last_id + 1u64;
        (*self
            .cached_ranges
            .entry(channel_id)
            .or_insert((last_id, next_id)))
        .1 = next_id;
    }

    fn stable_entries(
        &mut self,
        wb: &mut rocksdb::WriteBatch,
        task: &WriteTask,
    ) -> HashMap<u64, (u64, u64)> {
        assert_eq!(task.unstable_entries.is_empty(), false);

        let mut save_range = HashMap::new();
        task.unstable_entries.iter().for_each(|entry| {
            (*save_range
                .entry(entry.channel_id)
                .or_insert((entry.entry_id, entry.entry_id)))
            .1 = entry.entry_id;
        });

        save_range.iter().for_each(|(channel_id, v)| {
            self.save_channel_next_id(*channel_id, v.1);
            self.maybe_delete_range(wb, *channel_id, v.0)
        });

        task.unstable_entries.iter().for_each(|entry| {
            let key = make_log_key(self.id, entry.channel_id, entry.entry_id);
            wb.put(key, serde_json::to_string(&entry).expect("serde"));
        });

        save_range
    }

    fn stable_metas(&self, wb: &mut rocksdb::WriteBatch, task: &WriteTask) -> MetaAdaptor {
        let mut adaptor = self.meta_infos.new_adaptor();
        adaptor.save_update(task);
        adaptor.partial_serialize(wb);
        adaptor
    }

    fn read_first_id(
        db: &rocksdb::DB,
        id: u64,
        channel_id: u64,
        mode: rocksdb::IteratorMode,
    ) -> u64 {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_lower_bound(make_channel_lower_bound(id, channel_id));
        opts.set_iterate_upper_bound(make_channel_upper_bound(id, channel_id));
        db.iterator_opt(mode, opts)
            .next()
            .map(|(k, _)| extract_log_index(&k))
            .flatten()
            .map(|(_, _, id)| id)
            .unwrap_or(constant::INVALID_ID)
    }

    fn recover_ranges(id: u64, db: &rocksdb::DB, channels: Vec<u64>) -> HashMap<u64, (u64, u64)> {
        let mut result = HashMap::new();
        for channel_id in &channels {
            let first_id = Self::read_first_id(db, id, *channel_id, rocksdb::IteratorMode::Start);
            let next_id = if first_id == constant::INVALID_ID {
                // No such entries of indexes which with the same prefix exists.
                constant::INVALID_ID
            } else {
                Self::read_first_id(db, id, *channel_id, rocksdb::IteratorMode::End) + 1u64
            };
            result.insert(*channel_id, (first_id, next_id));
        }
        result
    }
}

impl LogStorage {
    fn build_empty_storage(id: u64, wb: &mut rocksdb::WriteBatch) {
        let mut metas = MetaInfos::new(id);
        metas
            .committed_stats
            .insert(constant::INDEX_CHANNEL_ID, 0);
        let init_state = InitialState {
            id: constant::INVALID_ID,
            term: constant::INITIAL_TERM,
        };
        metas
            .initial_states
            .insert(constant::INDEX_CHANNEL_ID, init_state);
        metas.hard_states.insert(
            constant::INDEX_CHANNEL_ID,
            HardState {
                voted_for: constant::INVALID_NODE_ID,
                current_term: constant::INITIAL_TERM,
            },
        );
        metas.serialize(wb);
    }

    fn build_init_storage(id: u64, members: &mut Vec<u64>, wb: &mut rocksdb::WriteBatch) {
        assert_eq!(members.is_empty(), false);
        members.sort();

        let mut metas = MetaInfos::new(id);
        metas.committed_stats = members
            .iter()
            .map(|id| (*id, constant::INVALID_ID))
            .collect();
        metas
            .committed_stats
            .insert(constant::INDEX_CHANNEL_ID, constant::INVALID_ID);

        let init_state = InitialState {
            id: constant::INVALID_ID,
            term: constant::INITIAL_TERM,
        };
        metas.initial_states = members.iter().map(|id| (*id, init_state.clone())).collect();
        metas
            .initial_states
            .insert(constant::INDEX_CHANNEL_ID, init_state.clone());

        let hard_state = HardState {
            voted_for: constant::INVALID_NODE_ID,
            current_term: constant::INITIAL_TERM,
        };
        metas.hard_states = members.iter().map(|id| (*id, hard_state.clone())).collect();
        metas
            .hard_states
            .insert(constant::INDEX_CHANNEL_ID, hard_state);

        let member_state = MemberState {
            applied: false,
            stage: ConfigStage::New,
        };
        metas.member_states = members
            .iter()
            .map(|id| (*id, member_state.clone()))
            .collect();

        // build config change entry
        let first_channel_id = members[0];
        let first_id = constant::INVALID_ID + 1u64;
        let first_term = constant::INITIAL_TERM + 1u64;

        let entry = Entry {
            request_id: constant::CONFIG_CHANGE_ID,
            channel_id: first_channel_id,
            entry_id: first_id,
            index_id: constant::INVALID_ID,
            channel_term: first_term,
            message: vec![],
            context: None,
            configs: Some(ChangeConfig {
                index_id: first_id,
                entry_id: first_id,
                term: first_term,
                stage: ConfigStage::Both,
                members: members.iter().cloned().collect(),
            }),
        };
        let index = LogIndex {
            channel_id: first_channel_id,
            entry_id: first_id,
            index_id: first_id,
            term: first_term,
            context: None,
        };

        info!(
            "node {} use first channel {} as the default config change entry channel",
            id, first_channel_id
        );

        let key = make_log_key(id, entry.channel_id, entry.entry_id);
        wb.put(key, serde_json::to_string(&entry).expect("serde"));
        let key = make_log_key(id, constant::INDEX_CHANNEL_ID, index.index_id);
        wb.put(key, serde_json::to_string(&index).expect("serde"));

        // update states
        metas
            .committed_stats
            .entry(constant::INDEX_CHANNEL_ID)
            .and_modify(|v| *v = first_id);
        metas
            .committed_stats
            .entry(first_channel_id)
            .and_modify(|v| *v = first_id);

        metas
            .hard_states
            .entry(constant::INDEX_CHANNEL_ID)
            .and_modify(|v| v.current_term = first_term);
        metas
            .hard_states
            .entry(first_channel_id)
            .and_modify(|v| v.current_term = first_term);

        metas.serialize(wb);
    }

    pub fn build_if_not_exists(id: u64, path: &str, mut members: Vec<u64>) -> Result<(), Error> {
        info!("try build new node {} at {}", id, path);

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path).map_err(to_io_error)?;
        if let Some(_) = db.get(make_exists_key(id)).map_err(to_io_error)? {
            info!("node {} already exists, skip build stage", id);
            return Ok(());
        }

        let mut wb = rocksdb::WriteBatch::default();
        if members.is_empty() {
            info!("node {} build members is empty", id);
            Self::build_empty_storage(id, &mut wb);
        } else {
            info!("node {} build members is {:?}", id, members);
            Self::build_init_storage(id, &mut members, &mut wb);
        }
        touch_exists_mark(&mut wb, id);

        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        db.write_opt(wb, &opts).map_err(to_io_error)?;

        info!("node {} build success", id);

        Ok(())
    }

    pub fn new(id: u64, path: &str) -> Result<LogStorage, Error> {
        info!("try open cons node {} at {}", id, path);
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path).map_err(to_io_error)?;
        let meta_infos = MetaInfos::deserialize(&db, id)?;

        // The channel should be setted up even if the member isn't applied.
        let mut channels = meta_infos
            .member_states
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        debug!("node {} storage cached channels: {:?}", id, channels);

        channels.push(constant::INDEX_CHANNEL_ID);
        let cached_ranges = Self::recover_ranges(id, &db, channels);
        debug!("node {} storage cached ranges: {:?}", id, cached_ranges);
        Ok(LogStorage {
            id,
            cached_ranges,
            db: Arc::new(db),
            meta_infos,
        })
    }

    pub fn write(&mut self, task: &WriteTask) -> Result<HashMap<u64, (u64, u64)>, Error> {
        let opts = rocksdb::WriteOptions::default();
        let mut wb = rocksdb::WriteBatch::default();

        let mut meta_adaptor: Option<MetaAdaptor> = None;
        if task.should_stable_metas || task.hard_states.is_some() {
            meta_adaptor = Some(self.stable_metas(&mut wb, task));
        }

        // [start, until]
        let mut write_range = if task.unstable_entries.is_empty() {
            HashMap::new()
        } else {
            self.stable_entries(&mut wb, task)
        };
        if !task.unstable_indexes.is_empty() {
            write_range.insert(constant::INDEX_CHANNEL_ID, self.stable_indexes(&mut wb, task));
        }

        self.db.write_opt(wb, &opts).map_err(to_io_error)?;
        match meta_adaptor {
            Some(mut a) => self.meta_infos.apply(&mut a),
            None => {}
        };
        Ok(write_range)
    }

    pub fn read_entries(
        &self,
        channel_id: u64,
        start: u64,
        end: u64,
    ) -> Result<(u64, Vec<Entry>), Error> {
        debug!("read channel {} entries [{}, {})", channel_id, start, end);
        assert_eq!(start >= 1, true);

        let prev_term = self.read_term(channel_id, start - 1u64)?;

        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_lower_bound(make_log_key(self.id, channel_id, start));
        read_opt.set_iterate_upper_bound(make_log_key(self.id, channel_id, end));
        let entries = self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opt)
            .map(|(_k, v)| serde_json::from_slice(&v).expect("serde"))
            .collect::<Vec<_>>();

        Ok((prev_term, entries))
    }

    pub fn read_term(&self, channel_id: u64, id: u64) -> Result<u64, Error> {
        let initial_state_opt = self.meta_infos.initial_states.get(&channel_id);
        if initial_state_opt.is_some() && initial_state_opt.unwrap().id == id {
            return Ok(initial_state_opt.unwrap().term);
        }
        let key = make_log_key(self.id, channel_id, id);
        self.db
            .get(&key)
            .map_err(to_io_error)?
            .ok_or(Error::new(
                ErrorKind::NotFound,
                format!("node {} channel {} no such key {} exists", self.id, channel_id, id),
            ))
            .map(|v| {
                if channel_id == constant::INDEX_CHANNEL_ID {
                    serde_json::from_slice::<LogIndex>(&v).expect("serde").term
                } else {
                    serde_json::from_slice::<Entry>(&v)
                        .expect("serde")
                        .channel_term
                }
            })
    }

    pub fn read_indexes(&self, start: u64, end: u64) -> Result<(u64, Vec<LogIndex>), Error> {
        debug!("read indexes [{}, {})", start, end);
        assert_eq!(start >= 1, true);

        let channel_id = constant::INDEX_CHANNEL_ID;
        let prev_term = self.read_term(channel_id, start - 1u64)?;

        let mut read_opt = rocksdb::ReadOptions::default();
        read_opt.set_iterate_lower_bound(make_log_key(self.id, channel_id, start));
        read_opt.set_iterate_upper_bound(make_log_key(self.id, channel_id, end));
        let indexes = self
            .db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opt)
            .map(|(_k, v)| serde_json::from_slice(&v).expect("serde"))
            .collect::<Vec<_>>();

        Ok((prev_term, indexes))
    }

    pub fn flushed_id(&self) -> u64 {
        self.channel_flushed_entry_id(constant::INDEX_CHANNEL_ID)
    }

    pub fn channel_flushed_entry_id(&self, channel_id: u64) -> u64 {
        self.cached_ranges
            .get(&channel_id)
            .map(|(s, e)| {
                if *s == *e {
                    // empty range
                    *e
                } else {
                    *e - 1u64
                }
            })
            .unwrap_or(constant::INVALID_ID)
    }

    pub fn range(&self, channel_id: u64) -> (u64, u64) {
        self.cached_ranges
            .get(&channel_id)
            .cloned()
            .unwrap_or((constant::INVALID_ID, constant::INVALID_ID))
    }

    pub fn membership(&self) -> HashMap<u64, MemberState> {
        self.meta_infos.member_states.clone()
    }

    pub fn hard_states(&self) -> HashMap<u64, HardState> {
        self.meta_infos.hard_states.clone()
    }

    pub fn reset(&mut self, desc: &SnapshotDesc) -> Result<(), Error> {
        use rocksdb::WriteBatch;

        self.meta_infos.member_states = desc.members.clone();
        self.meta_infos.committed_stats = desc
            .channel_metas
            .iter()
            .map(|(k, v)| (*k, v.id))
            .collect::<HashMap<_, _>>();

        self.meta_infos.initial_states = desc
            .channel_metas
            .iter()
            .map(|(k, v)| (*k, InitialState::from(v)))
            .collect();

        let lower = make_lower_bound(self.id);
        let upper = make_upper_bound(self.id);
        self.db
            .delete_file_in_range(lower, upper)
            .map_err(to_io_error)?;

        let mut wb = WriteBatch::default();
        wb.delete_range(make_lower_bound(self.id), make_upper_bound(self.id));
        self.meta_infos.serialize(&mut wb);
        self.db.write(wb).map_err(to_io_error)
    }

    pub fn approximate_size(&self) -> usize {
        let files = match self.db.live_files() {
            Err(e) => return 0,
            Ok(files) => files,
        };

        // FIXME(patrick) only one node in current rocksdb, so we can compute size by
        // accumulating all files size.
        files.iter().map(|file| file.size).sum()
    }

    pub fn release_log_entries(&mut self, ranges: HashMap<u64, u64>) -> Result<(), Error> {
        use rocksdb::WriteBatch;
        let mut wb = WriteBatch::default();
        for (channel_id, applied_id) in ranges {
            wb.delete_range(
                make_log_key(self.id, channel_id, 0),
                make_log_key(self.id, channel_id, applied_id),
            );
        }
        self.db.write(wb).map_err(to_io_error)
    }
}
