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

use std::collections::HashMap;
use std::io::{BufRead, Cursor, Read};
use std::sync::Arc;

use anyhow::Error;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use sdcons::constant;
use sdcons::types::{Entry, SnapshotDesc};

use base::proto::kv::*;

use crate::node::applier::{ApplyError, FiniteStateMachine};

const GLOBAL_REGION: u8 = 0x0;
const META_REGION: u8 = 0x1;
pub const DATA_REGION: u8 = 0x2;
const DATA_END_REGION: u8 = 0x3;
const END_REGION: u8 = DATA_END_REGION + 0x1;

#[derive(Debug, Clone)]
pub struct FSM {
    id: u64,
    db: Arc<rocksdb::DB>,
}

assert_impl_all!(FSM: Send, Sync);

impl FSM {
    fn make_prefix(&self, region: u8) -> Vec<u8> {
        let mut w = vec![];
        w.write_u64::<LittleEndian>(self.id).unwrap();
        w.write_u8(region).unwrap();
        w
    }

    fn make_user_key(&self, user_key: &[u8]) -> Vec<u8> {
        let mut w = vec![];
        w.write_u64::<LittleEndian>(self.id).unwrap();
        w.write_u8(DATA_REGION).unwrap();
        w.extend(user_key);
        w
    }

    fn make_meta_key(&self, channel_id: u64, desc: &[u8]) -> Vec<u8> {
        let mut w = vec![];
        w.write_u64::<LittleEndian>(self.id).unwrap();
        w.write_u8(META_REGION).unwrap();
        w.write_u64::<LittleEndian>(channel_id).unwrap();
        w.extend(desc);
        w
    }

    fn parse_meta_key(&self, key: &[u8]) -> Option<(u64, u64, Vec<u8>)> {
        let mut c = Cursor::new(key);
        let id = c.read_u64::<LittleEndian>().unwrap_or(0);
        c.consume(1);
        let channel_id = c.read_u64::<LittleEndian>().unwrap_or(0);
        let mut vec = Vec::new();
        c.read_to_end(&mut vec).unwrap();
        Some((id, channel_id, vec))
    }

    fn write_u64(&self, value: u64) -> Vec<u8> {
        let mut w = vec![];
        w.write_u64::<LittleEndian>(value).unwrap();
        w
    }

    fn parse_u64(&self, value: &[u8]) -> u64 {
        let mut c = Cursor::new(value);
        c.read_u64::<LittleEndian>().unwrap_or(0)
    }
}

impl FiniteStateMachine for FSM {
    type ApplyResult = PutResponse;

    fn applied_index_id(&self) -> u64 {
        self.channel_applied_entry_id(constant::INDEX_CHANNEL_ID)
    }

    fn channel_applied_entry_id(&self, channel_id: u64) -> u64 {
        match self
            .db
            .get(self.make_meta_key(channel_id, b"_applied_id"))
            .unwrap()
        {
            Some(v) => self.parse_u64(&v),
            None => 0,
        }
    }

    fn apply(&mut self, entry: &Entry) -> Result<Self::ApplyResult, ApplyError> {
        let mut wb = rocksdb::WriteBatch::default();
        wb.put(
            self.make_meta_key(entry.channel_id, b"_applied_id"),
            self.write_u64(entry.entry_id),
        );
        wb.put(
            self.make_meta_key(constant::INDEX_CHANNEL_ID, b"_applied_id"),
            self.write_u64(entry.index_id),
        );
        if constant::is_internal_request(entry.request_id) {
            self.db.write(wb).expect("write batch");
            return Err(ApplyError::InternalRequest);
        }

        let req: PutRequest = serde_json::from_slice(&entry.message).expect("deserialize");
        let user_key = self.make_user_key(&req.key);
        let value_or = self.db.get(&user_key).expect("get key");
        let header = match &req.cond {
            PutCond::Except(expect_value)
                if value_or
                    .as_ref()
                    .map(|v| *v != *expect_value)
                    .unwrap_or(true) =>
            {
                ResponseHeader {
                    code: ErrorCode::CasFailed,
                    msg: "cas failed".to_string(),
                }
            }
            PutCond::NotExists if value_or.is_some() => ResponseHeader {
                code: ErrorCode::CasFailed,
                msg: "cas failed".to_string(),
            },
            _ => ResponseHeader {
                code: ErrorCode::OK,
                msg: "ok".to_string(),
            },
        };

        if header.code == ErrorCode::OK {
            match &req.value {
                Some(v) => {
                    debug!(
                        "node {} put key {} with value {}, cond {:?}, value or is {:?}",
                        self.id,
                        String::from_utf8_lossy(&req.key),
                        String::from_utf8_lossy(&v),
                        req.cond,
                        value_or
                    );
                    wb.put(&user_key, v)
                }
                None => {
                    debug!("node {} delete key {}", self.id, String::from_utf8_lossy(&req.key));
                    wb.delete(&user_key)
                }
            }
        }
        self.db.write(wb).expect("write batch");

        Ok(PutResponse {
            header: header,
            previous_value: value_or,
        })
    }

    fn apply_snapshot(
        &mut self,
        url: String,
        indexes: HashMap<u64, u64>,
    ) -> Result<(), ApplyError> {
        let files = std::fs::read_dir(url)
            .expect("read_dir")
            .filter_map(|e| e.map(|e| e.path().clone()).ok())
            .collect();
        self.db
            .delete_file_in_range(self.make_prefix(DATA_REGION), self.make_prefix(DATA_END_REGION))
            .expect("delete file in range");
        self.db
            .delete_range_cf(
                self.db
                    .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
                    .expect("cf handle"),
                self.make_prefix(DATA_REGION),
                self.make_prefix(DATA_END_REGION),
            )
            .expect("delete file in range");
        self.db.ingest_external_file(files).expect("ingest");
        let mut wb = rocksdb::WriteBatch::default();
        for (channel_id, applied_id) in indexes {
            wb.put(self.make_meta_key(channel_id, b"_applied_id"), self.write_u64(applied_id));
        }
        self.db.write(wb).expect("write batch");
        Ok(())
    }

    fn checkpoint(&self, url: String) -> Result<HashMap<u64, u64>, ApplyError> {
        use rocksdb::{Direction, IteratorMode, Options, SstFileWriter};

        std::fs::create_dir_all(&url).expect("create dir all");

        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);

        let mut it = self.db.iterator(IteratorMode::Start);
        let prefix = self.make_prefix(META_REGION);
        let mut metas = HashMap::new();
        while let Some((k, v)) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }
            // Notice: only applied index
            if let Some((_id, channel_id, _desc)) = self.parse_meta_key(&k) {
                let value = self.parse_u64(&v);
                metas.insert(channel_id, value);
            }
        }

        let file_base_size = 1024 * 1024 * 64;
        let mut open = false;
        let mut index = 0;
        let prefix = self.make_prefix(DATA_REGION);
        it.set_mode(IteratorMode::From(&prefix, Direction::Forward));
        while let Some((k, v)) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }
            if writer.file_size() >= file_base_size {
                writer.finish().expect("finish sst writer");
                open = false;
            }
            if !open {
                index += 1;
                writer
                    .open(format!("{}/{}.sst", url, index))
                    .expect("open sst writer");
            }
            writer.put(k, v).expect("put");
        }
        if index > 0 {
            writer.finish().expect("finish sst writer");
        }
        Ok(metas)
    }
}

impl FSM {
    pub fn new(id: u64, db: Arc<rocksdb::DB>) -> Result<FSM, Error> {
        Ok(FSM { id, db })
    }
}
