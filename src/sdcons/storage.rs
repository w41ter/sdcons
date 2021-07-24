//! The crate `storage` defines a set of structs and traits which are used to
//! store/access log sequences.

// Copyright 2021 The sdcons Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
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

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Index;

use log::{debug, error, info, trace, warn};

use crate::constant::*;
use crate::types::*;

#[derive(Debug)]
pub(super) struct EntryQueue {
    pub member_id: u64,
    pub dummy_entry_id: u64,
    pub dummy_entry_term: u64,
    pub unstable_entry_id: u64,
    pub entries: VecDeque<Entry>,
}

impl EntryQueue {
    pub fn new(id: u64) -> EntryQueue {
        EntryQueue {
            member_id: id,
            dummy_entry_id: INVALID_ID,
            dummy_entry_term: INITIAL_TERM,
            unstable_entry_id: INVALID_ID + 1,
            entries: VecDeque::new(),
        }
    }

    pub fn recovery(id: u64, entry_id: u64, entry_term: u64) -> EntryQueue {
        EntryQueue {
            member_id: id,
            dummy_entry_id: entry_id,
            dummy_entry_term: entry_term,
            unstable_entry_id: entry_id + 1,
            entries: VecDeque::new(),
        }
    }

    fn mark_all_stabled(&mut self) {
        self.unstable_entry_id = self.last_entry_id() + 1;
    }

    fn first_entry_id(&self) -> u64 {
        self.entries
            .front()
            .map(|e| e.entry_id)
            .unwrap_or(self.dummy_entry_id + 1)
    }

    fn last_entry_id(&self) -> u64 {
        self.entries
            .back()
            .map(|e| e.entry_id)
            .unwrap_or(self.dummy_entry_id)
    }

    fn last_entry_meta(&self) -> (u64, u64) {
        self.entries
            .back()
            .map(|e| (e.entry_id, e.channel_term))
            .unwrap_or((self.dummy_entry_id, self.dummy_entry_term))
    }

    fn entry_term(&self, entry_id: u64) -> Option<u64> {
        debug_assert!(
            self.dummy_entry_id <= entry_id,
            "dummy_entry_id {}, entry id {}",
            self.dummy_entry_id,
            entry_id
        );
        if entry_id == self.dummy_entry_id {
            return Some(self.dummy_entry_term);
        }
        let offset = self.entry_offset(entry_id);
        if offset >= self.entries.len() {
            None
        } else {
            Some(self.entries.index(offset).channel_term)
        }
    }

    fn first_index_of_term(&self, channel_term: u64) -> u64 {
        let mut it = self.entries.iter().rev();
        while let Some(e) = it.next() {
            if e.channel_term < channel_term {
                return e.entry_id;
            }
        }
        self.dummy_entry_id
    }

    fn last_stabled_entry_id(&self) -> u64 {
        self.unstable_entry_id - 1
    }

    fn entry_offset(&self, entry_id: u64) -> usize {
        debug_assert!(
            entry_id > self.dummy_entry_id,
            "entry id {}, dummy entry id {}",
            entry_id,
            self.dummy_entry_id
        );
        (entry_id - self.dummy_entry_id) as usize - 1
    }

    fn clone_entry(&self, entry_id: u64) -> Option<Entry> {
        let offset = self.entry_offset(entry_id);
        self.entries.get(offset).cloned()
    }

    fn release_entries_until(&mut self, entry_id: u64) -> bool {
        if entry_id <= self.dummy_entry_id {
            return true;
        } else if self.unstable_entry_id <= entry_id {
            return false;
        } else {
            let offset = self.entry_offset(entry_id);
            self.entries.drain(0..offset);
            self.dummy_entry_id = entry_id - 1;
            true
        }
    }

    fn truncate_entries(&mut self, entry_id: u64) {
        debug_assert!(
            self.dummy_entry_id < entry_id,
            "dummy_entry_id {}, entry id {}",
            self.dummy_entry_id,
            entry_id
        );
        self.entries.truncate(self.entry_offset(entry_id));
    }

    fn last_config_change_entry(&self) -> Option<Entry> {
        self.entries
            .iter()
            .filter(|e| e.request_id == CONFIG_CHANGE_ID)
            .cloned()
            .last()
    }
}

#[derive(Debug)]
pub(super) struct MemStorage {
    pub id: u64,

    pub dummy_index_id: u64,
    pub dummy_index_term: u64,
    pub chosen_index_id: u64,
    pub applied_index_id: u64,
    pub unstable_index_id: u64,

    // receive from leader
    pub indexes: VecDeque<LogIndex>,
    pub entries_map: HashMap<u64, EntryQueue>,
}

impl MemStorage {
    pub fn new(id: u64) -> MemStorage {
        MemStorage {
            id,
            dummy_index_id: INVALID_ID,
            dummy_index_term: INITIAL_TERM,
            chosen_index_id: INVALID_ID,
            applied_index_id: INVALID_ID,
            unstable_index_id: INVALID_ID + 1,
            indexes: VecDeque::new(),
            entries_map: HashMap::new(),
        }
    }

    pub fn recovery(id: u64, channel_metas: &HashMap<u64, EntryMeta>) -> MemStorage {
        info!("node {} recovery mem storage with metas: {:?}", id, channel_metas);

        let indexes = VecDeque::new();
        let index_meta = channel_metas
            .get(&INDEX_CHANNEL_ID)
            .expect("INDEX CHANNEL ID must exists");
        let entries_map = channel_metas
            .iter()
            .filter(|(k, _)| **k != INDEX_CHANNEL_ID)
            .map(|(k, v)| (*k, EntryQueue::recovery(*k, v.id, v.term)))
            .collect::<HashMap<_, _>>();
        MemStorage {
            id,
            dummy_index_id: index_meta.id,
            dummy_index_term: index_meta.term,
            chosen_index_id: index_meta.id,
            applied_index_id: index_meta.id,
            unstable_index_id: index_meta.id + 1,
            indexes,
            entries_map,
        }
    }

    /// Only used in `RawNode::build()`, to mark all logs loaded from persistent
    /// device as stabled.
    pub fn mark_all_stabled(&mut self) {
        self.unstable_index_id = self.last_index_id() + 1;
        self.entries_map
            .iter_mut()
            .for_each(|(_, e)| e.mark_all_stabled());
    }

    /// Allocate a new index of a entry, it equals the last index id(or
    /// INVALID_INDEX_ID) + 1.
    pub fn allocate(&mut self, mut index: LogIndex) -> u64 {
        let next_index_id = self.last_index_id() + 1;
        index.index_id = next_index_id;
        self.indexes.push_back(index);
        next_index_id
    }

    pub fn append_entry(&mut self, id: u64, entry: Entry) {
        let buffer = self
            .entries_map
            .entry(id)
            .or_insert_with(|| EntryQueue::new(id));
        debug!("node {} channel {} append entry {:?}", self.id, id, entry);
        buffer.entries.push_back(entry);
    }

    pub fn is_term_miss_matched(
        &mut self,
        channel_id: u64,
        prev_entry_id: u64,
        prev_entry_term: u64,
    ) -> bool {
        if channel_id == INDEX_CHANNEL_ID {
            match self.index_term(prev_entry_id) {
                Some(local_term) if local_term == prev_entry_term => false,
                _ => true,
            }
        } else {
            let buffer = self
                .entries_map
                .entry(channel_id)
                .or_insert_with(|| EntryQueue::new(channel_id));
            match buffer.entry_term(prev_entry_id) {
                Some(local_term) if local_term == prev_entry_term => false,
                _ => true,
            }
        }
    }

    pub fn find_index_conflict(&mut self, prev_index_id: u64, prev_index_term: u64) -> u64 {
        let last_index_id = self.last_index_id();
        let mut id = if last_index_id < prev_index_id {
            last_index_id
        } else {
            prev_index_id
        };
        while let Some(index_term) = self.index_term(id) {
            if index_term < prev_index_term || id == self.dummy_index_id {
                break;
            }
            id -= 1;
        }
        id + 164
    }

    pub fn find_conflict(
        &mut self,
        channel_id: u64,
        prev_entry_id: u64,
        prev_entry_term: u64,
    ) -> u64 {
        let buffer = self
            .entries_map
            .entry(channel_id)
            .or_insert_with(|| EntryQueue::new(channel_id));
        let last_entry_id = buffer.last_entry_id();
        let mut id = if last_entry_id < prev_entry_id {
            last_entry_id
        } else {
            prev_entry_id
        };
        while let Some(entry_term) = buffer.entry_term(id) {
            if entry_term <= prev_entry_term || id == 0 {
                return id + 1u64;
            } else {
                id -= 1;
            }
        }
        debug_assert!(id <= last_entry_id, "id {}, last entry id {}", id, last_entry_id);
        id + 164
    }

    pub fn append_entries(&mut self, channel_id: u64, entries: Vec<Entry>) -> (u64, u64) {
        debug!("node {} channel {} append {:?}", self.id, channel_id, entries);
        let buffer = self
            .entries_map
            .entry(channel_id)
            .or_insert_with(|| EntryQueue::new(channel_id));

        if entries.is_empty() {
            let last_entry_id = buffer.last_entry_id();
            (last_entry_id + 1u64, last_entry_id)
        } else {
            let first_input_entry_id = entries.first().unwrap().entry_id;
            buffer.truncate_entries(first_input_entry_id);
            buffer.entries.extend(entries.into_iter());
            let last_entry_id = buffer.last_entry_id();
            (first_input_entry_id, last_entry_id)
        }
    }

    fn truncate_indexes(&mut self, index_id: u64) {
        self.indexes.truncate(self.index_offset(index_id));
    }

    pub fn extend_indexes(&mut self, indexes: Vec<LogIndex>) -> u64 {
        if !indexes.is_empty() {
            let first_index_id = indexes.first().unwrap().index_id;
            self.truncate_indexes(first_index_id);
            self.indexes.extend(indexes.into_iter());
        }

        if !self.indexes.is_empty() {
            self.indexes.back().unwrap().index_id
        } else {
            self.dummy_index_id
        }
    }

    pub fn extract_unapply_entries(&mut self, max_bytes: u64) -> (u64, u64, Vec<Entry>) {
        let mut entries = Vec::new();
        if self.indexes.is_empty() {
            trace!("there no unapply entries, because indexes is empty");
            return (0, 0, entries);
        }

        let mut next_index_id = self.applied_index_id + 1;
        let mut current_bytes = 0;
        while current_bytes < max_bytes && next_index_id <= self.chosen_index_id {
            let offset = self.index_offset(next_index_id);
            let index = &self.indexes[offset];
            if index.entry_id == INVALID_ID {
                // Skip no-op index
                next_index_id += 1;
                continue;
            }
            if let Some(buffer) = self.entries_map.get(&index.channel_id) {
                if let Some(mut entry) = buffer.clone_entry(index.entry_id) {
                    current_bytes += entry.message.len() as u64;
                    entry.index_id = next_index_id;
                    entries.push(entry);
                    next_index_id += 1;
                    continue;
                }
            }
            break;
        }
        (self.applied_index_id + 1u64, next_index_id - 1u64, entries)
    }

    pub fn channel_last_assigned_entry_id(&self, channel_id: u64) -> u64 {
        for offset in (0..self.indexes.len()).rev() {
            let index = &self.indexes[offset];
            if index.channel_id == channel_id && index.entry_id != INVALID_ID {
                return index.entry_id;
            }
        }
        self.entries_map
            .get(&channel_id)
            .map(|e| e.dummy_entry_id)
            .unwrap_or(INVALID_ID)
    }

    pub fn extract_unstable_entries(&mut self, _max_bytes: u64) -> Vec<Entry> {
        let mut entries = Vec::new();
        for (_proposer_id, remote_view) in self.entries_map.iter_mut() {
            let offset = remote_view.entry_offset(remote_view.unstable_entry_id);
            if offset >= remote_view.entries.len() {
                continue;
            }
            entries.extend(remote_view.entries.range(offset..).cloned());
        }
        entries
    }

    pub fn extract_unstable_indexes(&mut self) -> Vec<LogIndex> {
        let offset = self.index_offset(self.unstable_index_id);
        if offset >= self.indexes.len() {
            vec![]
        } else {
            self.indexes.range(offset..).cloned().collect()
        }
    }

    pub fn range(&self, from: u64, to: u64, max_bytes: u64) -> Vec<Entry> {
        let local_buffer = self.entries_map.index(&self.id);

        assert_ne!(local_buffer.entries.len(), 0);

        let mut entries = Vec::new();
        let mut current_bytes = 0;
        for idx in from..(to + 1) {
            let offset = local_buffer.entry_offset(idx);
            let entry = &local_buffer.entries[offset];
            entries.push(entry.clone());
            current_bytes += entry.message.len() as u64;
            if current_bytes >= max_bytes {
                break;
            }
        }
        entries
    }

    pub fn range_of(&self, channel_id: u64, from: u64, to: u64) -> Vec<Entry> {
        let remote_buffer = self.entries_map.index(&channel_id);

        let mut entries = Vec::new();
        for idx in from..(to + 1) {
            let offset = remote_buffer.entry_offset(idx);
            let entry = &remote_buffer.entries[offset];
            entries.push(entry.clone());
        }
        entries
    }

    pub fn range_of_metas(&self, channel_id: u64, from: u64, to: u64) -> Vec<EntryMeta> {
        let remote_buffer = self.entries_map.index(&channel_id);

        let mut entries = Vec::new();
        for idx in from..(to + 1) {
            let offset = remote_buffer.entry_offset(idx);
            let entry = &remote_buffer.entries[offset];
            entries.push(EntryMeta::from(entry));
        }
        entries
    }

    pub fn index_range(&self, from: u64, to: u64) -> Vec<LogIndex> {
        assert_ne!(self.indexes.len(), 0);
        let mut indexes = Vec::new();
        for idx in from..(to + 1) {
            let offset = self.index_offset(idx);
            indexes.push(self.indexes[offset].clone());
        }
        indexes
    }

    pub fn get_index(&self, index_id: u64) -> Option<LogIndex> {
        if index_id <= self.dummy_index_id {
            None
        } else {
            self.indexes.get(self.index_offset(index_id)).cloned()
        }
    }

    pub fn index_offset(&self, index_id: u64) -> usize {
        if index_id <= self.dummy_index_id {
            panic!("dummy index id {}, request index id {}", self.dummy_index_id, index_id);
        }
        (index_id - self.dummy_index_id) as usize - 1
    }

    pub fn channel_first_entry_id(&self, channel_id: u64) -> u64 {
        if channel_id == INDEX_CHANNEL_ID {
            self.dummy_index_id
        } else {
            self.entries_map
                .get(&channel_id)
                .map(|b| b.dummy_entry_id)
                .unwrap_or(INVALID_ID)
        }
    }

    pub fn last_index_id(&self) -> u64 {
        self.indexes
            .back()
            .map(|i| i.index_id)
            .unwrap_or(self.dummy_index_id)
    }

    pub fn next_index_id(&self) -> u64 {
        self.last_index_id() + 1u64
    }

    pub fn channel_next_entry_id(&self, channel_id: u64) -> u64 {
        self.channel_last_entry_id(channel_id) + 1u64
    }

    pub fn last_index_term(&self) -> u64 {
        let index_id = self.last_index_id();
        self.get_index(index_id)
            .map(|i| i.term)
            .unwrap_or(INITIAL_TERM)
    }

    pub fn channel_last_entry_id(&self, channel_id: u64) -> u64 {
        if channel_id == INDEX_CHANNEL_ID {
            self.last_index_id()
        } else {
            self.entries_map
                .get(&channel_id)
                .map(|b| b.last_entry_id())
                .unwrap_or(INVALID_ID)
        }
    }

    pub fn advance_chosen_index_id(&mut self, committed_map: HashMap<u64, u64>) {
        if !committed_map.contains_key(&INDEX_CHANNEL_ID) {
            return;
        }
        let committed_index_id = *committed_map.get(&INDEX_CHANNEL_ID).unwrap();
        let beg_index_id = self.chosen_index_id + 1u64;
        let end_index_id = committed_index_id + 1u64;
        for index_id in beg_index_id..end_index_id {
            let offset = self.index_offset(index_id);
            let index = &self.indexes[offset];
            if index.entry_id > *committed_map.get(&index.channel_id).unwrap_or(&INVALID_ID) {
                break;
            }
            self.chosen_index_id = index_id;
        }

        if self.chosen_index_id + 1 != beg_index_id {
            debug!(
                "node {} channel {} advance chosen index id from {} to {}",
                self.id,
                INDEX_CHANNEL_ID,
                beg_index_id - 1u64,
                self.chosen_index_id
            );
        }
    }

    pub fn submit_stable_result(&mut self, channel_id: u64, last_stabled_entry_id: u64) -> bool {
        if let Some(buffer) = self.entries_map.get_mut(&channel_id) {
            let previous_unstable_entry_id = buffer.unstable_entry_id;
            buffer.unstable_entry_id = last_stabled_entry_id + 1;
            if previous_unstable_entry_id != buffer.unstable_entry_id {
                debug!(
                    "node {} channel {} stable entries in [{}, {})",
                    self.id, channel_id, previous_unstable_entry_id, buffer.unstable_entry_id
                );
            }
            previous_unstable_entry_id < last_stabled_entry_id
        } else {
            debug!(
                "node {} channel {} stable entries to {}, but it not in current membership",
                self.id, channel_id, last_stabled_entry_id
            );
            false
        }
    }

    pub fn submit_applied_result(&mut self, applied_index_id: u64) -> bool {
        if applied_index_id == self.applied_index_id {
            return false;
        }

        debug!(
            "applied indexes in range [{}, {})",
            self.applied_index_id + 1,
            applied_index_id + 1
        );
        self.applied_index_id = applied_index_id;
        true
    }

    pub fn submit_stable_index_result(&mut self, index_id: u64) -> bool {
        if index_id + 1 == self.unstable_index_id {
            false
        } else {
            debug!("stabled indexes range [{}, {})", self.unstable_index_id, index_id + 1);
            self.unstable_index_id = index_id + 1;
            true
        }
    }

    /// Return the left-closed and right-open interval of applied index ids.
    pub fn applied_index_range(&self) -> (u64, u64) {
        (self.dummy_index_id + 1u64, self.applied_index_id + 1u64)
    }

    /// Release entries in memory until `last_index_id`
    pub fn release_entries_until(&mut self, mut last_index_id: u64) {
        if last_index_id <= self.dummy_index_id {
            return;
        }
        if self.applied_index_id + 1 < last_index_id {
            last_index_id = self.applied_index_id + 1;
        }
        let until_offset = self.index_offset(last_index_id);
        let mut proposer_map = HashMap::new();
        for offset in 0..until_offset {
            let index = &self.indexes[offset];
            *proposer_map
                .entry(index.channel_id)
                .or_insert(index.entry_id) = index.entry_id;
        }
        for (proposer_id, entry_id) in proposer_map {
            trace!("release remote {} entries until id {}", proposer_id, entry_id);
            self.entries_map
                .get_mut(&proposer_id)
                .unwrap()
                .release_entries_until(entry_id + 1u64);
        }

        trace!("release indexes until id {}", last_index_id);
        self.indexes.drain(0..until_offset);
        self.dummy_index_id = last_index_id - 1;
    }

    pub fn index_term(&self, index_id: u64) -> Option<u64> {
        debug_assert!(
            self.dummy_index_id <= index_id,
            "dummy index id {}, index id {}",
            self.dummy_index_id,
            index_id
        );
        if index_id == self.dummy_index_id {
            return Some(self.dummy_index_term);
        }
        let offset = self.index_offset(index_id);
        if offset >= self.indexes.len() {
            None
        } else {
            Some(self.indexes.index(offset).term)
        }
    }

    pub fn entry_term(&self, channel_id: u64, entry_id: u64) -> Option<u64> {
        self.entries_map
            .get(&channel_id)
            .map(|b| b.entry_term(entry_id))
            .flatten()
    }

    pub fn last_unchosen_config_change_entry(&self) -> Option<Entry> {
        let mut config_change_entry_map = HashMap::new();
        for (channel_id, entry_queue) in &self.entries_map {
            if let Some(entry) = entry_queue.last_config_change_entry() {
                config_change_entry_map.insert((*channel_id, entry.entry_id), entry);
            }
        }
        for index in self.indexes.iter().rev() {
            if index.index_id <= self.chosen_index_id {
                break;
            }
            if let Some(entry) = config_change_entry_map.get(&(index.channel_id, index.entry_id)) {
                return Some(entry.clone());
            }
        }
        None
    }

    pub fn channel_last_config_change_entry(&self, channel_id: u64) -> Option<Entry> {
        self.entries_map
            .get(&channel_id)
            .map(|e| e.last_config_change_entry())
            .flatten()
    }

    pub fn entry_index_id(&self, channel_id: u64, entry_id: u64) -> Option<u64> {
        self.indexes
            .iter()
            .filter(|i| i.entry_id == entry_id && i.channel_id == channel_id)
            .map(|i| i.index_id)
            .next()
    }
}

/// A trait is used by sdcons to fetch the persistent state.
pub trait LogMetaView {
    /// Read membership from the persistent device.
    fn membership(&self) -> HashMap<u64, MemberState>;
    /// Read hard states from the persistent device.
    fn hard_states(&self) -> HashMap<u64, HardState>;
    /// Read ranges for specified channel from the persistent device.
    fn range_of(&self, channel_id: u64) -> (u64, u64);
    /// Read the description of latest snapshot from the persistent device.
    fn latest_snapshot(&self) -> Option<SnapshotDesc>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Entry;
    use super::EntryQueue;
    use super::LogIndex;
    use super::MemStorage;
    use super::INITIAL_TERM;
    use super::INVALID_ID;

    fn new_index(member_id: u64, entry_id: u64, term: u64) -> LogIndex {
        LogIndex {
            channel_id: member_id,
            entry_id,
            term,
            index_id: 0,
            context: None,
        }
    }

    fn new_index_with_id(member_id: u64, index_id: u64, entry_id: u64, term: u64) -> LogIndex {
        LogIndex {
            channel_id: member_id,
            entry_id,
            term,
            index_id,
            context: None,
        }
    }

    fn new_entry(member_id: u64, entry_id: u64, term: u64) -> Entry {
        Entry {
            entry_id,
            index_id: INVALID_ID,
            request_id: 1,
            channel_id: member_id,
            channel_term: term,
            message: vec![0, 1, 2, 3],
            context: None,
            configs: None,
        }
    }

    #[test]
    fn entry_queue_last_id() {
        let member_id: u64 = 1;
        let queue = EntryQueue::new(member_id);

        assert_eq!(queue.last_entry_id(), INVALID_ID);
        assert_eq!(queue.last_stabled_entry_id(), INVALID_ID);

        assert_eq!(queue.entry_offset(INVALID_ID + 1), 0);
        assert_eq!(queue.entry_offset(INVALID_ID + 2), 1);
    }

    #[test]
    fn entry_queue_release() {
        let member_id: u64 = 1;
        let mut queue = EntryQueue::new(member_id);

        assert!(queue.release_entries_until(INVALID_ID));
        assert!(!queue.release_entries_until(INVALID_ID + 1));

        let entries = vec![
            new_entry(member_id, INVALID_ID + 1, 0),
            new_entry(member_id, INVALID_ID + 2, 0),
            new_entry(member_id, INVALID_ID + 3, 0),
            new_entry(member_id, INVALID_ID + 4, 0),
            new_entry(member_id, INVALID_ID + 5, 0),
        ];
        queue.entries.extend(entries.into_iter());
        assert_eq!(queue.last_entry_id(), 5);
        assert!(!queue.release_entries_until(INVALID_ID + 1));

        // stabled: [1, 3), unstabled: [3, 6)
        queue.unstable_entry_id = INVALID_ID + 3;

        // [2, 6)
        assert!(queue.release_entries_until(INVALID_ID + 2));
        assert_eq!(queue.last_entry_id(), 5);
        assert_eq!(queue.last_stabled_entry_id(), 2);
        assert_eq!(queue.entry_offset(INVALID_ID + 2), 0);

        assert!(!queue.release_entries_until(INVALID_ID + 3));
    }

    #[test]
    fn mem_storage_basic() {
        let local_id: u64 = 1;
        let remote_id: u64 = 2;
        let mut s = MemStorage::new(local_id);

        assert_eq!(s.last_index_id(), INVALID_ID);
        assert_eq!(s.last_index_term(), INITIAL_TERM);
        assert_eq!(s.channel_last_entry_id(local_id), INVALID_ID);
        assert_eq!(s.channel_last_entry_id(remote_id), INVALID_ID);

        // append
        let term = INITIAL_TERM + 1;
        let entry_id = INVALID_ID + 1;
        s.append_entry(local_id, new_entry(local_id, entry_id, term));
        s.append_entry(local_id, new_entry(local_id, entry_id + 1, term));
        s.append_entry(local_id, new_entry(local_id, entry_id + 2, term));
        s.append_entry(remote_id, new_entry(remote_id, entry_id, term + 1));
        s.append_entry(remote_id, new_entry(remote_id, entry_id + 1, term + 1));
        s.append_entry(remote_id, new_entry(remote_id, entry_id + 2, term + 1));

        assert_eq!(s.channel_last_entry_id(local_id), entry_id + 2);
        assert_eq!(s.last_index_id(), INVALID_ID);
        assert_eq!(s.last_index_term(), INITIAL_TERM);
        assert_eq!(s.channel_last_entry_id(remote_id), entry_id + 2);

        s.append_entry(remote_id, new_entry(remote_id, entry_id + 10, term + 1));

        // index
        let index_id = INVALID_ID + 1;
        s.allocate(new_index(local_id, entry_id, term));
        assert_eq!(s.last_index_id(), index_id);
        assert_eq!(s.last_index_term(), term);
        s.allocate(new_index(local_id, entry_id + 2, term + 2));
        assert_eq!(s.last_index_id(), index_id + 1);
        assert_eq!(s.last_index_term(), term + 2);

        s.extend_indexes(vec![new_index_with_id(remote_id, index_id + 3, entry_id, term)]);
    }

    #[test]
    fn mem_storage_stable() {
        let local_id = 1;
        let remote_id = 2;
        let mut s = MemStorage::new(local_id);

        // empty
        let entries = s.extract_unstable_entries(std::u64::MAX);
        assert_eq!(entries.len(), 0);
        let indexes = s.extract_unstable_indexes();
        assert_eq!(indexes.len(), 0);

        // append and submit result
        s.append_entry(local_id, new_entry(local_id, INVALID_ID + 1, 1));
        s.append_entry(remote_id, new_entry(remote_id, INVALID_ID + 1, 3));
        let entries = s.extract_unstable_entries(std::u64::MAX);
        assert_eq!(entries.len(), 2);
        for entry in entries {
            if entry.channel_id == local_id {
                assert_eq!(entry.channel_term, 1);
                assert_eq!(entry.entry_id, INVALID_ID + 1);
            } else {
                assert_eq!(entry.channel_id, remote_id);
                assert_eq!(entry.entry_id, INVALID_ID + 1);
                assert_eq!(entry.channel_term, 3);
            }
        }
        let mut submit_result = HashMap::new();
        submit_result.insert(local_id, 1);
        submit_result.insert(remote_id, 2);
        s.submit_stable_result(local_id, 1);
        s.submit_stable_result(remote_id, 2);

        let entries = s.extract_unstable_entries(std::u64::MAX);
        assert_eq!(entries.len(), 0);

        // append index and submit result.
        s.extend_indexes(vec![
            new_index_with_id(local_id, 1, 1, 1),
            new_index_with_id(remote_id, 2, 1, 3),
            new_index_with_id(remote_id, 3, 2, 3),
        ]);
        let indexes = s.extract_unstable_indexes();
        assert_eq!(indexes.len(), 3);
        let expects = [(1, 1), (2, 1), (2, 2)];
        for idx in 0..3 {
            assert_eq!(indexes[idx].index_id, idx as u64 + 1);
            assert_eq!(indexes[idx].channel_id, expects[idx].0);
            assert_eq!(indexes[idx].entry_id, expects[idx].1);
        }

        s.submit_stable_index_result(3);
        let indexes = s.extract_unstable_indexes();
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn mem_storage_apply() {
        let local_id = 1;
        let remote_id = 2;
        let mut s = MemStorage::new(local_id);

        s.append_entry(local_id, new_entry(local_id, 1, 1));
        s.append_entry(remote_id, new_entry(remote_id, 1, 2));
        s.append_entry(remote_id, new_entry(remote_id, 2, 2));
        s.extract_unstable_entries(std::u64::MAX);
        s.submit_stable_result(local_id, 1);
        s.submit_stable_result(remote_id, 2);

        let (_, _, entries) = s.extract_unapply_entries(std::u64::MAX);
        assert_eq!(entries.len(), 0);

        s.extend_indexes(vec![
            new_index_with_id(local_id, 1, 1, 3),
            new_index_with_id(remote_id, 2, 1, 3),
            new_index_with_id(remote_id, 3, 2, 3),
        ]);
        s.extract_unstable_indexes();
        s.submit_stable_index_result(3);

        s.chosen_index_id = 3;
        let (first, last, entries) = s.extract_unapply_entries(std::u64::MAX);
        assert_eq!(first, 1);
        assert_eq!(last, 3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].entry_id, 1);
        assert_eq!(entries[0].channel_id, local_id);

        assert_eq!(entries[1].entry_id, 1);
        assert_eq!(entries[1].channel_id, remote_id);

        assert_eq!(entries[2].entry_id, 2);
        assert_eq!(entries[2].channel_id, remote_id);
        s.submit_applied_result(last);

        s.append_entry(local_id, new_entry(local_id, 2, 3));
        let (first, last, entries) = s.extract_unapply_entries(std::u64::MAX);
        assert_eq!(first, 4);
        assert_eq!(last, 3);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn mem_storage_release() {
        let local_id = 1;
        let remote_id = 2;
        let mut s = MemStorage::new(local_id);
        s.append_entry(local_id, new_entry(local_id, 1, 1));
        s.append_entry(local_id, new_entry(local_id, 2, 1));
        s.append_entry(remote_id, new_entry(remote_id, 1, 2));
        s.extend_indexes(vec![
            new_index_with_id(remote_id, 1, 1, 3),
            new_index_with_id(local_id, 2, 1, 3),
            new_index_with_id(local_id, 3, 2, 3),
            new_index_with_id(remote_id, 4, 2, 3),
        ]);

        s.submit_stable_result(local_id, 2);
        s.submit_stable_result(remote_id, 2);
        s.submit_stable_index_result(4);

        s.chosen_index_id = 4;
        s.submit_applied_result(2);

        s.release_entries_until(4);
        assert_eq!(s.indexes.len(), 2);
    }
}
