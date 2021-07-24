//! The crate `types` defines a set types used by sdcons.

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

use serde::{Deserialize, Serialize};

use crate::constant::*;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HardState {
    pub voted_for: u64,
    pub current_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemberState {
    pub stage: ConfigStage,

    /// applied means this member has a active channel, that indicates that this
    /// member already applied.
    pub applied: bool,
}

impl Default for MemberState {
    fn default() -> MemberState {
        MemberState {
            stage: ConfigStage::New,
            applied: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMeta {
    pub id: u64,
    pub term: u64,
}

impl Default for EntryMeta {
    fn default() -> EntryMeta {
        EntryMeta {
            id: INVALID_ID,
            term: INITIAL_TERM,
        }
    }
}

impl Default for HardState {
    fn default() -> HardState {
        HardState {
            voted_for: INVALID_NODE_ID,
            current_term: INITIAL_TERM,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub message: Vec<u8>,
    pub context: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SnapshotDesc {
    pub located_id: u64,
    pub url: String,
    pub channel_metas: HashMap<u64, EntryMeta>,
    pub members: HashMap<u64, MemberState>,
}

impl SnapshotDesc {
    pub fn applied_ids(&self) -> HashMap<u64, u64> {
        self.channel_metas
            .iter()
            .map(|(id, v)| (*id, v.id))
            .collect()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
pub enum ConfigStage {
    Old,
    Both,
    New,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChangeConfig {
    pub index_id: u64,
    pub entry_id: u64,
    pub term: u64,
    pub stage: ConfigStage,
    pub members: HashSet<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    pub request_id: u64,
    pub channel_id: u64,
    pub index_id: u64,
    pub entry_id: u64,
    pub channel_term: u64,
    pub message: Vec<u8>,
    pub context: Option<Vec<u8>>,
    pub configs: Option<ChangeConfig>,
}

impl From<&Entry> for EntryMeta {
    fn from(entry: &Entry) -> Self {
        EntryMeta {
            id: entry.entry_id,
            term: entry.channel_term,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogIndex {
    pub channel_id: u64,
    pub entry_id: u64,
    pub index_id: u64,
    pub term: u64,
    pub context: Option<Vec<u8>>,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("request_id", &self.request_id)
            .field("channel_id", &self.channel_id)
            .field("entry_id", &self.entry_id)
            .field("channel_term", &self.channel_term)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteMsg {
    pub last_index_id: u64,
    pub last_index_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteReplyMsg {
    pub reject: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareMsg {
    pub learn: bool,
    pub committed_entry_id: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrepareReplyMsg {
    pub entry_metas: Vec<EntryMeta>,
    pub learn: bool,
    pub reject: bool,
}

impl std::fmt::Debug for PrepareReplyMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_entries = self.entry_metas.len();
        f.debug_struct("PrepareReplyMsg")
            .field("reject", &self.reject)
            .field("learn", &self.learn)
            .field("num_entries", &num_entries)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AppendMsg {
    pub committed_entry_id: u64,
    pub prev_entry_id: u64,
    pub prev_entry_term: u64,
    pub entries: Vec<Entry>,
}

impl std::fmt::Debug for AppendMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_entries = self.entries.len();
        f.debug_struct("AppendMsg")
            .field("committed_id", &self.committed_entry_id)
            .field("prev_id", &self.prev_entry_id)
            .field("prev_term", &self.prev_entry_term)
            .field("num_entries", &num_entries)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendReplyMsg {
    pub entry_id: u64,
    pub hint_id: u64,
    pub reject: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexMsg {
    pub committed_index_id: u64,
    pub prev_index_id: u64,
    pub prev_index_term: u64,
    pub indexes: Vec<LogIndex>,
}

impl std::fmt::Debug for IndexMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_indexes = self.indexes.len();
        f.debug_struct("IndexMsg")
            .field("committed_id", &self.committed_index_id)
            .field("prev_id", &self.prev_index_id)
            .field("prev_term", &self.prev_index_term)
            .field("num_indexes", &num_indexes)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReplyMsg {
    pub index_id: u64,
    pub hint_id: u64,
    pub reject: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitMsg {
    pub index_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareMsg {
    pub committed_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareReplyMsg {
    pub receiving_snapshot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotReplyMsg {
    pub received: bool,
    pub hints: HashMap<u64, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadMsg {
    pub request_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadReplyMsg {
    pub request_id: u64,
    pub recommend_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MsgDetail {
    None,
    Vote(VoteMsg),
    VoteReply(VoteReplyMsg),
    Prepare(PrepareMsg),
    PrepareReply(PrepareReplyMsg),
    Append(AppendMsg),
    AppendReply(AppendReplyMsg),
    Index(IndexMsg),
    IndexReply(IndexReplyMsg),
    Commit(CommitMsg),
    Declare(DeclareMsg),
    DeclareReply(DeclareReplyMsg),
    Snapshot(SnapshotDesc),
    SnapshotReply(SnapshotReplyMsg),
    Read(ReadMsg),
    ReadReply(ReadReplyMsg),
    TimeoutNow,
}

impl std::fmt::Display for MsgDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let msg = match &self {
            MsgDetail::Vote(_) => "Vote",
            MsgDetail::VoteReply(_) => "VoteReply",
            MsgDetail::Prepare(_) => "Prepare",
            MsgDetail::PrepareReply(_) => "PrepareReply",
            MsgDetail::Append(_) => "Append",
            MsgDetail::AppendReply(_) => "AppendReply",
            MsgDetail::Index(_) => "Index",
            MsgDetail::IndexReply(_) => "IndexReply",
            MsgDetail::Commit(_) => "Commit",
            MsgDetail::Declare(_) => "Declare",
            MsgDetail::DeclareReply(_) => "DeclareReply",
            MsgDetail::Snapshot(_) => "Snapshot",
            MsgDetail::SnapshotReply(_) => "SnapshotReply",
            MsgDetail::Read(_) => "Read",
            MsgDetail::ReadReply(_) => "ReadReply",
            _ => "none",
        };
        write!(f, "{}", msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub from: u64,

    /// Who this message send to.
    pub to: u64,

    /// The current index channel term of the message sender.
    pub index_term: u64,

    /// The id of channel of which this message details
    pub channel_id: u64,
    pub channel_term: u64,
    pub detail: MsgDetail,
}
