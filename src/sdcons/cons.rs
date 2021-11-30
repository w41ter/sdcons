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

use chrono::prelude::*;
use log::{debug, info, trace, warn};
use rand::{thread_rng, RngCore};

use crate::constant::*;
use crate::error::Error;
use crate::progress::*;
use crate::storage::*;
use crate::types::*;

/// The options for create a sdcons node.
#[derive(Debug)]
pub struct SdconsOption {
    /// Specify the base election timeout tick of sdcons, so a sdcons node will
    /// randomly choose a value between [`base_election_timeout_tick`,
    /// `2 * base_election_timeout_tick`] as election timeout.  If the node
    /// haven't received when eleciton timeout is elapsed, the node will
    /// promotes it's term and participants the electing progress of the new
    /// term.
    ///
    /// default: 3
    pub base_election_timeout_tick: u32,

    /// Specify the approximately limit of number of bytes of each write task
    /// issued by sdcons.
    ///
    /// default: 64KB
    pub max_stable_bytes: u64,

    /// Specify the approximately limit of number of bytes of each apply task
    /// issued by sdcons.
    ///
    /// default: 64KB
    pub max_apply_bytes: u64,

    /// Specify the approximately limit of number of bytes of each log
    /// replication.
    ///
    /// default: 128KB
    pub max_bcast_bytes: u64,
}

impl Default for SdconsOption {
    fn default() -> SdconsOption {
        SdconsOption {
            base_election_timeout_tick: 3,
            max_stable_bytes: 64 * 1024 * 1024,
            max_apply_bytes: 64 * 1024 * 1024,
            max_bcast_bytes: 128 * 1204 * 1024,
        }
    }
}

/// Randomly generate the next election timeout tick between `[base, 2 * base]`.
#[inline(always)]
fn generate_election_timeout_tick(base: u32) -> u32 {
    base + thread_rng().next_u32() % base + 1
}

/// Is a message sended from the leader of the channel specified by
/// `target_channel_id`?
#[inline(always)]
fn is_leader_claim_message(msg: &Message, target_channel_id: u64) -> bool {
    match &msg.detail {
        MsgDetail::Index(_) | MsgDetail::Declare(_) | MsgDetail::Append(_)
            if target_channel_id == msg.channel_id =>
        {
            true
        }
        _ => false,
    }
}

/// Read the config stage of optional PendingConfigs, if the optional value is
/// None, return ConfigStage::New.
#[inline(always)]
fn unwrap_config_stage(pending_configs: &Option<PendingConfigs>) -> ConfigStage {
    pending_configs
        .as_ref()
        .map(|p| p.stage)
        .unwrap_or(ConfigStage::New)
}

/// Is both optional pending configs comes from the same configuration log.
#[inline(always)]
fn is_same_pending_configs(lhs: &Option<PendingConfigs>, rhs: &Option<PendingConfigs>) -> bool {
    match (lhs, rhs) {
        (Some(l), Some(r)) => l.eq(r),
        _ => false,
    }
}

#[inline(always)]
fn active_members(descs: &HashMap<u64, MemberState>) -> Vec<u64> {
    // FIXME(patrick) check applied
    descs.iter().map(|(id, _d)| *id).collect::<Vec<_>>()
}

#[inline(always)]
fn filter_index_channel(channel_ids: &Vec<u64>) -> HashSet<u64> {
    channel_ids
        .iter()
        .cloned()
        .filter(|id| *id != INDEX_CHANNEL_ID)
        .collect::<HashSet<_>>()
}

/// A set of operations is used to control the inner state of sdcons.
pub enum Control {
    /// Remove the indexes and entries which has been applied in the
    /// memory-storage and release memory.
    ReleaseMemory,
    /// Transfer the current leadership to specified node.
    ///
    /// REQUIRES no leadership transfering exists
    TransferLeader(u64),
    /// Make the lease of index follower expired immediately and elects the new
    /// term's leader.
    TimeoutNow,
    /// Notify the state machine to generate an snapshot.
    Checkpoint,
}

/// A helper struct contains all infos about how to create a message.
///
/// User should read the term of previous log (id is `from_id()-1`), the logs
/// between range `[from_id(), until_id()]`, and provides those infos to
/// MsgBuilder to build pending messages.  After that, user is to send this
/// message to `remote_id()`.
///
/// **WARNING**:
/// User should check the value of `channel_id()` to determine whether invoke
/// whick build function.  If the value of is `INDEX_CHANNEL_ID`, invokes
/// `build_index_msg()`, otherwise invokes `build_append_msg()`.
#[derive(Debug)]
pub struct MsgBuilder {
    from: u64,
    to: u64,
    index_term: u64,
    channel_id: u64,
    channel_term: u64,
    committed_id: u64,
    first_id: u64,
    until_id: u64,
}

impl MsgBuilder {
    /// The node this pending message send from.
    pub fn local_id(&self) -> u64 {
        self.from
    }

    /// The node this pending message send to.
    pub fn remote_id(&self) -> u64 {
        self.to
    }

    /// The channel this message belongs to.
    pub fn channel_id(&self) -> u64 {
        self.channel_id
    }

    /// The first log id need to read.
    pub fn from_id(&self) -> u64 {
        self.first_id
    }

    /// The last log id need to read.
    pub fn until_id(&self) -> u64 {
        self.until_id
    }

    pub fn build_append_msg(builder: &Self, prev_term: u64, entries: Vec<Entry>) -> Message {
        assert_ne!(builder.channel_id, INDEX_CHANNEL_ID);
        Message {
            from: builder.from,
            to: builder.to,
            index_term: builder.index_term,
            channel_id: builder.channel_id,
            channel_term: builder.channel_term,
            detail: MsgDetail::Append(AppendMsg {
                committed_entry_id: builder.committed_id,
                prev_entry_id: builder.first_id - 1u64,
                prev_entry_term: prev_term,
                entries,
            }),
        }
    }

    pub fn build_index_msg(builder: &Self, prev_term: u64, indexes: Vec<LogIndex>) -> Message {
        assert_eq!(builder.channel_id, INDEX_CHANNEL_ID);
        Message {
            from: builder.from,
            to: builder.to,
            index_term: builder.index_term,
            channel_id: builder.channel_id,
            channel_term: builder.channel_term,
            detail: MsgDetail::Index(IndexMsg {
                committed_index_id: builder.committed_id,
                prev_index_id: builder.first_id - 1u64,
                prev_index_term: prev_term,
                indexes,
            }),
        }
    }
}

#[derive(Debug)]
struct RemoteSnapshotRecevingStates {
    id: u64,
    states: HashMap<u64, SnapshotRecevingState>,
}

impl RemoteSnapshotRecevingStates {
    fn new(id: u64) -> RemoteSnapshotRecevingStates {
        RemoteSnapshotRecevingStates {
            id,
            states: HashMap::new(),
        }
    }

    fn is_any_remote_receiving_snapshot(&self) -> bool {
        self.states.iter().map(|(_, v)| v.receiving).count() > 0
    }

    fn is_remote_receiving_snapshot(&self, remote_id: u64) -> bool {
        self.states
            .get(&remote_id)
            .map(|s| s.receiving)
            .unwrap_or(false)
    }

    fn get_mut_remote_snapshot_state(&mut self, remote_id: u64) -> &mut SnapshotRecevingState {
        self.states
            .entry(remote_id)
            .or_insert(SnapshotRecevingState {
                receiving: false,
                start_at: 0,
                last_heartbeat_at: 0,
            })
    }

    fn update_remote_snapshot_state(&mut self, remote_id: u64, receiving: bool) {
        let e = self.get_mut_remote_snapshot_state(remote_id);
        e.receiving = receiving;
        e.last_heartbeat_at = Local::now().timestamp_nanos();
        e.start_at = Local::now().timestamp_nanos();
    }

    fn maybe_update_remote_snapshot_state(&mut self, remote_id: u64, is_receving: bool) {
        let id = self.id;
        let e = self.get_mut_remote_snapshot_state(remote_id);
        if e.receiving {
            e.last_heartbeat_at = Local::now().timestamp_nanos();
            if !is_receving && e.last_heartbeat_at - e.start_at >= 1000 * 1000 * 1000 {
                // 1s
                info!("node {} remote {} receiving snapshot timeout", id, remote_id);
                e.receiving = false;
            }
        } else if is_receving {
            e.receiving = true;
            e.last_heartbeat_at = Local::now().timestamp_nanos();
            e.start_at = Local::now().timestamp_nanos();
        }
    }
}

/// Keeps the infos used about leadership transfering.
#[derive(Debug)]
struct TransferingRecord {
    /// The target node who leadership is transfering.
    pub to: u64,

    /// The channel where the transfering leadership.
    pub channel_id: u64,

    /// The number of elapsed ticks which the transfering progress passed.
    pub elapsed_tick: usize,
}

impl Default for TransferingRecord {
    fn default() -> TransferingRecord {
        TransferingRecord {
            to: INVALID_NODE_ID,
            channel_id: INDEX_CHANNEL_ID,
            elapsed_tick: 0,
        }
    }
}

impl TransferingRecord {
    fn new(channel_id: u64, to: u64) -> TransferingRecord {
        TransferingRecord {
            to,
            channel_id,
            elapsed_tick: 0,
        }
    }

    /// Is there exists a leadership transfering?
    fn doing(&self) -> bool {
        self.to != INVALID_NODE_ID
    }

    /// Update leadership transfering elapsed tick.
    fn elapsed(&mut self) {
        if self.to != INVALID_NODE_ID {
            self.elapsed_tick += 1;
        }
    }

    /// Is this leadership transfering timeouted.
    fn timeout(&self) -> bool {
        // FIXME(patrick) use base_election_timeout_tick
        self.to != INVALID_NODE_ID && self.elapsed_tick >= 3
    }

    fn abort(&mut self) {
        self.reset(INVALID_NODE_ID, INDEX_CHANNEL_ID);
    }

    fn reset(&mut self, to: u64, channel_id: u64) {
        self.to = to;
        self.channel_id = channel_id;
        self.elapsed_tick = 0;
    }

    // Try start the transfering progress, return false if there exists an
    // transfering and they targes are differents.
    fn initiate(&mut self, channel_id: u64, to: u64) -> bool {
        if self.to != INVALID_NODE_ID {
            self.channel_id == channel_id && self.to == to
        } else {
            self.reset(to, channel_id);
            true
        }
    }

    fn target_to(&self, channel_id: u64, to: u64) -> bool {
        self.to == to && self.channel_id == channel_id
    }
}

/// Keeps the infos used about membership changing.
#[derive(Debug)]
struct PendingConfigs {
    stage: ConfigStage,

    channel_id: u64,
    term: u64,

    /// The index id of both-stage entry, pending configs will enter new-stage
    /// if this index_id is chosen.
    index_id: u64,
    entry_id: u64,

    /// Target members proposed in configuration log.
    configs: HashSet<u64>,

    /// In new stages, but not contains in old stage.
    new_configs: HashSet<u64>,
    /// In old stages, but not contains in new stage.
    old_configs: HashSet<u64>,
}

impl PartialEq for PendingConfigs {
    fn eq(&self, other: &PendingConfigs) -> bool {
        // Two membership changing proposal is samed, iff the has same indxe id, term
        // and proposed by the same channel.
        if self.index_id == INVALID_ID || other.index_id == INVALID_ID {
            return false;
        }
        self.channel_id == other.channel_id
            && self.index_id == other.index_id
            && self.term == other.term
    }
}

impl PendingConfigs {
    fn new(
        channel_id: u64,
        index_id: u64,
        entry_id: u64,
        term: u64,
        stage: ConfigStage,
        configs: HashSet<u64>,
        old_members: &HashSet<u64>,
    ) -> PendingConfigs {
        let new_configs = configs
            .iter()
            .filter(|v| !old_members.contains(v))
            .cloned()
            .collect::<HashSet<_>>();
        let old_configs = old_members
            .iter()
            .filter(|id| !configs.contains(*id) && **id != INDEX_CHANNEL_ID)
            .cloned()
            .collect::<HashSet<_>>();
        PendingConfigs {
            stage,
            channel_id,
            term,
            index_id,
            entry_id,
            configs,
            new_configs,
            old_configs,
        }
    }

    fn desc(&self) -> ChangeConfig {
        ChangeConfig {
            index_id: self.index_id,
            entry_id: self.entry_id,
            term: self.term,
            stage: self.stage,
            members: self.configs.clone(),
        }
    }

    fn member_stage(&self, id: u64) -> MemberState {
        MemberState {
            applied: false,
            stage: if self.old_configs.contains(&id) {
                ConfigStage::Old
            } else if self.new_configs.contains(&id) {
                ConfigStage::New
            } else {
                ConfigStage::Both
            },
        }
    }

    fn get_member_stages(&self) -> HashMap<u64, MemberState> {
        self.configs
            .iter()
            .map(|id| (*id, self.member_stage(*id)))
            .collect()
    }
}

#[derive(Debug)]
pub(crate) struct Sdcons {
    option: SdconsOption,
    random_election_timeout_tick: u32,

    id: u64,
    log_buffer: MemStorage,
    channel_ids: Vec<u64>,
    channels: HashMap<u64, ChannelInfo>,
    pending_configs: Option<PendingConfigs>,

    next_read_request_id: u64,
    pending_reads: VecDeque<u64>,
    undoing_reads: Vec<u64>,

    choosen_id: u64,
    pending_id: u64,
    applied_id: u64,

    transfer: TransferingRecord,
    snapshot_state: SnapshotState,
    snapshot_receving_states: RemoteSnapshotRecevingStates,

    ready: Ready,
}

/// The struct `Ready` is mainly used to buffer the output generated by the
/// state machine after receiving request, messages and tick.  The `advance()`
/// function will take the value of `Ready`.
#[derive(Debug)]
pub(crate) struct Ready {
    // The range of chosen entries [first, last].  If there no any chosen entries,
    // last_apply_index_id + 1 = first_apply_index_id.
    pub first_apply_index_id: u64,
    pub last_apply_index_id: u64,
    pub chosen_entries: Vec<Entry>,

    pub unstable_indexes: Vec<LogIndex>,
    pub unstable_entries: Vec<Entry>,

    pub msgs: HashMap<u64, Vec<Message>>,
    pub cop_msgs: Option<Vec<MsgBuilder>>,

    /// The field `post_apply` indicates that the entries in field
    /// `chosen_entries` needs to be submitted to the applier after the metas is
    /// persisted.
    ///
    /// The field is mainly used in some special scenarios.  In these scenarios,
    /// the user needs to ensure that the committed ids is persisted before
    /// applying logs.
    pub post_apply: bool,
    pub should_checkpoint: bool,
    pub should_stable_metas: bool,
    pub pending_snapshot: Option<SnapshotDesc>,

    pub roles: HashMap<u64, Role>,
    pub hard_states: HashMap<u64, HardState>,
    pub member_states: HashMap<u64, MemberState>,
    pub committed_stats: HashMap<u64, u64>,

    pub finished_reads: HashMap<u64, u64>, // request_id -> committed_id,
}

impl Default for Ready {
    fn default() -> Self {
        Ready {
            first_apply_index_id: INVALID_ID,
            last_apply_index_id: INVALID_ID,
            should_checkpoint: false,
            should_stable_metas: false,
            post_apply: false,
            msgs: HashMap::new(),
            unstable_indexes: Vec::new(),
            unstable_entries: Vec::new(),
            chosen_entries: Vec::new(),
            cop_msgs: None,
            pending_snapshot: None,
            roles: HashMap::new(),
            hard_states: HashMap::new(),
            member_states: HashMap::new(),
            committed_stats: HashMap::new(),
            finished_reads: HashMap::new(),
        }
    }
}

impl Ready {
    fn send_msg(&mut self, msg: Message) {
        assert_ne!(msg.to, INVALID_NODE_ID);
        assert_ne!(msg.to, INDEX_CHANNEL_ID);
        self.msgs
            .entry(msg.channel_id)
            .or_insert(Vec::new())
            .push(msg);
    }

    fn send_cop_msg(&mut self, msg_builder: MsgBuilder) {
        assert_ne!(msg_builder.to, INDEX_CHANNEL_ID);
        assert_ne!(msg_builder.to, INVALID_NODE_ID);
        if let Some(v) = &mut self.cop_msgs {
            v.push(msg_builder);
        } else {
            self.cop_msgs = Some(vec![msg_builder])
        }
    }
}

impl Sdcons {
    fn assign_index(&mut self, channel_id: u64, entry_id: u64) -> u64 {
        assert_ne!(channel_id, INDEX_CHANNEL_ID);
        let index = LogIndex {
            channel_id,
            entry_id,
            index_id: 0,
            term: self.get_channel_term(INDEX_CHANNEL_ID),
            context: None,
        };
        let index_id = self.log_buffer.allocate(index);
        debug!(
            "node {} channel {} assign channel {} entry {} index {}",
            self.id, INDEX_CHANNEL_ID, channel_id, entry_id, index_id
        );
        index_id
    }

    fn is_index_leader(&self) -> bool {
        self.is_channel_leader(INDEX_CHANNEL_ID)
    }

    fn is_index_student(&self) -> bool {
        self.channels
            .get(&INDEX_CHANNEL_ID)
            .map(|i| i.is_student())
            .unwrap_or(false)
    }

    fn is_channel_leader(&self, channel_id: u64) -> bool {
        self.channels
            .get(&channel_id)
            .map(|i| i.is_leader())
            .unwrap_or(false)
    }

    fn is_channel_candidate(&self, channel_id: u64) -> bool {
        self.channels
            .get(&channel_id)
            .map(|i| i.is_candidate())
            .unwrap_or(false)
    }

    fn is_channel_follower(&self, channel_id: u64) -> bool {
        self.channels
            .get(&channel_id)
            .map(|i| i.is_follower())
            .unwrap_or(false)
    }

    fn is_local_channel_leader(&self) -> bool {
        self.is_channel_leader(self.id)
    }

    fn local_entry_channel_term(&self) -> u64 {
        self.get_channel_term(self.id)
    }

    fn next_entry_id(&self) -> u64 {
        self.channel_next_entry_id(self.id)
    }

    fn channel_next_entry_id(&self, channel_id: u64) -> u64 {
        self.log_buffer.channel_last_entry_id(channel_id) + 1u64
    }

    fn remote_ids(&self) -> Vec<u64> {
        self.channel_ids
            .iter()
            .map(|k| *k)
            .filter(|k| *k != self.id && *k != INDEX_CHANNEL_ID)
            .collect::<Vec<_>>()
    }

    fn get_member_states(&self) -> HashMap<u64, MemberState> {
        if let Some(pending_configs) = &self.pending_configs {
            match pending_configs.stage {
                ConfigStage::New | ConfigStage::Both => {
                    return pending_configs.get_member_stages();
                }
                _ => {}
            }
        }
        self.get_index_channel().get_member_states()
    }

    fn build_channel_if_not_exists(&mut self, channel_id: u64) {
        if !self.channels.contains_key(&channel_id) {
            self.channels.insert(
                channel_id,
                ChannelInfo::build(self.id, channel_id, self.get_member_states()),
            );
        }
    }

    fn build_msg_header(&self, channel_id: u64) -> Message {
        let index_term = self.get_index_term();
        let channel_term = if channel_id == INDEX_CHANNEL_ID {
            index_term
        } else {
            self.get_channel_term(channel_id)
        };
        Message {
            from: self.id,
            to: INVALID_NODE_ID,
            index_term,
            channel_id,
            channel_term,
            detail: MsgDetail::None,
        }
    }

    fn append_entry(&mut self, channel_id: u64, entry: Entry) {
        self.log_buffer.append_entry(channel_id, entry)
    }

    fn advance_choose_progress(&mut self) {
        // Only advance chosen index to matched id.
        let committed_map = self
            .channels
            .iter()
            .map(|(id, c)| (*id, c.get_local_safety_commit_id()))
            .collect::<HashMap<_, _>>();
        self.log_buffer.advance_chosen_index_id(committed_map);
    }

    fn bcast_index_channel<T>(&mut self, log_meta_view: &T)
    where
        T: LogMetaView,
    {
        let msg_header = self.build_msg_header(INDEX_CHANNEL_ID);
        let mem_first_id = self.log_buffer.channel_first_entry_id(INDEX_CHANNEL_ID);
        let mem_last_id = self.log_buffer.channel_last_entry_id(INDEX_CHANNEL_ID);
        let channel_info = self.channels.get_mut(&INDEX_CHANNEL_ID).unwrap();
        if !channel_info.is_leader() {
            return;
        }

        let config_stage = unwrap_config_stage(&self.pending_configs);
        let advanced = channel_info.try_advance_committed_id(config_stage);
        let committed_index_id = channel_info.committed_id;
        for (id, progress) in &mut channel_info.progress_map {
            if *id == self.id
                || progress.is_paused()
                || self
                    .snapshot_receving_states
                    .is_remote_receiving_snapshot(*id)
            {
                continue;
            }

            if progress.next_id <= mem_first_id {
                let (stabled_first_id, _) = log_meta_view.range_of(INDEX_CHANNEL_ID);
                if progress.next_id < stabled_first_id {
                    if let SnapshotState::Creating = self.snapshot_state {
                        continue;
                    } else if let Some(desc) = log_meta_view.latest_snapshot() {
                        debug!(
                            "node {} channel {} replicate snapshot to {}",
                            self.id, INDEX_CHANNEL_ID, *id
                        );
                        let mut msg = msg_header.clone();
                        msg.to = *id;
                        msg.detail = MsgDetail::Snapshot(desc);
                        self.ready.send_msg(msg);
                        self.snapshot_receving_states
                            .update_remote_snapshot_state(*id, true);
                    } else {
                        debug!("node {} channel {} try create snapshot", self.id, INDEX_CHANNEL_ID);
                        self.snapshot_state = SnapshotState::Creating;
                        self.ready.should_checkpoint = true;
                    }
                } else {
                    let builder = MsgBuilder {
                        from: self.id,
                        to: *id,
                        index_term: msg_header.index_term,
                        channel_id: msg_header.channel_id,
                        channel_term: msg_header.channel_term,
                        committed_id: committed_index_id,
                        first_id: progress.next_id,
                        until_id: mem_first_id,
                    };
                    debug!(
                        "node {} channel {} send build msg: {:?}",
                        self.id, INDEX_CHANNEL_ID, builder
                    );
                    self.ready.send_cop_msg(builder);
                    progress.step_building_msg();
                }
            } else if progress.next_id <= mem_last_id || advanced {
                let prev_index_id = progress.next_id - 1u64;
                debug!(
                    "node {} channel {} replicate indexes[{}, {}) to {}, advance {}",
                    self.id,
                    INDEX_CHANNEL_ID,
                    progress.next_id,
                    mem_last_id + 1u64,
                    *id,
                    advanced
                );
                let prev_index_term = self
                    .log_buffer
                    .index_term(prev_index_id)
                    .unwrap_or(INITIAL_TERM);
                let indexes = self.log_buffer.index_range(progress.next_id, mem_last_id);
                progress.replicate_to(mem_last_id);
                let mut msg = msg_header.clone();
                msg.to = *id;
                msg.detail = MsgDetail::Index(IndexMsg {
                    committed_index_id,
                    prev_index_id,
                    prev_index_term,
                    indexes,
                });
                self.ready.send_msg(msg);
            }
        }
    }

    fn bcast_entry_channel<T>(&mut self, channel_id: u64, log_meta_view: &T)
    where
        T: LogMetaView,
    {
        let msg_header = self.build_msg_header(channel_id);
        let first_id = self.log_buffer.channel_first_entry_id(channel_id);
        let last_id = self.log_buffer.channel_last_entry_id(channel_id);
        let channel_info = self.channels.get_mut(&channel_id).unwrap();
        if !channel_info.is_leader() {
            return;
        }
        let config_stage = unwrap_config_stage(&self.pending_configs);
        let advanced = channel_info.try_advance_committed_id(config_stage);
        let committed_entry_id = channel_info.committed_id;
        for (id, progress) in &mut channel_info.progress_map {
            if *id == self.id
                || progress.is_paused()
                || self
                    .snapshot_receving_states
                    .is_remote_receiving_snapshot(*id)
            {
                continue;
            }

            if progress.next_id <= first_id {
                let (stabled_first_id, _) = log_meta_view.range_of(INDEX_CHANNEL_ID);
                if progress.next_id < stabled_first_id {
                    if let SnapshotState::Creating = self.snapshot_state {
                        continue;
                    } else if let Some(desc) = log_meta_view.latest_snapshot() {
                        debug!(
                            "node {} channel {} replicate snapshot to {}",
                            self.id, channel_id, *id
                        );
                        let mut msg = msg_header.clone();
                        msg.to = *id;
                        msg.detail = MsgDetail::Snapshot(desc);
                        self.ready.send_msg(msg);
                        self.snapshot_receving_states
                            .update_remote_snapshot_state(*id, true);
                    } else {
                        debug!("node {} channel {} try create snapshot", self.id, channel_id);
                        self.snapshot_state = SnapshotState::Creating;
                        self.ready.should_checkpoint = true;
                    }
                } else {
                    let builder = MsgBuilder {
                        from: self.id,
                        to: *id,
                        index_term: msg_header.index_term,
                        channel_id: msg_header.channel_id,
                        channel_term: msg_header.channel_term,
                        committed_id: committed_entry_id,
                        first_id: progress.next_id,
                        until_id: first_id,
                    };
                    debug!("node {} channel {} send build msg: {:?}", self.id, channel_id, builder);
                    self.ready.send_cop_msg(builder);
                    progress.step_building_msg();
                }
            } else if progress.next_id <= last_id || advanced {
                let prev_entry_id = progress.next_id - 1u64;
                debug!(
                    "node {} channel {} replicate entries[{}, {}) to {}, advance {}",
                    self.id,
                    channel_id,
                    progress.next_id,
                    last_id + 1u64,
                    *id,
                    advanced
                );
                let prev_entry_term = self
                    .log_buffer
                    .entry_term(channel_id, prev_entry_id)
                    .unwrap_or(INITIAL_TERM);
                let entries = self
                    .log_buffer
                    .range_of(channel_id, progress.next_id, last_id);
                debug!("node {} channel {} entries is {:?}", self.id, channel_id, entries);
                progress.replicate_to(last_id);
                let mut msg = msg_header.clone();
                msg.to = *id;
                msg.detail = MsgDetail::Append(AppendMsg {
                    committed_entry_id,
                    prev_entry_id,
                    prev_entry_term,
                    entries,
                });
                self.ready.send_msg(msg);
            }
        }
    }

    fn extract_chosen_entries(&mut self) {
        let (first_index_id, last_index_id, entries) = self
            .log_buffer
            .extract_unapply_entries(self.option.max_apply_bytes);
        self.ready.first_apply_index_id = first_index_id;
        self.ready.last_apply_index_id = last_index_id;
        self.ready.chosen_entries = entries;
    }

    fn extract_unstable_entries(&mut self) {
        self.ready.unstable_entries = self
            .log_buffer
            .extract_unstable_entries(self.option.max_stable_bytes);
        self.ready.unstable_indexes = self.log_buffer.extract_unstable_indexes();
    }

    fn bcast_each_channels<L>(&mut self, log_meta_view: &L)
    where
        L: LogMetaView,
    {
        // FIXME(patrick) In the current design, different channels have different
        // progress. If one progress is paused, the progress of other channels cannot be
        // sensed temporarily.
        for idx in 0..self.channel_ids.len() {
            let channel_id = self.channel_ids[idx];
            if channel_id == INDEX_CHANNEL_ID {
                self.bcast_index_channel(log_meta_view);
            } else {
                self.bcast_entry_channel(channel_id, log_meta_view);
            }
        }
    }

    fn collect_state_snapshot(&mut self) {
        self.ready.hard_states = self
            .channels
            .iter()
            .map(|(i, c)| (*i, c.hard_state()))
            .collect::<HashMap<_, _>>();
        self.ready.roles = self
            .channels
            .iter()
            .map(|(i, c)| (*i, c.role))
            .collect::<HashMap<_, _>>();
        self.ready.committed_stats = self
            .channels
            .iter()
            .map(|(i, c)| (*i, c.get_local_safety_commit_id()))
            .collect::<HashMap<_, _>>();
        self.ready.member_states = self.get_member_states();
    }

    fn force_remote_step_down(
        &mut self,
        to: u64,
        old_term: u64,
        channel_id: u64,
        detail: &MsgDetail,
    ) {
        debug!("node {} channel {} force remote {} step down because: receive staled {} msg, old term: {}",
            self.id, channel_id, to, detail, old_term);
        let mut msg_header = self.build_msg_header(channel_id);
        msg_header.to = to;
        self.ready.send_msg(msg_header);
    }

    fn reject_staled_message(&mut self, msg: &Message) -> bool {
        if msg.index_term < self.get_channel_term(INDEX_CHANNEL_ID) {
            self.force_remote_step_down(msg.from, msg.index_term, INDEX_CHANNEL_ID, &msg.detail);
            true
        } else if msg.channel_id != INDEX_CHANNEL_ID
            && msg.channel_term < self.get_channel_term(msg.channel_id)
        {
            self.force_remote_step_down(msg.from, msg.channel_term, msg.channel_id, &msg.detail);
            true
        } else {
            false
        }
    }

    fn maybe_transfering_finised(&mut self, channel_id: u64, from: u64) {
        if self.transfer.target_to(channel_id, from) {
            info!(
                "node {} channel {} transfer leadership to {} success",
                self.id, channel_id, from
            );
            self.transfer = TransferingRecord::default();
        }
    }

    fn try_advance_channel_term(&mut self, msg: &Message) {
        let mut active_channels = vec![(INDEX_CHANNEL_ID, msg.index_term)];
        if msg.channel_id != INDEX_CHANNEL_ID {
            active_channels.push((msg.channel_id, msg.channel_term));
        }

        let mut advanced = false;
        for (channel_id, term) in active_channels {
            let target_leader_id = if is_leader_claim_message(msg, channel_id) {
                msg.from
            } else {
                INVALID_NODE_ID
            };
            self.build_channel_if_not_exists(channel_id);
            let last_id = self.log_buffer.channel_last_entry_id(channel_id);
            let channel_info = self.channels.get_mut(&channel_id).unwrap();
            if channel_info.term < term {
                channel_info.to_follower(target_leader_id, term, last_id, "high-term");
                self.maybe_transfering_finised(channel_id, msg.from);
                advanced = true;
            } else if (channel_info.is_candidate() || channel_info.has_leader())
                && target_leader_id != INVALID_NODE_ID
            {
                channel_info.to_follower(target_leader_id, term, last_id, "claim-leader");
                advanced = true;
            }
        }

        if advanced {
            // Advance pending read requests when leader is changed.
            self.submit_read_task();
        }
    }

    fn get_index_channel(&self) -> &ChannelInfo {
        self.channels.get(&INDEX_CHANNEL_ID).unwrap()
    }

    fn get_index_term(&self) -> u64 {
        self.channels.get(&INDEX_CHANNEL_ID).unwrap().term
    }

    fn get_channel_term(&self, channel_id: u64) -> u64 {
        self.channels
            .get(&channel_id)
            .map(|c| c.term)
            .unwrap_or(INITIAL_TERM)
    }

    fn maybe_assign_indexes(&mut self, channel_id: u64, first_entry_id: u64, last_entry_id: u64) {
        // leader order this entries and bcast to members.
        if self.is_index_leader() {
            let first_entry_id = std::cmp::max(
                self.log_buffer.channel_last_assigned_entry_id(channel_id) + 1u64,
                first_entry_id,
            );
            for idx in first_entry_id..(last_entry_id + 1) {
                self.assign_index(channel_id, idx);
            }
        }
    }

    fn abort_pending_configs(&mut self, reason: &str) {
        assert_eq!(self.pending_configs.is_some(), true);
        self.ready.should_stable_metas = true;
        let pending_configs = self.pending_configs.as_ref().unwrap();
        info!(
            "node {} channel {} because {}, abort and rollback {:?}",
            self.id,
            pending_configs.channel_id,
            reason,
            pending_configs.desc()
        );
        for (_channe_id, channel_info) in &mut self.channels {
            channel_info
                .rollback_config_change(&pending_configs.new_configs, &pending_configs.old_configs);
        }
    }

    fn maybe_update_config_change_stage(
        &mut self,
        channel_id: u64,
        first_entry_id: u64,
        _last_entry_id: u64,
    ) {
        assert_eq!(self.is_channel_follower(channel_id), true);

        // The log entries might is truncated by channel's leader, so try to abort
        // pending_configs.
        if let Some(p) = &self.pending_configs {
            if p.channel_id == channel_id && first_entry_id <= p.entry_id {
                self.abort_pending_configs(&"log entries was truncated");
            }
        }

        let entry = match self.log_buffer.channel_last_config_change_entry(channel_id) {
            Some(e) if e.entry_id >= first_entry_id => e,
            _ => return,
        };

        let config_change = entry.configs.unwrap();
        info!("node {} channel {} recieve {:?}", self.id, channel_id, config_change);

        let old_members = self.channel_ids.iter().cloned().collect::<HashSet<_>>();
        self.pending_configs = Some(PendingConfigs::new(
            entry.channel_id,
            config_change.index_id,
            config_change.entry_id,
            config_change.term,
            config_change.stage,
            config_change.members,
            &old_members,
        ));

        self.update_channels_config_stage();
    }

    fn handle_append(&mut self, from: u64, channel_id: u64, msg: AppendMsg) {
        debug!(
            "node {} channel {} receive append from {}: {:?}",
            self.id, channel_id, from, msg
        );

        assert_ne!(channel_id, INDEX_CHANNEL_ID);
        let mut reply_msg = AppendReplyMsg {
            reject: false,
            entry_id: msg.prev_entry_id + 1u64,
            hint_id: 0,
        };
        let length = msg.entries.len();
        let committed_entry_id = self.channels.get(&channel_id).unwrap().committed_id;
        if msg.prev_entry_id < committed_entry_id {
            reply_msg.reject = false;
            reply_msg.hint_id = committed_entry_id + 1u64;
        } else if self.log_buffer.is_term_miss_matched(
            channel_id,
            msg.prev_entry_id,
            msg.prev_entry_term,
        ) {
            let reject_entry_id =
                self.log_buffer
                    .find_conflict(channel_id, msg.prev_entry_id, msg.prev_entry_term);
            reply_msg.reject = true;
            reply_msg.hint_id = reject_entry_id;
            self.channels.get_mut(&channel_id).unwrap().reset_tick();
        } else {
            let (first_entry_id, last_entry_id) =
                self.log_buffer.append_entries(channel_id, msg.entries);
            reply_msg.reject = false;
            reply_msg.hint_id = last_entry_id + 1;
            self.maybe_update_config_change_stage(channel_id, first_entry_id, last_entry_id);
            self.maybe_assign_indexes(channel_id, first_entry_id, last_entry_id);
            let channel_info = self.channels.get_mut(&channel_id).unwrap();
            channel_info.update_committed_id(std::cmp::min(last_entry_id, msg.committed_entry_id));
            channel_info.reset_tick();
        }

        debug!(
            "node {} channel {} receive append from {}, prev id {}, prev term {}, length {}, reply: {:?}",
            self.id, channel_id, from,
            msg.prev_entry_id, msg.prev_entry_term, length,
            reply_msg
        );
        let mut msg_header = self.build_msg_header(channel_id);
        msg_header.to = from;
        msg_header.detail = MsgDetail::AppendReply(reply_msg);
        self.ready.send_msg(msg_header);
    }

    fn handle_append_reply(&mut self, from: u64, channel_id: u64, msg: AppendReplyMsg) {
        debug!("node {} channel {} receive from {}: {:?}", self.id, channel_id, from, msg);
        match self.channels.get_mut(&channel_id) {
            Some(c) => c.update_progress(from, msg.reject, msg.entry_id, msg.hint_id),
            None => {}
        };
    }

    fn handle_index(&mut self, from: u64, msg: IndexMsg) {
        info!(
            "node {} channel {} receive index msg from {}: {:?}",
            self.id, INDEX_CHANNEL_ID, from, msg
        );
        let mut reply_msg = IndexReplyMsg {
            reject: false,
            index_id: msg.prev_index_id + 1u64,
            hint_id: 0,
        };
        let length = msg.indexes.len();
        let committed_index_id = self.channels.get(&INDEX_CHANNEL_ID).unwrap().committed_id;
        if msg.prev_index_id < committed_index_id {
            reply_msg.reject = false;
            reply_msg.hint_id = committed_index_id + 1u64;
        } else if self.log_buffer.is_term_miss_matched(
            INDEX_CHANNEL_ID,
            msg.prev_index_id,
            msg.prev_index_term,
        ) {
            let reject_index_id = self
                .log_buffer
                .find_index_conflict(msg.prev_index_id, msg.prev_index_term);
            reply_msg.reject = true;
            reply_msg.hint_id = reject_index_id;
            self.channels
                .get_mut(&INDEX_CHANNEL_ID)
                .unwrap()
                .reset_tick();
        } else {
            let last_index_id = self.log_buffer.extend_indexes(msg.indexes);
            reply_msg.reject = false;
            reply_msg.hint_id = last_index_id + 1;
            let channel_info = self.channels.get_mut(&INDEX_CHANNEL_ID).unwrap();
            channel_info.update_committed_id(std::cmp::min(last_index_id, msg.committed_index_id));
            channel_info.reset_tick();
        }

        debug!(
            "node {} channel {} receive index from {}, prev id {}, prev term {}, length {}, reply: {:?}",
            self.id, INDEX_CHANNEL_ID, from,
            msg.prev_index_id, msg.prev_index_term, length,
            reply_msg
        );
        let mut msg_header = self.build_msg_header(INDEX_CHANNEL_ID);
        msg_header.to = from;
        msg_header.detail = MsgDetail::IndexReply(reply_msg);
        self.ready.send_msg(msg_header);
    }

    fn handle_index_reply(&mut self, from: u64, msg: IndexReplyMsg) {
        match self.channels.get_mut(&INDEX_CHANNEL_ID) {
            Some(c) => {
                c.update_progress(from, msg.reject, msg.index_id, msg.hint_id);
                if !msg.reject {
                    self.advance_transfer_progress(from);
                }
            }
            None => {}
        };
    }

    fn is_refreshed_index(&self, id: u64, term: u64) -> bool {
        let last_index_id = self.log_buffer.last_index_id();
        let last_index_term = self.log_buffer.last_index_term();
        // Local last index term is less than remote last index term
        term > last_index_term
            // ... last index term is equals, but local last index id is less than remote last index id.
            || (term == last_index_term && id >= last_index_id)
    }

    fn handle_vote(&mut self, from: u64, msg: VoteMsg) {
        let mut detail = VoteReplyMsg { reject: true };
        if self.is_refreshed_index(msg.last_index_id, msg.last_index_term) {
            detail.reject = !self
                .channels
                .get_mut(&INDEX_CHANNEL_ID)
                .unwrap()
                .try_make_promise(from);
        }

        info!(
            "node {} channel {} receive remote {}: {:?}, reply reject: {}",
            self.id, INDEX_CHANNEL_ID, from, msg, detail.reject
        );

        let mut msg_header = self.build_msg_header(INDEX_CHANNEL_ID);
        msg_header.to = from;
        msg_header.detail = MsgDetail::VoteReply(detail);
        self.ready.send_msg(msg_header);
    }

    fn handle_vote_reply(&mut self, from: u64, msg: VoteReplyMsg) {
        info!(
            "node {} channel {} receive remote {}: {:?}",
            self.id, INDEX_CHANNEL_ID, from, msg
        );
        if !msg.reject {
            self.receive_granted_vote(from);
        }
    }

    fn receive_granted_vote(&mut self, from: u64) {
        let config_stage = unwrap_config_stage(&self.pending_configs);
        let channel_info = self.channels.get_mut(&INDEX_CHANNEL_ID).unwrap();
        channel_info.receive_promise(from);
        if channel_info.receive_majority_promise(config_stage) {
            let last_index_id = self.log_buffer.last_index_id();
            channel_info.to_student(last_index_id, "granted");
            let missed_channel_ids = channel_info.missed_channel_ids();
            if missed_channel_ids.len() > 0 {
                // send prepare request
                debug!(
                    "node {} channel {} there have {} missed channel {:?} need to recovery",
                    self.id,
                    INDEX_CHANNEL_ID,
                    missed_channel_ids.len(),
                    missed_channel_ids
                );
                for channel_id in missed_channel_ids {
                    // Only send learned requset once, if this message is losted, the election
                    // progress is treated as failed.
                    self.bcast_prepare_request(channel_id, true);
                }
            } else {
                // There no any channel is missed, we could become a leader directly.
                debug!(
                    "node {} channel {} no any missed channel exists",
                    self.id, INDEX_CHANNEL_ID
                );
                self.all_channel_already_learned();
            }
        }
    }

    fn handle_prepare(&mut self, from: u64, channel_id: u64, msg: PrepareMsg) {
        let mut detail = PrepareReplyMsg {
            reject: false,
            learn: msg.learn,
            entry_metas: Vec::new(),
        };
        if !msg.learn {
            self.build_channel_if_not_exists(channel_id);
            detail.reject = !self
                .channels
                .get_mut(&channel_id)
                .unwrap()
                .try_make_promise(from);
        }

        if !detail.reject {
            let last_entry_id = self.log_buffer.channel_last_entry_id(channel_id);
            let first_entry_id = self.log_buffer.channel_first_entry_id(channel_id);
            let mut begin_entry_id = msg.committed_entry_id + 1u64;
            if begin_entry_id < first_entry_id {
                // A entry in range (msg.committed_entry_id, first_entry_id) isn't found,
                // that means those entries has already assigned indexes.
                begin_entry_id = first_entry_id + 1u64;
            }
            if msg.committed_entry_id < last_entry_id {
                detail.entry_metas =
                    self.log_buffer
                        .range_of_metas(channel_id, begin_entry_id, last_entry_id);
            }
        }

        info!(
            "node {} channel {} receive remote {}: {:?}, reply {}",
            self.id, channel_id, from, msg, detail.reject
        );

        let mut msg_header = self.build_msg_header(channel_id);
        msg_header.to = from;
        msg_header.detail = MsgDetail::PrepareReply(detail);
        self.ready.send_msg(msg_header);
    }

    fn handle_prepare_reply(&mut self, from: u64, channel_id: u64, msg: PrepareReplyMsg) {
        assert_ne!(channel_id, INDEX_CHANNEL_ID);
        debug!("node {} channel {} receive remote {}: {:?}", self.id, channel_id, from, msg);
        if msg.reject {
            return;
        }

        self.receive_prepare_promise(from, channel_id, msg.learn, msg.entry_metas);
    }

    fn handle_declare(&mut self, from: u64, channel_id: u64, msg: DeclareMsg) {
        let last_id = self.log_buffer.channel_last_entry_id(channel_id);
        debug!(
            "node {} channel {} receive {} declare msg, committed id {}, local last id {}",
            self.id, channel_id, from, msg.committed_id, last_id
        );
        if last_id < msg.committed_id {
            // TODO(patrick) receive unexpect msg
            debug!("receive unexpect declare msg");
        } else {
            // Receive declare message from remote, reset local tick count.
            self.build_channel_if_not_exists(channel_id);
            let channel_info = self.channels.get_mut(&channel_id).unwrap();
            channel_info.update_committed_id(msg.committed_id);
            channel_info.reset_tick();
        }

        let mut msg_header = self.build_msg_header(channel_id);
        msg_header.to = from;
        msg_header.detail = MsgDetail::DeclareReply(DeclareReplyMsg {
            receiving_snapshot: if let SnapshotState::Loading = self.snapshot_state {
                true
            } else {
                false
            },
        });
        self.ready.send_msg(msg_header);
    }

    fn handle_declare_reply(&mut self, from: u64, channel_id: u64, msg: DeclareReplyMsg) {
        match self.channels.get_mut(&channel_id) {
            Some(c) => {
                c.on_receive_msg(from);
                self.snapshot_receving_states
                    .maybe_update_remote_snapshot_state(from, msg.receiving_snapshot);
            }
            None => {}
        };
    }

    fn handle_read(&mut self, from: u64, msg: ReadMsg) {
        if !self.is_index_leader() {
            debug!(
                "node {} channel {} recieve read from {}: {:?}, but I am not index leader",
                self.id, INDEX_CHANNEL_ID, from, msg
            );
            return;
        }

        debug!(
            "node {} channel {} recieve read from {}: {:?}",
            self.id, INDEX_CHANNEL_ID, from, msg
        );
        let mut msg_header = self.build_msg_header(INDEX_CHANNEL_ID);
        msg_header.to = from;
        msg_header.detail = MsgDetail::ReadReply(ReadReplyMsg {
            request_id: msg.request_id,
            recommend_id: self.get_index_channel().current_term_safe_commit_id(),
        });
        self.ready.send_msg(msg_header);
    }

    fn apply_read_reply(&mut self, request_id: u64, recommend_id: u64) {
        let mut idx = 0;
        for value in &self.pending_reads {
            if *value > request_id {
                break;
            }
            // Read request requires no reading staled values from fsm, so that,
            // the `recommend_id` large than the actually `committed_id`, is acceptable.
            self.ready.finished_reads.insert(*value, recommend_id);
            idx += 1;
        }
        self.pending_reads.drain(0..idx);
    }

    fn handle_read_reply(&mut self, from: u64, msg: ReadReplyMsg) {
        self.apply_read_reply(msg.request_id, msg.recommend_id);
    }

    fn handle_timeout_now(&mut self, from: u64, channel_id: u64) {
        if !self.channel_ids.contains(&self.id) {
            warn!(
                "node {} receive timeout-now from {}, but current membership is empty",
                self.id, from
            );
            return;
        }

        let last_id = self.log_buffer.channel_last_entry_id(channel_id);
        let channel_info = self.channels.get_mut(&channel_id).unwrap();
        channel_info.to_candidate(last_id, "timeout-now");
        if channel_id == INDEX_CHANNEL_ID {
            self.bcast_vote_request();
        } else {
            self.bcast_prepare_request(channel_id, false);
        }
    }

    fn handle_snapshot(&mut self, from: u64, desc: SnapshotDesc) {
        // TODO(patrick) check staled request.
        if self.snapshot_state != SnapshotState::None {
            warn!(
                "node {} already in {:?} snapshot stage, ignore new snapshot",
                self.id, self.snapshot_state
            );
            return;
        }

        info!("node {} start receiving snapshot from {}: {:?}", self.id, from, desc);
        self.ready.pending_snapshot = Some(desc);
        self.snapshot_state = SnapshotState::Loading;
    }

    fn handle_snapshot_reply(&mut self, from: u64, msg: SnapshotReplyMsg) {
        self.snapshot_receving_states
            .update_remote_snapshot_state(from, false);
        if msg.received {
            info!("node {} remote {} receive snapshot finished", self.id, from);
            for (channel_id, next_id) in &msg.hints {
                match self.channels.get_mut(channel_id) {
                    Some(c) => c.update_progress(from, false, *next_id, *next_id),
                    None => {}
                };
            }
        } else {
            // TODO(patrick) Maybe we should trigger another snapshot?
            debug!("node {} remote {} reject snapshot, try trigger new once", self.id, from);
        }
    }

    fn dispatch_message(&mut self, msg: Message) {
        debug_assert_ne!(
            msg.from, self.id,
            "node {} receive a message {:?} from itself",
            self.id, msg
        );

        match msg.detail {
            MsgDetail::Append(d) => self.handle_append(msg.from, msg.channel_id, d),
            MsgDetail::AppendReply(d) => self.handle_append_reply(msg.from, msg.channel_id, d),
            MsgDetail::Index(d) => self.handle_index(msg.from, d),
            MsgDetail::IndexReply(d) => self.handle_index_reply(msg.from, d),
            MsgDetail::Vote(d) => self.handle_vote(msg.from, d),
            MsgDetail::VoteReply(d) => self.handle_vote_reply(msg.from, d),
            MsgDetail::Prepare(d) => self.handle_prepare(msg.from, msg.channel_id, d),
            MsgDetail::PrepareReply(d) => self.handle_prepare_reply(msg.from, msg.channel_id, d),
            MsgDetail::Declare(d) => self.handle_declare(msg.from, msg.channel_id, d),
            MsgDetail::DeclareReply(d) => self.handle_declare_reply(msg.from, msg.channel_id, d),
            MsgDetail::Snapshot(d) => self.handle_snapshot(msg.from, d),
            MsgDetail::SnapshotReply(d) => self.handle_snapshot_reply(msg.from, d),
            MsgDetail::Read(d) => self.handle_read(msg.from, d),
            MsgDetail::ReadReply(d) => self.handle_read_reply(msg.from, d),
            MsgDetail::TimeoutNow => self.handle_timeout_now(msg.from, msg.channel_id),
            MsgDetail::None => {}
            _ => panic!("unknown message {:?}", msg),
        }
    }

    fn bcast_heartbeats(&mut self, channel_id: u64) {
        let channel_info = self.channels.get(&channel_id).unwrap();
        debug!(
            "node {} channel {} bcast heartbeats, committed id {}",
            self.id, channel_id, channel_info.committed_id
        );
        let msg_header = self.build_msg_header(channel_id);
        for id in self.remote_ids() {
            let matched_committed_id = channel_info.matched_committed_id(id);
            debug!(
                "node {} channel {} send heartbeat to {}, matched committed id {}",
                self.id, channel_id, id, matched_committed_id
            );
            let mut msg = msg_header.clone();
            msg.to = id;
            msg.detail = MsgDetail::Declare(DeclareMsg {
                committed_id: matched_committed_id,
            });
            self.ready.send_msg(msg);
        }
    }

    fn bcast_vote_request(&mut self) {
        let mut msg_header = self.build_msg_header(INDEX_CHANNEL_ID);
        msg_header.detail = MsgDetail::Vote(VoteMsg {
            last_index_id: self.log_buffer.last_index_id(),
            last_index_term: self.log_buffer.last_index_term(),
        });
        for to in self.remote_ids() {
            let mut msg = msg_header.clone();
            msg.to = to;
            self.ready.send_msg(msg);
        }

        self.receive_granted_vote(self.id);
    }

    fn receive_channel_tick(&mut self, channel_id: u64, random_elect_timeout_tick: u32) -> bool {
        let local_id = self.id;
        let config_stage = unwrap_config_stage(&self.pending_configs);
        let contains_self = self.channel_ids.contains(&local_id);
        let is_index_leader = self.is_index_leader();
        let last_id = self.log_buffer.channel_last_entry_id(channel_id);
        let channel_info = self.channels.get_mut(&channel_id).unwrap();

        channel_info.elapsed_tick += 1;
        if channel_info.elapsed_tick >= random_elect_timeout_tick {
            self.random_election_timeout_tick =
                generate_election_timeout_tick(self.option.base_election_timeout_tick);
            return match &channel_info.role {
                Role::Leader | Role::Student => {
                    channel_info.elapsed_tick = 0;
                    if channel_info.advance_quorum_lease()
                        < channel_info.stage_majority(config_stage)
                    {
                        channel_info.to_follower(
                            INVALID_NODE_ID,
                            channel_info.term,
                            last_id,
                            "lost-quorum",
                        );
                        false
                    } else {
                        channel_info.is_leader()
                    }
                }
                Role::Follower | Role::Candidate
                    if contains_self
                        && (channel_id == INDEX_CHANNEL_ID
                            || is_index_leader
                            || channel_id == local_id) =>
                {
                    channel_info.to_candidate(last_id, "election timeout");
                    true
                }
                _ => {
                    channel_info.to_follower(
                        INVALID_NODE_ID,
                        channel_info.term + 1,
                        last_id,
                        "election timeout",
                    );
                    false
                }
            };
        }

        return Role::Leader == channel_info.role;
    }

    fn append_empty_entry(&mut self, channel_id: u64) {
        let entry_id = self.channel_next_entry_id(channel_id);
        let entry = Entry {
            request_id: INTERNAL_REQUEST,
            channel_id: channel_id,
            entry_id: entry_id,
            index_id: INVALID_ID,
            channel_term: self.get_channel_term(channel_id),
            message: Vec::new(),
            context: None,
            configs: None,
        };

        self.append_entry(channel_id, entry);
        if self.is_index_leader() {
            self.assign_index(channel_id, entry_id);
        }
    }

    fn channel_already_learned(&mut self, channel_id: u64) {
        let channel_info = self.channels.get_mut(&INDEX_CHANNEL_ID).unwrap();
        if channel_info.is_leader() {
            debug!(
                "node {} already step channel {} leader, ignore staled learn msg from channel {}",
                self.id, INDEX_CHANNEL_ID, channel_id
            );
            return;
        }

        channel_info.learned_voters.insert(channel_id);

        // We already learn all entries of that entry, we could do recovery progress and
        // step a leader.
        if channel_info.learned_voters.len() == channel_info.missed_voters.len() {
            assert_eq!(channel_info.learned_voters, channel_info.missed_voters);
            self.all_channel_already_learned();
        }
    }

    fn maybe_rebuild_pending_configs(&mut self) {
        let pending_configs = rebuild_pending_configs(self.id, &self.log_buffer, &self.channel_ids);
        if pending_configs.is_none()
            || is_same_pending_configs(&self.pending_configs, &pending_configs)
        {
            return;
        }

        if self.pending_configs.is_some() {
            self.abort_pending_configs("find refreshed pending configs");
        }
        self.pending_configs = pending_configs;
        self.update_channels_config_stage();
    }

    fn all_channel_already_learned(&mut self) {
        let last_index_id = self.log_buffer.last_index_id();
        let channel_info = self.channels.get_mut(&INDEX_CHANNEL_ID).unwrap();
        channel_info.to_leader(last_index_id, "learned");

        // Compute channels re-assign indexes ordering:
        // 1. avoiding assign a entry's index twice, recover missed voters first
        // 2. assign new index for entries recieved in the intervals of electing
        let mut recovery_channels: Vec<u64> = channel_info.missed_voters.iter().cloned().collect();
        recovery_channels.extend(
            self.channel_ids
                .iter()
                .filter(|id| !channel_info.missed_voters.contains(*id) && **id != INDEX_CHANNEL_ID)
                .cloned(),
        );
        info!(
            "node {} channel {} learned voters {:?}, missed voters {:?}",
            self.id, INDEX_CHANNEL_ID, channel_info.learned_voters, channel_info.missed_voters
        );
        for channel_id in recovery_channels {
            // There might exists some entires receiving in student state, using the last
            // entry id in local buffer as the last entry to assign index.
            let mut last_entry_id = self.log_buffer.channel_last_entry_id(channel_id);
            let last_assigned_entry_id = self.log_buffer.channel_last_assigned_entry_id(channel_id);
            let last_learned_entry_id = self
                .channels
                .get_mut(&channel_id)
                .unwrap()
                .last_learned_entry_meta()
                .id;
            if last_entry_id < last_learned_entry_id {
                // It means that current node must lost some entries.
                last_entry_id = last_learned_entry_id;
            }
            if last_entry_id < last_assigned_entry_id {
                warn!("node {} channel {} recovery channel {} indexes: the entries ({}, {}] is missing",
                      self.id, INDEX_CHANNEL_ID, channel_id, last_assigned_entry_id, last_entry_id);
                continue;
            }
            info!(
                "node {} channel {} re-assign index to channel {} recoveried entries ({}, {}]",
                self.id, INDEX_CHANNEL_ID, channel_id, last_assigned_entry_id, last_entry_id
            );
            for id in (last_assigned_entry_id + 1u64)..(last_entry_id + 1u64) {
                self.assign_index(channel_id, id);
            }
        }

        self.assign_index(self.id, INVALID_ID);
        self.maybe_rebuild_pending_configs();
        self.submit_read_task();
    }

    fn receive_prepare_promise(
        &mut self,
        from: u64,
        channel_id: u64,
        learn: bool,
        entry_metas: Vec<EntryMeta>,
    ) {
        assert_ne!(channel_id, INDEX_CHANNEL_ID);
        let config_stage = unwrap_config_stage(&self.pending_configs);
        let channel_info = self.channels.get_mut(&channel_id).unwrap();
        if channel_info.try_receive_prepare_entries(entry_metas) {
            info!("node {} channel {} learned entries from {}", self.id, channel_id, from);
        }

        channel_info.receive_promise(from);
        if channel_info.receive_majority_promise(config_stage) {
            // Save learned entries
            info!(
                "node {} channel {} has receive majority promised and learned {} entries",
                self.id,
                channel_id,
                channel_info.max_received_entries.len()
            );
            if self.id != channel_id && learn {
                self.channel_already_learned(channel_id);
            } else {
                // already receive majority response, we can step to this command channel's
                // leader.
                let channel_last_entry_id = self.log_buffer.channel_last_entry_id(channel_id);
                channel_info.to_leader(channel_last_entry_id, "granted");
                if channel_id == self.id {
                    // We only append no-op entry for ourself channel.
                    self.append_empty_entry(channel_id);
                }
            }
        }
    }

    fn promise_itself(&mut self, channel_id: u64, committed_entry_id: u64, learn: bool) {
        assert_ne!(channel_id, INDEX_CHANNEL_ID);
        let last_entry_id = self.log_buffer.channel_last_entry_id(channel_id);
        trace!(
            "node {} channel {} promise itself, committed entry id {}, last entry id {}",
            self.id,
            channel_id,
            committed_entry_id,
            last_entry_id
        );
        assert!(
            committed_entry_id <= last_entry_id,
            "committed entry id {} should less or equals to last entry id {}",
            committed_entry_id,
            last_entry_id,
        );
        let entry_metas = if learn {
            self.log_buffer
                .range_of_metas(channel_id, committed_entry_id + 1u64, last_entry_id)
        } else {
            Vec::new()
        };
        self.receive_prepare_promise(self.id, channel_id, learn, entry_metas);
    }

    fn bcast_prepare_request(&mut self, channel_id: u64, learn: bool) {
        let matched_committed_id = self
            .channels
            .get(&channel_id)
            .unwrap()
            .matched_committed_id(self.id);
        debug!(
            "node {} channel {} bcast prepare request to {:?}, committed id {}",
            self.id,
            channel_id,
            self.remote_ids(),
            matched_committed_id,
        );
        let mut header = self.build_msg_header(channel_id);
        header.detail = MsgDetail::Prepare(PrepareMsg {
            learn,
            committed_entry_id: matched_committed_id,
        });
        for to in self.remote_ids() {
            let mut msg = header.clone();
            msg.to = to;
            self.ready.send_msg(msg);
        }
        self.promise_itself(channel_id, matched_committed_id, learn);
    }

    fn send_timeout_now(&mut self, channel_id: u64, to: u64) {
        info!("node {} channel {} send timeout now to {}", self.id, channel_id, to);
        let mut msg_header = self.build_msg_header(channel_id);
        msg_header.to = to;
        msg_header.detail = MsgDetail::TimeoutNow;
        self.ready.send_msg(msg_header);
    }

    fn transfer_leader(&mut self, channel_id: u64, to: u64) {
        let last_id = self.log_buffer.channel_last_entry_id(channel_id);
        let channel_info = self.channels.get(&channel_id).unwrap();
        if channel_info.is_remote_matched(to, last_id) {
            self.send_timeout_now(channel_id, to);
        }
    }

    fn try_transfer_channel_leader(&mut self, channel_id: u64, to: u64) {
        if self.transfer.initiate(channel_id, to) {
            self.transfer_leader(channel_id, to);
        }
    }

    fn advance_transfer_progress(&mut self, by: u64) {
        if self.transfer.doing()
            && self.transfer.channel_id == INDEX_CHANNEL_ID
            && self.transfer.to == by
        {
            self.transfer_leader(self.transfer.channel_id, self.transfer.to);
        }
    }

    fn all_channels_tick(&mut self) {
        let random_elect_timeout_tick = self.random_election_timeout_tick;
        for idx in 0..self.channel_ids.len() {
            let channel_id = self.channel_ids[idx];
            if !self.receive_channel_tick(channel_id, random_elect_timeout_tick) {
                continue;
            }

            match self.channels.get(&channel_id).unwrap().role {
                Role::Leader => {
                    self.bcast_heartbeats(channel_id);
                    if channel_id != self.id
                        && channel_id != INDEX_CHANNEL_ID
                        && self.pending_configs.is_none()
                    {
                        self.try_transfer_channel_leader(channel_id, channel_id);
                    }
                }
                Role::Candidate if channel_id == INDEX_CHANNEL_ID => {
                    assert_eq!(self.channel_ids.contains(&self.id), true);
                    self.bcast_vote_request();
                }
                Role::Candidate => {
                    // for normal channels
                    assert_eq!(self.channel_ids.contains(&self.id), true);
                    self.bcast_prepare_request(channel_id, false);
                }
                _ => {
                    panic!("unexpected role")
                }
            }
        }
    }

    fn reset_status_by_snapshot(
        &mut self,
        hard_states: &HashMap<u64, HardState>,
        desc: &SnapshotDesc,
    ) {
        self.log_buffer = MemStorage::recovery(self.id, &desc.channel_metas);
        self.snapshot_receving_states = RemoteSnapshotRecevingStates::new(self.id);

        self.channel_ids = active_members(&desc.members);
        self.channel_ids.push(INDEX_CHANNEL_ID);
        self.channel_ids.sort();

        let mut channels = HashMap::new();
        for channel_id in &self.channel_ids {
            let channel_last_id = self.log_buffer.channel_last_entry_id(*channel_id);
            let desc = ChannelDesc {
                channel_id: *channel_id,
                committed_id: channel_last_id,
                last_id: channel_last_id,
                hard_state: hard_states
                    .get(channel_id)
                    .unwrap_or(&HardState::default())
                    .clone(),
                members: desc.members.clone(),
            };
            channels.insert(*channel_id, ChannelInfo::new(self.id, &desc));
        }

        self.channels = channels;
    }

    fn bcast_snapshot_finished(&mut self, received: bool, desc: &SnapshotDesc) {
        for channel_id in self.remote_ids() {
            let hints = desc
                .channel_metas
                .iter()
                .map(|(k, v)| (*k, v.id + 1u64))
                .collect::<HashMap<_, _>>();
            let mut msg = self.build_msg_header(channel_id);
            msg.to = channel_id;
            msg.detail = MsgDetail::SnapshotReply(SnapshotReplyMsg { received, hints });
            self.ready.send_msg(msg);
        }
    }

    #[cfg(test)]
    fn enter_replicate_state(&mut self) {
        self.channels
            .iter_mut()
            .map(|(_, c)| c.progress_map.iter_mut())
            .flatten()
            .for_each(|(_, p)| {
                p.state = ProgressState::Replicate;
                p.active = true;
            });
    }

    #[cfg(test)]
    fn reset_member_next_id(&mut self, nodes: Vec<u64>) {
        self.channels
            .iter_mut()
            .map(|(_, c)| c.progress_map.iter_mut())
            .flatten()
            .for_each(|(id, p)| {
                if nodes.contains(id) {
                    p.next_id = p.match_id + 1u64;
                }
            });
    }

    #[cfg(test)]
    fn remote_matched_id(&self, channel_id: u64, remote_id: u64) -> u64 {
        self.channels
            .get(&channel_id)
            .unwrap()
            .progress_map
            .get(&remote_id)
            .unwrap()
            .match_id
    }

    fn check_index_leader(&self) -> Result<(), Error> {
        if !self.is_index_leader() {
            Err(Error::NotLeader)
        } else {
            Ok(())
        }
    }

    fn check_local_leader(&self) -> Result<(), Error> {
        if !self.is_local_channel_leader() {
            Err(Error::NotLeader)
        } else {
            Ok(())
        }
    }

    fn check_transfer_leader(&self) -> Result<(), Error> {
        if self.transfer.doing() && self.transfer.channel_id == INDEX_CHANNEL_ID {
            Err(Error::Transfering)
        } else {
            Ok(())
        }
    }

    fn compaign_removing_channel_leaders(&mut self) -> bool {
        if let Some(p) = &self.pending_configs {
            let pending_channel_ids = p
                .old_configs
                .iter()
                .filter(|id| !self.is_channel_leader(**id))
                .cloned()
                .collect::<Vec<u64>>();
            debug!(
                "node {} channel {} pending configs old members {:?}, wait compaign channels {:?}",
                self.id, INDEX_CHANNEL_ID, p.old_configs, pending_channel_ids
            );
            if pending_channel_ids.is_empty() {
                return true;
            }
            for channel_id in pending_channel_ids {
                self.handle_timeout_now(self.id, channel_id);
            }
        }
        false
    }

    fn submit_config_change_entry(&mut self, configs: HashSet<u64>, stage: ConfigStage) -> u64 {
        assert_eq!(self.is_index_leader(), true);

        let entry_id = self.next_entry_id();
        let index_id = self.log_buffer.next_index_id();
        let local_term = self.local_entry_channel_term();
        let entry = Entry {
            request_id: CONFIG_CHANGE_ID,
            channel_id: self.id,
            entry_id: entry_id,
            index_id: INVALID_ID,
            channel_term: local_term,
            message: vec![],
            context: None,
            configs: Some(ChangeConfig {
                index_id,
                entry_id,
                term: local_term,
                stage,
                members: configs,
            }),
        };

        self.append_entry(self.id, entry);
        self.assign_index(self.id, entry_id)
    }

    fn update_channels_config_stage(&mut self) {
        assert_eq!(self.pending_configs.is_some(), true);
        let pending_configs = self.pending_configs.as_mut().unwrap();
        match &pending_configs.stage {
            ConfigStage::Old | ConfigStage::New => {}
            ConfigStage::Both => {
                for channel_id in &self.channel_ids {
                    self.channels
                        .get_mut(channel_id)
                        .unwrap()
                        .enter_both_config_stage(
                            &pending_configs.new_configs,
                            &pending_configs.old_configs,
                        );
                }
                self.ready.should_stable_metas = true;
            }
        }
    }

    fn setup_new_config_channels<L>(&mut self, log_meta_view: &L)
    where
        L: LogMetaView,
    {
        assert_eq!(self.pending_configs.is_some(), true);
        let pending_configs = self.pending_configs.as_ref().unwrap();
        let members: Vec<u64> = pending_configs.configs.iter().cloned().collect();
        let mut new_channel_ids = members.clone();
        new_channel_ids.push(INDEX_CHANNEL_ID);
        new_channel_ids.sort();
        info!(
            "node {} channel {} update channels from {:?} to {:?}",
            self.id, INDEX_CHANNEL_ID, self.channel_ids, new_channel_ids
        );
        self.channel_ids = new_channel_ids;

        for id in &pending_configs.old_configs {
            assert_ne!(*id, INDEX_CHANNEL_ID);
            self.channels.remove(&id);
        }
        let hard_states = log_meta_view.hard_states();
        for channel_id in &pending_configs.new_configs {
            if self.channels.contains_key(channel_id) {
                info!(
                    "node {} channel {} already exists, ignore add command",
                    self.id, *channel_id
                );
                continue;
            }
            let desc = ChannelDesc {
                channel_id: *channel_id,
                committed_id: 0, // we calculate committed id
                last_id: self.log_buffer.channel_last_entry_id(*channel_id),
                hard_state: hard_states
                    .get(channel_id)
                    .unwrap_or(&HardState::default())
                    .clone(),
                members: members
                    .iter()
                    .map(|id| (*id, MemberState::default()))
                    .collect(),
            };
            self.channels
                .insert(*channel_id, ChannelInfo::new(self.id, &desc));
        }
        for channel_id in &self.channel_ids {
            self.channels
                .get_mut(channel_id)
                .unwrap()
                .enter_new_config_stage(&pending_configs.new_configs, &pending_configs.old_configs);
        }
        self.ready.should_stable_metas = true;
    }

    fn is_pending_configs_in_new_stage(&self) -> bool {
        self.pending_configs
            .as_ref()
            .map(|c| c.stage)
            .unwrap_or(ConfigStage::Old)
            == ConfigStage::New
    }

    fn maybe_apply_config_change<L>(&mut self, log_meta_view: &L)
    where
        L: LogMetaView,
    {
        assert_eq!(self.pending_configs.is_some(), true);

        let pending_configs = self.pending_configs.as_ref().unwrap();
        assert_eq!(pending_configs.stage, ConfigStage::New);
        if pending_configs.index_id <= self.log_buffer.chosen_index_id {
            info!(
                "node {} channel {} config change to {:?} success",
                self.id, INDEX_CHANNEL_ID, pending_configs.configs
            );
            if self.is_index_leader() {
                // bcast the last msgs to all old stage members to help them apply configs as
                // much as possible.
                self.bcast_each_channels(log_meta_view);
            }
            self.setup_new_config_channels(log_meta_view);
            self.pending_configs = None;
            self.ready.should_stable_metas = true;
        }
    }

    fn maybe_advance_config_change_stage(&mut self) {
        assert_eq!(self.is_index_leader(), true);
        assert_eq!(self.pending_configs.is_some(), true);

        let pending_configs = self.pending_configs.as_mut().unwrap();
        match &pending_configs.stage {
            ConfigStage::Old => {
                info!(
                    "node {} channel {} config entry at {:?} has already compagin all channels leader, step to {:?}",
                    self.id, INDEX_CHANNEL_ID, ConfigStage::Old, ConfigStage::Both
                );
                pending_configs.stage = ConfigStage::Both;
                pending_configs.index_id = self.log_buffer.next_index_id();
                pending_configs.entry_id = self.log_buffer.channel_next_entry_id(self.id);
                let configs = pending_configs.configs.clone();
                self.submit_config_change_entry(configs, ConfigStage::Both);
                self.update_channels_config_stage();
            }
            ConfigStage::Both if pending_configs.index_id <= self.log_buffer.chosen_index_id => {
                info!(
                    "node {} channel {} config entry at {:?} has already choosen, step to {:?}",
                    self.id,
                    INDEX_CHANNEL_ID,
                    ConfigStage::Both,
                    ConfigStage::New,
                );
                self.ready.should_stable_metas = true;
                pending_configs.stage = ConfigStage::New;
                pending_configs.index_id = self.log_buffer.next_index_id();
                pending_configs.entry_id = self.log_buffer.channel_next_entry_id(self.id);
                let configs = pending_configs.configs.clone();
                self.submit_config_change_entry(configs, ConfigStage::New);
                self.update_channels_config_stage();
            }
            _ => {}
        }
    }

    fn check_pending_configs(&self) -> Result<(), Error> {
        if let None = &self.pending_configs {
            Ok(())
        } else {
            Err(Error::Busy)
        }
    }

    fn release_memory(&mut self) {
        let mut replicated_index_ids = self
            .channels
            .get(&INDEX_CHANNEL_ID)
            .unwrap()
            .progress_map
            .iter()
            .map(|(_, p)| p.match_id)
            .collect::<Vec<_>>();

        let total_numbers = replicated_index_ids.len();
        if total_numbers == 0 {
            return;
        }

        replicated_index_ids.sort();
        let majority_replicated_index_id =
            replicated_index_ids[total_numbers - crate::progress::majority(total_numbers)];

        let min_replicated_index_id = replicated_index_ids[0];
        let diff = majority_replicated_index_id - min_replicated_index_id;
        self.log_buffer.release_entries_until(if diff > 10000 {
            majority_replicated_index_id - (0.618 * diff as f64) as u64
        } else {
            min_replicated_index_id
        });
    }

    // FIXME: if no such leader was found, the read request will delay after
    // heartbeat. A follower forwards read requests to index leader.
    fn forward_read_requests(&mut self) -> bool {
        let index_leader_id = self.get_index_channel().leader_id;
        if index_leader_id == INVALID_NODE_ID || index_leader_id == self.id {
            warn!("node {} try forward read msg to leader, but no such leader are found", self.id);
            return false;
        }

        debug_assert_ne!(index_leader_id, INDEX_CHANNEL_ID);

        let mut msg = self.build_msg_header(INDEX_CHANNEL_ID);
        msg.to = index_leader_id;

        let last_request_id = self.next_read_request_id - 1u64;
        msg.detail = MsgDetail::Read(ReadMsg {
            request_id: last_request_id,
        });
        self.ready.send_msg(msg);

        debug!(
            "node {} forward read msg with request id {} to leader {}",
            self.id, last_request_id, index_leader_id
        );

        true
    }

    fn advance_read_requests(&mut self) {
        if self.undoing_reads.is_empty() {
            return;
        }

        let is_leader = self.is_index_leader();
        if !is_leader && !self.forward_read_requests() {
            return;
        }

        let mut ureads: Vec<u64> = vec![];
        std::mem::swap(&mut self.undoing_reads, &mut ureads);
        self.pending_reads.extend(ureads.into_iter());

        if is_leader {
            self.apply_read_reply(
                self.next_read_request_id,
                self.get_index_channel().current_term_safe_commit_id(),
            );
        }
    }

    fn submit_read_task(&mut self) -> u64 {
        let request_id = self.next_read_request_id;
        self.next_read_request_id += 1;
        self.undoing_reads.push(request_id);

        request_id
    }

    fn transfer_tick(&mut self) {
        self.transfer.elapsed();
        if self.transfer.timeout() {
            info!(
                "node {} channel {} transfer leadership to {} timeout, abort transfer task",
                self.id, self.transfer.channel_id, self.transfer.to
            );
            self.transfer.abort();
        }
    }
}

fn rebuild_pending_configs(
    id: u64,
    log_buffer: &MemStorage,
    channel_ids: &Vec<u64>,
) -> Option<PendingConfigs> {
    let entry = match log_buffer.last_unchosen_config_change_entry() {
        Some(e) => e,
        None => return None,
    };

    let config_change = entry
        .configs
        .as_ref()
        .expect("this entry MUST be a config change");
    let old_members = filter_index_channel(channel_ids);
    debug!(
        "node {} channel {} found pending config change {:?} {:?}, old members: {:?}",
        id, INDEX_CHANNEL_ID, entry, config_change, old_members
    );
    Some(PendingConfigs::new(
        entry.channel_id,
        config_change.index_id,
        config_change.entry_id,
        config_change.term,
        config_change.stage,
        config_change.members.clone(),
        &old_members,
    ))
}

fn rebuild_channels(
    id: u64,
    members: &HashMap<u64, MemberState>,
    log_buffer: &MemStorage,
    hard_states: &HashMap<u64, HardState>,
) -> (Vec<u64>, HashMap<u64, ChannelInfo>) {
    // sort channel ids to make sure INDEX CHANNEL always the first timeout
    // channel.
    let mut channel_ids = active_members(members);
    channel_ids.push(INDEX_CHANNEL_ID);
    channel_ids.sort();

    let mut channels = HashMap::new();
    for channel_id in &channel_ids {
        // When we recovery channels, using the applied entry id as the committed id is
        // safety.
        let committed_id = log_buffer.channel_first_entry_id(*channel_id);
        let desc = ChannelDesc {
            channel_id: *channel_id,
            committed_id: committed_id,
            last_id: log_buffer.channel_last_entry_id(*channel_id),
            hard_state: hard_states
                .get(channel_id)
                .unwrap_or(&HardState::default())
                .clone(),
            members: members.clone(),
        };
        channels.insert(*channel_id, ChannelInfo::new(id, &desc));
        assert_eq!(channels.get(channel_id).unwrap().get_local_match_id(), desc.last_id);
    }
    (channel_ids, channels)
}

impl Sdcons {
    pub fn new(
        id: u64,
        applied_id: u64,
        option: SdconsOption,
        log_buffer: MemStorage,
        membership: &HashMap<u64, MemberState>,
        hard_states: &HashMap<u64, HardState>,
    ) -> Sdcons {
        let (channel_ids, channels) = rebuild_channels(id, membership, &log_buffer, hard_states);
        let pending_configs = rebuild_pending_configs(id, &log_buffer, &channel_ids);
        let random_tick = generate_election_timeout_tick(option.base_election_timeout_tick);
        let mut s = Sdcons {
            id,
            random_election_timeout_tick: random_tick,
            option,
            log_buffer,
            channel_ids,
            channels,
            pending_configs,

            next_read_request_id: 1,
            pending_reads: VecDeque::new(),
            undoing_reads: Vec::new(),

            choosen_id: applied_id,
            pending_id: applied_id,
            applied_id: applied_id,

            transfer: TransferingRecord::default(),
            snapshot_state: SnapshotState::None,
            snapshot_receving_states: RemoteSnapshotRecevingStates::new(id),

            ready: Ready::default(),
        };

        if s.pending_configs.is_some() {
            s.update_channels_config_stage();
        }

        s
    }

    pub fn change_config(&mut self, members: Vec<u64>) -> Result<u64, Error> {
        self.check_index_leader()?;
        self.check_local_leader()?;
        self.check_transfer_leader()?;
        self.check_pending_configs()?;

        let channel_ids = filter_index_channel(&self.channel_ids);

        // Enter first stage, wait all leader's in
        let pending_configs = PendingConfigs::new(
            self.id,
            INVALID_ID,
            INVALID_ID,
            INITIAL_TERM,
            ConfigStage::Old,
            members.into_iter().collect(),
            &channel_ids,
        );

        debug!(
            "node {} receive change config: {:?} new configs: {:?}, old configs: {:?}",
            self.id,
            pending_configs.configs,
            pending_configs.new_configs,
            pending_configs.old_configs
        );

        self.pending_configs = Some(pending_configs);
        Ok(0)
    }

    pub fn submit_task(&mut self, request_id: u64, task: Task) -> Result<u64, Error> {
        self.check_local_leader()?;
        self.check_transfer_leader()?;

        let entry_id = self.next_entry_id();
        let entry = Entry {
            request_id,
            channel_id: self.id,
            entry_id: entry_id,
            index_id: INVALID_ID,
            channel_term: self.local_entry_channel_term(),
            message: task.message,
            context: task.context,
            configs: None,
        };

        self.append_entry(self.id, entry);
        if self.is_index_leader() {
            self.assign_index(self.id, entry_id);
        }

        Ok(entry_id)
    }

    // Got a committed index from order leader, user should guarranted request_id is
    // monotonically increasing.
    pub fn leased_read(&mut self) -> Result<u64, Error> {
        self.check_local_leader()?;
        self.check_transfer_leader()?;

        Ok(self.submit_read_task())
    }

    pub fn tick(&mut self) {
        self.transfer_tick();
        self.all_channels_tick();

        // Electing as removing channel leaders to ensure those channels won't recieve
        // any new proposal.
        if self.is_index_leader() {
            if self.compaign_removing_channel_leaders() {
                self.maybe_advance_config_change_stage();
            }
        } else if !self.pending_reads.is_empty() {
            // Otherwise, if there exists pending read reqeusts, forwards those request to
            // index leader.
            self.forward_read_requests();
        }
    }

    pub fn step(&mut self, msg: Message) {
        if self.reject_staled_message(&msg) {
            return;
        }
        self.try_advance_channel_term(&msg);
        self.dispatch_message(msg);
    }

    pub fn control(&mut self, c: Control) -> Result<(), Error> {
        match c {
            Control::Checkpoint => {
                if self.snapshot_state != SnapshotState::None {
                    warn!(
                        "node {} snapshot already in {:?} state, ignore checkpoint request",
                        self.id, self.snapshot_state
                    );
                } else {
                    self.snapshot_state = SnapshotState::Creating;
                }
            }
            Control::ReleaseMemory => {
                self.release_memory();
            }
            Control::TimeoutNow => {
                info!(
                    "node {} channel {} receive timeout now, start campaign",
                    self.id, INDEX_CHANNEL_ID
                );
                self.handle_timeout_now(self.id, INDEX_CHANNEL_ID);
            }
            Control::TransferLeader(node_id) => {
                self.check_index_leader()?;
                if node_id != self.id {
                    if !self.transfer.initiate(INDEX_CHANNEL_ID, node_id) {
                        return Err(Error::Transfering);
                    }

                    info!(
                        "node {} channel {} start transfer leadership to {}",
                        self.id, INDEX_CHANNEL_ID, node_id
                    );
                    self.advance_transfer_progress(node_id);
                } else {
                    warn!(
                        "node {} channel {} try transfer leadership to self",
                        self.id, INDEX_CHANNEL_ID
                    );
                }
            }
        }
        Ok(())
    }

    pub fn advance<L>(&mut self, log_meta_view: &L) -> Ready
    where
        L: LogMetaView,
    {
        self.bcast_each_channels(log_meta_view);
        self.advance_choose_progress();
        self.advance_read_requests();

        // Avoid stable or apply entries when appling snapshot.
        if self.snapshot_state != SnapshotState::Loading {
            self.extract_chosen_entries();
            self.extract_unstable_entries();
            if self.is_pending_configs_in_new_stage() {
                self.maybe_apply_config_change(log_meta_view);
            }
        }

        if self.ready.chosen_entries.len() > 0 {
            debug!("apply {:?}", self.ready.chosen_entries);
        }

        // WARNING: collect_state_snapshot should always executes after updating
        // operation.
        self.collect_state_snapshot();
        std::mem::take(&mut self.ready)
    }

    pub fn submit_apply_result(&mut self, _from: u64, to: u64) -> bool {
        self.log_buffer.submit_applied_result(to)
    }

    pub fn submit_stable_result(&mut self, channel_id: u64, _from: u64, to: u64) -> bool {
        if let Some(channel_info) = self.channels.get_mut(&channel_id) {
            channel_info.update_local_match(to);
        }
        if channel_id == INDEX_CHANNEL_ID {
            self.log_buffer.submit_stable_index_result(to)
        } else {
            self.log_buffer.submit_stable_result(channel_id, to)
        }
    }

    pub fn log_replicated(&mut self, node_id: u64, channel_id: u64, next_id: u64) {
        match self.channels.get_mut(&channel_id) {
            Some(c) => c.log_replicated(node_id, next_id),
            None => {}
        };
    }

    pub fn finish_snapshot_loading(
        &mut self,
        received: bool,
        hard_states: &HashMap<u64, HardState>,
        desc: &SnapshotDesc,
    ) {
        if received {
            self.reset_status_by_snapshot(hard_states, desc);
        }
        self.bcast_snapshot_finished(received, desc);
        self.snapshot_state = SnapshotState::None;
    }

    pub fn finish_checkpoint(&mut self) {
        self.snapshot_state = SnapshotState::None;
    }

    pub fn is_sending_snapshot(&self) -> bool {
        self.snapshot_receving_states
            .is_any_remote_receiving_snapshot()
    }

    pub fn leader_id(&self) -> Option<u64> {
        let leader = self.get_index_channel().leader_id;
        if leader == INVALID_NODE_ID {
            None
        } else {
            Some(leader)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::iter::FromIterator;

    use log::{Metadata, Record};

    struct SimpleLogger;
    impl log::Log for SimpleLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            println!(
                "[{} - {} - {}:{}] {}",
                record.level(),
                record.target(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            );
        }

        fn flush(&self) {}
    }

    static LOGGER: SimpleLogger = SimpleLogger;

    struct LogMeta {
        pub first_index_id: u64,
        pub last_index_id: u64,
        pub hard_state_map: HashMap<u64, HardState>,
        pub members: HashMap<u64, MemberState>,
    }

    impl Default for LogMeta {
        fn default() -> Self {
            LogMeta {
                first_index_id: INVALID_ID,
                last_index_id: INVALID_ID,
                hard_state_map: HashMap::new(),
                members: HashMap::new(),
            }
        }
    }

    impl LogMetaView for LogMeta {
        fn membership(&self) -> HashMap<u64, MemberState> {
            self.members.clone()
        }

        fn hard_states(&self) -> HashMap<u64, HardState> {
            self.hard_state_map.clone()
        }

        fn range_of(&self, _channel_id: u64) -> (u64, u64) {
            (self.first_index_id, self.last_index_id)
        }

        fn latest_snapshot(&self) -> Option<SnapshotDesc> {
            None
        }
    }

    static SETUP_LOGGER: std::sync::Once = std::sync::Once::new();
    fn init_sdcons(log_meta: &LogMeta) -> Sdcons {
        let local_id = 1;
        let mut mem_store = MemStorage::new(local_id);
        for (id, _) in &log_meta.members {
            mem_store.append_entries(*id, vec![]);
        }
        init_sdcons_with_mem_store(log_meta, mem_store)
    }

    fn init_sdcons_with_mem_store(log_meta: &LogMeta, mem_store: MemStorage) -> Sdcons {
        SETUP_LOGGER.call_once(|| {
            log::set_logger(&LOGGER)
                .map(|()| log::set_max_level(log::LevelFilter::Trace))
                .expect("init logger");
        });

        let local_id = 1;
        let opt = SdconsOption::default();
        let hard_states = log_meta.hard_states();
        let membership = log_meta.membership();
        Sdcons::new(local_id, INVALID_ID, opt, mem_store, &membership, &hard_states)
    }

    fn task(value: u8) -> Task {
        Task {
            message: vec![value],
            context: None,
        }
    }

    fn wait_timeout(sd: &mut Sdcons) {
        info!("wait node {} election timeout", sd.id);
        let timeout_ticks = sd.option.base_election_timeout_tick * 2;
        for _ in 0..timeout_ticks {
            sd.tick();
        }
    }

    fn new_entry(member_id: u64, entry_id: u64, term: u64) -> Entry {
        Entry {
            entry_id,
            index_id: INVALID_ID,
            request_id: 2,
            channel_id: member_id,
            channel_term: term,
            message: vec![0, 1, 2, 3],
            context: None,
            configs: None,
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

    fn accept_prepare(ready: &mut Ready, s: &mut Sdcons, entries_map: HashMap<u64, Vec<Entry>>) {
        info!("accept prepare msgs of node {}", s.id);
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        for msg in msgs {
            let mut reply = Message {
                from: msg.to,
                to: msg.from,
                index_term: msg.index_term,
                channel_id: msg.channel_id,
                channel_term: msg.channel_term,
                detail: MsgDetail::None,
            };
            if let MsgDetail::Prepare(p) = &msg.detail {
                let mut entries = entries_map.get(&msg.to).unwrap_or(&vec![]).clone();
                for entry in &mut entries {
                    entry.channel_id = msg.channel_id;
                }
                reply.detail = MsgDetail::PrepareReply(PrepareReplyMsg {
                    reject: false,
                    learn: p.learn,
                    entry_metas: entries.iter().map(|e| EntryMeta::from(e)).collect(),
                });
                s.step(reply);
            };
        }
    }

    fn accept_election(ready: &mut Ready, s: &mut Sdcons) {
        info!("accept all election msgs of node {}", s.id);
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        for msg in &msgs {
            let mut reply = Message {
                from: msg.to,
                to: msg.from,
                index_term: msg.index_term,
                channel_id: msg.channel_id,
                channel_term: msg.channel_term,
                detail: MsgDetail::None,
            };
            match &msg.detail {
                MsgDetail::Prepare(p) => {
                    reply.detail = MsgDetail::PrepareReply(PrepareReplyMsg {
                        reject: false,
                        learn: p.learn,
                        entry_metas: Vec::new(),
                    });
                    s.step(reply);
                }
                MsgDetail::Vote(_) => {
                    reply.detail = MsgDetail::VoteReply(VoteReplyMsg { reject: false });
                    s.step(reply);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn single_member_election() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        let mut s = init_sdcons(&meta);

        wait_timeout(&mut s);
        assert_eq!(s.is_index_leader(), true);
        assert_eq!(s.is_local_channel_leader(), true);
    }

    #[test]
    fn multi_member_election() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        meta.members.insert(2, MemberState::default());
        meta.members.insert(3, MemberState::default());
        let mut s = init_sdcons(&meta);

        wait_timeout(&mut s);
        assert_eq!(s.is_channel_candidate(s.id), true);
        assert_eq!(s.is_channel_candidate(INDEX_CHANNEL_ID), true);

        let mut ready = s.advance(&meta);
        accept_election(&mut ready, &mut s);

        assert_eq!(s.is_local_channel_leader(), true);
        assert_eq!(s.is_index_student(), true);

        let mut ready = s.advance(&meta);
        accept_election(&mut ready, &mut s);
        assert_eq!(s.is_index_leader(), true);
    }

    #[test]
    fn learn_refreshed_entries() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        meta.members.insert(2, MemberState::default());
        meta.members.insert(3, MemberState::default());
        meta.members.insert(4, MemberState::default());
        meta.members.insert(5, MemberState::default());
        meta.hard_state_map.insert(
            1,
            HardState {
                voted_for: INVALID_NODE_ID,
                current_term: 10,
            },
        );
        let mut s = init_sdcons(&meta);

        wait_timeout(&mut s);
        assert_eq!(s.is_channel_candidate(s.id), true);
        assert_eq!(s.is_channel_candidate(INDEX_CHANNEL_ID), true);

        let mut ready = s.advance(&meta);
        accept_election(&mut ready, &mut s);

        assert_eq!(s.is_local_channel_leader(), true);
        assert_eq!(s.is_index_student(), true);

        let mut ready = s.advance(&meta);
        let mut entries_map = HashMap::new();
        entries_map.insert(
            2,
            vec![
                new_entry(1, 1, 3),
                new_entry(1, 2, 3),
                new_entry(1, 3, 3),
                new_entry(1, 4, 4),
            ],
        );
        entries_map.insert(3, vec![new_entry(1, 1, 9)]);
        entries_map.insert(4, vec![new_entry(1, 1, 9)]);
        entries_map.insert(5, vec![new_entry(1, 1, 9)]);
        accept_prepare(&mut ready, &mut s, entries_map);

        assert_eq!(s.is_index_leader(), true);
        let c = s
            .channels
            .get(&INDEX_CHANNEL_ID)
            .unwrap()
            .learned_voters
            .iter()
            .next()
            .unwrap();
        let channel_info = s.channels.get(c).unwrap();
        let entry_meta = channel_info.last_learned_entry_meta();
        assert_eq!(entry_meta.id, 1);
        assert_eq!(entry_meta.term, 9);
    }

    #[test]
    fn assign_indexes_to_entries_recieved_in_electing() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);

        wait_timeout(&mut s);
        assert_eq!(s.is_channel_candidate(s.id), true);
        assert_eq!(s.is_channel_candidate(INDEX_CHANNEL_ID), true);

        let mut ready = s.advance(&meta);
        accept_election(&mut ready, &mut s);

        assert_eq!(s.is_local_channel_leader(), true);
        assert_eq!(s.is_index_student(), true);

        // receive new entries
        let requests = vec![1, 2, 3];
        submit_tasks(&mut s, &requests);

        let mut ready = s.advance(&meta);
        accept_prepare(&mut ready, &mut s, HashMap::new());

        s.enter_replicate_state();

        let ready = s.advance(&meta);
        accept_append_entries(&ready, &mut s, &HashSet::new());
        accept_indexes(&ready, &mut s, &HashSet::new());
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);

        s.enter_replicate_state();

        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &requests);
    }

    #[test]
    fn handle_vote_request() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        meta.members.insert(2, MemberState::default());
        meta.members.insert(3, MemberState::default());
        meta.hard_state_map.insert(
            INDEX_CHANNEL_ID,
            HardState {
                voted_for: INVALID_NODE_ID,
                current_term: 10,
            },
        );

        let mut mem_store = MemStorage::new(1);
        mem_store.append_entries(1, vec![]);
        mem_store.append_entries(2, vec![]);
        mem_store.append_entries(3, vec![]);
        mem_store
            .extend_indexes(vec![new_index_with_id(1, 1, 1, 2), new_index_with_id(1, 2, 3, 4)]);
        let mut s = init_sdcons_with_mem_store(&meta, mem_store);
        assert_eq!(s.get_index_term(), 10);
        assert_eq!(s.get_index_channel().voted_for, INVALID_NODE_ID);

        // reject staled term
        let mut msg = Message {
            from: 2,
            to: 1,
            index_term: 1,
            channel_id: INDEX_CHANNEL_ID,
            channel_term: 10,
            detail: MsgDetail::Vote(VoteMsg {
                last_index_id: 0,
                last_index_term: 0,
            }),
        };
        s.step(msg.clone());
        assert_eq!(s.get_index_channel().voted_for, INVALID_NODE_ID);

        // isn't refresh
        msg.index_term = 10;
        s.step(msg.clone());
        assert_eq!(s.get_index_channel().voted_for, INVALID_NODE_ID);

        // receive
        msg.detail = MsgDetail::Vote(VoteMsg {
            last_index_id: 10,
            last_index_term: 10,
        });
        s.step(msg.clone());
        assert_eq!(s.get_index_channel().voted_for, 2);

        // already promised.
        msg.from = 3;
        msg.detail = MsgDetail::Vote(VoteMsg {
            last_index_id: 11,
            last_index_term: 11,
        });
        s.step(msg.clone());
        assert_eq!(s.get_index_channel().voted_for, 2);
    }

    #[test]
    fn handle_prepare_req() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        meta.members.insert(2, MemberState::default());
        meta.members.insert(3, MemberState::default());
        meta.hard_state_map.insert(
            1,
            HardState {
                voted_for: INVALID_NODE_ID,
                current_term: 10,
            },
        );

        let mut mem_store = MemStorage::new(1);
        mem_store.append_entries(1, vec![]);
        mem_store.append_entries(2, vec![]);
        mem_store.append_entries(3, vec![]);
        mem_store
            .extend_indexes(vec![new_index_with_id(1, 1, 1, 2), new_index_with_id(1, 2, 3, 4)]);
        let mut s = init_sdcons_with_mem_store(&meta, mem_store);
        assert_eq!(s.channels.get(&1).unwrap().term, 10);
        assert_eq!(s.channels.get(&1).unwrap().voted_for, INVALID_NODE_ID);

        // receive
        let mut msg = Message {
            from: 2,
            to: 1,
            index_term: 1,
            channel_id: 1,
            channel_term: 10,
            detail: MsgDetail::Prepare(PrepareMsg {
                learn: false,
                committed_entry_id: 0,
            }),
        };
        s.step(msg.clone());
        assert_eq!(s.channels.get(&1).unwrap().voted_for, 2);

        // receive too.
        s.step(msg.clone());
        assert_eq!(s.channels.get(&1).unwrap().voted_for, 2);

        // already promised.
        msg.from = 3;
        s.step(msg.clone());
        assert_eq!(s.channels.get(&1).unwrap().voted_for, 2);
    }

    #[test]
    fn to_follower_when_receive_high_term() {
        let channels = vec![INDEX_CHANNEL_ID, 1];
        let roles = vec![Role::Follower, Role::Leader, Role::Student, Role::Candidate];
        for channel_id in channels {
            for role in &roles {
                debug!("execute role {} channel id {}", role, channel_id);

                let mut meta = LogMeta::default();
                meta.members.insert(1, MemberState::default());
                meta.members.insert(2, MemberState::default());
                meta.members.insert(3, MemberState::default());
                let mut s = init_sdcons(&meta);
                match role {
                    Role::Follower => {}
                    Role::Student | Role::Leader => {
                        wait_timeout(&mut s);
                        let mut ready = s.advance(&meta);
                        accept_election(&mut ready, &mut s);
                    }
                    Role::Candidate => {
                        wait_timeout(&mut s);
                    }
                }

                let index_term = if channel_id == INDEX_CHANNEL_ID {
                    10
                } else {
                    1
                };

                let msg = Message {
                    from: 2,
                    to: 1,
                    index_term,
                    channel_id: channel_id,
                    channel_term: 10,
                    detail: MsgDetail::None,
                };
                s.step(msg.clone());

                assert_eq!(s.is_channel_follower(channel_id), true);
                assert_eq!(s.get_channel_term(channel_id), 10);
            }
        }
    }

    #[test]
    fn to_follower_when_someone_declare_leader() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        meta.members.insert(2, MemberState::default());
        meta.members.insert(3, MemberState::default());
        let mut s = init_sdcons(&meta);
        wait_timeout(&mut s);
        let channel_term = s.get_channel_term(INDEX_CHANNEL_ID);
        let msg = Message {
            from: 2,
            to: 1,
            index_term: channel_term,
            channel_id: INDEX_CHANNEL_ID,
            channel_term: channel_term,
            detail: MsgDetail::Declare(DeclareMsg { committed_id: 0 }),
        };
        s.step(msg.clone());

        assert_eq!(s.is_channel_follower(INDEX_CHANNEL_ID), true);
        assert_eq!(s.get_channel_term(INDEX_CHANNEL_ID), channel_term);
    }

    fn init_log_meta_with_members(members: &Vec<u64>) -> LogMeta {
        let mut meta = LogMeta::default();
        for id in members {
            meta.members.insert(*id, MemberState::default());
        }
        meta
    }

    fn init_log_meta_with_default_members() -> LogMeta {
        init_log_meta_with_members(&vec![1, 2, 3])
    }

    #[test]
    fn bcast_indexes() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);
        wait_timeout(&mut s);
        let mut ready = s.advance(&meta);
        accept_election(&mut ready, &mut s);
        let mut ready = s.advance(&meta);
        accept_prepare(&mut ready, &mut s, HashMap::new());
        assert_eq!(s.is_index_leader(), true);
        let ready = s.advance(&meta);
        assert_eq!(ready.unstable_indexes.len() > 0, true);
    }

    fn submit_tasks(s: &mut Sdcons, requests: &Vec<u64>) {
        for req_id in requests {
            s.submit_task(
                *req_id + FIRST_USER_REQUEST,
                Task {
                    message: vec![0, 1],
                    context: None,
                },
            )
            .expect("submit task");
        }
    }

    fn stable_all_entries(ready: &Ready, s: &mut Sdcons) {
        let mut map = HashMap::new();
        for entry in &ready.unstable_entries {
            let v = map.entry(entry.channel_id).or_default();
            *v = entry.entry_id;
        }
        for (channel_id, v) in map {
            s.submit_stable_result(channel_id, 0, v);
        }
    }

    fn stable_indexes(ready: &Ready, s: &mut Sdcons) {
        if ready.unstable_indexes.is_empty() {
            return;
        }
        let first_id = ready
            .unstable_indexes
            .first()
            .map(|i| i.index_id)
            .unwrap_or(INVALID_ID);
        let last_id = ready
            .unstable_indexes
            .last()
            .map(|i| i.index_id)
            .unwrap_or(INVALID_ID);
        s.submit_stable_result(INDEX_CHANNEL_ID, first_id, last_id);
    }

    fn apply_entries(ready: &Ready, s: &mut Sdcons) {
        s.submit_apply_result(ready.first_apply_index_id, ready.last_apply_index_id);
    }

    fn assert_chosen_entries(ready: &Ready, requests: &Vec<u64>) {
        let chosen_requests = ready
            .chosen_entries
            .iter()
            .map(|e| e.request_id)
            .collect::<Vec<_>>();
        debug!("chosen entries {:?}", chosen_requests);
        for request_id in requests {
            let count = chosen_requests
                .iter()
                .filter(|id| **id == (request_id + FIRST_USER_REQUEST))
                .count();
            assert_eq!(count, 1);
        }
    }

    fn assert_chosen_config_change_entries(ready: &Ready) {
        let chosen_requests = ready
            .chosen_entries
            .iter()
            .map(|e| e.request_id)
            .collect::<Vec<_>>();
        debug!("chosen entries {:?}", chosen_requests);

        let count = chosen_requests
            .iter()
            .filter(|id| **id == CONFIG_CHANGE_ID)
            .count();
        assert_eq!(count, 1);
    }

    fn assert_replicate_entries(ready: &Ready, target_id: u64, requests: &Vec<u64>) {
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        let replicate_requests = msgs
            .iter()
            .filter_map(|m| {
                if m.to != target_id {
                    None
                } else if let MsgDetail::Append(p) = &m.detail {
                    Some(p.entries.clone())
                } else {
                    None
                }
            })
            .flatten()
            .map(|e| e.request_id)
            .collect::<HashSet<_>>();

        debug!("replicate entries {:?}", replicate_requests);
        for request_id in requests {
            let actual_request_id = request_id + FIRST_USER_REQUEST;
            assert_eq!(replicate_requests.contains(&actual_request_id), true);
        }
    }

    #[test]
    fn single_member_choose() {
        let mut meta = LogMeta::default();
        meta.members.insert(1, MemberState::default());
        let mut s = init_sdcons(&meta);
        wait_timeout(&mut s);
        assert_eq!(s.is_index_leader(), true);
        assert_eq!(s.is_channel_leader(1), true);

        let inputs = vec![vec![1, 2, 3], vec![4, 5, 6]];
        for requests in &inputs {
            submit_tasks(&mut s, requests);
            let ready = s.advance(&meta);
            assert_eq!(ready.chosen_entries.is_empty(), true);
            stable_all_entries(&ready, &mut s);
            stable_indexes(&ready, &mut s);

            let ready = s.advance(&meta);
            assert_chosen_entries(&ready, requests);
            apply_entries(&ready, &mut s);
        }
    }

    fn to_leader(s: &mut Sdcons, meta: &LogMeta) {
        if s.channels.len() == 2 {
            wait_timeout(s);
            assert_eq!(s.is_index_leader(), true);
            assert_eq!(s.get_index_channel().leader_id, 1);
            assert_eq!(s.is_channel_leader(1), true);
        } else {
            wait_timeout(s);
            let mut ready = s.advance(meta);
            accept_election(&mut ready, s);
            assert_eq!(s.is_index_student(), true);
            assert_eq!(s.get_index_channel().leader_id, 1);
            assert_eq!(s.is_local_channel_leader(), true);
            let mut ready = s.advance(meta);
            let map = HashMap::new();
            accept_prepare(&mut ready, s, map);
            assert_eq!(s.is_index_leader(), true);
        }
    }

    #[test]
    fn replicate_entries() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);
        to_leader(&mut s, &meta);
        s.enter_replicate_state();

        let entries = vec![1, 2, 3];
        submit_tasks(&mut s, &entries);
        let ready = s.advance(&meta);
        assert_replicate_entries(&ready, 2, &entries);
        assert_replicate_entries(&ready, 3, &entries);

        // replicate next entries.
        let entries = vec![4, 5, 6];
        submit_tasks(&mut s, &entries);
        let ready = s.advance(&meta);
        assert_replicate_entries(&ready, 2, &entries);
        assert_replicate_entries(&ready, 3, &entries);

        // replicate next one entry
        let entries = vec![7];
        submit_tasks(&mut s, &entries);
        let ready = s.advance(&meta);
        assert_replicate_entries(&ready, 2, &entries);
        assert_replicate_entries(&ready, 3, &entries);

        // replicate empty entries.
        let ready = s.advance(&meta);
        assert_eq!(ready.msgs.len(), 0);

        // reject and re-replicate entries.
        let index_term = s.get_index_term();
        let channel_term = s.get_channel_term(1);
        let msg = Message {
            from: 2,
            to: 1,
            index_term: index_term,
            channel_id: 1,
            channel_term: channel_term,
            detail: MsgDetail::AppendReply(AppendReplyMsg {
                reject: true,
                entry_id: 1,
                hint_id: 1,
            }),
        };
        s.step(msg.clone());
        let ready = s.advance(&meta);
        assert_replicate_entries(&ready, 2, &vec![1, 2, 3, 4, 5, 6, 7]);
    }

    fn declare_leader(s: &mut Sdcons, index_term: u64, channel_id: u64, channel_term: u64) {
        let msg = Message {
            from: 2,
            to: 1,
            index_term,
            channel_id,
            channel_term,
            detail: MsgDetail::Declare(DeclareMsg {
                committed_id: INVALID_ID,
            }),
        };
        s.step(msg);
    }

    fn declare_with_id(
        s: &mut Sdcons,
        from: u64,
        index_term: u64,
        channel_id: u64,
        channel_term: u64,
        committed_id: u64,
    ) {
        let msg = Message {
            from,
            to: s.id,
            index_term,
            channel_id,
            channel_term,
            detail: MsgDetail::Declare(DeclareMsg { committed_id }),
        };
        s.step(msg);
    }

    fn assert_append_reply(ready: &Ready, to: u64, entry_id: u64, reject: bool) {
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        let replies = msgs
            .iter()
            .filter_map(|m| {
                if m.to != to {
                    None
                } else if let MsgDetail::AppendReply(p) = &m.detail {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(replies.len(), 1);
        let reply = &replies[0];
        assert_eq!(reply.reject, reject);
        assert_eq!(reply.hint_id, entry_id);
    }

    fn append_specified_entries(
        s: &mut Sdcons,
        channel_id: u64,
        channel_term: u64,
        prev_id: u64,
        prev_term: u64,
        entries: Vec<Entry>,
    ) {
        append_specified_entries_with_id(
            s,
            channel_id,
            channel_term,
            prev_id,
            prev_term,
            0,
            entries,
        );
    }

    fn append_specified_entries_with_id(
        s: &mut Sdcons,
        channel_id: u64,
        channel_term: u64,
        prev_id: u64,
        prev_term: u64,
        committed_entry_id: u64,
        entries: Vec<Entry>,
    ) {
        let msg = Message {
            from: 2,
            to: 1,
            index_term: s.get_index_term(),
            channel_id,
            channel_term,
            detail: MsgDetail::Append(AppendMsg {
                committed_entry_id,
                prev_entry_id: prev_id,
                prev_entry_term: prev_term,
                entries,
            }),
        };
        s.step(msg);
    }

    fn append_entries(
        s: &mut Sdcons,
        channel_id: u64,
        channel_term: u64,
        prev_id: u64,
        prev_term: u64,
        requests: &Vec<u64>,
    ) {
        append_entries_with_id(s, channel_id, channel_term, prev_id, prev_term, 0, requests);
    }

    fn append_entries_with_id(
        s: &mut Sdcons,
        channel_id: u64,
        channel_term: u64,
        prev_id: u64,
        prev_term: u64,
        committed_entry_id: u64,
        requests: &Vec<u64>,
    ) {
        let mut entries = Vec::new();
        let mut id = prev_id + 1;
        for request_id in requests {
            entries.push(Entry {
                request_id: *request_id + FIRST_USER_REQUEST,
                channel_id,
                channel_term,
                entry_id: id,
                index_id: INVALID_ID,
                message: vec![1, 2, 3],
                context: None,
                configs: None,
            });
            id += 1;
        }

        debug!("append entries {:?} with term {}", requests, channel_term);
        append_specified_entries_with_id(
            s,
            channel_id,
            channel_term,
            prev_id,
            prev_term,
            committed_entry_id,
            entries,
        );
    }

    #[test]
    fn receive_entries() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);
        let index_term = 10;
        let channel_id = 2;
        let channel_term = 11;
        declare_leader(&mut s, index_term, channel_id, channel_term);

        // reject large append.
        append_entries(&mut s, channel_id, channel_term, 10, 2, &vec![1, 2, 3]);
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 1, true);

        append_entries(&mut s, channel_id, channel_term, 0, 0, &vec![1, 2, 3]);
        // current entries is [1, 2, 3]
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 4, false);

        // truncate and accept
        append_entries(&mut s, channel_id, channel_term + 1, 2, channel_term, &vec![3, 4, 5]);
        // current entries is [1, 2, 3, 4, 5]
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 6, false);

        // find conflict id
        append_entries(&mut s, channel_id, channel_term + 2, 5, channel_term, &vec![5, 6, 7]);
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 3, true);

        // reject staled request
        append_entries_with_id(
            &mut s,
            channel_id,
            channel_term + 2,
            5,
            channel_term + 1,
            5,
            &vec![6, 7, 8],
        );
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 9, false);

        append_entries(&mut s, channel_id, channel_term + 2, 3, channel_term, &vec![4, 5]);
        let ready = s.advance(&meta);
        assert_append_reply(&ready, channel_id, 6, false);
    }

    fn accept_append_entries(ready: &Ready, s: &mut Sdcons, skip_nodes: &HashSet<u64>) {
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        for msg in msgs {
            if let MsgDetail::Append(p) = &msg.detail {
                match p.entries.last() {
                    Some(e) => {
                        if skip_nodes.contains(&msg.to) {
                            debug!("skip append entries to node {}", msg.to);
                            continue;
                        }
                        let reply = Message {
                            from: msg.to,
                            to: msg.from,
                            index_term: msg.index_term,
                            channel_id: msg.channel_id,
                            channel_term: msg.channel_term,
                            detail: MsgDetail::AppendReply(AppendReplyMsg {
                                entry_id: p.prev_entry_id + 1u64,
                                hint_id: e.entry_id + 1u64,
                                reject: false,
                            }),
                        };
                        s.step(reply);
                    }
                    _ => {}
                }
            }
        }
    }

    fn accept_indexes(ready: &Ready, s: &mut Sdcons, skip_nodes: &HashSet<u64>) {
        let msgs = ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .collect::<Vec<_>>();
        for msg in &msgs {
            if let MsgDetail::Index(p) = &msg.detail {
                match p.indexes.last() {
                    Some(i) => {
                        if skip_nodes.contains(&msg.to) {
                            debug!("skip append entries to node {}", msg.to);
                            continue;
                        }
                        let reply = Message {
                            from: msg.to,
                            to: msg.from,
                            index_term: msg.index_term,
                            channel_id: msg.channel_id,
                            channel_term: msg.channel_term,
                            detail: MsgDetail::IndexReply(IndexReplyMsg {
                                index_id: p.prev_index_id,
                                hint_id: i.index_id + 1u64,
                                reject: false,
                            }),
                        };
                        s.step(reply);
                    }
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn multi_member_chosen() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);
        to_leader(&mut s, &meta);
        s.enter_replicate_state();

        let requests = vec![1];
        submit_tasks(&mut s, &requests);

        let mut skip_nodes = HashSet::new();

        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);
        accept_append_entries(&ready, &mut s, &skip_nodes);
        accept_indexes(&ready, &mut s, &skip_nodes);

        // won't apply if entries not stabled.
        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);

        // only stable entries
        stable_all_entries(&ready, &mut s);
        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);

        stable_indexes(&ready, &mut s);
        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &requests);
        s.submit_apply_result(ready.first_apply_index_id, ready.last_apply_index_id);

        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);

        // only 3 response
        skip_nodes.insert(2);
        let requests = vec![2, 3, 4];
        submit_tasks(&mut s, &requests);

        let ready = s.advance(&meta);
        accept_append_entries(&ready, &mut s, &skip_nodes);
        accept_indexes(&ready, &mut s, &skip_nodes);

        // won't apply if entries not stabled.
        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);

        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);

        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &requests);
        s.submit_apply_result(ready.first_apply_index_id, ready.last_apply_index_id);

        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);
    }

    #[test]
    fn multi_member_receive_chosen() {
        let meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&meta);

        let requests = vec![1, 2, 3];
        let indexes = vec![
            new_index_with_id(2, 1, 1, 2),
            new_index_with_id(2, 2, 2, 2),
            new_index_with_id(2, 3, 3, 2),
        ];
        append_entries(&mut s, 2, 2, INVALID_ID, INITIAL_TERM, &requests);
        replicate_indexes(2, INVALID_ID, INITIAL_TERM, &indexes, &mut s);
        declare_with_id(&mut s, 2, 2, 2, 2, 3); // channel 2 commit 3
        declare_with_id(&mut s, 2, 2, INDEX_CHANNEL_ID, 2, 3); // channel 0 commit 3

        let ready = s.advance(&meta);
        assert_eq!(ready.chosen_entries.len(), 0);
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);

        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &requests);
    }

    fn replicate_indexes(
        from: u64,
        prev_id: u64,
        prev_term: u64,
        indexes: &Vec<LogIndex>,
        s: &mut Sdcons,
    ) {
        let index_term = s.get_index_term();
        let msg = Message {
            from,
            to: s.id,
            index_term,
            channel_id: INDEX_CHANNEL_ID,
            channel_term: index_term,
            detail: MsgDetail::Index(IndexMsg {
                committed_index_id: 0,
                prev_index_id: prev_id,
                prev_index_term: prev_term,
                indexes: indexes.clone(),
            }),
        };
        s.step(msg);
    }

    #[test]
    fn only_assign_index_once() {
        // 1. 5 nodes, 2 is leader
        // 2. 1 replicate entries to 2
        // 3. 2 assign index and replicate indexes to 3
        // 4. now 2 step down, 3 is elected as leader
        // 5. 1 replicate entries to 3
        // 6. 3 should recongnize the assigned entries.
        let meta = init_log_meta_with_members(&vec![1, 2, 3, 4, 5]);
        let mut s = init_sdcons(&meta);

        // 2 replicate indexes to 1
        let indexes = vec![new_index_with_id(2, 1, 1, 1)];
        replicate_indexes(2, INVALID_ID, INITIAL_TERM, &indexes, &mut s);

        // 1 step to leader
        to_leader(&mut s, &meta);

        // 2 replicate entries to 1
        append_entries(&mut s, 2, 2, INVALID_ID, INITIAL_TERM, &vec![1]);

        // 1 apply entries
        let ready = s.advance(&meta);
        let index_term = s.get_index_term();
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);
        accept_append_entries(&ready, &mut s, &HashSet::new());
        accept_indexes(&ready, &mut s, &HashSet::new());
        declare_with_id(&mut s, 2, index_term, 2, 2, 1); // commit entry
        assert_eq!(ready.chosen_entries.len(), 0);

        debug!("validate advance indexes: {:?}", s.log_buffer.indexes);
        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &vec![1]);
    }

    #[test]
    fn enter_both_stage() {
        let meta = init_log_meta_with_members(&vec![1]);
        let mut s = init_sdcons(&meta);
        to_leader(&mut s, &meta);

        assert_eq!(s.pending_configs.is_none(), true);

        s.change_config(vec![1, 2, 3]).expect("success");
        assert_eq!(s.pending_configs.is_some(), true);
        assert_eq!(s.pending_configs.as_ref().unwrap().stage, ConfigStage::Old);

        s.tick();
        s.advance(&meta);
        assert_eq!(s.pending_configs.is_some(), true);
        assert_eq!(s.pending_configs.as_ref().unwrap().stage, ConfigStage::Both);
    }

    #[test]
    fn committed_should_reach_majority_in_both_stage() {
        let meta = init_log_meta_with_members(&vec![1, 2, 3]);
        let mut s = init_sdcons(&meta);
        to_leader(&mut s, &meta);

        assert_eq!(s.pending_configs.is_none(), true);

        s.change_config(vec![1, 4, 5]).expect("success");
        assert_eq!(s.pending_configs.is_some(), true);
        assert_eq!(s.pending_configs.as_ref().unwrap().stage, ConfigStage::Old);

        s.tick();
        assert_eq!(s.is_channel_candidate(2), true);
        assert_eq!(s.is_channel_candidate(3), true);
        assert_eq!(s.pending_configs.is_some(), true);
        assert_eq!(s.pending_configs.as_ref().unwrap().stage, ConfigStage::Old);

        let mut ready = s.advance(&meta);
        accept_prepare(&mut ready, &mut s, HashMap::new());
        accept_append_entries(&ready, &mut s, &HashSet::new());
        accept_indexes(&ready, &mut s, &HashSet::new());
        assert_eq!(s.is_channel_leader(2), true);
        assert_eq!(s.is_channel_leader(3), true);

        s.tick();
        assert_eq!(s.pending_configs.is_some(), true);
        assert_eq!(s.pending_configs.as_ref().unwrap().stage, ConfigStage::Both);

        s.enter_replicate_state();

        // try commit entries
        let mut skip_nodes = HashSet::new();
        skip_nodes.insert(4);
        skip_nodes.insert(5);
        let ready = s.advance(&meta);
        let apply_id = ready.last_apply_index_id;
        apply_entries(&ready, &mut s);
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);
        accept_append_entries(&ready, &mut s, &skip_nodes);
        accept_indexes(&ready, &mut s, &skip_nodes);
        let ready = s.advance(&meta);
        assert_eq!(ready.first_apply_index_id, apply_id + 1);
        assert_eq!(ready.chosen_entries.len(), 0);

        s.enter_replicate_state();

        debug!("both stage recieve marjority response");
        s.reset_member_next_id(vec![4, 5]);

        let ready = s.advance(&meta);
        accept_append_entries(&ready, &mut s, &HashSet::new());
        accept_indexes(&ready, &mut s, &HashSet::new());
        let ready = s.advance(&meta);
        assert_chosen_config_change_entries(&ready);
    }

    fn to_both_stage(s: &mut Sdcons, log_meta: &LogMeta, new_configs: Vec<u64>) {
        to_leader(s, log_meta);
        s.change_config(new_configs).expect("success");
        s.tick();

        // elect as channel leader
        let mut ready = s.advance(log_meta);
        accept_prepare(&mut ready, s, HashMap::new());
        accept_append_entries(&ready, s, &HashSet::new());
        accept_indexes(&ready, s, &HashSet::new());

        // enter both stage
        s.enter_replicate_state();
        s.tick();
    }

    fn advance_all(s: &mut Sdcons, log_meta: &LogMeta, skip_nodes: &HashSet<u64>) {
        let ready = s.advance(log_meta);
        apply_entries(&ready, s);
        stable_all_entries(&ready, s);
        stable_indexes(&ready, s);
        accept_append_entries(&ready, s, skip_nodes);
        accept_indexes(&ready, s, skip_nodes);
    }

    fn advance_all_and_count_msg<F>(
        s: &mut Sdcons,
        log_meta: &LogMeta,
        skip_nodes: &HashSet<u64>,
        is_expect_msg_fn: F,
    ) -> usize
    where
        F: Fn(&Message) -> bool,
    {
        let ready = s.advance(log_meta);
        apply_entries(&ready, s);
        stable_all_entries(&ready, s);
        stable_indexes(&ready, s);
        accept_append_entries(&ready, s, skip_nodes);
        accept_indexes(&ready, s, skip_nodes);

        ready
            .msgs
            .iter()
            .map(|(_id, msgs)| msgs.iter())
            .flatten()
            .filter(|msg| is_expect_msg_fn(msg))
            .count()
    }

    fn to_new_stage(s: &mut Sdcons, log_meta: &LogMeta, new_configs: Vec<u64>) {
        to_both_stage(s, log_meta, new_configs);
        s.enter_replicate_state();
        // accept both stage config change
        advance_all(s, log_meta, &HashSet::new());
        // apply both stage config change
        advance_all(s, log_meta, &HashSet::new());

        // change to new stage.
        s.tick();
        assert_eq!(s.is_pending_configs_in_new_stage(), true);
    }

    #[test]
    fn apply_config_change() {
        let meta = init_log_meta_with_members(&vec![1, 2, 3]);
        let mut s = init_sdcons(&meta);
        to_new_stage(&mut s, &meta, vec![1, 4, 5]);

        debug!("should apply config changes");

        submit_tasks(&mut s, &vec![1, 2, 3]);
        let ready = s.advance(&meta);
        apply_entries(&ready, &mut s);
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);

        // New configs reach majority
        let skip_nodes: HashSet<u64> = vec![1, 3].iter().cloned().collect();
        accept_append_entries(&ready, &mut s, &skip_nodes);
        accept_indexes(&ready, &mut s, &skip_nodes);

        // Apply pending configs
        s.tick();
        let ready = s.advance(&meta);
        assert_chosen_entries(&ready, &vec![1, 2, 3]);
        let _ = s.advance(&meta);
        assert_eq!(s.pending_configs.is_none(), true);
    }

    #[test]
    fn rollback_config_change() {
        let meta = init_log_meta_with_members(&vec![1, 2, 3]);
        let mut s = init_sdcons(&meta);
        let next_id = INVALID_ID + 1u64;
        let mut channel_id = 2;
        let mut entry = Entry {
            request_id: CONFIG_CHANGE_ID,
            channel_id: channel_id,
            channel_term: 1,
            entry_id: next_id,
            index_id: INVALID_ID,
            message: vec![],
            context: None,
            configs: Some(ChangeConfig {
                index_id: next_id,
                entry_id: next_id,
                term: 1,
                stage: ConfigStage::Both,
                members: HashSet::from_iter(vec![1, 4, 5].into_iter()),
            }),
        };
        let mut index = LogIndex {
            channel_id: channel_id,
            entry_id: next_id,
            index_id: next_id,
            term: 1,
            context: None,
        };

        assert_eq!(s.pending_configs.is_none(), true);
        replicate_indexes(channel_id, INVALID_ID, INITIAL_TERM, &vec![index.clone()], &mut s);
        append_specified_entries(
            &mut s,
            channel_id,
            1,
            INVALID_ID,
            INITIAL_TERM,
            vec![entry.clone()],
        );
        advance_all(&mut s, &meta, &HashSet::new());

        assert_eq!(s.pending_configs.is_some(), true);
        let pending_configs = s.pending_configs.as_ref().unwrap();
        assert_eq!(pending_configs.stage, ConfigStage::Both);

        // receive large id
        channel_id += 1;
        index.channel_id = channel_id;
        index.term = 2;
        entry.channel_id = channel_id;
        entry.channel_term = 2;
        entry.configs = Some(ChangeConfig {
            index_id: next_id,
            entry_id: next_id,
            term: 2,
            stage: ConfigStage::New,
            members: HashSet::from_iter(vec![1, 4, 5].into_iter()),
        });
        replicate_indexes(channel_id, INVALID_ID, INITIAL_TERM, &vec![index], &mut s);
        append_specified_entries(&mut s, channel_id, 1, INVALID_ID, INITIAL_TERM, vec![entry]);
        advance_all(&mut s, &meta, &HashSet::new());

        assert_eq!(s.pending_configs.is_some(), true);
        let pending_configs = s.pending_configs.as_ref().unwrap();
        assert_eq!(pending_configs.stage, ConfigStage::New);
        assert_eq!(pending_configs.configs.contains(&2), false);
    }

    #[test]
    fn recovery_both_stage_config_change() {
        let mut log_meta = LogMeta::default();
        let mut desc = MemberState {
            stage: ConfigStage::Both,
            applied: true,
        };
        log_meta.members.insert(1, desc.clone());
        desc.stage = ConfigStage::Old;
        log_meta.members.insert(2, desc.clone());
        log_meta.members.insert(3, desc.clone());

        desc.stage = ConfigStage::New;
        desc.applied = false;
        log_meta.members.insert(4, desc.clone());
        log_meta.members.insert(5, desc.clone());

        let local_id = 1;
        let mut mem_store = MemStorage::new(local_id);
        for (id, _) in &log_meta.members {
            mem_store.append_entries(*id, vec![]);
        }

        let next_id = INVALID_ID + 1u64;
        let channel_id = 2;
        let entry = Entry {
            request_id: CONFIG_CHANGE_ID,
            channel_id: channel_id,
            channel_term: 1,
            entry_id: next_id,
            index_id: INVALID_ID,
            message: vec![],
            context: None,
            configs: Some(ChangeConfig {
                index_id: next_id,
                entry_id: next_id,
                term: 1,
                stage: ConfigStage::Both,
                members: HashSet::from_iter(vec![1, 4, 5].into_iter()),
            }),
        };
        mem_store.append_entries(channel_id, vec![entry]);

        let index = LogIndex {
            channel_id: channel_id,
            entry_id: next_id,
            index_id: next_id,
            term: 1,
            context: None,
        };
        mem_store.extend_indexes(vec![index]);

        let mut s = init_sdcons_with_mem_store(&log_meta, mem_store);
        assert_eq!(s.pending_configs.is_some(), true);
        let pending_configs = s.pending_configs.as_ref().unwrap();
        assert_eq!(pending_configs.stage, ConfigStage::Both);
        assert_eq!(pending_configs.old_configs.contains(&2), true);
        assert_eq!(pending_configs.old_configs.contains(&3), true);
        // FIXME:
        // assert_eq!(pending_configs.new_configs.contains(&4), true);
        // assert_eq!(pending_configs.new_configs.contains(&5), true);
        assert_eq!(pending_configs.new_configs.contains(&1), false);
        assert_eq!(pending_configs.old_configs.contains(&1), false);
        assert_eq!(pending_configs.configs.contains(&1), true);

        s.tick();
        s.advance(&log_meta);

        let channel_ids = vec![INDEX_CHANNEL_ID, 1, 2, 3, 4, 5];
        assert_eq!(s.channel_ids, channel_ids);
    }

    #[test]
    fn recovery_new_stage_config_change() {
        let mut log_meta = LogMeta::default();
        let mut desc = MemberState {
            stage: ConfigStage::Both,
            applied: true,
        };

        // Member state is still in both stage.
        log_meta.members.insert(1, desc.clone());
        desc.stage = ConfigStage::Old;
        log_meta.members.insert(2, desc.clone());
        log_meta.members.insert(3, desc.clone());

        desc.stage = ConfigStage::New;
        desc.applied = false;
        log_meta.members.insert(4, desc.clone());
        log_meta.members.insert(5, desc.clone());

        let local_id = 1;
        let mut mem_store = MemStorage::new(local_id);
        for (id, _) in &log_meta.members {
            mem_store.append_entries(*id, vec![]);
        }

        let next_id = INVALID_ID + 1u64;
        let channel_id = 2;
        let entry = Entry {
            request_id: CONFIG_CHANGE_ID,
            channel_id: channel_id,
            channel_term: 1,
            entry_id: next_id,
            index_id: INVALID_ID,
            message: vec![],
            context: None,
            configs: Some(ChangeConfig {
                index_id: next_id,
                entry_id: next_id,
                term: 1,
                stage: ConfigStage::New,
                members: HashSet::from_iter(vec![1, 4, 5].into_iter()),
            }),
        };
        mem_store.append_entries(channel_id, vec![entry]);

        let index = LogIndex {
            channel_id: channel_id,
            entry_id: next_id,
            index_id: next_id,
            term: 1,
            context: None,
        };
        mem_store.extend_indexes(vec![index]);

        let mut s = init_sdcons_with_mem_store(&log_meta, mem_store);
        assert_eq!(s.pending_configs.is_some(), true);
        let pending_configs = s.pending_configs.as_ref().unwrap();
        info!("pending configs: {:?}", pending_configs);
        assert_eq!(pending_configs.stage, ConfigStage::New);
        assert_eq!(pending_configs.old_configs.contains(&2), true);
        assert_eq!(pending_configs.old_configs.contains(&3), true);
        // FIXME:
        // assert_eq!(pending_configs.new_configs.contains(&4), true);
        // assert_eq!(pending_configs.new_configs.contains(&5), true);
        // assert_eq!(pending_configs.new_configs.contains(&1), false);
        assert_eq!(pending_configs.old_configs.contains(&1), false);
        assert_eq!(pending_configs.configs.contains(&1), true);

        // commit index and config change entry.
        declare_with_id(&mut s, 2, 2, 0, 2, 1);
        declare_with_id(&mut s, 2, 2, 2, 2, 1);

        s.advance(&log_meta);
        let channel_ids = vec![INDEX_CHANNEL_ID, 1, 4, 5];
        assert_eq!(s.channel_ids, channel_ids);
    }

    fn is_timeout_now(msg: &Message) -> bool {
        println!("msg: {:?}", msg);
        if let MsgDetail::TimeoutNow = msg.detail {
            true
        } else {
            false
        }
    }

    #[test]
    fn transfer_leadership_directly() {
        let meta = init_log_meta_with_members(&vec![1, 2, 3]);
        let mut s = init_sdcons(&meta);
        let res = s.control(Control::TransferLeader(2));
        assert_eq!(res.is_err(), true);

        to_leader(&mut s, &meta);
        advance_all(&mut s, &meta, &HashSet::new());

        let res = s.control(Control::TransferLeader(2));
        assert_eq!(res.is_ok(), true);

        let res = s.control(Control::TransferLeader(2));
        assert_eq!(res.is_ok(), true);

        let count = advance_all_and_count_msg(&mut s, &meta, &HashSet::new(), is_timeout_now);
        assert_eq!(count > 0, true);
    }

    #[test]
    fn transfer_leadership_after_matched() {
        let meta = init_log_meta_with_members(&vec![1, 2, 3]);
        let mut s = init_sdcons(&meta);
        to_leader(&mut s, &meta);

        let requests = vec![1];
        submit_tasks(&mut s, &requests);

        let res = s.control(Control::TransferLeader(2));
        assert_eq!(res.is_ok(), true);

        let count = advance_all_and_count_msg(&mut s, &meta, &HashSet::new(), is_timeout_now);
        assert_eq!(count, 0);

        let last_id = s.channel_next_entry_id(INDEX_CHANNEL_ID) - 1;
        assert_eq!(s.remote_matched_id(INDEX_CHANNEL_ID, 2), last_id);

        let count = advance_all_and_count_msg(&mut s, &meta, &HashSet::new(), is_timeout_now);
        assert_eq!(count > 0, true);
    }

    #[test]
    fn index_leader_read_index() {
        let meta = init_log_meta_with_members(&vec![1]);
        let mut mem_store = MemStorage::new(1);
        mem_store.append_entries(1, vec![]);
        mem_store.append_entries(2, vec![]);
        mem_store.append_entries(3, vec![]);
        mem_store
            .extend_indexes(vec![new_index_with_id(1, 1, 1, 2), new_index_with_id(1, 2, 3, 4)]);
        let mut s = init_sdcons_with_mem_store(&meta, mem_store);

        to_leader(&mut s, &meta);

        let r = s.leased_read();
        assert_eq!(r.is_ok(), true);
        let request_id = r.unwrap();

        submit_tasks(&mut s, &vec![4, 5, 6]);

        // Index leader handle request immediately, and index should large than the prev
        // term last id.
        let ready = s.advance(&meta);
        apply_entries(&ready, &mut s);
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);
        assert_eq!(ready.finished_reads.contains_key(&request_id), true);
        assert_eq!(*ready.finished_reads.get(&request_id).unwrap(), 2);

        s.advance(&meta);

        // Index should large than last index id
        let request_id = s.leased_read().unwrap();
        let ready = s.advance(&meta);
        assert_eq!(ready.finished_reads.contains_key(&request_id), true);
        info!("print {:?}", ready.finished_reads);
        assert_eq!(*ready.finished_reads.get(&request_id).unwrap() >= 6, true);
    }

    #[test]
    fn index_leader_receive_read_index() {
        let meta = init_log_meta_with_members(&vec![1]);
        let mut mem_store = MemStorage::new(1);
        mem_store.append_entries(1, vec![]);
        mem_store.append_entries(2, vec![]);
        mem_store.append_entries(3, vec![]);
        mem_store
            .extend_indexes(vec![new_index_with_id(1, 1, 1, 2), new_index_with_id(1, 2, 3, 4)]);
        let mut s = init_sdcons_with_mem_store(&meta, mem_store);

        to_leader(&mut s, &meta);

        let sender = 2;
        let request_id = 123;
        let mut msg = s.build_msg_header(INDEX_CHANNEL_ID);
        msg.from = sender;
        msg.to = 1;
        msg.detail = MsgDetail::Read(ReadMsg { request_id });
        s.step(msg);

        submit_tasks(&mut s, &vec![4, 5, 6]);

        // Index leader handle request immediately, and index should large than the prev
        // term last id.
        let ready = s.advance(&meta);
        apply_entries(&ready, &mut s);
        stable_all_entries(&ready, &mut s);
        stable_indexes(&ready, &mut s);
        assert_eq!(ready.msgs.len(), 1);
        info!("print {:?}", ready.msgs);
        let msgs = ready.msgs.get(&INDEX_CHANNEL_ID).unwrap();
        assert_eq!(msgs.len(), 1);
        if let MsgDetail::ReadReply(reply) = &msgs[0].detail {
            assert_eq!(reply.request_id, request_id);
            assert_eq!(reply.recommend_id, 2);
        }

        s.advance(&meta);

        // Index should large than last index id
        let request_id = 123123;
        let mut msg = s.build_msg_header(INDEX_CHANNEL_ID);
        msg.from = sender;
        msg.to = 1;
        msg.detail = MsgDetail::Read(ReadMsg { request_id });
        s.step(msg);

        let ready = s.advance(&meta);
        assert_eq!(ready.msgs.len(), 1);
        let msgs = ready.msgs.get(&INDEX_CHANNEL_ID).unwrap();
        assert_eq!(msgs.len(), 1);
        if let MsgDetail::ReadReply(reply) = &msgs[0].detail {
            assert_eq!(reply.request_id, request_id);
            assert_eq!(reply.recommend_id >= 6, true);
        }
    }

    #[test]
    fn follower_retry_read_index_when_heartbeat() {
        let mut log_meta = init_log_meta_with_default_members();
        let mut s = init_sdcons(&log_meta);

        to_leader(&mut s, &log_meta);
        let next_channel_term = s.get_index_term() + 1;
        declare_leader(&mut s, next_channel_term, INDEX_CHANNEL_ID, next_channel_term);

        assert_eq!(s.is_index_leader(), false);
        assert_eq!(s.is_local_channel_leader(), true);
        s.advance(&log_meta);

        // invoke read_index(), follower will forwards this request to index leader.
        let request_id = s.leased_read().unwrap();
        let ready = s.advance(&log_meta);
        assert_eq!(ready.msgs.len(), 1);
        let msgs = ready.msgs.get(&INDEX_CHANNEL_ID).unwrap();
        info!("{:?}", msgs);
        assert_eq!(msgs.len(), 1);
        if let MsgDetail::Read(req) = &msgs[0].detail {
            assert_eq!(req.request_id, request_id);
        }

        // request timeouted, retry.
        s.tick();
        let ready = s.advance(&log_meta);
        let msgs = ready.msgs.get(&INDEX_CHANNEL_ID).unwrap();
        info!("{:?}", msgs);
        assert_eq!(msgs.len(), 1);
        if let MsgDetail::Read(req) = &msgs[0].detail {
            assert_eq!(req.request_id, request_id);
        }

        // receive leader response
        let sender = 2;
        let mut msg = s.build_msg_header(INDEX_CHANNEL_ID);
        msg.from = sender;
        msg.to = 1;
        msg.detail = MsgDetail::ReadReply(ReadReplyMsg {
            request_id,
            recommend_id: 6,
        });
        s.step(msg);
        let ready = s.advance(&log_meta);
        assert_eq!(ready.finished_reads.contains_key(&request_id), true);
        info!("print {:?}", ready.finished_reads);
        assert_eq!(*ready.finished_reads.get(&request_id).unwrap() >= 6, true);
    }
}
