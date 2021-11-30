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

use std::collections::{HashMap, HashSet};

use crate::constant::*;
use crate::types::{ConfigStage, EntryMeta, HardState, MemberState};

use chrono::prelude::*;
use log::{debug, info, trace, warn};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Leader,
    Student,
    Follower,
    Candidate,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[inline(always)]
fn clone_last_log_meta(metas: &Vec<EntryMeta>) -> EntryMeta {
    metas
        .last()
        .map(|e| e.clone())
        .unwrap_or(EntryMeta::default())
}

#[inline(always)]
pub fn majority(len: usize) -> usize {
    (len + 1) / 2
}

#[derive(Debug, Eq, PartialEq)]
pub enum SnapshotState {
    Creating,
    Loading,
    None,
}

#[derive(Debug)]
pub(crate) struct SnapshotRecevingState {
    pub receiving: bool,
    pub start_at: i64,
    pub last_heartbeat_at: i64,
}

#[derive(Debug, Clone)]
pub struct Inflights {
    pub pos: usize,
    pub cnt: usize,
    pub cap: usize,
    pub buf: Vec<u64>,
}

impl Inflights {
    fn new(cap: usize) -> Inflights {
        let mut buf = Vec::new();
        buf.resize(cap, 0);
        Inflights {
            pos: 0,
            cnt: 0,
            cap,
            buf,
        }
    }

    fn full(&self) -> bool {
        self.cnt == self.cap
    }

    fn reset(&mut self) {
        self.cnt = 0;
        self.pos = 0;
    }

    fn add(&mut self, id: u64) {
        if self.full() {
            return;
        }

        let next = (self.pos + self.cnt) % self.cap;
        self.buf[next] = id;
        self.cnt += 1;
    }

    fn free_to(&mut self, to: u64) {
        if self.cnt == 0 || to < self.buf[self.pos] {
            // out of the left window
            return;
        }

        let mut idx = self.pos;
        let mut i = 0;
        while i < self.cnt {
            if to < self.buf[idx] {
                break;
            }
            idx = (idx + 1) % self.cap;
            i += 1;
        }

        self.cnt -= i;
        self.pos = idx;
        if self.cnt == 0 {
            self.pos = 0;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressState {
    Probe,
    Replicate,
    MsgBuilding,
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub node_id: u64,

    pub match_id: u64,
    pub next_id: u64,

    pub granted: bool,
    pub active: bool,

    pub paused: bool,
    pub inflights: Inflights,
    pub state: ProgressState,
    pub belongs_to: ConfigStage,
}

impl Progress {
    pub fn new(node_id: u64, belongs_to: ConfigStage) -> Progress {
        Progress {
            node_id,
            match_id: INVALID_ID,
            next_id: INVALID_ID + 1,

            granted: false,
            active: false,

            paused: false,
            inflights: Inflights::new(128),
            state: ProgressState::Probe,
            belongs_to,
        }
    }

    pub fn reset(&mut self, last_id: u64) {
        self.match_id = INVALID_ID;
        self.next_id = last_id + 1;
        self.granted = false;
        self.active = true;
        self.state = ProgressState::Probe;
    }

    pub fn try_advance_matched_id(&mut self, hint_id: u64) -> bool {
        self.paused = false;
        if self.match_id + 1u64 < hint_id {
            self.match_id = hint_id - 1u64;
            if self.next_id <= self.match_id {
                self.next_id = self.match_id + 1u64;
            }
            match &self.state {
                ProgressState::Probe => {
                    self.inflights.reset();
                    self.state = ProgressState::Replicate;
                }
                ProgressState::Replicate => {
                    self.inflights.free_to(self.match_id);
                }
                _ => {}
            }
            true
        } else {
            false
        }
    }

    pub fn try_reset_next_id(&mut self, reject_id: u64, hint_id: u64) -> bool {
        self.paused = false;
        // Ignore staled reply.
        match &self.state {
            ProgressState::Replicate if reject_id > self.match_id => {
                self.state = ProgressState::Probe;
                self.next_id = self.match_id + 1u64;
                true
            }
            ProgressState::Probe if reject_id == self.next_id => {
                self.next_id = std::cmp::max(1, hint_id);
                true
            }
            _ => false,
        }
    }

    pub fn on_receive_msg(&mut self) {
        self.active = true;
    }

    pub fn is_paused(&self) -> bool {
        match &self.state {
            ProgressState::Probe => self.paused,
            ProgressState::MsgBuilding => true,
            ProgressState::Replicate => self.inflights.full(),
        }
    }

    pub fn step_building_msg(&mut self) {
        self.state = ProgressState::MsgBuilding;
    }

    pub fn step_builded(&mut self, hint_id: u64) {
        self.state = ProgressState::Probe;
        self.paused = false;
        if self.match_id <= hint_id {
            self.next_id = hint_id;
        }
    }

    pub fn replicate_to(&mut self, last_id: u64) {
        if last_id <= self.match_id {
            return;
        }
        match &self.state {
            ProgressState::Probe => {
                self.paused = true;
            }
            ProgressState::Replicate => {
                self.inflights.add(last_id);
                self.next_id = last_id + 1u64;
            }
            _ => {}
        }
    }

    pub fn belongs_to_stage(&self, stage: ConfigStage) -> bool {
        self.belongs_to == stage || self.belongs_to == ConfigStage::Both
    }
}

#[derive(Debug)]
pub struct ChannelDesc {
    pub channel_id: u64,
    pub committed_id: u64,
    pub last_id: u64,
    pub hard_state: HardState,
    pub members: HashMap<u64, MemberState>,
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub local_id: u64,
    pub channel_id: u64,

    pub progress_map: HashMap<u64, Progress>,

    pub voted_for: u64,
    pub term: u64,
    pub committed_id: u64,
    pub prev_term_last_id: u64,

    pub leader_id: u64,
    pub role: Role,

    pub last_liveness_at: i64,
    pub elapsed_tick: u32,

    // learned entry metas from remote, only used for command instance.
    pub max_received_entries: Vec<EntryMeta>,

    /// Id set of the members who won't granted to me.
    pub missed_voters: HashSet<u64>,
    pub learned_voters: HashSet<u64>,
}

impl ChannelInfo {
    pub fn build(local_id: u64, channel_id: u64, descs: HashMap<u64, MemberState>) -> ChannelInfo {
        let desc = ChannelDesc {
            channel_id,
            committed_id: INVALID_ID,
            last_id: INVALID_ID,
            hard_state: HardState::default(),
            members: descs,
        };
        ChannelInfo::new(local_id, &desc)
    }

    pub fn new(local_id: u64, channel_desc: &ChannelDesc) -> ChannelInfo {
        let mut progress_map = channel_desc
            .members
            .iter()
            .map(|(id, desc)| (*id, Progress::new(*id, desc.stage)))
            .collect::<HashMap<u64, Progress>>();

        let mut initial_local_matched_id = 0;
        for (id, p) in &mut progress_map {
            p.reset(channel_desc.last_id);
            // We need apply chosen entries immediately, so update match index to advance
            // safety committed id.
            if *id == local_id {
                p.match_id = channel_desc.last_id;
                initial_local_matched_id = channel_desc.last_id;
            }
        }

        info!(
            "node {} setup channel {}, voted_for {}, term {}, committed_id {}, last_id {}, matched_id {}",
            local_id,
            channel_desc.channel_id,
            channel_desc.hard_state.voted_for,
            channel_desc.hard_state.current_term,
            channel_desc.committed_id,
            channel_desc.last_id,
            initial_local_matched_id
        );

        ChannelInfo {
            local_id,
            channel_id: channel_desc.channel_id,
            progress_map,
            voted_for: channel_desc.hard_state.voted_for,
            term: channel_desc.hard_state.current_term,
            committed_id: channel_desc.committed_id,
            prev_term_last_id: channel_desc.last_id,
            leader_id: INVALID_NODE_ID,
            role: Role::Follower,
            elapsed_tick: 0,
            last_liveness_at: Local::now().timestamp_nanos(),
            max_received_entries: Vec::new(),
            missed_voters: HashSet::new(),
            learned_voters: HashSet::new(),
        }
    }

    pub fn current_term(&self) -> u64 {
        return self.term;
    }

    pub fn last_learned_entry_meta(&self) -> EntryMeta {
        clone_last_log_meta(&self.max_received_entries)
    }

    pub fn try_receive_prepare_entries(&mut self, entries: Vec<EntryMeta>) -> bool {
        let local_last = clone_last_log_meta(&self.max_received_entries);
        let remote_last = clone_last_log_meta(&entries);
        if remote_last.term > local_last.term
            || (remote_last.term == local_last.term && local_last.id < remote_last.id)
        {
            debug!(
                "node {} channel {} receive prepare entries last id {}, term: {}",
                self.local_id, self.channel_id, remote_last.id, remote_last.term
            );
            self.max_received_entries = entries;
            true
        } else {
            false
        }
    }

    pub fn reset_tick(&mut self) {
        self.elapsed_tick = 0;
    }

    pub fn on_receive_msg(&mut self, from: u64) {
        if let Some(p) = self.progress_map.get_mut(&from) {
            p.on_receive_msg();
        }
    }

    pub fn advance_quorum_lease(&mut self) -> usize {
        let mut count = 0;
        for (_, p) in &mut self.progress_map {
            if p.active {
                count += 1;
                p.active = false;
            }
        }
        count
    }

    fn reset(&mut self, last_id: u64) {
        let local_id = self.local_id;
        self.progress_map.iter_mut().for_each(|(id, progress)| {
            let last_match_id = progress.match_id;
            progress.reset(last_id);
            if *id == local_id {
                progress.match_id = std::cmp::max(last_match_id, last_id);
            }
        });
    }

    pub fn to_leader(&mut self, last_id: u64, reason: &'static str) {
        let _prev_role = self.to(Role::Leader, reason);
        self.leader_id = self.local_id;
        self.reset(last_id);
    }

    pub fn to_student(&mut self, last_id: u64, reason: &'static str) {
        let _prev_role = self.to(Role::Student, reason);
        self.leader_id = self.local_id;
        self.missed_voters = self
            .progress_map
            .iter()
            .filter(|(_, p)| !p.granted)
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        self.learned_voters = HashSet::new();
        self.max_received_entries.clear();
        self.reset(last_id);
    }

    pub fn to_candidate(&mut self, last_id: u64, reason: &'static str) {
        let _prev_role = self.to(Role::Candidate, reason);
        self.term += 1;
        self.leader_id = INVALID_NODE_ID;
        self.prev_term_last_id = last_id;
        self.voted_for = self.local_id;
        self.max_received_entries.clear();
        self.reset(INVALID_ID);
    }

    pub fn to_follower(
        &mut self,
        leader_id: u64,
        target_term: u64,
        last_id: u64,
        reason: &'static str,
    ) {
        let _prev_role = self.to(Role::Follower, reason);
        if self.term < target_term {
            info!(
                "node {} channel {} advance term from {} to {}",
                self.local_id, self.channel_id, self.term, target_term
            );
            self.term = target_term;
            self.voted_for = INVALID_NODE_ID;
            self.prev_term_last_id = last_id;
        }
        if self.leader_id != leader_id {
            info!(
                "node {} channel {} follow leader {} from {} at term {}",
                self.local_id, self.channel_id, leader_id, self.leader_id, self.term
            );
            self.leader_id = leader_id;
        }
        self.reset(INVALID_ID);
    }

    fn to(&mut self, target_role: Role, reason: &'static str) -> Role {
        info!(
            "node {} channel {} change role from {} to {} at term {}, reason: {}",
            self.local_id, self.channel_id, self.role, target_role, self.term, reason
        );
        let prev_role = self.role;
        self.role = target_role;
        self.elapsed_tick = 0;
        prev_role
    }

    pub fn is_already_promise_others(&self, node_id: u64) -> bool {
        // If current node has voted before and the voted node isn't msg.from
        self.voted_for != INVALID_NODE_ID && self.voted_for != node_id
    }

    pub fn try_make_promise(&mut self, node_id: u64) -> bool {
        if !self.is_already_promise_others(node_id) {
            debug!(
                "node {} channel {} take promise for {}",
                self.local_id, self.channel_id, node_id
            );
            self.voted_for = node_id;
            self.reset_tick();
            true
        } else {
            false
        }
    }

    pub fn receive_promise(&mut self, node_id: u64) {
        match self.progress_map.get_mut(&node_id) {
            Some(p) => p.granted = true,
            None => {}
        };
    }

    pub fn stage_majority(&self, stage: ConfigStage) -> usize {
        majority(
            self.progress_map
                .iter()
                .filter(|(_, p)| p.belongs_to_stage(stage))
                .count(),
        )
    }

    pub fn current_term_safe_commit_id(&self) -> u64 {
        std::cmp::max(self.committed_id, self.prev_term_last_id)
    }

    fn receive_stage_majority_promise(&self, stage: ConfigStage) -> bool {
        let granted_members = self
            .progress_map
            .iter()
            .filter(|(_, p)| p.granted && p.belongs_to_stage(stage))
            .map(|(k, _)| *k)
            .collect::<Vec<u64>>();

        return granted_members.len() >= self.stage_majority(stage);
    }

    pub fn receive_majority_promise(&self, stage: ConfigStage) -> bool {
        match &stage {
            ConfigStage::Both => {
                self.receive_stage_majority_promise(ConfigStage::Old)
                    && self.receive_stage_majority_promise(ConfigStage::New)
            }
            _ => self.receive_stage_majority_promise(ConfigStage::New),
        }
    }

    fn compute_candidate_id(&self, stage: ConfigStage) -> u64 {
        let mut match_ids = self
            .progress_map
            .iter()
            .filter(|(_, p)| p.belongs_to_stage(stage))
            .map(|(_, p)| p.match_id)
            .collect::<Vec<_>>();
        match_ids.sort();
        let total_numbers = match_ids.len();
        if total_numbers == 0 {
            self.committed_id
        } else {
            match_ids[total_numbers - majority(total_numbers)]
        }
    }

    pub fn try_advance_committed_id(&mut self, stage: ConfigStage) -> bool {
        let candidate_id = match &stage {
            ConfigStage::Both => std::cmp::min(
                self.compute_candidate_id(ConfigStage::New),
                self.compute_candidate_id(ConfigStage::Old),
            ),
            _ => self.compute_candidate_id(ConfigStage::New),
        };
        if candidate_id > std::cmp::max(self.prev_term_last_id, self.committed_id) {
            trace!(
                "node {} channel {} advance committed index from {} to {}, stage: {:?}",
                self.local_id,
                self.channel_id,
                self.committed_id,
                candidate_id,
                stage
            );
            self.committed_id = candidate_id;
            true
        } else {
            false
        }
    }

    pub fn update_committed_id(&mut self, id: u64) {
        if self.committed_id < id {
            debug!(
                "node {} channel {} update committed id from {} to {}",
                self.local_id, self.channel_id, self.committed_id, id
            );
            self.committed_id = id;
        }
    }

    pub fn update_progress(&mut self, remote: u64, reject: bool, msg_id: u64, hint_id: u64) {
        let is_leader = self.is_leader();
        if let Some(progress) = self.progress_map.get_mut(&remote) {
            progress.on_receive_msg();
            if !reject {
                debug!(
                    "node {} channel {} try advance remote {} match id to {}",
                    self.local_id,
                    self.channel_id,
                    remote,
                    hint_id - 1u64
                );
                progress.try_advance_matched_id(hint_id);
            } else if is_leader {
                debug!(
                    "node {} channel {} try reset remote {} next id to {}",
                    self.local_id, self.channel_id, remote, hint_id
                );
                progress.try_reset_next_id(msg_id, hint_id);
            } else {
                warn!(
                    "node {} channel id {} receive a rejected index request {:?} but I isn't a leader any more",
                    self.local_id, self.channel_id, reject
                );
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn is_candidate(&self) -> bool {
        self.role == Role::Candidate
    }

    pub fn is_follower(&self) -> bool {
        self.role == Role::Follower
    }

    pub fn is_student(&self) -> bool {
        self.role == Role::Student
    }

    pub fn has_leader(&self) -> bool {
        self.leader_id != INVALID_NODE_ID
    }

    pub fn is_remote_matched(&self, remote: u64, last_id: u64) -> bool {
        if let Some(progress) = self.progress_map.get(&remote) {
            if progress.match_id == last_id {
                return true;
            }
        }
        false
    }

    pub fn missed_channel_ids(&self) -> Vec<u64> {
        let mut missed_channel_ids = Vec::new();
        for channel_id in &self.missed_voters {
            if self.learned_voters.contains(&channel_id) {
                continue;
            }
            missed_channel_ids.push(*channel_id);
        }
        missed_channel_ids
    }

    pub fn update_local_match(&mut self, to: u64) {
        if let Some(progress) = self.progress_map.get_mut(&self.local_id) {
            debug!(
                "node {} channel {} update the local matched entry id from {} to {}",
                self.local_id, self.channel_id, progress.match_id, to
            );
            assert_eq!(progress.match_id <= to, true);
            progress.match_id = to;
        }
    }

    pub fn get_local_match_id(&self) -> u64 {
        self.progress_map
            .get(&self.local_id)
            .map(|p| p.match_id)
            .unwrap_or(INVALID_ID)
    }

    pub fn get_local_safety_commit_id(&self) -> u64 {
        std::cmp::min(self.committed_id, self.get_local_match_id())
    }

    pub fn log_replicated(&mut self, node_id: u64, next_id: u64) {
        self.progress_map
            .get_mut(&node_id)
            .map(|p| p.step_builded(next_id));
    }

    pub fn hard_state(&self) -> HardState {
        HardState {
            voted_for: self.voted_for,
            current_term: self.term,
        }
    }

    pub fn enter_both_config_stage(
        &mut self,
        new_configs: &HashSet<u64>,
        old_configs: &HashSet<u64>,
    ) {
        debug!(
            "node {} channel {} enter both stage: new configs {:?}, old configs {:?}",
            self.local_id, self.channel_id, new_configs, old_configs
        );
        for (id, progress) in &mut self.progress_map {
            progress.belongs_to = if old_configs.contains(id) {
                ConfigStage::Old
            } else {
                ConfigStage::Both
            };
        }

        for id in new_configs {
            if let Some(p) = self.progress_map.get_mut(id) {
                p.belongs_to = ConfigStage::New;
                continue;
            }
            self.progress_map
                .insert(*id, Progress::new(*id, ConfigStage::New));
        }
    }

    pub fn enter_new_config_stage(
        &mut self,
        new_configs: &HashSet<u64>,
        old_configs: &HashSet<u64>,
    ) {
        debug!(
            "node {} channel {} enter new stage: new configs {:?}, old configs {:?}",
            self.local_id, self.channel_id, new_configs, old_configs
        );
        for id in old_configs {
            self.progress_map.remove(id);
        }
        for (_id, progress) in &mut self.progress_map {
            progress.belongs_to = ConfigStage::New;
        }
    }

    pub fn rollback_config_change(
        &mut self,
        new_configs: &HashSet<u64>,
        old_configs: &HashSet<u64>,
    ) {
        self.enter_new_config_stage(old_configs, new_configs);
    }

    pub fn get_member_states(&self) -> HashMap<u64, MemberState> {
        self.progress_map
            .iter()
            .filter(|(id, _)| **id != INDEX_CHANNEL_ID)
            .map(|(id, _)| (*id, MemberState::default()))
            .collect::<HashMap<_, _>>()
    }

    pub fn matched_committed_id(&self, id: u64) -> u64 {
        let match_id = self.progress_map.get(&id).unwrap().match_id;
        std::cmp::min(match_id, self.committed_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_to_probe() {
        let mut p = Progress::new(1, ConfigStage::New);
        let last_id = 10;
        p.reset(last_id);
        assert_eq!(p.next_id, last_id + 1u64);
        assert_eq!(p.match_id, INVALID_ID);
        assert_eq!(p.state, ProgressState::Probe);

        let mut hint_id = 5;
        p.try_advance_matched_id(hint_id);
        assert_eq!(p.match_id, hint_id - 1u64);
        assert_eq!(p.next_id, last_id + 1u64);
        assert_eq!(p.state, ProgressState::Replicate);

        p.reset(123);
        assert_eq!(p.state, ProgressState::Probe);
    }

    #[test]
    fn update_match_next_id() {
        let mut p = Progress::new(1, ConfigStage::New);
        let last_id = 10;
        p.reset(last_id);
        assert_eq!(p.next_id, last_id + 1u64);
        assert_eq!(p.match_id, INVALID_ID);
        assert_eq!(p.state, ProgressState::Probe);

        let mut hint_id = 5;
        p.try_advance_matched_id(hint_id);
        assert_eq!(p.match_id, hint_id - 1u64);
        assert_eq!(p.next_id, last_id + 1u64);
        assert_eq!(p.state, ProgressState::Replicate);

        // ignore staled request
        hint_id = 3;
        p.try_advance_matched_id(hint_id);
        assert_eq!(p.match_id, 4);
        assert_eq!(p.next_id, last_id + 1u64);

        // advance to next_id
        p.try_advance_matched_id(last_id + 1u64);
        assert_eq!(p.match_id, last_id);
        assert_eq!(p.next_id, last_id + 1u64);

        // ignore staled reseet request
        p.next_id = 100;
        p.try_reset_next_id(hint_id, hint_id);
        assert_eq!(p.match_id, last_id);
        assert_eq!(p.next_id, 100);

        p.state = ProgressState::Replicate;
        p.try_reset_next_id(p.next_id, 50);
        assert_eq!(p.match_id, last_id);
        assert_eq!(p.next_id, p.match_id + 1u64);

        assert_eq!(p.state, ProgressState::Probe);
        p.try_reset_next_id(p.next_id, 50);
        assert_eq!(p.match_id, last_id);
        assert_eq!(p.next_id, 50);
    }
}
