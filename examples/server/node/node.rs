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
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use anyhow::Error;
use log::{debug, info};
use sdcons::constant;
use sdcons::types::{
    Entry, EntryMeta, HardState, LogIndex, MemberState, Message, SnapshotDesc, Task,
};
use sdcons::{Control, RawNode, SdconsOption};

use super::applier::{Applier, FiniteStateMachine};
use super::snap_mgr::{FileBasedSnapshotMeta, SnapshotMgr};
use super::storage::LogStorage;
use super::util::{create_promise, PromiseMap, ProposeFuture, ProposePromise};
use crate::DownloadBuilder;

#[derive(Debug)]
enum InnerMsg {
    ApplyResult((u64, u64)),
    Replicated((u64, u64, u64)),
    SnapshotApplied((bool, u64, FileBasedSnapshotMeta)),
    Checkpointed((u64, FileBasedSnapshotMeta, HashMap<u64, u64>)),
}

#[derive(Debug)]
struct LogReader<'a, F>
where
    F: FiniteStateMachine,
{
    storage: &'a LogStorage,
    fsm: &'a F,
}

impl<'a, F> sdcons::LogReader for LogReader<'a, F>
where
    F: FiniteStateMachine,
{
    fn applied_index_id(&self) -> u64 {
        self.fsm.applied_index_id()
    }

    fn channel_applied_entry_id(&self, channel_id: u64) -> u64 {
        self.fsm.channel_applied_entry_id(channel_id)
    }

    fn read_meta(&self, channel_id: u64, id: u64) -> Result<u64, std::io::Error> {
        use std::io::{Error, ErrorKind};
        match self.storage.read_term(channel_id, id) {
            Ok(term) => Ok(term),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        }
    }

    fn read_entries(
        &self,
        channel_id: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, std::io::Error> {
        use std::io::{Error, ErrorKind};
        match self.storage.read_entries(channel_id, start, end) {
            Ok((_, entries)) => Ok(entries),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        }
    }

    fn read_indexes(&self, start: u64, end: u64) -> Result<Vec<LogIndex>, std::io::Error> {
        use std::io::{Error, ErrorKind};
        match self.storage.read_indexes(start, end) {
            Ok((_, indexes)) => Ok(indexes),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        }
    }
}

#[derive(Debug)]
struct AdvanceTemplate<'a, F: FiniteStateMachine> {
    base_path: &'a str,
    sender: Sender<InnerMsg>,
    applier: &'a mut Applier<F>,
    storage: &'a LogStorage,
    messages: &'a mut Vec<Message>,
    promise_map: &'a mut PromiseMap<Result<F::ApplyResult, sdcons::Error>>,
    loading: &'a mut AtomicBool,
}

impl<'a, F: FiniteStateMachine> sdcons::RoleObserver for AdvanceTemplate<'a, F> {
    fn start_leading(&self, channel_id: u64, term: u64) {}
    fn stop_leading(&self, channel_id: u64, term: u64) {}
    fn start_following(&self, channel_id: u64, term: u64) {}
    fn stop_following(&self, channel_id: u64, term: u64) {}
}

impl<'a, F: FiniteStateMachine> AdvanceTemplate<'a, F> {
    fn new(
        base_path: &'a str,
        sender: Sender<InnerMsg>,
        applier: &'a mut Applier<F>,
        storage: &'a LogStorage,
        messages: &'a mut Vec<Message>,
        promise_map: &'a mut PromiseMap<Result<F::ApplyResult, sdcons::Error>>,
        loading: &'a mut AtomicBool,
    ) -> Self {
        AdvanceTemplate {
            base_path,
            sender,
            applier,
            storage,
            messages,
            promise_map,
            loading,
        }
    }
}

impl<'a, F: FiniteStateMachine> sdcons::AdvanceTemplate for AdvanceTemplate<'a, F> {
    fn submit_read_replies(&mut self, finished_reads: &mut HashMap<u64, u64>) {
        if finished_reads.is_empty() {
            return;
        }

        self.applier.apply_read_indexes(finished_reads.clone());
    }

    fn submit_apply_task(&mut self, first_id: u64, last_id: u64, entries: &mut Vec<Entry>) {
        if entries.is_empty() {
            return;
        }

        debug!("submit apply task in range [{}, {}]", first_id, last_id);

        let requests = entries.iter().map(|e| e.request_id).collect::<Vec<_>>();
        let promise_map = self
            .promise_map
            .drain_filter(|id, _| requests.contains(id))
            .collect();
        assert_eq!(self.loading.load(Ordering::Acquire), false);
        self.applier.apply(first_id, last_id, entries, promise_map);
        self.sender
            .send(InnerMsg::ApplyResult((first_id, last_id)))
            .unwrap();
    }

    fn apply_snapshot(&mut self, snapshot_desc: &SnapshotDesc) {
        let sender = self.sender.clone();
        self.loading.store(true, Ordering::AcqRel);
        self.applier
            .apply_snapshot(snapshot_desc.clone(), move |received, id, meta| {
                sender
                    .send(InnerMsg::SnapshotApplied((received, id, meta)))
                    .unwrap();
            });
    }

    fn checkpoint(&mut self) {
        let sender = self.sender.clone();

        self.applier.checkpoint(
            format!("{}/checkpoint", self.base_path),
            move |timestamp_nanos, snapshot_meta, applied_ids| {
                sender
                    .send(InnerMsg::Checkpointed((timestamp_nanos, snapshot_meta, applied_ids)))
                    .unwrap();
            },
        );
    }

    fn build_replicate_msgs(&mut self, builders: &mut Vec<sdcons::MsgBuilder>) {
        for builder in builders {
            let channel_id = builder.channel_id();
            let start_id = builder.from_id();
            let next_id = builder.until_id() + 1;
            let msg = if channel_id == constant::INDEX_CHANNEL_ID {
                let (prev_term, indexes) = self
                    .storage
                    .read_indexes(start_id, next_id)
                    .expect("read rocksdb");
                sdcons::MsgBuilder::build_index_msg(&builder, prev_term, indexes)
            } else {
                let (prev_term, entries) = self
                    .storage
                    .read_entries(channel_id, start_id, next_id)
                    .expect("read rocksdb");
                sdcons::MsgBuilder::build_append_msg(&builder, prev_term, entries)
            };
            self.messages.push(msg);
            self.sender
                .send(InnerMsg::Replicated((builder.remote_id(), channel_id, next_id)))
                .unwrap();
        }
    }

    fn emit_msgs(&mut self, msgs: &mut Vec<Message>) {
        self.messages.extend(msgs.iter().cloned());
    }
}

#[derive(Debug)]
pub struct LogMetaView<'a> {
    storage: &'a LogStorage,
    snap_mgr: &'a SnapshotMgr,
}

impl<'a> sdcons::LogMetaView for LogMetaView<'a> {
    fn membership(&self) -> HashMap<u64, MemberState> {
        self.storage.membership()
    }

    fn hard_states(&self) -> HashMap<u64, HardState> {
        self.storage.hard_states()
    }

    fn range_of(&self, channel_id: u64) -> (u64, u64) {
        self.storage.range(channel_id)
    }

    fn latest_snapshot(&self) -> Option<SnapshotDesc> {
        self.snap_mgr.latest_snapshot()
    }
}

#[derive(Debug)]
pub struct Node<F: FiniteStateMachine> {
    id: u64,
    path: String,
    pub request_id: u64,
    pub applier: Applier<F>,
    pub storage: LogStorage,
    pub raw_node: RawNode,
    pub messages: Vec<Message>,
    pub promise_map: PromiseMap<Result<F::ApplyResult, sdcons::Error>>,
    pub pending_reads: Vec<ProposePromise<Result<(), sdcons::Error>>>,
    pub snapshot_loading: AtomicBool,

    snap_mgr: SnapshotMgr,

    release_memory_tick: usize,
    release_log_entries_tick: usize,

    sender: Sender<InnerMsg>,
    receiver: Receiver<InnerMsg>,
    waker: Option<Waker>,
    ready: bool,
}

impl<F: FiniteStateMachine> Node<F> {
    pub fn build_if_not_exists(id: u64, path: &str, members: Vec<u64>) -> Result<(), Error> {
        LogStorage::build_if_not_exists(id, &format!("{}/wal", path), members)?;
        Ok(())
    }

    pub fn new(id: u64, path: &str, fsm: F, builder: DownloadBuilder) -> Result<Node<F>, Error> {
        let storage = LogStorage::new(id, &format!("{}/wal", path))?;
        let mut opts = SdconsOption::default();
        let log_reader = LogReader {
            storage: &storage,
            fsm: &fsm,
        };
        let snap_mgr = SnapshotMgr::new(id, &format!("{}/checkpoint", path))?;
        let log_view = LogMetaView {
            storage: &storage,
            snap_mgr: &snap_mgr,
        };

        info!("build node with options: {:?}", opts);
        let raw_node = RawNode::build(id, opts, &log_view, &log_reader)?;
        let applier = Applier::new(id, fsm, builder);

        let (tx, rx) = channel();
        Ok(Node {
            id,
            path: String::from(path),
            request_id: constant::FIRST_USER_REQUEST,
            applier,
            storage,
            raw_node,
            messages: Vec::new(),
            promise_map: PromiseMap::new(),
            pending_reads: Vec::new(),
            snapshot_loading: AtomicBool::new(false),
            release_memory_tick: 0,
            release_log_entries_tick: 0,
            snap_mgr,
            sender: tx,
            receiver: rx,
            waker: None,
            ready: false,
        })
    }

    pub fn node_id(&self) -> u64 {
        self.id
    }

    pub fn tick(&mut self) {
        self.raw_node.tick();
        self.fire();
    }

    pub fn step(&mut self, msg: Message) {
        self.raw_node.step(msg);
        self.fire();
    }

    pub fn submit_task(
        &mut self,
        task: Task,
    ) -> ProposeFuture<Result<F::ApplyResult, sdcons::Error>> {
        let (future, mut promise) = create_promise::<Result<F::ApplyResult, sdcons::Error>>();
        if let Err(err) = self.raw_node.submit_task(self.request_id, task) {
            promise.finish(Err(err));
        } else {
            self.promise_map.insert(self.request_id, promise);
            self.request_id += 1;
            self.fire();
        }
        future
    }

    pub fn leased_read(&mut self) -> ProposeFuture<Result<(), sdcons::Error>> {
        let (future, promise) = create_promise::<Result<(), sdcons::Error>>();
        self.pending_reads.push(promise);
        self.fire();
        future
    }

    pub fn change_config(&mut self, members: Vec<u64>) -> Result<u64, Error> {
        self.fire();
        Ok(self.raw_node.change_config(members)?)
    }

    pub fn control(&mut self, _c: Control) -> Result<(), Error> {
        self.fire();
        Ok(self.raw_node.control(_c)?)
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.raw_node.leader_id()
    }

    fn comsume_inner_msgs(&mut self) -> bool {
        let mut cnt = 0;
        while let Ok(msg) = self.receiver.try_recv() {
            cnt += 1;
            match msg {
                InnerMsg::ApplyResult((from, to)) => {
                    self.raw_node.submit_apply_result(from, to);
                }
                InnerMsg::Replicated((remote_id, channel_id, next_id)) => {
                    self.raw_node.log_replicated(remote_id, channel_id, next_id);
                }
                InnerMsg::SnapshotApplied((received, id, meta)) => {
                    if received {
                        self.storage.reset(&meta.desc).expect("reset log storage");
                    }

                    let hard_states = self.storage.hard_states();
                    self.raw_node
                        .finish_snapshot_loading(received, &hard_states, &meta.desc);
                    self.snapshot_loading.store(false, Ordering::Release);
                    self.snap_mgr.add_exists_snapshot(id, meta);
                }
                InnerMsg::Checkpointed((timestamp_nanos, mut snapshot_meta, applied_ids)) => {
                    let channel_metas = applied_ids
                        .iter()
                        .map(|(channel_id, applied_id)| {
                            (
                                *channel_id,
                                EntryMeta {
                                    id: *applied_id,
                                    term: self
                                        .storage
                                        .read_term(*channel_id, *applied_id)
                                        .unwrap_or(constant::INITIAL_TERM),
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    snapshot_meta.desc.members = self.storage.membership();
                    snapshot_meta.desc.channel_metas = channel_metas;
                    self.snap_mgr
                        .add_new_snapshot(timestamp_nanos, snapshot_meta);
                    self.raw_node.finish_checkpoint();
                }
                _ => {}
            }
        }
        cnt > 0
    }

    pub fn advance(&mut self) {
        self.ready = false;

        if !self.pending_reads.is_empty() {
            let mut pending_reads = Vec::new();
            std::mem::swap(&mut pending_reads, &mut self.pending_reads);
            match self.raw_node.leased_read() {
                Ok(req_id) => self.applier.save_read_requests(req_id, pending_reads),
                Err(err) => self.applier.notify_failed_requests(err, pending_reads),
            }
        }

        let mut template = AdvanceTemplate::new(
            &self.path,
            self.sender.clone(),
            &mut self.applier,
            &self.storage,
            &mut self.messages,
            &mut self.promise_map,
            &mut self.snapshot_loading,
        );
        let write_task = {
            let log_meta_view = LogMetaView {
                storage: &self.storage,
                snap_mgr: &self.snap_mgr,
            };
            self.raw_node.advance(&log_meta_view, &mut template)
        };

        let mut ready = false;
        if let Some(task) = write_task {
            ready = true;
            let s = &mut self.storage;
            let write_result = s.write(&task).expect("write rocksdb");
            for (channel_id, range) in write_result {
                self.raw_node
                    .submit_stable_result(channel_id, range.0, range.1);
            }
            if let Some(post_ready) = task.post_ready {
                let mut template = AdvanceTemplate::new(
                    &self.path,
                    self.sender.clone(),
                    &mut self.applier,
                    &self.storage,
                    &mut self.messages,
                    &mut self.promise_map,
                    &mut self.snapshot_loading,
                );
                self.raw_node.post_advance(post_ready, &mut template);
            }
        }

        if self.comsume_inner_msgs() {
            ready = true;
        }

        self.try_recycle_resources();
        if ready {
            self.fire();
        }
    }

    pub fn try_recycle_resources(&mut self) {
        self.release_memory_tick += 1;
        if self.release_memory_tick > 100 {
            self.release_memory_tick = 0;
            self.raw_node
                .control(Control::ReleaseMemory)
                .expect("release memory");
        }

        if self.snap_mgr.count() > 1 && !self.raw_node.is_sending_snapshot() {
            self.snap_mgr.release_useless_snapshots();
        }

        self.release_log_entries_tick += 1;
        if self.release_log_entries_tick > 10000
            && self.storage.approximate_size() > 256 * 1024 * 1024
        {
            self.release_log_entries_tick = 0;
            if let Some(snapshot_desc) = self.snap_mgr.latest_snapshot() {
                let applied_ids = snapshot_desc.applied_ids();
                self.storage
                    .release_log_entries(applied_ids)
                    .expect("release log entries");
            } else {
                self.raw_node
                    .control(Control::Checkpoint)
                    .expect("checkpoint");
            }
        }
    }

    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }

    pub fn set_waker(&mut self, waker: Waker) {
        self.waker = Some(waker);
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    fn fire(&mut self) {
        self.ready = true;
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeFuture<F: FiniteStateMachine> {
    pub node: Arc<Mutex<Node<F>>>,
}

impl<F> NodeFuture<F>
where
    F: FiniteStateMachine,
{
    pub fn new(node: Arc<Mutex<Node<F>>>) -> NodeFuture<F> {
        NodeFuture { node }
    }
}

impl<F> Future for NodeFuture<F>
where
    F: FiniteStateMachine,
{
    type Output = Arc<Mutex<Node<F>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut node = self.node.lock().unwrap();
        if node.is_ready() {
            Poll::Ready(self.node.clone())
        } else {
            node.set_waker(cx.waker().clone());
            Poll::Pending
        }
    }
}
