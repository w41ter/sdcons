//! # sdcons
//!
//! `sdcons` is an implementation of geo-replicated distributed consensus
//! algorithm. It is based on the paper:
//!
//! > SDPaxos: Building Efficient Semi-Decentralized Geo-replicated State
//! Machines (ACM Symposium on Cloud Computing 2018, SoCC '18)
//!
//! Like original SDPaxos, `sdcons` provides one-round-trip latency reading and
//! writing capabilities under realistic configurations (deployed with three or
//! five nodes).
//!
//! In addition, I ported some optimizations mentioned in the raft algorithm to
//! SDPaxos. Including:
//!
//! - log term
//! - leader election
//! - leadership transfering
//! - membership configuration (joint consensus)
//! - batched & pipelined log replication
//! - lease reading
//!
//! These optimizations make SDPaxos easier to implement and verify.
//!
//! However, the current `sdcons` has not yet implemented the **Straggler
//! detection** mentioned in the SDPaxos paper.
//!
//! Unlike SDPaxos, `sdcons` uses the term `channel` to represent the paxos
//! commiting progress. The Ordering instance is represented as Index, the
//! Command instance is represented as Entry. Because the term index is used to
//! represent Ordering instance, I choose term id to represent the index in
//! raft.

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

extern crate chrono;
extern crate log;
extern crate rand;
extern crate serde;
extern crate thiserror;

mod cons;
mod progress;
mod raw_node;
mod error;

pub mod constant;
pub mod storage;
pub mod types;

pub use crate::error::Error;
pub use crate::cons::{Control, MsgBuilder, SdconsOption};
pub use crate::raw_node::{
    AdvanceTemplate, LogReader, PostReady, RawNode, RoleObserver, WriteTask,
};
pub use crate::storage::LogMetaView;
