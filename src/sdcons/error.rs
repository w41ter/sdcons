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

use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    /// Current node isn't the leader of the channel of self.
    #[error("I am not leader")]
    NotLeader,

    /// Indicates there has already exists another transfering.
    #[error("another transfering is in progress")]
    Transfering,

    /// The state machine is busy, the submited request is rejected.
    #[error("system is busy")]
    Busy,

    #[error("broken io request")]
    Io(Arc<std::io::Error>),

    #[error("invalid node id (expected in range [1, {}))", std::u64::MAX)]
    InvalidNodeId(u64),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(Arc::new(err))
    }
}
