//! The crate `constant` defines a set constant used by sdcons.

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

/// A special value is used to mark illegal or invalid id of node.
pub const INVALID_NODE_ID: u64 = std::u64::MAX;

/// A special value is used to represent the index channel.
pub const INDEX_CHANNEL_ID: u64 = 0;

/// A special value is used to mark invalid log, any valid id of index or entry
/// will large than zero.
pub const INVALID_ID: u64 = 0;

/// The initialized term of new constructed channels.
pub const INITIAL_TERM: u64 = 0;

/// A special value is used to mark the internal request, eg: no-op index or
/// entry.
pub const INTERNAL_REQUEST: u64 = 0;

/// A special value is used to mark the config change index and entry.
pub const CONFIG_CHANGE_ID: u64 = 1;

/// The request submitted by the user starts with this value.
pub const FIRST_USER_REQUEST: u64 = 2;

/// A helper function is used to detect whether a request is submitted by the
/// internal implemation.
pub fn is_internal_request(request_id: u64) -> bool {
    request_id == INTERNAL_REQUEST || request_id == CONFIG_CHANGE_ID
}
