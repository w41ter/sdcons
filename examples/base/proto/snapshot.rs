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

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotDetail {
    pub ref_id: u32,
    pub checksum: u32,
    pub applied_ids: HashMap<u64, u64>,
    pub resources: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResourceMeta {
    pub resource_id: u64,
    pub checksum: u32,
    pub size: usize,
    pub uri: String,
}

// Usage:
//      let detail = Snapshot::connect("target_snapshot_url");
//      for uri in &detail.resources {
//          let resource_id = Snapshot::open(detail.ref_id, uri.clone());
//          loop {
//              match Snapshot::next(resource_id) {
//                  Some(p) => {}
//                  None => break;
//              }
//          }
//          let resource_meta = Snapshot::close(resource_id);
//      }
//      Snapshot::shutdown(detail.ref_id);
//
#[tarpc::service]
pub trait Snapshot {
    async fn connect(url: String) -> Result<SnapshotDetail, String>;
    async fn shutdown(ref_id: u32) -> Option<()>;

    async fn open(ref_id: u32, uri: String) -> Result<u64, String>;
    async fn next(resource_id: u64) -> Option<Vec<u8>>;
    async fn close(resource_id: u64) -> Option<ResourceMeta>;
}
