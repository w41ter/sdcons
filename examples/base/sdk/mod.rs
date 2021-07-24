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

pub mod channel;
pub mod client;
pub mod flusher;

use std::net::SocketAddr;

use crate::proto::cons;
use crate::proto::kv;
use crate::proto::snapshot as ps;

pub type FlusherChannelMap = channel::ChannelMap<cons::FlusherClient>;
pub type KvChannelMap = channel::ChannelMap<kv::KvClient>;
pub type SnapshotChannelMap = channel::ChannelMap<ps::SnapshotClient>;

pub async fn new_flusher_connect(addr: SocketAddr) -> std::io::Result<cons::FlusherClient> {
    use tarpc::client::Config;
    use tarpc::serde_transport::tcp;
    use tokio_serde::formats::Json;
    let transport = tcp::connect(addr, Json::default).await?;
    cons::FlusherClient::new(Config::default(), transport).spawn()
}

pub async fn new_kv_connect(addr: SocketAddr) -> std::io::Result<kv::KvClient> {
    use tarpc::client::Config;
    use tarpc::serde_transport::tcp;
    use tokio_serde::formats::Json;
    let transport = tcp::connect(addr, Json::default).await?;
    kv::KvClient::new(Config::default(), transport).spawn()
}

pub async fn new_snapshot_client(addr: SocketAddr) -> std::io::Result<ps::SnapshotClient> {
    use tarpc::client::Config;
    use tarpc::serde_transport::tcp;
    use tokio_serde::formats::Json;
    let transport = tcp::connect(addr, Json::default).await?;
    ps::SnapshotClient::new(Config::default(), transport).spawn()
}
