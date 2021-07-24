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
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug)]
struct InnerChannelMap<C: Clone> {
    cur_idx: usize,
    addrs: Vec<SocketAddr>,
    channels: HashMap<SocketAddr, C>,
}

impl<C> InnerChannelMap<C>
where
    C: Clone,
{
    fn new() -> InnerChannelMap<C> {
        InnerChannelMap {
            cur_idx: 0,
            addrs: Vec::new(),
            channels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelMap<C: Clone> {
    clients: Arc<Mutex<InnerChannelMap<C>>>,
}

impl<C: Clone> ChannelMap<C> {
    pub fn new() -> ChannelMap<C> {
        ChannelMap {
            clients: Arc::new(Mutex::new(InnerChannelMap::new())),
        }
    }

    pub fn select(&self, addr: SocketAddr) -> Option<C> {
        self.clients.lock().unwrap().channels.get(&addr).cloned()
    }

    pub fn next(&self) -> Option<C> {
        let mut inner = self.clients.lock().unwrap();
        if inner.addrs.is_empty() {
            return None;
        }
        let addr = inner.addrs[inner.cur_idx];
        inner.cur_idx = (inner.cur_idx + 1) % inner.addrs.len();
        inner.channels.get(&addr).cloned()
    }

    pub fn insert_if_not_exists(&self, addr: SocketAddr, client: C) -> bool {
        let mut inner = self.clients.lock().unwrap();
        if inner.channels.contains_key(&addr) {
            false
        } else {
            inner.channels.insert(addr, client);
            true
        }
    }

    pub fn erase(&self, addr: SocketAddr) {
        let mut inner = self.clients.lock().unwrap();
        inner.channels.remove(&addr);
        inner.addrs = inner.channels.keys().cloned().collect();
    }
}
