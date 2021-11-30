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
use std::fmt::Debug;
use std::iter::Iterator;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Error;
use sdcons::types::Message;
use serde::{Deserialize, Serialize};

pub trait AddressResolver: Debug {
    fn insert(&self, id: u64, addr: SocketAddr);
    fn resolve(&self, id: u64) -> Option<SocketAddr>;
    fn resolve_msg(&self, msg: &Message) -> Option<SocketAddr>;
    fn next(&self) -> Option<SocketAddr>;
    fn all(&self) -> HashMap<u64, SocketAddr>;
    fn ids(&self) -> Vec<u64>;
    fn refresh(&self) -> Result<(), Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddressMap {
    pub cur_idx: usize,
    pub addrs: Vec<SocketAddr>,
    pub address_map: HashMap<u64, SocketAddr>,
}

#[derive(Debug, Clone)]
pub struct FileBasedResolver {
    file_name: String,
    clients: Arc<Mutex<AddressMap>>,
}

impl FileBasedResolver {
    pub fn new(file_name: String) -> FileBasedResolver {
        let clients = AddressMap {
            cur_idx: 0,
            addrs: Vec::new(),
            address_map: HashMap::new(),
        };
        FileBasedResolver {
            file_name,
            clients: Arc::new(Mutex::new(clients)),
        }
    }
}

impl AddressResolver for FileBasedResolver {
    fn insert(&self, id: u64, addr: SocketAddr) {
        let mut inner = self.clients.lock().unwrap();
        inner.address_map.insert(id, addr);
    }

    fn resolve(&self, id: u64) -> Option<SocketAddr> {
        let inner = self.clients.lock().unwrap();
        inner.address_map.get(&id).cloned()
    }

    fn resolve_msg(&self, msg: &Message) -> Option<SocketAddr> {
        self.resolve(msg.to)
    }

    fn next(&self) -> Option<SocketAddr> {
        let mut inner = self.clients.lock().unwrap();
        if inner.addrs.is_empty() {
            return None;
        }

        let cur_idx = (inner.cur_idx + 1) % inner.addrs.len();
        inner.cur_idx = cur_idx;
        Some(inner.addrs[cur_idx])
    }

    fn all(&self) -> HashMap<u64, SocketAddr> {
        self.clients.lock().unwrap().address_map.clone()
    }

    fn ids(&self) -> Vec<u64> {
        self.clients
            .lock()
            .unwrap()
            .address_map
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }

    fn refresh(&self) -> Result<(), Error> {
        let content = std::fs::read_to_string(&self.file_name)?;
        let mut address_map: HashMap<u64, SocketAddr> = serde_json::from_str(&content)?;
        let mut addrs = address_map.iter().map(|(_, v)| *v).collect::<Vec<_>>();
        let mut inner = self.clients.lock().unwrap();
        std::mem::swap(&mut inner.address_map, &mut address_map);
        std::mem::swap(&mut inner.addrs, &mut addrs);
        if inner.cur_idx >= inner.address_map.len() {
            inner.cur_idx = 0;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StaticResolver {
    clients: Arc<Mutex<AddressMap>>,
}

impl StaticResolver {
    pub fn new(address_map: HashMap<u64, SocketAddr>) -> StaticResolver {
        let addrs = address_map.iter().map(|(_, addr)| addr.clone()).collect();
        let clients = AddressMap {
            cur_idx: 0,
            addrs,
            address_map,
        };

        StaticResolver {
            clients: Arc::new(Mutex::new(clients)),
        }
    }
}

impl AddressResolver for StaticResolver {
    fn insert(&self, id: u64, addr: SocketAddr) {
        let mut inner = self.clients.lock().unwrap();
        inner.address_map.insert(id, addr);
    }

    fn resolve(&self, id: u64) -> Option<SocketAddr> {
        let inner = self.clients.lock().unwrap();
        inner.address_map.get(&id).cloned()
    }

    fn resolve_msg(&self, msg: &Message) -> Option<SocketAddr> {
        self.resolve(msg.to)
    }

    fn next(&self) -> Option<SocketAddr> {
        let mut inner = self.clients.lock().unwrap();
        if inner.addrs.is_empty() {
            return None;
        }

        let cur_idx = (inner.cur_idx + 1) % inner.addrs.len();
        inner.cur_idx = cur_idx;
        Some(inner.addrs[cur_idx])
    }

    fn all(&self) -> HashMap<u64, SocketAddr> {
        self.clients.lock().unwrap().address_map.clone()
    }

    fn ids(&self) -> Vec<u64> {
        self.clients
            .lock()
            .unwrap()
            .address_map
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }

    fn refresh(&self) -> Result<(), Error> {
        Ok(())
    }
}
