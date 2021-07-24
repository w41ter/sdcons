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

use std::io;
use std::net::SocketAddr;

use log::warn;
use tarpc::context;
use thiserror::Error;

use crate::proto::kv;
use crate::resolver::AddressResolver;
use crate::sdk::new_kv_connect;
use crate::sdk::KvChannelMap;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("connect to remote timeout")]
    ConnectTimeout(#[from] io::Error),
    #[error("request timeout")]
    RequestTimeout(String),
    #[error("no available connection")]
    NoAvailableConnection,
    #[error("not leader")]
    NotLeader(Option<u64>),
    #[error("inner")]
    Inner(String),
    #[error("cas failed")]
    CasFailed,
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    // Speicified the target channel address.
    pub addr: Option<SocketAddr>,
    pub timeout_ms: u64,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            addr: None,
            timeout_ms: std::i64::MAX as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriteOptions {
    // Speicified the target channel address.
    pub addr: Option<SocketAddr>,
    pub timeout_ms: u64,
    pub cond: kv::PutCond,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            addr: None,
            timeout_ms: std::i64::MAX as u64,
            cond: kv::PutCond::None,
        }
    }
}

#[derive(Debug)]
struct CostRecord {
    pub time: chrono::DateTime<chrono::Local>,
    pub total_cost_ms: u64,
    pub count: usize,
}

impl CostRecord {
    fn new() -> CostRecord {
        CostRecord {
            time: chrono::Local::now(),
            total_cost_ms: 0,
            count: 0,
        }
    }

    fn switch(&mut self) -> u64 {
        let now = chrono::Local::now();
        let cost_ms = (self.time - now).num_milliseconds();
        if cost_ms > 0 {
            self.count += 1;
            self.total_cost_ms += cost_ms as u64;
            self.time = now;
            return cost_ms as u64;
        }
        return 0;
    }

    fn total_cost(&self) -> u64 {
        self.total_cost_ms
    }

    fn delay_ms(&self) -> u64 {
        let delay = 1 << self.count;
        if delay > 256 {
            256
        } else {
            delay
        }
    }
}

#[derive(Debug)]
pub struct Client<R: AddressResolver> {
    channel_map: KvChannelMap,
    address_resolver: R,
}

impl<R> Client<R>
where
    R: AddressResolver,
{
    pub fn new(resolver: R) -> Client<R> {
        Client {
            channel_map: KvChannelMap::new(),
            address_resolver: resolver,
        }
    }
}

impl<R> Client<R>
where
    R: AddressResolver,
{
    pub async fn resolve_channel(
        &self,
        addr: &Option<SocketAddr>,
    ) -> Option<(SocketAddr, kv::KvClient)> {
        let addr = match addr {
            Some(addr) => *addr,
            None => match self.address_resolver.next() {
                Some(addr) => addr,
                None => return None,
            },
        };
        let res = self.channel_map.select(addr);
        if res.is_none() {
            let channel = match new_kv_connect(addr).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("make connect to {}: {}", addr, e.to_string());
                    return None;
                }
            };
            self.channel_map.insert_if_not_exists(addr, channel.clone());
            Some((addr, channel))
        } else {
            res.map(|channel| (addr, channel))
        }
    }

    pub async fn get(
        &self,
        ctx: context::Context,
        options: &ReadOptions,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, KvError> {
        match self.resolve_channel(&options.addr).await {
            None => Err(KvError::NoAvailableConnection),
            Some((addr, channel)) => {
                let resp = match channel.get(ctx, key).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("get request from {}: {}", addr, e.to_string());
                        self.channel_map.erase(addr);
                        return Err(e)?;
                    }
                };
                match resp.header.code {
                    kv::ErrorCode::OK => Ok(resp.value),
                    kv::ErrorCode::NotLeader => Err(KvError::NotLeader(None)),
                    _ => Err(KvError::Inner(resp.header.msg)),
                }
            }
        }
    }

    pub async fn put(
        &self,
        ctx: context::Context,
        options: &WriteOptions,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, KvError> {
        match self.resolve_channel(&options.addr).await {
            None => Err(KvError::NoAvailableConnection),
            Some((addr, channel)) => {
                let req = kv::PutRequest {
                    key,
                    value,
                    cond: options.cond.clone(),
                };
                let resp = match channel.put(ctx, req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("put request from {}: {}", addr, e.to_string());
                        self.channel_map.erase(addr);
                        return Err(e)?;
                    }
                };
                match resp.header.code {
                    kv::ErrorCode::OK => Ok(resp.previous_value),
                    kv::ErrorCode::NotLeader => Err(KvError::NotLeader(None)),
                    kv::ErrorCode::CasFailed => Err(KvError::CasFailed),
                    _ => Err(KvError::Inner(resp.header.msg)),
                }
            }
        }
    }

    pub async fn change_config(
        &self,
        ctx: context::Context,
        addr: SocketAddr,
        new_members: Vec<u64>,
    ) -> Result<(), KvError> {
        match self.resolve_channel(&Some(addr)).await {
            None => Err(KvError::NoAvailableConnection),
            Some((addr, channel)) => {
                let resp = match channel.change_config(ctx, new_members).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("change config request from {}: {}", addr, e.to_string());
                        self.channel_map.erase(addr);
                        return Err(e)?;
                    }
                };
                if resp.header.code == kv::ErrorCode::NotLeader {
                    Err(KvError::NotLeader(resp.actual_leader))
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RetryClient<R: AddressResolver> {
    client: Client<R>,
}

impl<R> RetryClient<R>
where
    R: AddressResolver,
{
    pub fn new(resolver: R) -> RetryClient<R> {
        RetryClient {
            client: Client::new(resolver),
        }
    }
}

impl<R> RetryClient<R>
where
    R: AddressResolver,
{
    pub async fn get(
        &self,
        ctx: context::Context,
        options: &ReadOptions,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, KvError> {
        use tokio::time::{sleep, Duration};

        let mut saved_options = options.clone();
        let mut record = CostRecord::new();
        loop {
            match self.client.get(ctx, &saved_options, key.clone()).await {
                Ok(v) => return Ok(v),
                Err(KvError::NotLeader(_)) => {
                    let cost_ms = record.switch();
                    if cost_ms < saved_options.timeout_ms {
                        saved_options.timeout_ms -= cost_ms;
                        sleep(Duration::from_millis(record.delay_ms())).await;
                        continue;
                    }
                    warn!("get request timeout, total cost {} ms", record.total_cost());
                    return Err(KvError::RequestTimeout(format!(
                        "total cost {} ms",
                        record.total_cost()
                    )));
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn put(
        &self,
        ctx: context::Context,
        options: &WriteOptions,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, KvError> {
        use tokio::time::{sleep, Duration};

        let mut saved_options = options.clone();
        let mut record = CostRecord::new();
        loop {
            match self
                .client
                .put(ctx, options, key.clone(), value.clone())
                .await
            {
                Ok(v) => return Ok(v),
                Err(KvError::NotLeader(_)) => {
                    let cost_ms = record.switch();
                    if cost_ms < saved_options.timeout_ms {
                        saved_options.timeout_ms -= cost_ms;
                        sleep(Duration::from_millis(record.delay_ms())).await;
                        continue;
                    }
                    warn!("put request timeout, total cost {} ms", record.total_cost());
                    return Err(KvError::RequestTimeout(format!(
                        "total cost {} ms",
                        record.total_cost()
                    )));
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn change_config(
        &self,
        ctx: context::Context,
        addr: SocketAddr,
        new_members: Vec<u64>,
    ) -> Result<(), KvError> {
        self.client.change_config(ctx, addr, new_members).await
    }
}
