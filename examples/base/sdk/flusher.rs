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
use sdcons::types::Message;
use tarpc::context;
use thiserror::Error;

use crate::proto::cons;
use crate::resolver::AddressResolver;
use crate::sdk::new_flusher_connect;
use crate::sdk::FlusherChannelMap;

#[derive(Error, Debug)]
pub enum FlusherError {
    #[error("connect to remote timeout")]
    Timeout(#[from] io::Error),
    #[error("no available connection")]
    NoAvailableConnection,
}

#[derive(Debug)]
pub struct MsgSender<R: AddressResolver> {
    channel_map: FlusherChannelMap,
    address_resolver: R,
}

impl<R> MsgSender<R>
where
    R: AddressResolver,
{
    pub fn new(resolver: R) -> MsgSender<R> {
        MsgSender {
            channel_map: FlusherChannelMap::new(),
            address_resolver: resolver,
        }
    }
}

impl<R> MsgSender<R>
where
    R: AddressResolver,
{
    pub async fn resolve_channel(&self, id: u64) -> Option<(SocketAddr, cons::FlusherClient)> {
        match self.address_resolver.resolve(id) {
            None => {
                warn!("resolve address of {}: no such node exists in address table", id);
                None
            }
            Some(addr) => {
                let res = self.channel_map.select(addr);
                if res.is_none() {
                    let channel = match new_flusher_connect(addr).await {
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
        }
    }

    pub async fn send(&self, ctx: context::Context, msg: Message) -> Result<(), FlusherError> {
        match self.resolve_channel(msg.to).await {
            None => Err(FlusherError::NoAvailableConnection),
            Some((addr, channel)) => {
                let req = cons::SdconsRequest { msgs: vec![msg] };
                match channel.send(ctx, req).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        self.channel_map.erase(addr);
                        Err(e)?
                    }
                }
            }
        }
    }
}
