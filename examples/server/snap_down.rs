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

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::Write;

use base::proto::snapshot;
use base::resolver::AddressResolver;
use base::sdk::new_snapshot_client;
use base::sdk::SnapshotChannelMap;
use crc::crc32::{Digest, IEEE};
use crc::Hasher32;
use log::warn;
use sdcons::types::{Message, SnapshotDesc};
use tarpc::context;
use thiserror::Error;

use crate::node::FileBasedSnapshotMeta;

#[derive(Error, Debug)]
pub enum FlusherError {
    #[error("connect to remote timeout")]
    Timeout(#[from] io::Error),
    #[error("no available connection")]
    NoAvailableConnection,
}

async fn resolve_channel<R>(
    address_resolver: &R,
    id: u64,
) -> std::io::Result<snapshot::SnapshotClient>
where
    R: AddressResolver,
{
    match address_resolver.resolve(id) {
        None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "connect")),
        Some(addr) => new_snapshot_client(addr).await,
    }
}

#[derive(Debug)]
pub struct Downloader {
    local_id: u64,
    base_path: String,
    channel: snapshot::SnapshotClient,
}

fn io_error_string<E: std::error::Error>(e: E) -> String {
    format!("{}", e)
}

impl Downloader {
    pub async fn download(&self, url: String) -> Result<(u64, FileBasedSnapshotMeta), String> {
        let now = chrono::Local::now().timestamp_nanos();
        let ctx = context::current();
        let detail = self
            .channel
            .connect(ctx, url)
            .await
            .map_err(io_error_string)
            .flatten()?;
        for uri in &detail.resources {
            let resource_id = self
                .channel
                .open(ctx, detail.ref_id, uri.clone())
                .await
                .map_err(io_error_string)
                .flatten()?;
            let local_url = format!("{}/{}/{}", self.base_path, now, uri);
            let mut file = File::create(local_url).map_err(io_error_string)?;
            let mut digest = Digest::new(IEEE);
            loop {
                match self
                    .channel
                    .next(ctx, resource_id)
                    .await
                    .map_err(io_error_string)?
                {
                    Some(p) => {
                        file.write(&p).map_err(io_error_string)?;
                        digest.write(&p);
                    }
                    None => break,
                }
            }
            file.sync_all().map_err(io_error_string)?;
            let resource_meta = match self
                .channel
                .close(ctx, resource_id)
                .await
                .map_err(io_error_string)?
            {
                Some(r) => r,
                None => return Err(String::from("pipe borken")),
            };
            if uri == "META" {
                continue;
            }
            let checksum = digest.sum32();
            if checksum != resource_meta.checksum {
                return Err(String::from("wrong checksum"));
            }
        }
        if self
            .channel
            .shutdown(ctx, detail.ref_id)
            .await
            .map_err(io_error_string)?
            .is_none()
        {
            return Err(String::from("pipe borken"));
        }

        let meta_name = format!("{}/{}/META", self.base_path, now);
        let file = File::open(meta_name.clone()).map_err(io_error_string)?;
        let mut snapshot_meta: FileBasedSnapshotMeta =
            serde_json::from_reader(file).map_err(io_error_string)?;
        snapshot_meta.desc.located_id = self.local_id;
        snapshot_meta.desc.url = format!("{}/{}", self.base_path, now);

        let file = File::create(meta_name).map_err(io_error_string)?;
        serde_json::to_writer(&file, &snapshot_meta).map_err(io_error_string)?;

        File::create(format!("{}/{}/OK", self.base_path, now)).map_err(io_error_string)?;

        Ok((now as u64, snapshot_meta))
    }
}

#[derive(Debug, Clone)]
pub struct DownloadBuilder<R>
where
    R: AddressResolver + Clone,
{
    local_id: u64,
    base_path: String,
    resolver: R,
}

impl<R> DownloadBuilder<R>
where
    R: AddressResolver + Clone,
{
    pub fn new(local_id: u64, base_path: String, resolver: R) -> DownloadBuilder<R> {
        DownloadBuilder {
            local_id,
            base_path,
            resolver,
        }
    }
}

impl<R> DownloadBuilder<R>
where
    R: AddressResolver + Clone,
{
    pub async fn build(&self, id: u64) -> Result<Downloader, std::io::Error> {
        let channel = resolve_channel(&self.resolver, id).await?;
        Ok(Downloader {
            local_id: self.local_id,
            base_path: self.base_path.clone(),
            channel,
        })
    }
}
