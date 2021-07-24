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
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use base::proto::snapshot::*;
use tarpc::context;

use crate::node::{FileBasedSnapshotMeta, FileMeta};

#[derive(Debug)]
struct Channel {
    url: String,
    last_heartbeat_ns: i64,
    next_resource_id: u32,
    resource_chunk_meta: HashMap<u32, usize>,
    resource_map: HashMap<u32, String>,

    snapshot_meta: FileBasedSnapshotMeta,
}

#[derive(Debug)]
struct InnerService {
    next_service_id: u32,
    channels: HashMap<u32, Channel>,
}

#[derive(Debug, Clone)]
pub struct SnapshotService {
    base_path: String,
    inner: Arc<Mutex<InnerService>>,
}

impl SnapshotService {
    pub fn new(base_path: String) -> SnapshotService {
        SnapshotService {
            base_path,
            inner: Arc::new(Mutex::new(InnerService {
                next_service_id: 1,
                channels: HashMap::new(),
            })),
        }
    }

    pub fn setup_channel_recycle(&self) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let exit_flags = Arc::new(AtomicBool::new(false));
        let return_exit_flags = exit_flags.clone();
        let inner = self.inner.clone();
        let now = chrono::Local::now().timestamp_nanos();
        let handler = std::thread::spawn(move || {
            while !exit_flags.load(Ordering::Acquire) {
                let mut inner_service = inner.lock().unwrap();
                let timeout_channels = inner_service
                    .channels
                    .iter()
                    .filter(|(_id, c)| now - c.last_heartbeat_ns >= 1000 * 1000 * 1000 * 3) // 3s
                    .map(|(id, _c)| *id)
                    .collect::<Vec<_>>();
                for channel_id in timeout_channels {
                    inner_service.channels.remove(&channel_id);
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });

        (return_exit_flags, handler)
    }
}

fn to_error_string<E: std::error::Error>(e: E) -> String {
    format!("{}", e)
}

#[tarpc::server]
impl Snapshot for SnapshotService {
    async fn connect(self, _: context::Context, url: String) -> Result<SnapshotDetail, String> {
        let meta_name = format!("{}/META", url);
        let meta_file = File::open(meta_name).map_err(to_error_string)?;
        let meta: FileBasedSnapshotMeta =
            serde_json::from_reader(&meta_file).map_err(to_error_string)?;

        let channel = Channel {
            url,
            next_resource_id: 1,
            last_heartbeat_ns: chrono::Local::now().timestamp_nanos(),
            resource_chunk_meta: HashMap::new(),
            resource_map: HashMap::new(),
            snapshot_meta: meta.clone(),
        };

        let service_id = {
            let mut inner_service = self.inner.lock().unwrap();
            let service_id = inner_service.next_service_id;
            inner_service.next_service_id += 1;
            inner_service.channels.insert(service_id, channel);
            service_id
        };

        let mut resources = meta
            .files
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        resources.push(String::from("META"));

        Ok(SnapshotDetail {
            ref_id: service_id,
            checksum: meta.checksum,
            applied_ids: meta
                .desc
                .channel_metas
                .iter()
                .map(|(c, m)| (*c, m.id))
                .collect(),
            resources,
        })
    }

    async fn shutdown(self, _: context::Context, ref_id: u32) -> Option<()> {
        let mut inner_service = self.inner.lock().unwrap();
        if let None = inner_service.channels.remove(&ref_id) {
            return None;
        }
        if inner_service.channels.is_empty() {
            inner_service.next_service_id = 1;
        }
        Some(())
    }

    async fn open(self, _: context::Context, ref_id: u32, uri: String) -> Result<u64, String> {
        let mut inner_service = self.inner.lock().unwrap();
        let channel = match inner_service.channels.get_mut(&ref_id) {
            Some(c) => c,
            None => return Err(String::from("no such channel exists")),
        };
        channel.last_heartbeat_ns = chrono::Local::now().timestamp_nanos();
        if channel.snapshot_meta.files.get(&uri).is_none() {
            return Err(String::from("no such file exists"));
        };
        let resource_id = channel.next_resource_id;
        channel.next_resource_id += 1;
        channel.resource_chunk_meta.insert(resource_id, 0);
        channel.resource_map.insert(resource_id, uri);
        let global_resource_id = ((ref_id as u64) << 32) | (resource_id as u64);
        Ok(global_resource_id)
    }

    async fn next(self, _: context::Context, resource_id: u64) -> Option<Vec<u8>> {
        let local_resource_id = resource_id as u32;
        let service_id = (resource_id >> 32) as u32;

        let (uri, offset) = {
            let mut inner_service = self.inner.lock().unwrap();
            let channel = match inner_service.channels.get_mut(&service_id) {
                Some(c) => c,
                None => return None,
            };
            channel.last_heartbeat_ns = chrono::Local::now().timestamp_nanos();
            let offset = match channel.resource_chunk_meta.get(&local_resource_id) {
                Some(v) => v,
                None => return None,
            };
            let uri = channel.resource_map.get(&local_resource_id).unwrap();
            (uri.clone(), *offset)
        };

        let filename = format!("{}/{}", self.base_path, uri);
        let mut file = match File::open(filename) {
            Ok(f) => f,
            Err(_) => return None,
        };

        match file.seek(SeekFrom::Start(offset as u64)) {
            Ok(_) => (),
            Err(_) => return None,
        };

        let mut buf = Vec::with_capacity(4096);
        buf.resize(4096, 0);
        loop {
            let size = match file.read(&mut buf) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return None,
            };
            buf.resize(size, 0);

            {
                let mut inner_service = self.inner.lock().unwrap();
                let channel = match inner_service.channels.get_mut(&service_id) {
                    Some(c) => c,
                    None => return None,
                };
                match channel.resource_chunk_meta.get_mut(&local_resource_id) {
                    Some(v) if *v == offset => *v += size,
                    _ => return None,
                }
            }

            return Some(buf);
        }
    }

    async fn close(self, _: context::Context, resource_id: u64) -> Option<ResourceMeta> {
        let local_resource_id = resource_id as u32;
        let service_id = (resource_id >> 32) as u32;
        let mut inner_service = self.inner.lock().unwrap();
        let channel = match inner_service.channels.get_mut(&service_id) {
            Some(c) => c,
            None => return None,
        };
        channel.last_heartbeat_ns = chrono::Local::now().timestamp_nanos();
        if channel
            .resource_chunk_meta
            .remove(&local_resource_id)
            .is_none()
        {
            return None;
        }
        let uri = channel.resource_map.get(&local_resource_id).unwrap();
        let file_meta = channel.snapshot_meta.files.get(uri).unwrap();
        Some(ResourceMeta {
            resource_id,
            checksum: file_meta.checksum,
            size: 0,
            uri: uri.clone(),
        })
    }
}
