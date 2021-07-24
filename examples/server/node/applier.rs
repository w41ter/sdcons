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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::{BufReader, Read, Write};

use chrono::Local;
use log::{debug, warn};
use sdcons::types::{Entry, SnapshotDesc};
use thiserror::Error as BasicError;

use super::snap_mgr::{FileBasedSnapshotMeta, FileMeta};
use super::util::{PromiseMap, ProposePromise};
use crate::DownloadBuilder;

struct DigestWriter {
    digest: crc::crc32::Digest,
}

impl DigestWriter {
    fn new() -> DigestWriter {
        use crc::crc32;
        DigestWriter {
            digest: crc32::Digest::new(crc32::IEEE),
        }
    }
}

impl Write for DigestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use crc::Hasher32;
        self.digest.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, BasicError)]
pub enum ApplyError {
    #[error("internal request")]
    InternalRequest,
    #[error("failed")]
    Failed,
}

pub trait FiniteStateMachine: Clone + Send + 'static {
    type ApplyResult: Debug;

    fn applied_index_id(&self) -> u64;
    fn channel_applied_entry_id(&self, channel_id: u64) -> u64;
    fn apply(&mut self, entry: &Entry) -> Result<Self::ApplyResult, ApplyError>;
    fn apply_snapshot(&mut self, url: String, indexes: HashMap<u64, u64>)
        -> Result<(), ApplyError>;
    fn checkpoint(&self, url: String) -> Result<HashMap<u64, u64>, ApplyError>;
}

#[derive(Debug)]
pub struct Applier<F>
where
    F: FiniteStateMachine,
{
    id: u64,
    applied_id: u64,
    fsm: F,
    builder: DownloadBuilder,
    read_requests: HashMap<u64, Vec<ProposePromise<Result<(), sdcons::Error>>>>,
    finished_reads: BTreeMap<u64, Vec<u64>>,
}

impl<F: FiniteStateMachine> Applier<F> {
    pub fn new(id: u64, fsm: F, builder: DownloadBuilder) -> Applier<F> {
        Applier {
            id,
            applied_id: 0,
            fsm,
            builder,
            read_requests: HashMap::new(),
            finished_reads: BTreeMap::new(),
        }
    }

    pub fn save_read_requests(
        &mut self,
        request_id: u64,
        futures: Vec<ProposePromise<Result<(), sdcons::Error>>>,
    ) {
        debug!("node {} save {} read requests with id {}", self.id, futures.len(), request_id);
        self.read_requests.insert(request_id, futures);
    }

    pub fn notify_failed_requests(
        &mut self,
        err: sdcons::Error,
        futures: Vec<ProposePromise<Result<(), sdcons::Error>>>,
    ) {
        debug!("node {} notify failed read request", self.id);
        for mut promise in futures {
            promise.finish(Err(err.clone()));
        }
    }

    pub fn notify_read_requests(&mut self) {
        let keeped_id = self.applied_id + 1;
        let mut keeped_reads = self.finished_reads.split_off(&keeped_id);
        std::mem::swap(&mut keeped_reads, &mut self.finished_reads);

        for (_, request_ids) in keeped_reads {
            for request_id in request_ids {
                if let Some(futures) = self.read_requests.remove(&request_id) {
                    for mut promise in futures {
                        promise.finish(Ok(()));
                    }
                }
            }
        }
    }

    pub fn apply_read_indexes(&mut self, finished_reads: HashMap<u64, u64>) {
        let mut min_id = std::u64::MAX;
        for (request_id, committed_id) in finished_reads {
            min_id = std::cmp::min(committed_id, min_id);
            self.finished_reads
                .entry(committed_id)
                .or_insert(Vec::new())
                .push(request_id);
        }

        if min_id <= self.applied_id {
            self.notify_read_requests();
        }
    }

    pub fn apply(
        &mut self,
        first_id: u64,
        last_id: u64,
        entries: &Vec<Entry>,
        mut futures: PromiseMap<Result<F::ApplyResult, sdcons::Error>>,
    ) {
        // 1. apply
        for entry in entries {
            let request_id = entry.request_id;
            let apply_result = match self.fsm.apply(entry) {
                Ok(ar) => ar,
                Err(ApplyError::InternalRequest) => {
                    continue;
                }
                Err(e) => panic!("FSM::apply {}", e),
            };

            // 2. invoke futures
            match futures.remove(&request_id) {
                Some(mut f) => f.finish(Ok(apply_result)),
                None => {}
            }
        }

        self.applied_id = last_id;
        self.notify_read_requests();
    }

    pub fn apply_snapshot<Fn>(&mut self, snapshot: SnapshotDesc, cb: Fn)
    where
        Fn: FnOnce(bool, u64, FileBasedSnapshotMeta) -> () + Send + 'static,
    {
        let local_id = self.id;
        let mut fsm = self.fsm.clone();
        let builder = self.builder.clone();
        std::thread::spawn(move || {
            use tokio::runtime::Runtime;
            let remote_id = snapshot.located_id;
            let remote_url = snapshot.url.clone();

            let runtime = Runtime::new().unwrap();
            let download_result = runtime.block_on(async {
                let downloader = builder
                    .build(remote_id)
                    .await
                    .expect("resolve remote address");
                downloader.download(remote_url).await
            });
            let (id, snapshot_meta) = match download_result {
                Ok(v) => v,
                Err(msg) => {
                    warn!("node {} download snapshot from {}: {}", local_id, remote_id, msg);
                    let snapshot_meta = FileBasedSnapshotMeta {
                        desc: snapshot,
                        checksum: 0,
                        files: BTreeMap::new(),
                    };
                    cb(false, 0, snapshot_meta);
                    return;
                }
            };

            let indexes = snapshot
                .channel_metas
                .iter()
                .map(|(k, v)| (*k, v.id))
                .collect();
            fsm.apply_snapshot(snapshot.url.clone(), indexes)
                .expect("apply snapshot");

            cb(true, id, snapshot_meta);

            // FIXME(patrick) notify read tasks.
        });
    }

    pub fn checkpoint<Fn>(&self, url: String, cb: Fn)
    where
        Fn: FnOnce(u64, FileBasedSnapshotMeta, HashMap<u64, u64>) -> () + Send + 'static,
    {
        let fsm = self.fsm.clone();
        let local_id = self.id;
        std::thread::spawn(move || {
            let timestamp = Local::now().timestamp_nanos();
            let base_url = format!("{}/{}", url, timestamp);
            let data_url = format!("{}/DATA", base_url);
            let applied_ids = fsm.checkpoint(data_url.clone()).expect("checkpoint");

            let mut files = Vec::new();
            let data_path = std::path::Path::new(&data_url);
            if !data_path.is_dir() {
                files.push(String::from("DATA"));
            } else {
                for entry in std::fs::read_dir(data_path.clone()).expect("read checkpoint files") {
                    let entry = entry.expect("read file entry");
                    if entry.path().is_dir() {
                        continue;
                    }
                    let uri = format!(
                        "DATA/{}",
                        entry.file_name().to_str().expect("invalid UTF-8 format")
                    );
                    files.push(uri);
                }
            }

            use crc::Hasher32;

            files.sort();
            let mut file_metas = BTreeMap::new();
            let mut base_checksum = 0;
            for file_name in files {
                let file_url = format!("{}/{}", base_url, file_name);
                let mut file = std::fs::File::open(file_url).expect("open file");
                let mut writer = DigestWriter::new();
                std::io::copy(&mut file, &mut writer).expect("checksum");
                let checksum = writer.digest.sum32();
                let meta = FileMeta {
                    uri: file_name.clone(),
                    checksum,
                    file_size: 0,
                };
                file_metas.insert(file_name, meta);
                base_checksum ^= checksum;
            }

            let meta = FileBasedSnapshotMeta {
                desc: SnapshotDesc {
                    located_id: local_id,
                    url: base_url,
                    channel_metas: HashMap::new(),
                    members: HashMap::new(),
                },
                checksum: base_checksum,
                files: file_metas,
            };

            cb(timestamp as u64, meta, applied_ids);
        });
    }
}
