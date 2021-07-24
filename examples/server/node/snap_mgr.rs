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
use std::fs::File;

use log::warn;
use sdcons::types::{EntryMeta, MemberState, SnapshotDesc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileMeta {
    pub uri: String,
    pub checksum: u32,
    pub file_size: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileBasedSnapshotMeta {
    pub desc: SnapshotDesc,
    pub checksum: u32,
    pub files: BTreeMap<String, FileMeta>,
}

fn to_io_error<E>(e: E) -> std::io::Error
where
    E: std::error::Error,
{
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{}", e))
}

fn rebuild_snapshot(
    id: u64,
    path: &str,
    entry: &std::fs::DirEntry,
) -> Result<(u64, FileBasedSnapshotMeta), std::io::Error> {
    let id = entry
        .file_name()
        .to_str()
        .expect("invalid UTF-8 format")
        .parse::<u64>()
        .map_err(to_io_error)?;
    let snapshot_ok_name = format!("{}/{}/OK", path, id);
    File::open(snapshot_ok_name.clone())?;
    let snapshot_meta_name = format!("{}/{}/META", path, id);
    let file = File::open(snapshot_meta_name.clone())?;
    let meta: FileBasedSnapshotMeta = serde_json::from_reader(file)?;

    Ok((id, meta))
}

fn rebuild_snapshot_mgr(
    id: u64,
    path: &str,
) -> Result<BTreeMap<u64, FileBasedSnapshotMeta>, std::io::Error> {
    std::fs::create_dir_all(path)?;
    let mut snapshots = BTreeMap::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if !entry.path().is_dir() {
            continue;
        }

        let (id, desc) = match rebuild_snapshot(id, path, &entry) {
            Ok(v) => v,
            Err(e) => {
                warn!("node {} recovery snapshot {:?}: {}", id, entry.path(), e);
                continue;
            }
        };
        snapshots.insert(id, desc);
    }
    Ok(snapshots)
}

#[derive(Debug)]
pub struct SnapshotMgr {
    id: u64,
    path: String,
    snapshots: BTreeMap<u64, FileBasedSnapshotMeta>,
}

impl SnapshotMgr {
    pub fn new(id: u64, path: &str) -> Result<SnapshotMgr, std::io::Error> {
        let snapshots = rebuild_snapshot_mgr(id, path)?;
        Ok(SnapshotMgr {
            id,
            path: String::from(path),
            snapshots: snapshots,
        })
    }

    pub fn latest_snapshot(&self) -> Option<SnapshotDesc> {
        self.snapshots.last_key_value().map(|(_, v)| v.desc.clone())
    }

    pub fn add_new_snapshot(&mut self, id: u64, meta: FileBasedSnapshotMeta) {
        let snapshot_meta_name = format!("{}/{}/META", self.path, id);
        let mut file = File::create(snapshot_meta_name).expect("create snapshot meta");
        serde_json::to_writer(&mut file, &meta).expect("save snapshot meta");
        let snapshot_ok_name = format!("{}/{}/OK", self.path, id);
        File::create(snapshot_ok_name).expect("create snapshot ok");
        self.snapshots.insert(id, meta);
    }

    pub fn add_exists_snapshot(&mut self, id: u64, meta: FileBasedSnapshotMeta) {
        self.snapshots.insert(id, meta);
    }

    pub fn release_useless_snapshots(&mut self) {
        while self.snapshots.len() > 1 {
            let id = self.snapshots.first_key_value().map(|(k, _)| *k).unwrap();
            let snapshot_dir = format!("{}/{}", self.path, id);
            std::fs::remove_dir_all(snapshot_dir).expect("remove snapshot");
            self.snapshots.remove(&id);
        }
    }

    pub fn count(&self) -> usize {
        self.snapshots.len()
    }
}
