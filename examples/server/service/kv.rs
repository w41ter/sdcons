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

use std::sync::{Arc, Mutex};

use base::proto::kv::*;
use byteorder::{LittleEndian, WriteBytesExt};
use log::{debug, trace};
use sdcons::types::*;
use sdcons::Error as SDError;
use tarpc::context;

use crate::node::store::DATA_REGION;
use crate::node::store::FSM;
use crate::node::Node;

type SharedNode<T> = Arc<Mutex<Node<T>>>;

#[derive(Debug, Clone)]
pub struct SimpleKv {
    db: Arc<rocksdb::DB>,
}

impl SimpleKv {
    pub fn new(db: Arc<rocksdb::DB>) -> SimpleKv {
        SimpleKv { db }
    }
}

#[derive(Debug, Clone)]
pub struct KvService {
    id: u64,
    kv: SimpleKv,
    node: SharedNode<FSM>,
}

impl KvService {
    pub fn new(kv: SimpleKv, node: SharedNode<FSM>) -> KvService {
        let id = node.lock().unwrap().node_id();
        KvService { id, kv, node }
    }
}

fn make_user_key(id: u64, user_key: &[u8]) -> Vec<u8> {
    let mut w = vec![];
    w.write_u64::<LittleEndian>(id).unwrap();
    w.write_u8(DATA_REGION).unwrap();
    w.extend(user_key);
    w
}

#[tarpc::server]
impl Kv for KvService {
    async fn get(self, _ctx: context::Context, key: Vec<u8>) -> GetResponse {
        debug!(
            "node {} receive get request with key {}",
            self.id,
            String::from_utf8(key.clone()).unwrap()
        );
        let future = {
            let mut node = self.node.lock().unwrap();
            node.leased_read()
        };
        if let Err(err) = future.await {
            debug!("node {} handle read request: {}", self.id, err.to_string());
            return GetResponse {
                header: ResponseHeader {
                    code: ErrorCode::NotLeader, // treat all sderror as not leader.
                    msg: err.to_string(),
                },
                value: None,
            };
        }

        let actually_key = make_user_key(self.id, &key);
        match self.kv.db.get(&actually_key).expect("read kv") {
            Some(v) => GetResponse {
                header: ResponseHeader {
                    code: ErrorCode::OK,
                    msg: "ok".to_string(),
                },
                value: Some(v.clone()),
            },
            None => GetResponse {
                header: ResponseHeader {
                    code: ErrorCode::OK,
                    msg: "ok".to_string(),
                },
                value: None,
            },
        }
    }

    async fn put(self, _ctx: context::Context, req: PutRequest) -> PutResponse {
        debug!("node {} receive new request {:?}", self.id, req);
        let message = serde_json::to_string(&req).expect("serde put request to json");
        let task = Task {
            message: message.into_bytes(),
            context: None,
        };
        let future = {
            let mut node = self.node.lock().unwrap();
            node.submit_task(task)
        };
        match future.await {
            Ok(resp) => resp,
            Err(err) => {
                debug!("node {} handle put request: {}", self.id, err.to_string());
                PutResponse {
                    header: ResponseHeader {
                        code: ErrorCode::NotLeader, // treat all sderror as not leader.
                        msg: err.to_string(),
                    },
                    previous_value: None,
                }
            }
        }
    }

    async fn change_config(
        self,
        _ctx: context::Context,
        new_members: Vec<u64>,
    ) -> ChangeConfigResponse {
        debug!("node {} receive new config members: {:?}", self.id, new_members);

        let mut node = self.node.lock().unwrap();
        match node.change_config(new_members) {
            Ok(_) => ChangeConfigResponse {
                header: ResponseHeader {
                    code: ErrorCode::OK,
                    msg: String::from("ok"),
                },
                actual_leader: None,
            },
            Err(e) => match e.downcast_ref::<sdcons::Error>() {
                Some(sdcons::Error::NotLeader) => ChangeConfigResponse {
                    header: ResponseHeader {
                        code: ErrorCode::NotLeader,
                        msg: String::from("not leader"),
                    },
                    actual_leader: node.leader_id(),
                },
                _ => ChangeConfigResponse {
                    header: ResponseHeader {
                        code: ErrorCode::Inner,
                        msg: format!("{}", e),
                    },
                    actual_leader: None,
                },
            },
        }
    }
}
