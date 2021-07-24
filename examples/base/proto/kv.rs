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

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ErrorCode {
    OK,
    CasFailed,
    Inner,
    NotLeader,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PutCond {
    None,
    NotExists,
    Except(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseHeader {
    pub code: ErrorCode,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub header: ResponseHeader,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutRequest {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // if value is empty, delete this key
    pub cond: PutCond,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    pub header: ResponseHeader,
    pub previous_value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeConfigResponse {
    pub header: ResponseHeader,
    pub actual_leader: Option<u64>,
}

#[tarpc::service]
pub trait Kv {
    async fn get(key: Vec<u8>) -> GetResponse;
    async fn put(req: PutRequest) -> PutResponse;
    async fn change_config(new_members: Vec<u64>) -> ChangeConfigResponse;
}
