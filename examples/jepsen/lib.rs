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

extern crate jni;
extern crate log;
extern crate tarpc;
extern crate tokio;

use std::collections::HashMap;
use std::ffi::CStr;
use std::net::SocketAddr;

use base::proto::kv;
use base::resolver::{AddressResolver, StaticResolver};
use base::sdk::client;
use base::sdk::client::{KvError, RetryClient};
use base::slog::SimpleLogger;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::sys::jobject;
use jni::sys::jstring;
use jni::JNIEnv;
use log::warn;
use tarpc::context;
use tokio::runtime::Runtime;

fn null_string() -> jstring {
    std::ptr::null_mut() as jstring
}

fn handle_error(env: &JNIEnv, e: KvError) {
    match e {
        KvError::ConnectTimeout(inner) => {
            env.throw_new("sdcons/KvClient$TimeoutException", format!("{}", inner).as_str())
                .expect("throw exception");
        }
        KvError::RequestTimeout(msg) => {
            env.throw_new("sdcons/KvClient$TimeoutException", format!("detail: {}", msg))
                .expect("throw exception");
        }
        KvError::CasFailed => {
            env.throw_new("sdcons/KvClient$CasException", "cas failed")
                .expect("throw exception");
        }
        KvError::Inner(detail) => {
            env.throw_new("sdcons/KvClient$FailedException", format!("inner error: {}", detail))
                .expect("throw exception");
        }
        _ => {
            env.throw_new("sdcons/KvClient$FailedException", format!("{:?}", e))
                .expect("throw exception");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_Get(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
) -> jstring {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let opts = client::ReadOptions::default();
        let key = key.as_bytes().to_vec();
        match client.get(ctx, &opts, key).await {
            Ok(Some(value)) => env
                .new_string(String::from_utf8(value).expect("invalid UTF-8 format"))
                .expect("Couldn't create java string")
                .into_inner(),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
                null_string()
            }
            Err(e) => {
                handle_error(&env, e);
                null_string()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_Put(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
    value: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let value: String = env
        .get_string(value)
        .expect("Could't get java string")
        .into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();
        match client.put(ctx, &opts, key, Some(value)).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_PutWithExpect(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
    value: JString,
    expect: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let value: String = env
        .get_string(value)
        .expect("Could't get java string")
        .into();
    let expect: String = env
        .get_string(expect)
        .expect("Could't get java string")
        .into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let mut opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();
        let expect = expect.as_bytes().to_vec();
        opts.cond = kv::PutCond::Except(expect);
        match client.put(ctx, &opts, key, Some(value)).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_PutWithNotExists(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
    value: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let value: String = env
        .get_string(value)
        .expect("Could't get java string")
        .into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let mut opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();
        opts.cond = kv::PutCond::NotExists;
        match client.put(ctx, &opts, key, Some(value)).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_Delete(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        match client.put(ctx, &opts, key, None).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_DeleteWithExpect(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
    expect: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let expect: String = env
        .get_string(expect)
        .expect("Could't get java string")
        .into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let mut opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        let expect = expect.as_bytes().to_vec();
        opts.cond = kv::PutCond::Except(expect);
        match client.put(ctx, &opts, key, None).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_DeleteWithNotExists(
    env: JNIEnv,
    class: JClass,
    handler: jlong,
    key: JString,
) {
    let client = unsafe { &mut *(handler as *mut RetryClient<StaticResolver>) };
    let key: String = env.get_string(key).expect("Could't get java string").into();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = context::current();
        let mut opts = client::WriteOptions::default();
        let key = key.as_bytes().to_vec();
        opts.cond = kv::PutCond::NotExists;
        match client.put(ctx, &opts, key, None).await {
            Ok(Some(_)) => (),
            Ok(None) => {
                env.throw_new("sdcons/KvClient$KeyNotFoundException", "key not found")
                    .expect("throw exception");
            }
            Err(e) => {
                handle_error(&env, e);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_acquire(
    env: JNIEnv,
    class: JClass,
    address: JClass,
) -> jlong {
    let mut server_addresses = HashMap::new();

    let entry_set = env
        .call_method(address, "entrySet", "()Ljava/util/Set;", &vec![])
        .expect("invoke HashMap.entrySet()")
        .l()
        .unwrap();
    let iterator = env
        .call_method(entry_set, "iterator", "()Ljava/util/Iterator;", &vec![])
        .expect("invoke EntrySet.iterator()")
        .l()
        .unwrap();
    loop {
        let has_next = env
            .call_method(iterator, "hasNext", "()Z", &vec![])
            .expect("invoke Iterator.hasNext()")
            .z()
            .expect("hasNext() should return boolean");
        if !has_next {
            break;
        }

        let item = env
            .call_method(iterator, "next", "()Ljava/lang/Object;", &vec![])
            .expect("invoke Iterator.next()")
            .l()
            .unwrap();

        let key = env
            .call_method(item, "getKey", "()Ljava/lang/Object;", &vec![])
            .expect("invoke Iterator.getKey()")
            .l()
            .unwrap();
        let value = env
            .call_method(item, "getValue", "()Ljava/lang/Object;", &vec![])
            .expect("invoke Iterator.getValue()")
            .l()
            .unwrap();
        let id = env
            .call_method(key, "intValue", "()I", &vec![])
            .expect("invoke Integer.intValue()")
            .i()
            .expect("intValue() should return int") as u64;

        let addr: String = env
            .get_string(JString::from(value))
            .expect("Could't get java string")
            .into();
        let sock_addr = match addr.parse::<SocketAddr>() {
            Ok(s) => s,
            Err(e) => {
                warn!("parse socket address {}: {}", addr, e);
                continue;
            }
        };
        server_addresses.insert(id, sock_addr);
    }

    let resolver = StaticResolver::new(server_addresses);
    Box::into_raw(Box::new(client::RetryClient::new(resolver))) as jlong
}

#[no_mangle]
pub extern "system" fn Java_sdcons_KvClient_release(env: JNIEnv, class: JClass, handler: jlong) {
    unsafe { Box::from_raw(handler as *mut client::RetryClient<StaticResolver>) };
}
