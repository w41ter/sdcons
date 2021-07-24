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

use anyhow::Error;

use std::collections::HashMap;
use std::future::Future;
use std::ops::Index;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
struct ProposeSharedState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

impl<T> Default for ProposeSharedState<T> {
    fn default() -> ProposeSharedState<T> {
        ProposeSharedState {
            result: None,
            waker: None,
        }
    }
}

#[derive(Debug)]
pub struct ProposeFuture<T> {
    inner: Arc<Mutex<ProposeSharedState<T>>>,
}

#[derive(Debug)]
pub struct ProposePromise<T> {
    inner: Arc<Mutex<ProposeSharedState<T>>>,
}

impl<T> Clone for ProposeFuture<T> {
    fn clone(&self) -> Self {
        ProposeFuture {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for ProposePromise<T> {
    fn clone(&self) -> Self {
        ProposePromise {
            inner: self.inner.clone(),
        }
    }
}

pub fn create_promise<T>() -> (ProposeFuture<T>, ProposePromise<T>) {
    let inner = Arc::new(Mutex::new(ProposeSharedState::default()));
    let future = ProposeFuture {
        inner: inner.clone(),
    };
    let promise = ProposePromise {
        inner: inner.clone(),
    };
    (future, promise)
}

impl<T> ProposePromise<T> {
    pub fn finish(&mut self, result: T) {
        let mut shared_state = self.inner.lock().unwrap();
        shared_state.result = Some(result);
        if let Some(waker) = shared_state.waker.take() {
            waker.wake();
        }
    }
}

impl<T> Future for ProposeFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared_state = self.inner.lock().unwrap();
        if shared_state.result.is_some() {
            Poll::Ready(shared_state.result.take().unwrap())
        } else {
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub type FutureMap<T> = HashMap<u64, ProposeFuture<T>>;
pub type PromiseMap<T> = HashMap<u64, ProposePromise<T>>;
