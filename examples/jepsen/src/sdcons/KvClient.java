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

package sdcons;

import java.util.HashMap;

public class KvClient {
    static {
        System.loadLibrary("native_sdk");
    }

    public static class CasException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CasException(String msg) {
            super(msg);
        }
    }

    public static class KeyNotFoundException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public KeyNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class FailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public FailedException(String msg) {
            super(msg);
        }
    }

    public static class TimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public TimeoutException(String msg) {
            super(msg);
        }
    }

    private long handler;

    public KvClient(HashMap<Integer, String> servers) {
        this.handler = acquire(servers);
    }

    @Override
    public void finalize() {
        release(this.handler);
    }

    public String Get(String key) {
        return Get(this.handler, key);
    }

    public void Put(String key, String value) {
        Put(this.handler, key, value);
    }

    public void PutWithExpect(String key, String value, String expect) {
        PutWithExpect(this.handler, key, value, expect);
    }

    public void PutWithNotExists(String key, String value) {
        PutWithNotExists(this.handler, key, value);
    }

    public void Delete(String key) {
        Delete(this.handler, key);
    }

    public void DeleteWithExpect(String key, String expect) {
        DeleteWithExpect(this.handler, key, expect);
    }

    public void DeleteWithNotExists(String key) {
        DeleteWithNotExists(this.handler, key);
    }

    private native static String Get(long handler, String key);
    private native static void Put(long handler, String key, String value);
    private native static void PutWithExpect(long handler, String key, String value, String expect);
    private native static void PutWithNotExists(long handler, String key, String value);
    private native static void Delete(long handler, String key);
    private native static void DeleteWithExpect(long handler, String key, String expect);
    private native static void DeleteWithNotExists(long handler, String key);

    private native static long acquire(HashMap<Integer, String> servers);
    private native static void release(long handler);
}
