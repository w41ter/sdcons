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

use std::collections::HashSet;

use chrono::{DateTime, Local};
use log::{Level, Metadata, Record};

#[derive(Debug)]
pub struct SimpleLogger;

impl SimpleLogger {
    fn prefix(level: Level) -> String {
        let now: DateTime<Local> = Local::now();
        let p = match level {
            Level::Debug => "D",
            Level::Info => "I",
            Level::Error => "E",
            Level::Warn => "W",
            Level::Trace => "T",
        };
        [p, &now.format("%Y-%m-%d %H:%M:%S").to_string()].join("")
    }
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let mut allowed_targets = HashSet::new();
        allowed_targets.insert("sdcons");
        allowed_targets.insert("server");
        if allowed_targets.contains(metadata.target()) {
            return true;
        }
        if metadata.target().starts_with("tokio")
            || metadata.target().starts_with("mio")
            || metadata.target().starts_with("tarpc")
            || metadata.target().starts_with("rustyline")
        {
            return false;
        }
        if metadata.level() == log::Level::Trace {
            return false;
        }
        return true;
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "[{} - {}:{}] {}",
                Self::prefix(record.level()),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}
