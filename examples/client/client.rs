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

extern crate base;
extern crate clap;
#[macro_use]
extern crate lazy_static;
extern crate rustyline;
extern crate serde;
extern crate tarpc;
extern crate tokio;
extern crate tokio_serde;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

use base::proto::kv;
use base::resolver::{AddressResolver, FileBasedResolver};
use base::sdk::client;
use base::slog::SimpleLogger;
use clap::{App, Arg};
use rustyline::{error::ReadlineError, Editor};
use serde::Deserialize;
use tarpc::context;

static LOGGER: SimpleLogger = SimpleLogger;

#[derive(Deserialize)]
struct Config {
    named_file: String,
}

#[cfg(not(debug_assertions))]
fn config_argument() -> Arg<'static, 'static> {
    Arg::with_name("config")
        .short("c")
        .long("config")
        .takes_value(true)
        .default_value("/opt/tiger/simplekv/config.toml")
        .help("path of config file")
}

#[cfg(debug_assertions)]
fn config_argument() -> Arg<'static, 'static> {
    Arg::with_name("config")
        .short("c")
        .long("config")
        .takes_value(true)
        .default_value("config/client-debug.toml")
        .help("path of config file")
}

#[derive(Debug, Eq, PartialEq)]
pub enum Token {
    GET,
    PUT,
    DELETE,
    HELP,
    HINT,
    TIMEOUT,
    IF,
    NOT,
    EXISTS,
    EXPECT,
    ID(Vec<u8>),
}

lazy_static! {
    pub static ref TOKEN_MAP: HashMap<Vec<u8>, Token> = {
        let mut m = HashMap::new();
        m.insert(Vec::from(&b"get"[..]), Token::GET);
        m.insert(Vec::from(&b"put"[..]), Token::PUT);
        m.insert(Vec::from(&b"delete"[..]), Token::DELETE);
        m.insert(Vec::from(&b"help"[..]), Token::HELP);
        m.insert(Vec::from(&b"hint"[..]), Token::HINT);
        m.insert(Vec::from(&b"timeout"[..]), Token::TIMEOUT);
        m.insert(Vec::from(&b"if"[..]), Token::IF);
        m.insert(Vec::from(&b"not"[..]), Token::NOT);
        m.insert(Vec::from(&b"exists"[..]), Token::EXISTS);
        m.insert(Vec::from(&b"expect"[..]), Token::EXPECT);
        m
    };
}

fn display(ident: usize, msg: &str) {
    println!("{:ident$} {}", ' ', msg, ident = ident);
}

fn display_command(ident: usize, cmd: &str, width: usize, desc: &str) {
    println!("{:ident$} {:<width$} {}", ' ', cmd, desc, ident = ident, width = width);
}

fn show_usage(kind: &str) {
    println!("usage: cmd [args] [options] [hint `host`] [timeout `timeout_ms`]");
    if kind == String::from("all") {
        println!("example:");
        display(4, "put key value if not exists hint '127.0.0.1:8080' timeout 10");
        println!("");
    }
    println!("command: ");
    if kind == String::from("get") || kind == String::from("all") {
        display_command(2, "get", 10, "read key from kv store");
        display(4, "args: key");
        println!("");
    }
    if kind == String::from("put") || kind == String::from("all") {
        display_command(2, "put", 10, "write key into kv store");
        display(4, "args: key value");
        display(4, "options: ");
        display(6, "- if not exists");
        display(6, "write key only there no key already exists");
        display(6, "- expect `previous value`");
        display(6, "write key only there exists expected previous values");
        println!("");
    }
    if kind == String::from("delete") || kind == String::from("all") {
        display_command(2, "delete", 10, "delete key from kv store");
        display(4, "args: key");
        println!("");
    }
    if kind == String::from("help") || kind == String::from("all") {
        display_command(2, "help", 10, "show tips");
        println!("");
    }
}

fn skip_space(input: &[u8]) -> &[u8] {
    for i in 0..input.len() {
        if input[i] != b' ' && input[i] != b'\t' && input[i] != 10 {
            return &input[i..];
        }
    }
    return &input[input.len()..];
}

fn is_eof(input: &[u8]) -> bool {
    skip_space(input).is_empty()
}

fn read_entry(input: &[u8]) -> Option<(&[u8], Vec<u8>)> {
    let mut entry = vec![];
    let len = input.len();
    let mut i = 0;
    while i < len {
        if input[i] == b'\\' {
            if i + 1 == len {
                println!("unexpect escape, found EOF");
                return None;
            }
            match input[i + 1] {
                b'n' => entry.push(b'\n'),
                b't' => entry.push(b'\t'),
                b'r' => entry.push(b'\r'),
                b' ' => entry.push(b' '),
                b'\\' => entry.push(b'\\'),
                _ => {
                    println!("unknown escape: {:?}", input[i + 1]);
                    return None;
                }
            }
            i += 2;
            continue;
        } else if 33 <= input[i] && input[i] < 127 {
            // from ! to `
            entry.push(input[i]);
            i += 1;
            continue;
        } else {
            return Some((&input[i..], entry));
        }
    }
    if entry.is_empty() {
        None
    } else {
        Some((&input[input.len()..], entry))
    }
}

fn expect_token(input: &[u8], tok: Token) -> Option<&[u8]> {
    if let Some((input, value)) = read_entry(input) {
        match TOKEN_MAP.get(&value) {
            Some(got_tok) if *got_tok == tok => {
                return Some(input);
            }
            _ => {}
        }
    }
    None
}

fn expect_tokens(mut input: &[u8], toks: Vec<Token>) -> Option<&[u8]> {
    for tok in toks {
        input = match expect_token(input, tok) {
            Some(i) => skip_space(i),
            None => return None,
        };
    }
    Some(input)
}

fn read_numeric(input: &[u8]) -> Option<(&[u8], u64)> {
    match read_entry(input) {
        Some((input, value)) => {
            match std::str::from_utf8(&value)
                .expect("invalid utf-8 sequence")
                .parse::<u64>()
            {
                Ok(v) => return Some((input, v)),
                Err(e) => {
                    println!("expect numeric, but parse got: {}", e);
                }
            }
        }
        _ => {}
    }
    None
}

fn parse_place_hint(input: &[u8]) -> (&[u8], Option<SocketAddr>) {
    let input = skip_space(input);
    if let Some(input) = expect_token(input, Token::HINT) {
        let input = skip_space(input);
        match read_entry(input) {
            Some((input, value)) => {
                match std::str::from_utf8(&value)
                    .expect("invalid utf-8 sequence")
                    .parse::<SocketAddr>()
                {
                    Ok(addr) => return (input, Some(addr)),
                    Err(e) => {
                        println!("parse socket addr: {}", e);
                    }
                }
            }
            None => {
                println!("expect socket addr, but got nothing");
            }
        };
    }
    (input, None)
}

fn parse_timeout_ms(input: &[u8]) -> (&[u8], u64) {
    let input = skip_space(input);
    if let Some(input) = expect_token(input, Token::TIMEOUT) {
        let input = skip_space(input);
        return match read_numeric(input) {
            Some((input, number)) => (input, number),
            None => {
                println!("expect numeric, but found: {:?}", input);
                (input, 0)
            }
        };
    }
    // TODO(patrick) default timeout value
    (input, 0)
}

type Client = client::RetryClient<FileBasedResolver>;

async fn parse_get_request(client: &Client, input: &[u8]) {
    let input = skip_space(input);
    let (input, key) = match read_entry(input) {
        Some(v) => v,
        None => {
            println!("expect key, but nothing found");
            show_usage("get");
            return;
        }
    };
    let (input, hint) = parse_place_hint(input);
    let (input, timeout_ms) = parse_timeout_ms(input);
    if !is_eof(input) {
        println!(
            "unexpect input sequences: {}",
            std::str::from_utf8(input).expect("invalid UTF-8 sequences")
        );
        show_usage("get");
        return;
    }

    let read_opts = client::ReadOptions {
        addr: hint,
        timeout_ms,
    };

    match client.get(context::current(), &read_opts, key).await {
        Err(e) => {
            println!("send request: {:?}: {:?}", read_opts, e);
        }
        Ok(Some(value)) => match std::str::from_utf8(&value) {
            Ok(v) => println!("value is {}", v),
            Err(_) => println!("value => {:?}", value),
        },
        Ok(None) => {
            println!("no such key exists");
        }
    }
}

fn parse_put_cond(input: &[u8]) -> (&[u8], kv::PutCond) {
    let input = skip_space(input);
    if let Some(input) = expect_token(input, Token::EXPECT) {
        let input = skip_space(input);
        match read_entry(input) {
            Some((input, value)) => (input, kv::PutCond::Except(value)),
            None => (input, kv::PutCond::None),
        }
    } else {
        let input = skip_space(input);
        match expect_tokens(input, vec![Token::IF, Token::NOT, Token::EXISTS]) {
            Some(input) => (input, kv::PutCond::NotExists),
            None => (input, kv::PutCond::None),
        }
    }
}

async fn parse_put_request(client: &Client, input: &[u8]) {
    let input = skip_space(input);
    let (input, key) = match read_entry(input) {
        Some((input, key)) => (input, key),
        None => {
            println!("expect key, but nothing found");
            show_usage("put");
            return;
        }
    };
    let input = skip_space(input);
    let (input, value) = match read_entry(input) {
        Some((input, value)) => (input, value),
        None => {
            println!("expect value, but nothing found");
            show_usage("put");
            return;
        }
    };

    let (input, cond) = parse_put_cond(input);
    let (input, hint) = parse_place_hint(input);
    let (input, timeout_ms) = parse_timeout_ms(input);
    if !is_eof(input) {
        println!(
            "unexpect input sequences: {}",
            std::str::from_utf8(input).expect("invalid UTF-8 sequences")
        );
        show_usage("put");
        return;
    }

    let opt = client::WriteOptions {
        addr: hint,
        cond,
        timeout_ms,
    };

    match client.put(context::current(), &opt, key, Some(value)).await {
        Err(client::KvError::CasFailed) => {
            println!("cas failed");
        }
        Err(e) => {
            println!("send request: {:?}: {:?}", opt, e);
        }
        Ok(Some(value)) => match std::str::from_utf8(&value) {
            Ok(v) => println!("put success, previous value is {}", v),
            Err(_) => println!("put success, previous value => {:?}", value),
        },
        Ok(None) => {
            println!("put success");
        }
    }
}

async fn parse_delete_request(client: &Client, input: &[u8]) {
    let input = skip_space(input);
    let (input, key) = match read_entry(input) {
        Some((input, key)) => (input, key),
        None => {
            println!("expect key, but nothing found");
            show_usage("delete");
            return;
        }
    };

    let (input, hint) = parse_place_hint(input);
    let (input, timeout_ms) = parse_timeout_ms(input);
    if !is_eof(input) {
        println!(
            "unexpect input sequences: {}",
            std::str::from_utf8(input).expect("invalid UTF-8 sequences")
        );
        show_usage("delete");
        return;
    }

    let opt = client::WriteOptions {
        addr: hint,
        cond: kv::PutCond::None,
        timeout_ms,
    };

    match client.put(context::current(), &opt, key, None).await {
        Err(e) => {
            println!("send request: {:?}: {:?}", opt, e);
        }
        Ok(Some(value)) => match std::str::from_utf8(&value) {
            Ok(v) => println!("delete success, previous value is {}", v),
            Err(_) => println!("delete success, previous value => {:?}", value),
        },
        Ok(None) => {
            println!("delete success");
        }
    }
}

async fn parse_and_execute(client: &Client, input: &[u8]) {
    let input = skip_space(input);
    if let Some(input) = expect_token(input, Token::GET) {
        parse_get_request(client, input).await;
    } else if let Some(input) = expect_token(input, Token::PUT) {
        parse_put_request(client, input).await;
    } else if let Some(input) = expect_token(input, Token::DELETE) {
        parse_delete_request(client, input).await;
    } else if let Some(_) = expect_token(input, Token::HELP) {
        show_usage("all");
    } else if is_eof(input) {
        return;
    } else {
        println!(
            "unknown sequences: {}",
            std::str::from_utf8(input).expect("invalid UTF-8 sequence")
        );
        show_usage("all");
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Trace))
        .expect("init logger");

    let matches = App::new("exmaple/client")
        .version("v0.0.1")
        .author("Patrick")
        .about("simple kv test client")
        .arg(config_argument())
        .get_matches();

    let config_file = matches.value_of("config").expect("config is required");
    let content = std::fs::read_to_string(config_file).expect("read config file failed");
    let config: Config = toml::from_str(&content).expect("read config from file");

    let resolver = FileBasedResolver::new(config.named_file);
    resolver.refresh().expect("resolve address");
    let address_map = resolver.all();
    println!("address: {:?}", address_map);

    let kv_client = client::RetryClient::new(resolver.clone());

    let mut editor = Editor::<()>::new();
    loop {
        let readline = editor.readline(">> ");
        match readline {
            Ok(line) => {
                editor.add_history_entry(line.as_str());
                parse_and_execute(&kv_client, line.as_str().as_bytes()).await;
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
