#![allow(unused_imports)]
use std::{
    io::{BufReader, Write, prelude::*},
    net::{TcpListener, TcpStream},
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use threadpool::ThreadPool;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pool = ThreadPool::new(4);
    let store = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let store_clone = Arc::clone(&store);
                pool.execute(move || {
                    handle_connection(stream, store_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..bytes_read]);

        let commands = parse_commands(&request);

        for command in commands {
            match command {
                Command::Ping => {
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                Command::Echo(msg) => {
                    let response = format!("${}\r\n{}\r\n", msg.len(), msg);
                    stream.write_all(response.as_bytes()).unwrap();
                }
                Command::Set(key, value, expiry_ms) => {
                    let mut store = store.lock().unwrap();
                    let expiry = expiry_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
                    store.insert(key, (value, expiry));
                    stream.write_all(b"+OK\r\n").unwrap();
                }
                Command::Get(key) => {
                    let mut store = store.lock().unwrap();
                    if let Some((value, expiry)) = store.get(&key) {
                        // Check if key has expired
                        if expiry.map_or(false, |exp| Instant::now() > exp) {
                            // Key expired, remove it
                            store.remove(&key);
                            stream.write_all(b"$-1\r\n").unwrap();
                        } else {
                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                            stream.write_all(response.as_bytes()).unwrap();
                        }
                    } else {
                        stream.write_all(b"$-1\r\n").unwrap();
                    }
                }
            }
        }
    }
}

enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>), // key, value, optional expiry in ms
    Get(String),
}

fn parse_commands(request: &str) -> Vec<Command> {
    let mut commands = Vec::new();
    let lines: Vec<&str> = request.split("\r\n").collect();
    let mut i = 0;

    while i < lines.len() {
        if lines[i].starts_with('*') {
            // Array indicator
            i += 1;
            if i < lines.len() && lines[i].starts_with('$') {
                i += 1;
                if i < lines.len() {
                    let cmd = lines[i].to_uppercase();
                    i += 1;

                    match cmd.as_str() {
                        "PING" => {
                            commands.push(Command::Ping);
                        }
                        "ECHO" => {
                            // Skip the length indicator
                            if i < lines.len() && lines[i].starts_with('$') {
                                i += 1;
                                if i < lines.len() {
                                    commands.push(Command::Echo(lines[i].to_string()));
                                    i += 1;
                                }
                            }
                        }
                        "SET" => {
                            // Skip the length indicator for key
                            if i < lines.len() && lines[i].starts_with('$') {
                                i += 1;
                                if i < lines.len() {
                                    let key = lines[i].to_string();
                                    i += 1;
                                    // Skip the length indicator for value
                                    if i < lines.len() && lines[i].starts_with('$') {
                                        i += 1;
                                        if i < lines.len() {
                                            let value = lines[i].to_string();
                                            i += 1;

                                            // Check for PX (milliseconds) or EX (seconds) flag
                                            let mut expiry_ms = None;
                                            if i < lines.len() && lines[i].starts_with('$') {
                                                i += 1;
                                                if i < lines.len() {
                                                    let flag = lines[i].to_uppercase();
                                                    i += 1;

                                                    if flag == "PX" || flag == "EX" {
                                                        // Get the expiry value
                                                        if i < lines.len() && lines[i].starts_with('$') {
                                                            i += 1;
                                                            if i < lines.len() {
                                                                if let Ok(val) = lines[i].parse::<u64>() {
                                                                    expiry_ms = Some(if flag == "EX" { val * 1000 } else { val });
                                                                }
                                                                i += 1;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            commands.push(Command::Set(key, value, expiry_ms));
                                        }
                                    }
                                }
                            }
                        }
                        "GET" => {
                            // Skip the length indicator
                            if i < lines.len() && lines[i].starts_with('$') {
                                i += 1;
                                if i < lines.len() {
                                    commands.push(Command::Get(lines[i].to_string()));
                                    i += 1;
                                }
                            }
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
            }
        } else {
            i += 1;
        }
    }

    commands
}
