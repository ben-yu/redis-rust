#![allow(unused_imports)]
use std::{
    io::{BufReader, Write, prelude::*},
    net::{TcpListener, TcpStream},
};
use threadpool::ThreadPool;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                pool.execute(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..bytes_read]);

        println!("Request: {}", request);

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
            }
        }
    }
}

enum Command {
    Ping,
    Echo(String),
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
