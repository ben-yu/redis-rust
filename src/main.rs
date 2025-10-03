#![allow(unused_imports)]
use std::{
    io::{BufReader, Write, prelude::*},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream);
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

        // Count PING commands in the request
        let ping_count = request.matches("*1\r\n$4\r\nPING\r\n").count();

        // Send the same number of PONGs back
        for _ in 0..ping_count {
            stream.write_all(b"+PONG\r\n").unwrap();
        }
    }
}
