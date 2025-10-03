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
    let store = Arc::new(Mutex::new(Store::new()));

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

enum Value {
    String(String, Option<Instant>), // value, optional expiry
    List(Vec<String>),
}

struct Store {
    data: HashMap<String, Value>,
}

impl Store {
    fn new() -> Self {
        Store {
            data: HashMap::new(),
        }
    }

    fn set(&mut self, key: String, value: String, expiry: Option<Instant>) {
        self.data.insert(key, Value::String(value, expiry));
    }

    fn get(&self, key: &str) -> Option<String> {
        match self.data.get(key) {
            Some(Value::String(val, expiry)) => {
                if expiry.map_or(false, |exp| Instant::now() > exp) {
                    None // Expired
                } else {
                    Some(val.clone())
                }
            }
            _ => None,
        }
    }

    fn remove_if_expired(&mut self, key: &str) -> bool {
        if let Some(Value::String(_, expiry)) = self.data.get(key) {
            if expiry.map_or(false, |exp| Instant::now() > exp) {
                self.data.remove(key);
                return true;
            }
        }
        false
    }

    fn rpush(&mut self, key: String, values: Vec<String>) -> usize {
        let list = self.data
            .entry(key)
            .or_insert_with(|| Value::List(Vec::new()));

        match list {
            Value::List(l) => {
                l.extend(values);
                l.len()
            }
            _ => 0, // Key exists but is not a list
        }
    }
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = match stream.read(&mut buf) {
            Ok(0) => break, // Connection closed
            Ok(n) => n,
            Err(_) => break, // Error reading
        };

        let request = String::from_utf8_lossy(&buf[..bytes_read]);

        let commands = parse_commands(&request);

        for command in commands {
            let result = match command {
                Command::Ping => {
                    stream.write_all(b"+PONG\r\n")
                }
                Command::Echo(msg) => {
                    let response = format!("${}\r\n{}\r\n", msg.len(), msg);
                    stream.write_all(response.as_bytes())
                }
                Command::Set(key, value, expiry_ms) => {
                    let mut store = store.lock().unwrap();
                    let expiry = expiry_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
                    store.set(key, value, expiry);
                    stream.write_all(b"+OK\r\n")
                }
                Command::Get(key) => {
                    let mut store = store.lock().unwrap();
                    if store.remove_if_expired(&key) {
                        stream.write_all(b"$-1\r\n")
                    } else if let Some(value) = store.get(&key) {
                        let response = format!("${}\r\n{}\r\n", value.len(), value);
                        stream.write_all(response.as_bytes())
                    } else {
                        stream.write_all(b"$-1\r\n")
                    }
                }
                Command::RPush(key, values) => {
                    let mut store = store.lock().unwrap();
                    let len = store.rpush(key, values);
                    let response = format!(":{}\r\n", len);
                    stream.write_all(response.as_bytes())
                }
            };

            if result.is_err() {
                break;
            }
        }
    }
}

enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>), // key, value, optional expiry in ms
    Get(String),
    RPush(String, Vec<String>), // key, values
}

struct Parser<'a> {
    lines: Vec<&'a str>,
    index: usize,
}

impl<'a> Parser<'a> {
    fn new(request: &'a str) -> Self {
        Parser {
            lines: request.split("\r\n").collect(),
            index: 0,
        }
    }

    fn peek(&self) -> Option<&str> {
        if self.index < self.lines.len() {
            Some(self.lines[self.index])
        } else {
            None
        }
    }

    fn advance(&mut self) {
        self.index += 1;
    }

    fn read_bulk_string(&mut self) -> Option<String> {
        // Expect a bulk string in format: $<length>\r\n<data>\r\n
        if let Some(line) = self.peek() {
            if line.starts_with('$') {
                self.advance();
                if self.index < self.lines.len() {
                    let data = self.lines[self.index].to_string();
                    self.advance();
                    return Some(data);
                }
            }
        }
        None
    }

    fn parse_command(&mut self) -> Option<Command> {
        // Check for array indicator
        if let Some(line) = self.peek() {
            if !line.starts_with('*') {
                self.advance();
                return None;
            }
            self.advance();
        } else {
            return None;
        }

        // Read command name
        let cmd_name = self.read_bulk_string()?.to_uppercase();

        match cmd_name.as_str() {
            "PING" => Some(Command::Ping),
            "ECHO" => {
                let msg = self.read_bulk_string()?;
                Some(Command::Echo(msg))
            }
            "SET" => {
                let key = self.read_bulk_string()?;
                let value = self.read_bulk_string()?;

                // Check for optional PX or EX flag
                let expiry_ms = if let Some(flag_str) = self.read_bulk_string() {
                    let flag = flag_str.to_uppercase();
                    if flag == "PX" || flag == "EX" {
                        if let Some(val_str) = self.read_bulk_string() {
                            if let Ok(val) = val_str.parse::<u64>() {
                                Some(if flag == "EX" { val * 1000 } else { val })
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                Some(Command::Set(key, value, expiry_ms))
            }
            "GET" => {
                let key = self.read_bulk_string()?;
                Some(Command::Get(key))
            }
            "RPUSH" => {
                let key = self.read_bulk_string()?;
                let mut values = Vec::new();

                // Read all remaining values
                while let Some(value) = self.read_bulk_string() {
                    values.push(value);
                }

                if values.is_empty() {
                    None
                } else {
                    Some(Command::RPush(key, values))
                }
            }
            _ => None,
        }
    }
}

fn parse_commands(request: &str) -> Vec<Command> {
    let mut parser = Parser::new(request);
    let mut commands = Vec::new();

    while parser.index < parser.lines.len() {
        if let Some(command) = parser.parse_command() {
            commands.push(command);
        } else {
            parser.advance();
        }
    }

    commands
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    use std::sync::atomic::{AtomicU16, Ordering};
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(6380);

    fn start_test_server() -> (thread::JoinHandle<()>, u16) {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let handle = thread::spawn(move || {
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
            let pool = ThreadPool::new(4);
            let store = Arc::new(Mutex::new(Store::new()));

            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let store_clone = Arc::clone(&store);
                        pool.execute(move || {
                            handle_connection(stream, store_clone);
                        });
                    }
                    Err(_) => break,
                }
            }
        });
        (handle, port)
    }

    fn send_command(port: u16, command: &str) -> String {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream.write_all(command.as_bytes()).unwrap();
        stream.flush().unwrap();

        // Give server time to process
        thread::sleep(Duration::from_millis(50));

        let mut buf = [0; 512];
        let bytes_read = stream.read(&mut buf).unwrap();
        String::from_utf8_lossy(&buf[..bytes_read]).to_string()
    }

    #[test]
    fn test_ping_command() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100)); // Wait for server to start

        let response = send_command(port, "*1\r\n$4\r\nPING\r\n");
        assert_eq!(response, "+PONG\r\n");
    }

    #[test]
    fn test_echo_command() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        assert_eq!(response, "$5\r\nhello\r\n");
    }

    #[test]
    fn test_set_and_get_commands() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // SET command
        let set_response = send_command(port, "*3\r\n$3\r\nSET\r\n$6\r\ntestkey\r\n$9\r\ntestvalue\r\n");
        assert_eq!(set_response, "+OK\r\n");

        // GET command
        let get_response = send_command(port, "*2\r\n$3\r\nGET\r\n$6\r\ntestkey\r\n");
        assert_eq!(get_response, "$9\r\ntestvalue\r\n");
    }

    #[test]
    fn test_get_nonexistent_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$3\r\nGET\r\n$10\r\nnonexistent\r\n");
        assert_eq!(response, "$-1\r\n");
    }

    #[test]
    fn test_set_with_expiry() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // SET with PX (expires in 100ms)
        let set_response = send_command(port, "*5\r\n$3\r\nSET\r\n$9\r\nexpirekey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n");
        assert_eq!(set_response, "+OK\r\n");

        // GET immediately - should exist
        let get_response1 = send_command(port, "*2\r\n$3\r\nGET\r\n$9\r\nexpirekey\r\n");
        assert_eq!(get_response1, "$5\r\nvalue\r\n");

        // Wait for expiry
        thread::sleep(Duration::from_millis(150));

        // GET after expiry - should return null
        let get_response2 = send_command(port, "*2\r\n$3\r\nGET\r\n$9\r\nexpirekey\r\n");
        assert_eq!(get_response2, "$-1\r\n");
    }

    #[test]
    fn test_parse_ping() {
        let request = "*1\r\n$4\r\nPING\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], Command::Ping));
    }

    #[test]
    fn test_parse_ping_lowercase() {
        let request = "*1\r\n$4\r\nping\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], Command::Ping));
    }

    #[test]
    fn test_parse_echo() {
        let request = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Echo(msg) => assert_eq!(msg, "hello"),
            _ => panic!("Expected Echo command"),
        }
    }

    #[test]
    fn test_parse_echo_empty_string() {
        let request = "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Echo(msg) => assert_eq!(msg, ""),
            _ => panic!("Expected Echo command"),
        }
    }

    #[test]
    fn test_parse_set_simple() {
        let request = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Set(key, value, expiry) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(*expiry, None);
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_parse_set_with_px() {
        let request = "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Set(key, value, expiry) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(*expiry, Some(1000));
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_parse_set_with_ex() {
        let request = "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Set(key, value, expiry) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(*expiry, Some(10000)); // Converted to milliseconds
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_parse_set_with_px_lowercase() {
        let request = "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$3\r\n500\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Set(key, value, expiry) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(*expiry, Some(500));
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_parse_get() {
        let request = "*2\r\n$3\r\nGET\r\n$6\r\nmykey1\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Get(key) => assert_eq!(key, "mykey1"),
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_parse_multiple_pings() {
        let request = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], Command::Ping));
        assert!(matches!(commands[1], Command::Ping));
    }

    #[test]
    fn test_parse_mixed_commands() {
        let request = "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 4);
        assert!(matches!(commands[0], Command::Ping));
        match &commands[1] {
            Command::Echo(msg) => assert_eq!(msg, "hi"),
            _ => panic!("Expected Echo command"),
        }
        match &commands[2] {
            Command::Set(key, value, expiry) => {
                assert_eq!(key, "a");
                assert_eq!(value, "b");
                assert_eq!(*expiry, None);
            }
            _ => panic!("Expected Set command"),
        }
        match &commands[3] {
            Command::Get(key) => assert_eq!(key, "a"),
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_parse_empty_request() {
        let request = "";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 0);
    }

    #[test]
    fn test_parse_invalid_command() {
        let request = "*1\r\n$7\r\nINVALID\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 0); // Should not parse unknown commands
    }

    #[test]
    fn test_multiple_pings() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(response, "+PONG\r\n+PONG\r\n");
    }

    #[test]
    fn test_parse_rpush_single_value() {
        let request = "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::RPush(key, values) => {
                assert_eq!(key, "mylist");
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], "value");
            }
            _ => panic!("Expected RPush command"),
        }
    }

    #[test]
    fn test_parse_rpush_multiple_values() {
        let request = "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::RPush(key, values) => {
                assert_eq!(key, "mylist");
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], "v1");
                assert_eq!(values[1], "v2");
                assert_eq!(values[2], "v3");
            }
            _ => panic!("Expected RPush command"),
        }
    }

    #[test]
    fn test_rpush_new_list() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");
        assert_eq!(response, ":1\r\n");
    }

    #[test]
    fn test_rpush_multiple_values() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // First push
        let response1 = send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
        assert_eq!(response1, ":2\r\n");

        // Second push
        let response2 = send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv3\r\n");
        assert_eq!(response2, ":3\r\n");
    }
}
