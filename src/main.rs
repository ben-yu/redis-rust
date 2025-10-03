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

    fn lpush(&mut self, key: String, values: Vec<String>) -> usize {
        let list = self.data
            .entry(key)
            .or_insert_with(|| Value::List(Vec::new()));

        match list {
            Value::List(l) => {
                // Insert each element at the beginning
                // This reverses the order: LPUSH list a b c → [c, b, a]
                for value in values {
                    l.insert(0, value);
                }
                l.len()
            }
            _ => 0, // Key exists but is not a list
        }
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Option<Vec<String>> {
        match self.data.get(key) {
            Some(Value::List(list)) => {
                let len = list.len() as i64;
                if len == 0 {
                    return Some(Vec::new());
                }

                // Handle negative indices
                let start_idx = if start < 0 {
                    (len + start).max(0)
                } else {
                    start
                };

                let stop_idx = if stop < 0 {
                    (len + stop).max(0)
                } else {
                    stop
                };

                // If start is beyond list length or start > stop, return empty array
                if start_idx >= len || start_idx > stop_idx {
                    return Some(Vec::new());
                }

                // Clamp stop to valid range
                let stop_idx = stop_idx.min(len - 1);

                Some(list[start_idx as usize..=stop_idx as usize].to_vec())
            }
            _ => None, // Key doesn't exist or is not a list
        }
    }

    fn llen(&self, key: &str) -> usize {
        match self.data.get(key) {
            Some(Value::List(list)) => list.len(),
            _ => 0, // Key doesn't exist or is not a list
        }
    }

    fn lpop(&mut self, key: &str, count: Option<usize>) -> Option<Vec<String>> {
        match self.data.get_mut(key) {
            Some(Value::List(list)) => {
                if list.is_empty() {
                    return None;
                }

                let count = count.unwrap_or(1);
                let pop_count = count.min(list.len());

                let mut result = Vec::new();
                for _ in 0..pop_count {
                    result.push(list.remove(0));
                }

                // Remove the key if the list is now empty
                if list.is_empty() {
                    self.data.remove(key);
                }

                Some(result)
            }
            _ => None, // Key doesn't exist or is not a list
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
                Command::LPush(key, values) => {
                    let mut store = store.lock().unwrap();
                    let len = store.lpush(key, values);
                    let response = format!(":{}\r\n", len);
                    stream.write_all(response.as_bytes())
                }
                Command::LRange(key, start, stop) => {
                    let store = store.lock().unwrap();
                    if let Some(values) = store.lrange(&key, start, stop) {
                        // Array response: *<count>\r\n followed by bulk strings
                        let mut response = format!("*{}\r\n", values.len());
                        for value in values {
                            response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                        }
                        stream.write_all(response.as_bytes())
                    } else {
                        // Key doesn't exist or is not a list
                        stream.write_all(b"*0\r\n")
                    }
                }
                Command::LLen(key) => {
                    let store = store.lock().unwrap();
                    let len = store.llen(&key);
                    let response = format!(":{}\r\n", len);
                    stream.write_all(response.as_bytes())
                }
                Command::LPop(key, count) => {
                    let mut store = store.lock().unwrap();
                    if let Some(values) = store.lpop(&key, count) {
                        if count.is_some() {
                            // With count: return array
                            let mut response = format!("*{}\r\n", values.len());
                            for value in values {
                                response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                            }
                            stream.write_all(response.as_bytes())
                        } else {
                            // Without count: return single bulk string
                            let value = &values[0];
                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                            stream.write_all(response.as_bytes())
                        }
                    } else {
                        // Key doesn't exist or is empty
                        stream.write_all(b"$-1\r\n")
                    }
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
    LPush(String, Vec<String>), // key, values
    LRange(String, i64, i64), // key, start, stop
    LLen(String), // key
    LPop(String, Option<usize>), // key, optional count
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
            "LPUSH" => {
                let key = self.read_bulk_string()?;
                let mut values = Vec::new();

                // Read all remaining values
                while let Some(value) = self.read_bulk_string() {
                    values.push(value);
                }

                if values.is_empty() {
                    None
                } else {
                    Some(Command::LPush(key, values))
                }
            }
            "LRANGE" => {
                let key = self.read_bulk_string()?;
                let start_str = self.read_bulk_string()?;
                let stop_str = self.read_bulk_string()?;

                let start = start_str.parse::<i64>().ok()?;
                let stop = stop_str.parse::<i64>().ok()?;

                Some(Command::LRange(key, start, stop))
            }
            "LLEN" => {
                let key = self.read_bulk_string()?;
                Some(Command::LLen(key))
            }
            "LPOP" => {
                let key = self.read_bulk_string()?;
                // Check for optional count argument
                let count = if let Some(count_str) = self.read_bulk_string() {
                    count_str.parse::<usize>().ok()
                } else {
                    None
                };
                Some(Command::LPop(key, count))
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
            println!("Listening on port {}", port);
            let pool = ThreadPool::new(10);
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

    #[test]
    fn test_parse_lrange() {
        let request = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$1\r\n2\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LRange(key, start, stop) => {
                assert_eq!(key, "mylist");
                assert_eq!(*start, 0);
                assert_eq!(*stop, 2);
            }
            _ => panic!("Expected LRange command"),
        }
    }

    #[test]
    fn test_parse_lrange_negative_indices() {
        let request = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n-3\r\n$2\r\n-1\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LRange(key, start, stop) => {
                assert_eq!(key, "mylist");
                assert_eq!(*start, -3);
                assert_eq!(*stop, -1);
            }
            _ => panic!("Expected LRange command"),
        }
    }

    #[test]
    fn test_lrange_basic() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 3 elements
        send_command(port, "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");

        // Get all elements
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$1\r\n2\r\n");
        assert_eq!(response, "*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");
    }

    #[test]
    fn test_lrange_partial() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 5 elements
        send_command(port, "*7\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n");

        // Get elements 1-3
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n3\r\n");
        assert_eq!(response, "*3\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n");
    }

    #[test]
    fn test_lrange_negative_indices() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 5 elements
        send_command(port, "*7\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n");

        // Get last 3 elements using negative indices
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n-3\r\n$2\r\n-1\r\n");
        assert_eq!(response, "*3\r\n$2\r\nv3\r\n$2\r\nv4\r\n$2\r\nv5\r\n");
    }

    #[test]
    fn test_lrange_nonexistent_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$10\r\nnonexistent\r\n$1\r\n0\r\n$1\r\n5\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_lrange_empty_range() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list
        send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n");

        // Request range where start > stop
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n5\r\n$1\r\n2\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_parse_lpush_single_value() {
        let request = "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LPush(key, values) => {
                assert_eq!(key, "mylist");
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], "value");
            }
            _ => panic!("Expected LPush command"),
        }
    }

    #[test]
    fn test_parse_lpush_multiple_values() {
        let request = "*5\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LPush(key, values) => {
                assert_eq!(key, "mylist");
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], "v1");
                assert_eq!(values[1], "v2");
                assert_eq!(values[2], "v3");
            }
            _ => panic!("Expected LPush command"),
        }
    }

    #[test]
    fn test_lpush_new_list() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");
        assert_eq!(response, ":1\r\n");

        // Verify it was inserted at the beginning
        let lrange_response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(lrange_response, "*1\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn test_lpush_multiple_values() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // First push: LPUSH mylist v1 v2
        // v1 is inserted first at position 0: [v1]
        // v2 is inserted next at position 0: [v2, v1]
        let response1 = send_command(port, "*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
        assert_eq!(response1, ":2\r\n");

        // Second push: LPUSH mylist v3
        // v3 is inserted at position 0: [v3, v2, v1]
        let response2 = send_command(port, "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$2\r\nv3\r\n");
        assert_eq!(response2, ":3\r\n");

        // Verify order: v3, v2, v1
        let lrange_response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(lrange_response, "*3\r\n$2\r\nv3\r\n$2\r\nv2\r\n$2\r\nv1\r\n");
    }

    #[test]
    fn test_lpush_prepends_to_existing() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create list with RPUSH
        send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n");

        // Prepend with LPUSH
        send_command(port, "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n");

        // Verify order: c, a, b
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(response, "*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n");
    }

    #[test]
    fn test_lpush_reverses_order() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Push multiple values at once - they should be reversed
        // LPUSH inserts one by one at the front, so last arg ends up first
        send_command(port, "*5\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n");

        // Should be: 3, 2, 1 (reversed because each is inserted at position 0)
        let response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(response, "*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n");
    }

    #[test]
    fn test_parse_llen() {
        let request = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LLen(key) => {
                assert_eq!(key, "mylist");
            }
            _ => panic!("Expected LLen command"),
        }
    }

    #[test]
    fn test_llen_existing_list() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 3 elements
        send_command(port, "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");

        // Get length
        let response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(response, ":3\r\n");
    }

    #[test]
    fn test_llen_nonexistent_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$10\r\nnonexistent\r\n");
        assert_eq!(response, ":0\r\n");
    }

    #[test]
    fn test_llen_empty_list() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create an empty list (shouldn't actually be stored, but test anyway)
        // Get length of non-existent list
        let response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$9\r\nemptylist\r\n");
        assert_eq!(response, ":0\r\n");
    }

    #[test]
    fn test_llen_after_operations() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // RPUSH 2 elements
        send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n");
        let response1 = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(response1, ":2\r\n");

        // LPUSH 1 element
        send_command(port, "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n");
        let response2 = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(response2, ":3\r\n");

        // RPUSH 2 more elements
        send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nd\r\n$1\r\ne\r\n");
        let response3 = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(response3, ":5\r\n");
    }

    #[test]
    fn test_llen_wrong_type() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Set a string value
        send_command(port, "*3\r\n$3\r\nSET\r\n$6\r\nmykey1\r\n$5\r\nvalue\r\n");

        // Try to get length - should return 0 since it's not a list
        let response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmykey1\r\n");
        assert_eq!(response, ":0\r\n");
    }

    #[test]
    fn test_parse_lpop_no_count() {
        let request = "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LPop(key, count) => {
                assert_eq!(key, "mylist");
                assert_eq!(*count, None);
            }
            _ => panic!("Expected LPop command"),
        }
    }

    #[test]
    fn test_parse_lpop_with_count() {
        let request = "*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n3\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::LPop(key, count) => {
                assert_eq!(key, "mylist");
                assert_eq!(*count, Some(3));
            }
            _ => panic!("Expected LPop command"),
        }
    }

    #[test]
    fn test_lpop_single_element() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list
        send_command(port, "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

        // Pop one element (default behavior)
        let response = send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
        assert_eq!(response, "$1\r\na\r\n");

        // Verify list now has 2 elements
        let llen_response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(llen_response, ":2\r\n");
    }

    #[test]
    fn test_lpop_with_count() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 5 elements
        send_command(port, "*7\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n");

        // Pop 3 elements
        let response = send_command(port, "*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n3\r\n");
        assert_eq!(response, "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

        // Verify list now has 2 elements (d, e)
        let lrange_response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(lrange_response, "*2\r\n$1\r\nd\r\n$1\r\ne\r\n");
    }

    #[test]
    fn test_lpop_count_exceeds_length() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with 2 elements
        send_command(port, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n");

        // Pop 5 elements (more than available)
        let response = send_command(port, "*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n5\r\n");
        assert_eq!(response, "*2\r\n$1\r\na\r\n$1\r\nb\r\n");

        // List should be empty and removed
        let llen_response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(llen_response, ":0\r\n");
    }

    #[test]
    fn test_lpop_nonexistent_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$4\r\nLPOP\r\n$10\r\nnonexistent\r\n");
        assert_eq!(response, "$-1\r\n");
    }

    #[test]
    fn test_lpop_empty_list() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create and then empty a list
        send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n");
        send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");

        // Try to pop from empty list
        let response = send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
        assert_eq!(response, "$-1\r\n");
    }

    #[test]
    fn test_lpop_removes_key_when_empty() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with one element
        send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n");

        // Pop the only element
        send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");

        // Verify key no longer exists
        let llen_response = send_command(port, "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        assert_eq!(llen_response, ":0\r\n");
    }

    #[test]
    fn test_lpop_order() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Use LPUSH to create list [3, 2, 1]
        send_command(port, "*5\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n");

        // Pop one element - should get 3
        let response1 = send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
        assert_eq!(response1, "$1\r\n3\r\n");

        // Pop one more - should get 2
        let response2 = send_command(port, "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
        assert_eq!(response2, "$1\r\n2\r\n");

        // Final element should be 1
        let lrange_response = send_command(port, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        assert_eq!(lrange_response, "*1\r\n$1\r\n1\r\n");
    }
}
