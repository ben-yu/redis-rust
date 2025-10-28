#![allow(unused_imports)]
use std::{
    io::{BufReader, Write, prelude::*},
    net::{TcpListener, TcpStream},
    collections::{HashMap, BTreeMap, HashSet},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
    thread,
    env,
};
use threadpool::ThreadPool;

fn main() {
    // Parse command-line arguments for port and replication
    let args: Vec<String> = env::args().collect();
    let mut port = 6379; // Default port
    let mut replica_of: Option<(String, u16)> = None; // (host, port)
    let mut dir = "/tmp/redis-files".to_string(); // Default directory
    let mut dbfilename = "dump.rdb".to_string(); // Default db filename

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            port = args[i + 1].parse().unwrap_or(6379);
            i += 2;
        } else if args[i] == "--replicaof" && i + 1 < args.len() {
            // Parse "host port" as a single string
            let replicaof_str = &args[i + 1];
            let parts: Vec<&str> = replicaof_str.split_whitespace().collect();
            if parts.len() == 2 {
                let host = parts[0].to_string();
                let master_port = parts[1].parse().unwrap_or(6379);
                replica_of = Some((host, master_port));
            }
            i += 2;
        } else if args[i] == "--dir" && i + 1 < args.len() {
            dir = args[i + 1].clone();
            i += 2;
        } else if args[i] == "--dbfilename" && i + 1 < args.len() {
            dbfilename = args[i + 1].clone();
            i += 2;
        } else {
            i += 1;
        }
    }

    let role = if replica_of.is_some() { "slave" } else { "master" };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    println!("Listening on port {} as {}", port, role);
    let pool = ThreadPool::new(10);
    let store = Arc::new(Mutex::new(Store::new()));
    let role = Arc::new(role.to_string());
    let replicas: Arc<Mutex<Vec<ReplicaInfo>>> = Arc::new(Mutex::new(Vec::new()));
    let config_dir = Arc::new(dir);
    let config_dbfilename = Arc::new(dbfilename);
    let pubsub = Arc::new(Mutex::new(PubSub::new()));

    // If in replica mode, initiate handshake with master
    if let Some((master_host, master_port)) = replica_of {
        println!("Connecting to master at {}:{}", master_host, master_port);
        match TcpStream::connect(format!("{}:{}", master_host, master_port)) {
            Ok(mut master_stream) => {
                println!("Connected to master, starting handshake");

                // Step 1: Send PING as RESP array
                let ping_command = "*1\r\n$4\r\nPING\r\n";
                if let Err(e) = master_stream.write_all(ping_command.as_bytes()) {
                    eprintln!("Failed to send PING to master: {}", e);
                } else {
                    println!("Sent PING to master");

                    // Read PING response
                    let mut buf = [0; 512];
                    match master_stream.read(&mut buf) {
                        Ok(n) => {
                            let response = String::from_utf8_lossy(&buf[..n]);
                            println!("Received PING response: {:?}", response);
                        }
                        Err(e) => {
                            eprintln!("Failed to read PING response: {}", e);
                        }
                    }

                    // Step 2: Send REPLCONF listening-port
                    let port_str = port.to_string();
                    let replconf_port = format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                        port_str.len(),
                        port_str
                    );
                    if let Err(e) = master_stream.write_all(replconf_port.as_bytes()) {
                        eprintln!("Failed to send REPLCONF listening-port: {}", e);
                    } else {
                        println!("Sent REPLCONF listening-port {}", port);

                        // Read REPLCONF response
                        let mut buf = [0; 512];
                        match master_stream.read(&mut buf) {
                            Ok(n) => {
                                let response = String::from_utf8_lossy(&buf[..n]);
                                println!("Received REPLCONF listening-port response: {:?}", response);
                            }
                            Err(e) => {
                                eprintln!("Failed to read REPLCONF response: {}", e);
                            }
                        }
                    }

                    // Step 3: Send REPLCONF capa psync2
                    let replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    if let Err(e) = master_stream.write_all(replconf_capa.as_bytes()) {
                        eprintln!("Failed to send REPLCONF capa: {}", e);
                    } else {
                        println!("Sent REPLCONF capa psync2");

                        // Read REPLCONF capa response
                        let mut buf = [0; 512];
                        match master_stream.read(&mut buf) {
                            Ok(n) => {
                                let response = String::from_utf8_lossy(&buf[..n]);
                                println!("Received REPLCONF capa response: {:?}", response);
                            }
                            Err(e) => {
                                eprintln!("Failed to read REPLCONF capa response: {}", e);
                            }
                        }
                    }

                    // Step 4: Send PSYNC ? -1
                    let psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                    if let Err(e) = master_stream.write_all(psync_command.as_bytes()) {
                        eprintln!("Failed to send PSYNC: {}", e);
                    } else {
                        println!("Sent PSYNC ? -1");

                        // Read PSYNC response (FULLRESYNC line)
                        let mut buf = [0; 512];
                        match master_stream.read(&mut buf) {
                            Ok(n) => {
                                let data = &buf[..n];
                                let response = String::from_utf8_lossy(data);
                                println!("Received PSYNC response ({} bytes)", n);

                                // Find where FULLRESYNC ends
                                if let Some(fullresync_end) = response.find("\r\n") {
                                    let fullresync_line = &response[..fullresync_end];
                                    println!("FULLRESYNC: {}", fullresync_line);

                                    // Check if RDB bulk string is in the same read
                                    let after_fullresync = &response[fullresync_end + 2..];
                                    let mut rdb_length: Option<usize> = None;
                                    let mut already_read_rdb = 0;

                                    if let Some(rdb_header_end) = after_fullresync.find("\r\n") {
                                        if after_fullresync.starts_with('$') {
                                            let length_str = &after_fullresync[1..rdb_header_end];
                                            if let Ok(len) = length_str.parse::<usize>() {
                                                rdb_length = Some(len);
                                                let fullresync_line_len = fullresync_end + 2;
                                                let rdb_header_len = rdb_header_end + 2;
                                                let rdb_data_start = fullresync_line_len + rdb_header_len;
                                                if n > rdb_data_start {
                                                    already_read_rdb = n - rdb_data_start;
                                                }
                                            }
                                        }
                                    }

                                    // Now read the RDB file (either remainder or all of it)
                                    if let Some(len) = rdb_length {
                                        // RDB bulk string header was in first read
                                        println!("RDB file length: {} bytes (header in first read)", len);
                                        println!("Already read {} bytes of RDB data", already_read_rdb);

                                        if already_read_rdb < len {
                                            let remaining = len - already_read_rdb;
                                            let mut rdb_buf = vec![0u8; remaining];
                                            if let Ok(_) = master_stream.read_exact(&mut rdb_buf) {
                                                println!("Read remaining {} bytes of RDB", remaining);
                                            }
                                        }
                                    } else {
                                        // RDB bulk string comes in next read
                                        println!("Waiting for RDB bulk string header...");
                                        let mut rdb_header_buf = [0; 16];
                                        if let Ok(n) = master_stream.read(&mut rdb_header_buf) {
                                            let header = String::from_utf8_lossy(&rdb_header_buf[..n]);
                                            println!("Received RDB header ({} bytes): {:?}", n, header);

                                            if let Some(header_end) = header.find("\r\n") {
                                                if header.starts_with('$') {
                                                    let length_str = &header[1..header_end];
                                                    if let Ok(len) = length_str.parse::<usize>() {
                                                        println!("RDB file length: {} bytes", len);

                                                        // Check if any RDB data came with the header
                                                        let header_total_len = header_end + 2;
                                                        let rdb_data_in_header = if n > header_total_len {
                                                            n - header_total_len
                                                        } else {
                                                            0
                                                        };

                                                        println!("RDB data with header: {} bytes", rdb_data_in_header);

                                                        // Read the rest of the RDB file
                                                        if rdb_data_in_header < len {
                                                            let remaining = len - rdb_data_in_header;
                                                            let mut rdb_buf = vec![0u8; remaining];
                                                            if let Ok(_) = master_stream.read_exact(&mut rdb_buf) {
                                                                println!("Read remaining {} bytes of RDB", remaining);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    println!("RDB file fully consumed");
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to read PSYNC response: {}", e);
                            }
                        }
                    }

                    println!("Handshake completed successfully");

                    // Spawn a thread to continuously read commands from master
                    let store_for_replication = Arc::clone(&store);
                    let role_for_replication = Arc::clone(&role);
                    thread::spawn(move || {
                        println!("Starting replication command processor");
                        let mut buf = [0; 512];
                        let mut repl_offset: usize = 0;

                        loop {
                            match master_stream.read(&mut buf) {
                                Ok(0) => {
                                    println!("Master connection closed");
                                    break;
                                }
                                Ok(n) => {
                                    let request = String::from_utf8_lossy(&buf[..n]);
                                    println!("Received from master ({} bytes): {:?}", n, request);

                                    // Parse and execute commands from master
                                    let commands = parse_commands(&request);
                                    for command in commands {
                                        println!("Executing command from master: {:?}", command);

                                        // Check if this is REPLCONF GETACK
                                        if let Command::ReplConf(args) = &command {
                                            if args.len() >= 1 && args[0].to_uppercase() == "GETACK" {
                                                // Respond with REPLCONF ACK <offset>
                                                // The offset should be the current offset BEFORE processing this GETACK
                                                let ack_response = format!(
                                                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                                    repl_offset.to_string().len(),
                                                    repl_offset
                                                );
                                                if let Err(e) = master_stream.write_all(ack_response.as_bytes()) {
                                                    eprintln!("Failed to send ACK: {}", e);
                                                    break;
                                                }
                                                println!("Sent REPLCONF ACK {} (before counting {} bytes)", repl_offset, n);
                                            }
                                        } else {
                                            // Execute command but don't send response back to master
                                            execute_command_silently(command, &store_for_replication, role_for_replication.as_str());
                                        }
                                    }

                                    // Update replication offset with number of bytes processed
                                    // This includes REPLCONF GETACK commands
                                    repl_offset += n;
                                    println!("Replication offset now: {}", repl_offset);
                                }
                                Err(e) => {
                                    eprintln!("Error reading from master: {}", e);
                                    break;
                                }
                            }
                        }
                        println!("Replication command processor stopped");
                    });
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to master: {}", e);
            }
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let store_clone = Arc::clone(&store);
                let role_clone = Arc::clone(&role);
                let replicas_clone = Arc::clone(&replicas);
                let dir_clone = Arc::clone(&config_dir);
                let dbfilename_clone = Arc::clone(&config_dbfilename);
                let pubsub_clone = Arc::clone(&pubsub);
                pool.execute(move || {
                    handle_connection(stream, store_clone, role_clone, replicas_clone, dir_clone, dbfilename_clone, pubsub_clone);
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
    Stream(BTreeMap<String, Vec<(String, String)>>), // stream_id -> [(field, value)]
}

struct ReplicaInfo {
    stream: TcpStream,
    offset: Arc<Mutex<usize>>,
}

impl ReplicaInfo {
    fn new(stream: TcpStream) -> Self {
        ReplicaInfo {
            stream,
            offset: Arc::new(Mutex::new(0)),
        }
    }
}

// PubSub system to track channel subscribers
// Messages are sent as (channel_name, message_content)
struct PubSub {
    // Map of channel name to list of subscriber senders
    channels: HashMap<String, Vec<mpsc::Sender<(String, String)>>>,
}

impl PubSub {
    fn new() -> Self {
        PubSub {
            channels: HashMap::new(),
        }
    }

    fn subscribe(&mut self, channel: String, sender: mpsc::Sender<(String, String)>) {
        self.channels
            .entry(channel)
            .or_insert_with(Vec::new)
            .push(sender);
    }

    fn unsubscribe(&mut self, channel: &str, sender_id: usize) {
        if let Some(senders) = self.channels.get_mut(channel) {
            if sender_id < senders.len() {
                senders.remove(sender_id);
            }
            if senders.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    fn publish(&self, channel: &str, message: &str) -> usize {
        if let Some(senders) = self.channels.get(channel) {
            let mut count = 0;
            for sender in senders {
                if sender.send((channel.to_string(), message.to_string())).is_ok() {
                    count += 1;
                }
            }
            count
        } else {
            0
        }
    }

    fn subscriber_count(&self, channel: &str) -> usize {
        self.channels.get(channel).map_or(0, |s| s.len())
    }
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

    fn incr(&mut self, key: String) -> Result<i64, String> {
        match self.data.get_mut(&key) {
            Some(Value::String(val, expiry)) => {
                // Check if expired
                if expiry.map_or(false, |exp| Instant::now() > exp) {
                    // Treat as non-existent
                    self.data.insert(key, Value::String("1".to_string(), None));
                    return Ok(1);
                }

                // Try to parse as integer
                match val.parse::<i64>() {
                    Ok(num) => {
                        let new_val = num + 1;
                        *val = new_val.to_string();
                        Ok(new_val)
                    }
                    Err(_) => Err("ERR value is not an integer or out of range".to_string()),
                }
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                // Key doesn't exist, initialize to 1
                self.data.insert(key, Value::String("1".to_string(), None));
                Ok(1)
            }
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

    fn get_type(&self, key: &str) -> &str {
        match self.data.get(key) {
            Some(Value::String(_, expiry)) => {
                if expiry.map_or(false, |exp| Instant::now() > exp) {
                    "none" // Expired
                } else {
                    "string"
                }
            }
            Some(Value::List(_)) => "list",
            Some(Value::Stream(_)) => "stream",
            None => "none",
        }
    }

    fn xrange(&self, key: &str, start: &str, end: &str) -> Option<Vec<(String, Vec<(String, String)>)>> {
        match self.data.get(key) {
            Some(Value::Stream(btree)) => {
                let mut results = Vec::new();

                // Handle special start/end values
                let start_inclusive = start == "-";
                let end_inclusive = end == "+";

                for (entry_id, fields) in btree.iter() {
                    // Check if entry is within range
                    let after_start = start_inclusive || entry_id.as_str() >= start;
                    let before_end = end_inclusive || entry_id.as_str() <= end;

                    if after_start && before_end {
                        results.push((entry_id.clone(), fields.clone()));
                    }
                }

                Some(results)
            }
            _ => None, // Key doesn't exist or is not a stream
        }
    }

    fn xread(&self, streams: &[(String, String)]) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)> {
        let mut results = Vec::new();

        for (key, start_id) in streams {
            if let Some(Value::Stream(btree)) = self.data.get(key) {
                let mut entries = Vec::new();

                for (entry_id, fields) in btree.iter() {
                    // For XREAD, we want entries AFTER the specified ID (exclusive)
                    if entry_id.as_str() > start_id.as_str() {
                        entries.push((entry_id.clone(), fields.clone()));
                    }
                }

                // Only include streams that have entries
                if !entries.is_empty() {
                    results.push((key.clone(), entries));
                }
            }
        }

        results
    }

    fn get_max_stream_id(&self, key: &str) -> Option<String> {
        match self.data.get(key) {
            Some(Value::Stream(btree)) => {
                btree.keys().last().map(|id| id.clone())
            }
            _ => None,
        }
    }

    fn xadd(&mut self, key: String, id: String, fields: Vec<(String, String)>) -> Result<String, String> {
        // Get or create the stream
        let stream = self.data
            .entry(key)
            .or_insert_with(|| Value::Stream(BTreeMap::new()));

        match stream {
            Value::Stream(btree) => {
                // Handle auto-generated IDs
                let final_id = if id == "*" {
                    // Generate ID: milliseconds-sequence
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();

                    let now_str = now.to_string();
                    // Find the highest sequence number for this millisecond
                    let mut max_seq: Option<u64> = None;
                    for existing_id in btree.keys() {
                        if let Some((ms, s)) = existing_id.split_once('-') {
                            if ms == now_str {
                                if let Ok(seq_num) = s.parse::<u64>() {
                                    max_seq = Some(max_seq.map_or(seq_num, |current| current.max(seq_num)));
                                }
                            }
                        }
                    }

                    // If entries exist with this millisecond, increment the max sequence
                    // Otherwise start at 0
                    let seq = max_seq.map_or(0, |s| s + 1);
                    format!("{}-{}", now, seq)
                } else if id.ends_with("-*") {
                    // Partial ID: milliseconds-*, generate sequence
                    let millis = id.trim_end_matches("-*");

                    // Find the highest sequence number for this millisecond
                    let mut max_seq: Option<u64> = None;
                    for existing_id in btree.keys() {
                        if let Some((ms, s)) = existing_id.split_once('-') {
                            if ms == millis {
                                if let Ok(seq_num) = s.parse::<u64>() {
                                    max_seq = Some(max_seq.map_or(seq_num, |current| current.max(seq_num)));
                                }
                            }
                        }
                    }

                    // If entries exist with this millisecond, increment the max sequence
                    // Otherwise: if time part is 0, start at 1; else start at 0
                    let seq = if let Some(max) = max_seq {
                        max + 1
                    } else if millis == "0" {
                        1
                    } else {
                        0
                    };

                    format!("{}-{}", millis, seq)
                } else {
                    id.clone()
                };

                // Validate ID is not 0-0
                if final_id == "0-0" {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }

                // Validate ID is greater than existing IDs
                if let Some(last_id) = btree.keys().last() {
                    if !is_id_greater(&final_id, last_id) {
                        return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                    }
                }

                // Add the entry
                btree.insert(final_id.clone(), fields);
                Ok(final_id)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
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

    fn blpop(&mut self, keys: &[String]) -> Option<(String, String)> {
        // Try to pop from each key in order
        for key in keys {
            if let Some(Value::List(list)) = self.data.get_mut(key) {
                if !list.is_empty() {
                    let value = list.remove(0);

                    // Remove the key if the list is now empty
                    if list.is_empty() {
                        self.data.remove(key);
                    }

                    return Some((key.clone(), value));
                }
            }
        }
        None
    }
}

fn is_id_greater(id1: &str, id2: &str) -> bool {
    // Compare two stream IDs in format "milliseconds-sequence"
    let parse_id = |id: &str| -> Option<(u64, u64)> {
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() == 2 {
            let ms = parts[0].parse::<u64>().ok()?;
            let seq = parts[1].parse::<u64>().ok()?;
            Some((ms, seq))
        } else {
            None
        }
    };

    if let (Some((ms1, seq1)), Some((ms2, seq2))) = (parse_id(id1), parse_id(id2)) {
        if ms1 != ms2 {
            ms1 > ms2
        } else {
            seq1 > seq2
        }
    } else {
        false
    }
}

fn execute_command_to_string(command: Command, store: &Arc<Mutex<Store>>, role: &str) -> String {
    match command {
        Command::Ping => "+PONG\r\n".to_string(),
        Command::Echo(msg) => format!("${}\r\n{}\r\n", msg.len(), msg),
        Command::Info(_section) => {
            // Return server information as bulk string
            // Each line is key:value format
            let info = format!(
                "role:{}\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
                role
            );
            format!("${}\r\n{}\r\n", info.len(), info)
        }
        Command::Set(key, value, expiry_ms) => {
            let mut store = store.lock().unwrap();
            let expiry = expiry_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
            store.set(key, value, expiry);
            "+OK\r\n".to_string()
        }
        Command::Get(key) => {
            let mut store = store.lock().unwrap();
            if store.remove_if_expired(&key) {
                "$-1\r\n".to_string()
            } else if let Some(value) = store.get(&key) {
                format!("${}\r\n{}\r\n", value.len(), value)
            } else {
                "$-1\r\n".to_string()
            }
        }
        Command::Incr(key) => {
            let mut store = store.lock().unwrap();
            match store.incr(key) {
                Ok(value) => format!(":{}\r\n", value),
                Err(err) => format!("-{}\r\n", err),
            }
        }
        Command::RPush(key, values) => {
            let mut store = store.lock().unwrap();
            let len = store.rpush(key, values);
            format!(":{}\r\n", len)
        }
        Command::LPush(key, values) => {
            let mut store = store.lock().unwrap();
            let len = store.lpush(key, values);
            format!(":{}\r\n", len)
        }
        Command::LRange(key, start, stop) => {
            let store = store.lock().unwrap();
            if let Some(values) = store.lrange(&key, start, stop) {
                let mut response = format!("*{}\r\n", values.len());
                for value in values {
                    response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
                response
            } else {
                "*0\r\n".to_string()
            }
        }
        Command::LLen(key) => {
            let store = store.lock().unwrap();
            let len = store.llen(&key);
            format!(":{}\r\n", len)
        }
        Command::LPop(key, count) => {
            let mut store = store.lock().unwrap();
            if let Some(values) = store.lpop(&key, count) {
                if count.is_some() {
                    let mut response = format!("*{}\r\n", values.len());
                    for value in values {
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                    response
                } else {
                    let value = &values[0];
                    format!("${}\r\n{}\r\n", value.len(), value)
                }
            } else {
                "$-1\r\n".to_string()
            }
        }
        Command::BLPop(_, _) => {
            // BLPop cannot be used in transactions (blocking operation)
            "-ERR BLPOP cannot be used in transactions\r\n".to_string()
        }
        Command::Type(key) => {
            let store = store.lock().unwrap();
            let type_str = store.get_type(&key);
            format!("+{}\r\n", type_str)
        }
        Command::XAdd(key, id, fields) => {
            let mut store = store.lock().unwrap();
            match store.xadd(key, id, fields) {
                Ok(generated_id) => format!("${}\r\n{}\r\n", generated_id.len(), generated_id),
                Err(err) => format!("-{}\r\n", err),
            }
        }
        Command::XRange(key, start, end) => {
            let store = store.lock().unwrap();
            if let Some(entries) = store.xrange(&key, &start, &end) {
                let mut response = format!("*{}\r\n", entries.len());
                for (id, fields) in entries {
                    response.push_str(&format!("*2\r\n${}\r\n{}\r\n", id.len(), id));
                    response.push_str(&format!("*{}\r\n", fields.len() * 2));
                    for (field, value) in fields {
                        response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                }
                response
            } else {
                "*0\r\n".to_string()
            }
        }
        Command::XRead(_, _) => {
            // XRead with blocking cannot be used in transactions
            "-ERR XREAD with BLOCK cannot be used in transactions\r\n".to_string()
        }
        Command::ReplConf(_args) => {
            // REPLCONF always responds with +OK
            "+OK\r\n".to_string()
        }
        Command::PSync(_repl_id, _offset) => {
            // PSYNC responds with FULLRESYNC
            "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_string()
        }
        Command::Wait(_, _) => {
            // WAIT is handled specially in handle_connection
            ":0\r\n".to_string()
        }
        Command::Config(_, _) => {
            // CONFIG is handled specially in handle_connection with access to config values
            "*0\r\n".to_string()
        }
        Command::Subscribe(_) => {
            // SUBSCRIBE is handled specially in handle_connection with pubsub access
            "+OK\r\n".to_string()
        }
        Command::Unsubscribe(_) | Command::PSubscribe(_) | Command::PUnsubscribe(_) => {
            // Handled specially in handle_connection
            "+OK\r\n".to_string()
        }
        Command::Publish(_, _) => {
            // PUBLISH is handled specially in handle_connection with pubsub access
            ":0\r\n".to_string()
        }
        Command::Quit => {
            // QUIT is handled specially in handle_connection
            "+OK\r\n".to_string()
        }
        Command::Multi | Command::Exec | Command::Discard => {
            // These are handled specially and should not reach here
            "+OK\r\n".to_string()
        }
    }
}

// Execute command silently (for replicas processing commands from master)
// This function executes the command but doesn't generate any response
fn execute_command_silently(command: Command, store: &Arc<Mutex<Store>>, _role: &str) {
    match command {
        Command::Set(key, value, expiry_ms) => {
            let mut store = store.lock().unwrap();
            let expiry = expiry_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
            store.set(key, value, expiry);
        }
        Command::Incr(key) => {
            let mut store = store.lock().unwrap();
            let _ = store.incr(key);
        }
        Command::RPush(key, values) => {
            let mut store = store.lock().unwrap();
            store.rpush(key, values);
        }
        Command::LPush(key, values) => {
            let mut store = store.lock().unwrap();
            store.lpush(key, values);
        }
        Command::LPop(key, count) => {
            let mut store = store.lock().unwrap();
            store.lpop(&key, count);
        }
        Command::XAdd(key, id, fields) => {
            let mut store = store.lock().unwrap();
            let _ = store.xadd(key, id, fields);
        }
        _ => {
            // Other commands are ignored on replicas during replication
            println!("Ignoring non-write command from master: {:?}", command);
        }
    }
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>, role: Arc<String>, replicas: Arc<Mutex<Vec<ReplicaInfo>>>, config_dir: Arc<String>, config_dbfilename: Arc<String>, pubsub: Arc<Mutex<PubSub>>) {
    // Set stream to non-blocking mode for pub/sub support
    stream.set_nonblocking(true).ok();

    let mut buf = [0; 512];
    let mut in_transaction = false;
    let mut queued_commands: Vec<Command> = Vec::new();

    // Track subscription state for this connection
    let mut in_subscribe_mode = false;
    let mut subscribed_channels: HashSet<String> = HashSet::new();
    let mut subscription_rx: Option<mpsc::Receiver<(String, String)>> = None;
    let mut subscription_tx: Option<mpsc::Sender<(String, String)>> = None;

    loop {
        // Check for published messages if subscribed
        if let Some(ref rx) = subscription_rx {
            match rx.try_recv() {
                Ok((channel, message)) => {
                    // Send published message to subscriber
                    // Format: *3\r\n$7\r\nmessage\r\n$<channel_len>\r\n<channel>\r\n$<msg_len>\r\n<message>\r\n
                    let response = format!(
                        "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        channel.len(),
                        channel,
                        message.len(),
                        message
                    );
                    if stream.write_all(response.as_bytes()).is_err() {
                        break;
                    }
                    continue; // Check for more messages
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // No messages, continue to read commands
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Channel closed
                    break;
                }
            }
        }

        let bytes_read = match stream.read(&mut buf) {
            Ok(0) => break, // Connection closed
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available, sleep briefly and check for messages again
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(_) => break, // Other error
        };

        let request = String::from_utf8_lossy(&buf[..bytes_read]);

        let commands = parse_commands(&request);

        for command in commands {
            // Check if command is allowed in subscribe mode
            if in_subscribe_mode {
                let allowed = matches!(command,
                    Command::Subscribe(_) |
                    Command::Unsubscribe(_) |
                    Command::PSubscribe(_) |
                    Command::PUnsubscribe(_) |
                    Command::Ping |
                    Command::Quit
                );

                if !allowed {
                    let error_msg = format!(
                        "-ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
                        command.name()
                    );
                    let _ = stream.write_all(error_msg.as_bytes());
                    continue;
                }
            }

            // Handle MULTI command specially
            if matches!(command, Command::Multi) {
                if in_transaction {
                    // Already in transaction
                    let _ = stream.write_all(b"-ERR MULTI calls can not be nested\r\n");
                } else {
                    in_transaction = true;
                    let _ = stream.write_all(b"+OK\r\n");
                }
                continue;
            }

            // Handle EXEC command specially
            if matches!(command, Command::Exec) {
                if !in_transaction {
                    // Not in transaction
                    let _ = stream.write_all(b"-ERR EXEC without MULTI\r\n");
                } else {
                    // Execute all queued commands and collect results
                    let mut results = Vec::new();

                    for queued_cmd in queued_commands.drain(..) {
                        let result = execute_command_to_string(queued_cmd, &store, role.as_str());
                        results.push(result);
                    }

                    // Reset transaction state
                    in_transaction = false;

                    // Return array of results
                    let mut response = format!("*{}\r\n", results.len());
                    for result in results {
                        response.push_str(&result);
                    }
                    let _ = stream.write_all(response.as_bytes());
                }
                continue;
            }

            // Handle DISCARD command specially
            if matches!(command, Command::Discard) {
                if !in_transaction {
                    // Not in transaction
                    let _ = stream.write_all(b"-ERR DISCARD without MULTI\r\n");
                } else {
                    // Clear queued commands and reset transaction state
                    queued_commands.clear();
                    in_transaction = false;
                    let _ = stream.write_all(b"+OK\r\n");
                }
                continue;
            }

            // If in transaction, queue the command instead of executing
            if in_transaction {
                queued_commands.push(command);
                let _ = stream.write_all(b"+QUEUED\r\n");
                continue;
            }

            // Check if this is a write command (before the command is potentially moved)
            let is_write = command.is_write_command();
            let resp_for_replicas = if is_write && role.as_str() == "master" {
                command.to_resp_array()
            } else {
                None
            };

            // Execute command normally if not in transaction
            let result = match command {
                Command::Ping => {
                    if in_subscribe_mode {
                        // In subscribe mode, PING responds with a RESP array
                        // *2\r\n$4\r\npong\r\n$0\r\n\r\n
                        stream.write_all(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
                    } else {
                        stream.write_all(b"+PONG\r\n")
                    }
                }
                Command::Echo(msg) => {
                    let response = format!("${}\r\n{}\r\n", msg.len(), msg);
                    stream.write_all(response.as_bytes())
                }
                Command::Info(_section) => {
                    // Return server information as bulk string
                    let info = format!(
                        "role:{}\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n",
                        role.as_str()
                    );
                    let response = format!("${}\r\n{}\r\n", info.len(), info);
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
                Command::Incr(key) => {
                    let mut store = store.lock().unwrap();
                    match store.incr(key) {
                        Ok(value) => {
                            let response = format!(":{}\r\n", value);
                            stream.write_all(response.as_bytes())
                        }
                        Err(err) => {
                            let response = format!("-{}\r\n", err);
                            stream.write_all(response.as_bytes())
                        }
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
                Command::BLPop(keys, timeout_secs) => {
                    // If timeout is 0, wait indefinitely
                    let deadline = if timeout_secs == 0.0 {
                        None
                    } else {
                        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
                    };

                    loop {
                        // Try to pop from one of the keys
                        {
                            let mut store = store.lock().unwrap();
                            if let Some((key, value)) = store.blpop(&keys) {
                                // Found an element, return it
                                let response = format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    key.len(), key, value.len(), value);
                                break stream.write_all(response.as_bytes());
                            }
                        }

                        // No element available, check timeout
                        if let Some(deadline) = deadline {
                            if Instant::now() >= deadline {
                                // Timeout reached, return null
                                break stream.write_all(b"*-1\r\n");
                            }
                        }
                        // If deadline is None (timeout = 0), we never timeout

                        // Sleep briefly before trying again
                        thread::sleep(Duration::from_millis(10));
                    }
                }
                Command::Type(key) => {
                    let store = store.lock().unwrap();
                    let type_str = store.get_type(&key);
                    let response = format!("+{}\r\n", type_str);
                    stream.write_all(response.as_bytes())
                }
                Command::XAdd(key, id, fields) => {
                    let mut store = store.lock().unwrap();
                    match store.xadd(key, id, fields) {
                        Ok(entry_id) => {
                            let response = format!("${}\r\n{}\r\n", entry_id.len(), entry_id);
                            stream.write_all(response.as_bytes())
                        }
                        Err(err_msg) => {
                            let response = format!("-{}\r\n", err_msg);
                            stream.write_all(response.as_bytes())
                        }
                    }
                }
                Command::XRange(key, start, end) => {
                    let store = store.lock().unwrap();
                    if let Some(entries) = store.xrange(&key, &start, &end) {
                        // Format: *<count>\r\n
                        // For each entry: *2\r\n$<id_len>\r\n<id>\r\n*<field_count*2>\r\n<field-value pairs>
                        let mut response = format!("*{}\r\n", entries.len());

                        for (entry_id, fields) in entries {
                            // Each entry is an array of 2 elements: [id, [field, value, field, value, ...]]
                            response.push_str("*2\r\n");

                            // Entry ID
                            response.push_str(&format!("${}\r\n{}\r\n", entry_id.len(), entry_id));

                            // Fields array (flat list of field-value pairs)
                            response.push_str(&format!("*{}\r\n", fields.len() * 2));
                            for (field, value) in fields {
                                response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                                response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                            }
                        }

                        stream.write_all(response.as_bytes())
                    } else {
                        // Key doesn't exist or is not a stream - return empty array
                        stream.write_all(b"*0\r\n")
                    }
                }
                Command::XRead(block_ms, streams) => {
                    // Resolve $ to the current maximum ID for each stream
                    let resolved_streams: Vec<(String, String)> = {
                        let store = store.lock().unwrap();
                        streams.iter().map(|(key, start_id)| {
                            if start_id == "$" {
                                // Replace $ with the maximum ID in the stream, or "0-0" if stream doesn't exist
                                let max_id = store.get_max_stream_id(key).unwrap_or_else(|| "0-0".to_string());
                                (key.clone(), max_id)
                            } else {
                                (key.clone(), start_id.clone())
                            }
                        }).collect()
                    };

                    // Determine if we should block and for how long
                    let should_block = block_ms.is_some();
                    let deadline = block_ms.and_then(|ms| {
                        if ms == 0 {
                            None // Block indefinitely
                        } else {
                            Some(Instant::now() + Duration::from_millis(ms))
                        }
                    });

                    loop {
                        let results = {
                            let store = store.lock().unwrap();
                            store.xread(&resolved_streams)
                        };

                        if !results.is_empty() || !should_block {
                            // Format: *<stream_count>\r\n
                            // For each stream: *2\r\n$<key_len>\r\n<key>\r\n*<entries_count>\r\n<entries>
                            let mut response = format!("*{}\r\n", results.len());

                            for (key, entries) in results {
                                // Each stream result is [key, entries]
                                response.push_str("*2\r\n");

                                // Stream key
                                response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));

                                // Entries array
                                response.push_str(&format!("*{}\r\n", entries.len()));

                                for (entry_id, fields) in entries {
                                    // Each entry is [id, [field, value, ...]]
                                    response.push_str("*2\r\n");

                                    // Entry ID
                                    response.push_str(&format!("${}\r\n{}\r\n", entry_id.len(), entry_id));

                                    // Fields array
                                    response.push_str(&format!("*{}\r\n", fields.len() * 2));
                                    for (field, value) in fields {
                                        response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                                    }
                                }
                            }

                            break stream.write_all(response.as_bytes());
                        }

                        // Check if we should timeout
                        if let Some(dl) = deadline {
                            if Instant::now() >= dl {
                                // Timeout - return null
                                break stream.write_all(b"*-1\r\n");
                            }
                        }

                        // Sleep briefly before checking again
                        thread::sleep(Duration::from_millis(10));
                    }
                }
                Command::ReplConf(_args) => {
                    // REPLCONF always responds with +OK
                    stream.write_all(b"+OK\r\n")
                }
                Command::PSync(_repl_id, _offset) => {
                    // PSYNC responds with FULLRESYNC followed by an empty RDB file
                    let response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
                    stream.write_all(response.as_bytes()).ok();

                    // Send empty RDB file as a RESP bulk string
                    // Empty RDB file in hex: 524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2
                    let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
                    let rdb_response = format!("${}\r\n", empty_rdb.len());
                    stream.write_all(rdb_response.as_bytes()).ok();
                    let result = stream.write_all(&empty_rdb);

                    // Save a clone of this stream to the replicas list
                    if let Ok(replica_stream) = stream.try_clone() {
                        let mut replicas_lock = replicas.lock().unwrap();
                        replicas_lock.push(ReplicaInfo::new(replica_stream));
                        println!("Added replica connection, total replicas: {}", replicas_lock.len());
                    }

                    result
                }
                Command::Wait(numreplicas, timeout) => {
                    // WAIT command: wait for at least numreplicas to acknowledge writes
                    let start = Instant::now();
                    let timeout_duration = Duration::from_millis(timeout);

                    // If there are no replicas, return 0 immediately
                    let replicas_lock = replicas.lock().unwrap();
                    let replica_count = replicas_lock.len();

                    if replica_count == 0 {
                        drop(replicas_lock);
                        let response = format!(":0\r\n");
                        stream.write_all(response.as_bytes())
                    } else {
                        // Collect expected offsets and check if all are 0 (no writes yet)
                        let mut expected_offsets = Vec::new();
                        let mut has_any_writes = false;

                        for replica_info in replicas_lock.iter() {
                            let offset = *replica_info.offset.lock().unwrap();
                            expected_offsets.push(offset);
                            if offset > 0 {
                                has_any_writes = true;
                            }
                        }

                        // If no writes have been propagated, return the replica count immediately
                        if !has_any_writes {
                            drop(replicas_lock);
                            let response = format!(":{}\r\n", replica_count);
                            stream.write_all(response.as_bytes())
                        } else {
                            // Send REPLCONF GETACK * to all replicas
                            let getack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                            let mut streams_to_read = Vec::new();

                            for replica_info in replicas_lock.iter() {
                                // Send GETACK command
                                if let Ok(mut write_stream) = replica_info.stream.try_clone() {
                                    write_stream.write_all(getack_cmd.as_bytes()).ok();

                                    // Clone for reading
                                    if let Ok(read_stream) = replica_info.stream.try_clone() {
                                        read_stream.set_nonblocking(false).ok();
                                        streams_to_read.push(read_stream);
                                    }
                                }
                            }

                            drop(replicas_lock);

                            // Now wait for ACK responses with timeout
                            let mut ack_count = 0;

                            for (idx, read_stream) in streams_to_read.iter_mut().enumerate() {
                                let expected_offset = expected_offsets.get(idx).copied().unwrap_or(0);
                                let elapsed = start.elapsed();

                                if elapsed >= timeout_duration {
                                    break;
                                }

                                let remaining_timeout = timeout_duration.saturating_sub(elapsed);
                                if remaining_timeout.is_zero() {
                                    break;
                                }

                                // Set read timeout
                                read_stream.set_read_timeout(Some(remaining_timeout)).ok();
                                let mut buf = [0u8; 512];

                                if let Ok(n) = read_stream.read(&mut buf) {
                                    if n > 0 {
                                        let response = String::from_utf8_lossy(&buf[..n]);
                                        // Parse REPLCONF ACK <offset>
                                        // Format: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
                                        let parts: Vec<&str> = response.split("\r\n").collect();

                                        // Look for the offset value (it's after "ACK")
                                        let mut found_ack = false;
                                        for part in parts.iter() {
                                            if found_ack && !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()) {
                                                if let Ok(ack_offset) = part.parse::<usize>() {
                                                    if ack_offset >= expected_offset {
                                                        ack_count += 1;
                                                    }
                                                    break;
                                                }
                                            }
                                            if *part == "ACK" {
                                                found_ack = true;
                                            }
                                        }
                                    }
                                }

                                if ack_count >= numreplicas {
                                    break;
                                }
                            }

                            let response = format!(":{}\r\n", ack_count);
                            stream.write_all(response.as_bytes())
                        }
                    }
                }
                Command::Config(subcommand, parameter) => {
                    // CONFIG GET command
                    if subcommand.to_uppercase() == "GET" {
                        let param_lower = parameter.to_lowercase();
                        let response = match param_lower.as_str() {
                            "dir" => {
                                // Return as RESP array: *2\r\n$3\r\ndir\r\n$<len>\r\n<value>\r\n
                                format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", config_dir.len(), config_dir.as_str())
                            }
                            "dbfilename" => {
                                // Return as RESP array: *2\r\n$10\r\ndbfilename\r\n$<len>\r\n<value>\r\n
                                format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n", config_dbfilename.len(), config_dbfilename.as_str())
                            }
                            _ => {
                                // Unknown parameter, return empty array
                                "*0\r\n".to_string()
                            }
                        };
                        stream.write_all(response.as_bytes())
                    } else {
                        // Only GET is supported
                        stream.write_all(b"-ERR Unknown CONFIG subcommand\r\n")
                    }
                }
                Command::Subscribe(channels) => {
                    // SUBSCRIBE command - register client to channels
                    // Enter subscribe mode
                    in_subscribe_mode = true;

                    // Create a channel for receiving published messages if not already created
                    if subscription_tx.is_none() {
                        let (tx, rx) = mpsc::channel::<(String, String)>();
                        subscription_tx = Some(tx);
                        subscription_rx = Some(rx);
                    }

                    let tx = subscription_tx.as_ref().unwrap();

                    // Subscribe to each channel
                    let mut pubsub_lock = pubsub.lock().unwrap();

                    for channel in &channels {
                        if !subscribed_channels.contains(channel) {
                            pubsub_lock.subscribe(channel.clone(), tx.clone());
                            subscribed_channels.insert(channel.clone());
                        }

                        // Send subscription confirmation for each channel
                        // Format: *3\r\n$9\r\nsubscribe\r\n$<channel_len>\r\n<channel>\r\n:<count>\r\n
                        let response = format!(
                            "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                            channel.len(),
                            channel,
                            subscribed_channels.len()
                        );
                        stream.write_all(response.as_bytes()).ok();
                    }

                    drop(pubsub_lock);
                    Ok(())
                }
                Command::Unsubscribe(channels) => {
                    // UNSUBSCRIBE command - remove client from channels
                    if channels.is_empty() {
                        // Unsubscribe from all channels
                        subscribed_channels.clear();
                        let response = format!("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
                        stream.write_all(response.as_bytes())
                    } else {
                        // Unsubscribe from specific channels
                        for channel in &channels {
                            subscribed_channels.remove(channel);
                            let response = format!(
                                "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                                channel.len(),
                                channel,
                                subscribed_channels.len()
                            );
                            stream.write_all(response.as_bytes()).ok();
                        }
                        Ok(())
                    }
                }
                Command::PSubscribe(_patterns) => {
                    // Pattern-based subscribe (simplified - just acknowledge)
                    in_subscribe_mode = true;
                    stream.write_all(b"+OK\r\n")
                }
                Command::PUnsubscribe(_patterns) => {
                    // Pattern-based unsubscribe (simplified - just acknowledge)
                    stream.write_all(b"+OK\r\n")
                }
                Command::Publish(channel, message) => {
                    // PUBLISH command - send message to all subscribers of the channel
                    let pubsub_lock = pubsub.lock().unwrap();
                    let count = pubsub_lock.publish(&channel, &message);
                    drop(pubsub_lock);

                    // Return the number of clients that received the message
                    let response = format!(":{}\r\n", count);
                    stream.write_all(response.as_bytes())
                }
                Command::Quit => {
                    // QUIT - close connection gracefully
                    stream.write_all(b"+OK\r\n").ok();
                    break;
                }
                Command::Multi | Command::Exec | Command::Discard => {
                    // Multi, Exec, and Discard are handled above before reaching this match
                    // This case is unreachable but needed for exhaustiveness
                    stream.write_all(b"+OK\r\n")
                }
            };

            // Propagate write commands to replicas if we're a master
            if let Some(resp) = resp_for_replicas {
                let mut replicas_lock = replicas.lock().unwrap();
                let mut valid_replicas = Vec::new();

                for mut replica_info in replicas_lock.drain(..) {
                    if replica_info.stream.write_all(resp.as_bytes()).is_ok() {
                        // Update the expected offset for this replica
                        let mut offset = replica_info.offset.lock().unwrap();
                        *offset += resp.len();
                        drop(offset);
                        valid_replicas.push(replica_info);
                    }
                }

                *replicas_lock = valid_replicas;
            }

            if result.is_err() {
                break;
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Info(Option<String>), // optional section (e.g., "replication")
    Set(String, String, Option<u64>), // key, value, optional expiry in ms
    Get(String),
    Incr(String), // key
    RPush(String, Vec<String>), // key, values
    LPush(String, Vec<String>), // key, values
    LRange(String, i64, i64), // key, start, stop
    LLen(String), // key
    LPop(String, Option<usize>), // key, optional count
    BLPop(Vec<String>, f64), // keys, timeout in seconds
    Type(String), // key
    XAdd(String, String, Vec<(String, String)>), // key, id, fields
    XRange(String, String, String), // key, start, end
    XRead(Option<u64>, Vec<(String, String)>), // optional block_ms, [(key, start_id)]
    Multi, // Start transaction
    Exec, // Execute transaction
    Discard, // Discard transaction
    ReplConf(Vec<String>), // Replication configuration
    PSync(String, String), // replication_id, offset
    Wait(usize, u64), // numreplicas, timeout in milliseconds
    Config(String, String), // subcommand (GET/SET), parameter
    Subscribe(Vec<String>), // channels to subscribe to
    Unsubscribe(Vec<String>), // channels to unsubscribe from (empty = all)
    PSubscribe(Vec<String>), // pattern-based subscribe
    PUnsubscribe(Vec<String>), // pattern-based unsubscribe
    Publish(String, String), // channel, message
    Quit, // Close the connection
}

impl Command {
    // Get the command name in lowercase
    fn name(&self) -> &str {
        match self {
            Command::Ping => "ping",
            Command::Echo(_) => "echo",
            Command::Info(_) => "info",
            Command::Set(_, _, _) => "set",
            Command::Get(_) => "get",
            Command::Incr(_) => "incr",
            Command::RPush(_, _) => "rpush",
            Command::LPush(_, _) => "lpush",
            Command::LRange(_, _, _) => "lrange",
            Command::LLen(_) => "llen",
            Command::LPop(_, _) => "lpop",
            Command::BLPop(_, _) => "blpop",
            Command::Type(_) => "type",
            Command::XAdd(_, _, _) => "xadd",
            Command::XRange(_, _, _) => "xrange",
            Command::XRead(_, _) => "xread",
            Command::Multi => "multi",
            Command::Exec => "exec",
            Command::Discard => "discard",
            Command::ReplConf(_) => "replconf",
            Command::PSync(_, _) => "psync",
            Command::Wait(_, _) => "wait",
            Command::Config(_, _) => "config",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::PSubscribe(_) => "psubscribe",
            Command::PUnsubscribe(_) => "punsubscribe",
            Command::Publish(_, _) => "publish",
            Command::Quit => "quit",
        }
    }

    // Check if this is a write command that should be propagated to replicas
    fn is_write_command(&self) -> bool {
        matches!(self,
            Command::Set(_, _, _) |
            Command::Incr(_) |
            Command::RPush(_, _) |
            Command::LPush(_, _) |
            Command::LPop(_, _) |
            Command::XAdd(_, _, _)
        )
    }

    // Convert command to RESP array format for propagation
    fn to_resp_array(&self) -> Option<String> {
        match self {
            Command::Set(key, value, expiry_ms) => {
                if let Some(ms) = expiry_ms {
                    let ms_str = ms.to_string();
                    let parts = vec!["SET", key.as_str(), value.as_str(), "px", ms_str.as_str()];
                    Some(encode_resp_array(&parts))
                } else {
                    let parts = vec!["SET", key.as_str(), value.as_str()];
                    Some(encode_resp_array(&parts))
                }
            }
            Command::Incr(key) => {
                let parts = vec!["INCR", key.as_str()];
                Some(encode_resp_array(&parts))
            }
            Command::RPush(key, values) => {
                let mut parts = vec!["RPUSH", key.as_str()];
                for v in values {
                    parts.push(v.as_str());
                }
                Some(encode_resp_array(&parts))
            }
            Command::LPush(key, values) => {
                let mut parts = vec!["LPUSH", key.as_str()];
                for v in values {
                    parts.push(v.as_str());
                }
                Some(encode_resp_array(&parts))
            }
            Command::LPop(key, count) => {
                if let Some(c) = count {
                    let count_str = c.to_string();
                    let parts = vec!["LPOP", key.as_str(), count_str.as_str()];
                    Some(encode_resp_array(&parts))
                } else {
                    let parts = vec!["LPOP", key.as_str()];
                    Some(encode_resp_array(&parts))
                }
            }
            Command::XAdd(key, id, fields) => {
                let mut parts = vec!["XADD", key.as_str(), id.as_str()];
                for (field, value) in fields {
                    parts.push(field.as_str());
                    parts.push(value.as_str());
                }
                Some(encode_resp_array(&parts))
            }
            _ => None,
        }
    }
}

// Helper function to encode a RESP array
fn encode_resp_array(parts: &[&str]) -> String {
    let mut result = format!("*{}\r\n", parts.len());
    for part in parts {
        result.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    result
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
            "INFO" => {
                // INFO can optionally take a section argument
                let section = self.read_bulk_string();
                Some(Command::Info(section))
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
            "INCR" => {
                let key = self.read_bulk_string()?;
                Some(Command::Incr(key))
            }
            "MULTI" => {
                Some(Command::Multi)
            }
            "EXEC" => {
                Some(Command::Exec)
            }
            "DISCARD" => {
                Some(Command::Discard)
            }
            "REPLCONF" => {
                // Read all remaining arguments
                let mut args = Vec::new();
                while let Some(arg) = self.read_bulk_string() {
                    args.push(arg);
                }
                Some(Command::ReplConf(args))
            }
            "PSYNC" => {
                let repl_id = self.read_bulk_string()?;
                let offset = self.read_bulk_string()?;
                Some(Command::PSync(repl_id, offset))
            }
            "WAIT" => {
                let numreplicas = self.read_bulk_string()?.parse::<usize>().ok()?;
                let timeout = self.read_bulk_string()?.parse::<u64>().ok()?;
                Some(Command::Wait(numreplicas, timeout))
            }
            "CONFIG" => {
                let subcommand = self.read_bulk_string()?;
                let parameter = self.read_bulk_string()?;
                Some(Command::Config(subcommand, parameter))
            }
            "SUBSCRIBE" => {
                let mut channels = Vec::new();
                while let Some(channel) = self.read_bulk_string() {
                    channels.push(channel);
                }
                if channels.is_empty() {
                    None
                } else {
                    Some(Command::Subscribe(channels))
                }
            }
            "UNSUBSCRIBE" => {
                let mut channels = Vec::new();
                while let Some(channel) = self.read_bulk_string() {
                    channels.push(channel);
                }
                Some(Command::Unsubscribe(channels))
            }
            "PSUBSCRIBE" => {
                let mut patterns = Vec::new();
                while let Some(pattern) = self.read_bulk_string() {
                    patterns.push(pattern);
                }
                if patterns.is_empty() {
                    None
                } else {
                    Some(Command::PSubscribe(patterns))
                }
            }
            "PUNSUBSCRIBE" => {
                let mut patterns = Vec::new();
                while let Some(pattern) = self.read_bulk_string() {
                    patterns.push(pattern);
                }
                Some(Command::PUnsubscribe(patterns))
            }
            "PUBLISH" => {
                let channel = self.read_bulk_string()?;
                let message = self.read_bulk_string()?;
                Some(Command::Publish(channel, message))
            }
            "QUIT" => {
                Some(Command::Quit)
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
            "BLPOP" => {
                // Read keys until we hit the timeout value
                // The last argument is always the timeout
                let mut args = Vec::new();
                while let Some(arg) = self.read_bulk_string() {
                    args.push(arg);
                }

                if args.len() < 2 {
                    return None; // Need at least one key and a timeout
                }

                // Last arg is the timeout
                let timeout_str = args.pop()?;
                let timeout = timeout_str.parse::<f64>().ok()?;

                // Rest are keys
                let keys = args;

                Some(Command::BLPop(keys, timeout))
            }
            "TYPE" => {
                let key = self.read_bulk_string()?;
                Some(Command::Type(key))
            }
            "XADD" => {
                let key = self.read_bulk_string()?;
                let id = self.read_bulk_string()?;

                // Read field-value pairs
                let mut fields = Vec::new();
                while let Some(field) = self.read_bulk_string() {
                    if let Some(value) = self.read_bulk_string() {
                        fields.push((field, value));
                    } else {
                        return None; // Odd number of field-value arguments
                    }
                }

                if fields.is_empty() {
                    None
                } else {
                    Some(Command::XAdd(key, id, fields))
                }
            }
            "XRANGE" => {
                let key = self.read_bulk_string()?;
                let start = self.read_bulk_string()?;
                let end = self.read_bulk_string()?;
                Some(Command::XRange(key, start, end))
            }
            "XREAD" => {
                // XREAD [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
                let mut block_ms = None;
                let mut args = Vec::new();

                // Read all remaining arguments
                while let Some(arg) = self.read_bulk_string() {
                    args.push(arg);
                }

                // Parse arguments
                let mut i = 0;
                if i < args.len() && args[i].to_uppercase() == "BLOCK" {
                    i += 1;
                    if i < args.len() {
                        if let Ok(ms) = args[i].parse::<u64>() {
                            block_ms = Some(ms);
                        }
                        i += 1;
                    }
                }

                // Expect STREAMS keyword
                if i >= args.len() || args[i].to_uppercase() != "STREAMS" {
                    return None;
                }
                i += 1;

                // Read keys and IDs
                let remaining = &args[i..];
                if remaining.is_empty() || remaining.len() % 2 != 0 {
                    return None; // Need equal number of keys and IDs
                }

                let count = remaining.len() / 2;
                let keys = &remaining[..count];
                let ids = &remaining[count..];

                let streams: Vec<(String, String)> = keys.iter()
                    .zip(ids.iter())
                    .map(|(k, id)| (k.clone(), id.clone()))
                    .collect();

                Some(Command::XRead(block_ms, streams))
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
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(5380);

    fn start_test_server() -> (thread::JoinHandle<()>, u16) {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let handle = thread::spawn(move || {
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

            println!("Listening on port {}", port);
            let pool = ThreadPool::new(10);
            let store = Arc::new(Mutex::new(Store::new()));
            let role = Arc::new("master".to_string());
            let replicas: Arc<Mutex<Vec<ReplicaInfo>>> = Arc::new(Mutex::new(Vec::new()));

            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let store_clone = Arc::clone(&store);
                        let role_clone = Arc::clone(&role);
                        let replicas_clone = Arc::clone(&replicas);
                        let dir_clone = Arc::clone(&config_dir);
                        let dbfilename_clone = Arc::clone(&config_dbfilename);
                        let pubsub_clone = Arc::clone(&pubsub);
                        pool.execute(move || {
                            handle_connection(stream, store_clone, role_clone, replicas_clone, dir_clone, dbfilename_clone, pubsub_clone);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No connection available, sleep briefly and try again
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                }
            }
        });

        // Give server time to start listening
        thread::sleep(Duration::from_millis(50));

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

    fn send_command_with_stream(stream: &mut TcpStream, command: &str) -> String {
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
    fn test_incr_nonexistent_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assert_eq!(response, ":1\r\n");
    }

    #[test]
    fn test_incr_existing_key() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Set initial value
        send_command(port, "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n10\r\n");

        // Increment
        let response1 = send_command(port, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assert_eq!(response1, ":11\r\n");

        // Increment again
        let response2 = send_command(port, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assert_eq!(response2, ":12\r\n");
    }

    #[test]
    fn test_incr_not_integer() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Set non-integer value
        send_command(port, "*3\r\n$3\r\nSET\r\n$6\r\nmykey1\r\n$5\r\nhello\r\n");

        // Try to increment
        let response = send_command(port, "*2\r\n$4\r\nINCR\r\n$6\r\nmykey1\r\n");
        assert!(response.starts_with("-ERR"));
    }

    #[test]
    fn test_incr_wrong_type() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list
        send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");

        // Try to increment
        let response = send_command(port, "*2\r\n$4\r\nINCR\r\n$6\r\nmylist\r\n");
        assert!(response.starts_with("-WRONGTYPE"));
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
    fn test_parse_info() {
        let request = "*1\r\n$4\r\nINFO\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Info(section) => assert!(section.is_none()),
            _ => panic!("Expected Info command"),
        }
    }

    #[test]
    fn test_parse_info_with_section() {
        let request = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Info(section) => assert_eq!(section.as_ref().unwrap(), "replication"),
            _ => panic!("Expected Info command"),
        }
    }

    #[test]
    fn test_info_command() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*1\r\n$4\r\nINFO\r\n");

        // Should return bulk string
        assert!(response.starts_with("$"));
        // Should contain role:master
        assert!(response.contains("role:master"));
        // Should contain master_replid
        assert!(response.contains("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
        // Should contain master_repl_offset
        assert!(response.contains("master_repl_offset:0"));
    }

    #[test]
    fn test_info_replication_command() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let response = send_command(port, "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n");

        // Should return bulk string
        assert!(response.starts_with("$"));
        // Should contain role:master
        assert!(response.contains("role:master"));
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
    fn test_parse_incr() {
        let request = "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::Incr(key) => assert_eq!(key, "counter"),
            _ => panic!("Expected Incr command"),
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

    #[test]
    fn test_parse_blpop() {
        let request = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n5\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::BLPop(keys, timeout) => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], "mylist");
                assert_eq!(*timeout, 5.0);
            }
            _ => panic!("Expected BLPop command"),
        }
    }

    #[test]
    fn test_blpop_immediate() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a list with one element
        send_command(port, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");

        // BLPOP should return immediately since element is available
        let response = send_command(port, "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n5\r\n");
        assert_eq!(response, "*2\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn test_blpop_timeout() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let start = Instant::now();
        // BLPOP on non-existent list with 1 second timeout
        let response = send_command(port, "*3\r\n$5\r\nBLPOP\r\n$10\r\nnonexistent\r\n$1\r\n1\r\n");
        let elapsed = start.elapsed();

        assert_eq!(response, "*-1\r\n"); // Null response
        assert!(elapsed >= Duration::from_secs(1)); // Should have waited at least 1 second
    }

    #[test]
    fn test_parse_xadd() {
        let request = "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::XAdd(key, id, fields) => {
                assert_eq!(key, "mystream");
                assert_eq!(id, "*");
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "field");
                assert_eq!(fields[0].1, "value");
            }
            _ => panic!("Expected XAdd command"),
        }
    }

    #[test]
    fn test_parse_xadd_multiple_fields() {
        let request = "*7\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-0\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::XAdd(key, id, fields) => {
                assert_eq!(key, "mystream");
                assert_eq!(id, "1526919030474-0");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], ("field1".to_string(), "value1".to_string()));
                assert_eq!(fields[1], ("field2".to_string(), "value2".to_string()));
            }
            _ => panic!("Expected XAdd command"),
        }
    }

    #[test]
    fn test_xadd_with_explicit_id() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entry with explicit ID
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n");
        assert_eq!(response, "$15\r\n1526919030474-0\r\n");

        // Verify stream type
        let type_response = send_command(port, "*2\r\n$4\r\nTYPE\r\n$8\r\nmystream\r\n");
        assert_eq!(type_response, "+stream\r\n");
    }

    #[test]
    fn test_xadd_with_auto_id() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entry with auto-generated ID
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$11\r\ntemperature\r\n$2\r\n36\r\n");

        // Should return a valid ID in format milliseconds-sequence
        assert!(response.starts_with("$"));
        assert!(response.contains("-"));
    }

    #[test]
    fn test_xadd_multiple_fields() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entry with multiple field-value pairs
        let response = send_command(port, "*7\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n");
        assert_eq!(response, "$15\r\n1526919030474-0\r\n");
    }

    #[test]
    fn test_xadd_sequential_entries() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add first entry
        let response1 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        assert_eq!(response1, "$15\r\n1526919030474-0\r\n");

        // Add second entry with higher ID
        let response2 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-1\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        assert_eq!(response2, "$15\r\n1526919030474-1\r\n");
    }

    #[test]
    fn test_xadd_id_must_be_greater() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add first entry
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-1\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // Try to add entry with same or lower ID - should fail
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$15\r\n1526919030474-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        assert!(response.starts_with("-ERR"));
    }

    #[test]
    fn test_xadd_zero_id_not_allowed() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Try to add entry with 0-0 ID - should fail
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        assert!(response.starts_with("-ERR"));
    }

    #[test]
    fn test_xadd_creates_stream_automatically() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Verify stream doesn't exist
        let type_response1 = send_command(port, "*2\r\n$4\r\nTYPE\r\n$9\r\nnewstream\r\n");
        assert_eq!(type_response1, "+none\r\n");

        // Add entry - should create stream automatically
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$9\r\nnewstream\r\n$15\r\n1526919030474-0\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        assert_eq!(response, "$15\r\n1526919030474-0\r\n");

        // Verify stream now exists
        let type_response2 = send_command(port, "*2\r\n$4\r\nTYPE\r\n$9\r\nnewstream\r\n");
        assert_eq!(type_response2, "+stream\r\n");
    }

    #[test]
    fn test_xadd_wrong_type() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Create a string key
        send_command(port, "*3\r\n$3\r\nSET\r\n$6\r\nmykey1\r\n$5\r\nvalue\r\n");

        // Try to use XADD on string key - should fail
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$6\r\nmykey1\r\n$15\r\n1526919030474-0\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        assert!(response.starts_with("-"));
    }

    #[test]
    fn test_xadd_partial_auto_id_starts_at_zero() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // First entry with partial auto ID should start at 0
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        assert_eq!(response, "$5\r\n100-0\r\n");
    }

    #[test]
    fn test_xadd_partial_auto_id_increments() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add first entry with time part 100
        let response1 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        assert_eq!(response1, "$5\r\n100-0\r\n");

        // Add second entry with same time part - should increment to 1
        let response2 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        assert_eq!(response2, "$5\r\n100-1\r\n");

        // Add third entry - should increment to 2
        let response3 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");
        assert_eq!(response3, "$5\r\n100-2\r\n");
    }

    #[test]
    fn test_xadd_partial_auto_id_zero_starts_at_one() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // When time part is 0, first sequence should be 1, not 0
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$2\r\n0-*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        assert_eq!(response, "$3\r\n0-1\r\n");
    }

    #[test]
    fn test_xadd_partial_auto_id_zero_increments() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // First entry with time part 0 should start at 1
        let response1 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$2\r\n0-*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        assert_eq!(response1, "$3\r\n0-1\r\n");

        // Second entry should increment to 2
        let response2 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$2\r\n0-*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        assert_eq!(response2, "$3\r\n0-2\r\n");
    }

    #[test]
    fn test_xadd_partial_auto_id_different_times() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries with time part 100
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Add entry with time part 200 - should start at 0 again
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n200-*\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");
        assert_eq!(response, "$5\r\n200-0\r\n");
    }

    #[test]
    fn test_xadd_full_auto_id_increments_same_millisecond() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add first entry with full auto ID
        let response1 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // Extract the ID from response1
        let parts: Vec<&str> = response1.split("\r\n").collect();
        let id1 = parts[1];

        // Immediately add another entry - if same millisecond, sequence should increment
        let response2 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        let parts2: Vec<&str> = response2.split("\r\n").collect();
        let id2 = parts2[1];

        // Parse both IDs
        let id1_parts: Vec<&str> = id1.split('-').collect();
        let id2_parts: Vec<&str> = id2.split('-').collect();

        let ms1 = id1_parts[0];
        let seq1 = id1_parts[1].parse::<u64>().unwrap();
        let ms2 = id2_parts[0];
        let seq2 = id2_parts[1].parse::<u64>().unwrap();

        // If same millisecond, sequence should increment
        if ms1 == ms2 {
            assert_eq!(seq2, seq1 + 1);
        } else {
            // If different millisecond, second should be greater
            assert!(ms2.parse::<u64>().unwrap() > ms1.parse::<u64>().unwrap());
        }
    }

    #[test]
    fn test_xadd_mixed_explicit_and_auto_seq() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add explicit entry with sequence 5
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-5\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // Add auto-sequence entry - should use 6
        let response = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        assert_eq!(response, "$5\r\n100-6\r\n");

        // Add another explicit entry with sequence 10
        send_command(port, "*6\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$6\r\n100-10\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");

        // Add auto-sequence entry - should use 11
        let response2 = send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n100-*\r\n$5\r\nfield\r\n$6\r\nvalue4\r\n");
        assert_eq!(response2, "$6\r\n100-11\r\n");
    }

    #[test]
    fn test_parse_xrange() {
        let request = "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\n200-0\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::XRange(key, start, end) => {
                assert_eq!(key, "mystream");
                assert_eq!(start, "100-0");
                assert_eq!(end, "200-0");
            }
            _ => panic!("Expected XRange command"),
        }
    }

    #[test]
    fn test_xrange_basic() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");

        // Get range from 100-0 to 200-0
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\n200-0\r\n");

        // Should return 2 entries
        assert!(response.starts_with("*2\r\n"));
        assert!(response.contains("100-0"));
        assert!(response.contains("200-0"));
        assert!(response.contains("value1"));
        assert!(response.contains("value2"));
        assert!(!response.contains("value3"));
    }

    #[test]
    fn test_xrange_inclusive() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Range is inclusive - exact match on both ends should be included
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\n200-0\r\n");
        assert!(response.contains("100-0"));
        assert!(response.contains("200-0"));
    }

    #[test]
    fn test_xrange_minus_plus() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");

        // Use - for start (minimum) and + for end (maximum)
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n");

        // Should return all entries
        assert!(response.starts_with("*3\r\n"));
        assert!(response.contains("100-0"));
        assert!(response.contains("200-0"));
        assert!(response.contains("300-0"));
    }

    #[test]
    fn test_xrange_empty_stream() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Query non-existent stream
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xrange_no_matches() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Query range that doesn't match any entries
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$5\r\n500-0\r\n$5\r\n600-0\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xrange_multiple_fields() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entry with multiple fields
        send_command(port, "*7\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n");

        // Get the entry
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n");

        // Should contain both fields
        assert!(response.contains("100-0"));
        assert!(response.contains("temperature"));
        assert!(response.contains("36"));
        assert!(response.contains("humidity"));
        assert!(response.contains("95"));
    }

    #[test]
    fn test_xrange_partial_range() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");

        // Get from start to 200-0
        let response = send_command(port, "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$5\r\n200-0\r\n");
        assert!(response.starts_with("*2\r\n"));
        assert!(response.contains("100-0"));
        assert!(response.contains("200-0"));
        assert!(!response.contains("300-0"));
    }

    #[test]
    fn test_parse_xread() {
        let request = "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::XRead(block_ms, streams) => {
                assert_eq!(*block_ms, None);
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].0, "mystream");
                assert_eq!(streams[0].1, "100-0");
            }
            _ => panic!("Expected XRead command"),
        }
    }

    #[test]
    fn test_parse_xread_block() {
        let request = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            Command::XRead(block_ms, streams) => {
                assert_eq!(*block_ms, Some(1000));
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].0, "mystream");
                assert_eq!(streams[0].1, "100-0");
            }
            _ => panic!("Expected XRead command"),
        }
    }

    #[test]
    fn test_xread_basic() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");

        // Read entries after 100-0
        let response = send_command(port, "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n");

        // Should return entries 200-0 and 300-0 (entries AFTER 100-0)
        assert!(response.starts_with("*1\r\n")); // 1 stream
        assert!(response.contains("mystream"));
        assert!(!response.contains("100-0")); // 100-0 should not be included (exclusive)
        assert!(response.contains("200-0"));
        assert!(response.contains("300-0"));
    }

    #[test]
    fn test_xread_multiple_streams() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries to first stream
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Add entries to second stream
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream2\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream2\r\n$5\r\n400-0\r\n$5\r\nfield\r\n$6\r\nvalue4\r\n");

        // Read from both streams
        let response = send_command(port, "*6\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$7\r\nstream1\r\n$7\r\nstream2\r\n$5\r\n100-0\r\n$5\r\n300-0\r\n");

        // Should return 2 streams
        assert!(response.starts_with("*2\r\n"));
        assert!(response.contains("stream1"));
        assert!(response.contains("stream2"));
        assert!(response.contains("200-0"));
        assert!(response.contains("400-0"));
    }

    #[test]
    fn test_xread_no_new_entries() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // Read after the last entry - should return empty
        let response = send_command(port, "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xread_nonexistent_stream() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Read from non-existent stream
        let response = send_command(port, "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xread_dollar_sign() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some existing entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // Read with $ (should not return existing entries)
        let response = send_command(port, "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xread_block_timeout() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add an entry
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        let start = Instant::now();
        // XREAD with BLOCK that will timeout (no new entries after 100-0)
        let response = send_command(port, "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n500\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n");
        let elapsed = start.elapsed();

        // Should timeout and return null array
        assert_eq!(response, "*-1\r\n");
        // Should have waited at least 500ms
        assert!(elapsed >= Duration::from_millis(500));
    }

    #[test]
    fn test_xread_dollar_with_block_new_entries() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some existing entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Spawn a thread to add a new entry after a short delay
        use std::thread;
        let port_clone = port;
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            send_command(port_clone, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");
        });

        let start = Instant::now();
        // XREAD with BLOCK and $ - should wait for new entry
        let response = send_command(port, "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n");
        let elapsed = start.elapsed();

        // Should return the new entry (300-0) that was added after the call
        assert!(response.contains("mystream"));
        assert!(response.contains("300-0"));
        assert!(response.contains("value3"));
        // Should NOT contain the old entries
        assert!(!response.contains("100-0"));
        assert!(!response.contains("200-0"));
        // Should have waited at least 200ms (the delay before adding)
        assert!(elapsed >= Duration::from_millis(200));
    }

    #[test]
    fn test_xread_dollar_nonblocking_returns_empty() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add some existing entries
        send_command(port, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");

        // XREAD without BLOCK and $ should return empty (no new entries yet)
        let response = send_command(port, "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n");
        assert_eq!(response, "*0\r\n");
    }

    #[test]
    fn test_xread_dollar_with_nonexistent_stream() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Spawn a thread to create stream and add entry after delay
        use std::thread;
        let port_clone = port;
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            send_command(port_clone, "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        });

        let start = Instant::now();
        // XREAD with BLOCK and $ on non-existent stream - should wait and return new entry
        let response = send_command(port, "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n");
        let elapsed = start.elapsed();

        // Should return the new entry
        assert!(response.contains("mystream"));
        assert!(response.contains("100-0"));
        assert!(response.contains("value1"));
        // Should have waited at least 200ms
        assert!(elapsed >= Duration::from_millis(200));
    }

    #[test]
    fn test_xread_dollar_multiple_streams() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        // Add entries to both streams
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$5\r\n100-0\r\n$5\r\nfield\r\n$6\r\nvalue1\r\n");
        send_command(port, "*5\r\n$4\r\nXADD\r\n$7\r\nstream2\r\n$5\r\n200-0\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n");

        // Spawn thread to add new entries to both streams
        use std::thread;
        let port_clone = port;
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            send_command(port_clone, "*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$5\r\n300-0\r\n$5\r\nfield\r\n$6\r\nvalue3\r\n");
            send_command(port_clone, "*5\r\n$4\r\nXADD\r\n$7\r\nstream2\r\n$5\r\n400-0\r\n$5\r\nfield\r\n$6\r\nvalue4\r\n");
        });

        // XREAD with BLOCK and $ on both streams
        let response = send_command(port, "*8\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$7\r\nstream1\r\n$7\r\nstream2\r\n$1\r\n$\r\n$1\r\n$\r\n");

        // Should return at least one new entry (might not get both if timing is off)
        // At minimum, should get stream1 with 300-0
        assert!(response.contains("stream1"));
        assert!(response.contains("300-0"));
        assert!(response.contains("value3"));
        // Should NOT contain old entries
        assert!(!response.contains("100-0"));
        assert!(!response.contains("200-0"));
    }

    #[test]
    fn test_parse_multi() {
        let request = "*1\r\n$5\r\nMULTI\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], Command::Multi));
    }

    #[test]
    fn test_multi_basic() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let response = send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response, "+OK\r\n");
    }

    #[test]
    fn test_multi_queues_commands() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Start transaction
        let multi_response = send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(multi_response, "+OK\r\n");

        // Queue a SET command
        let set_response = send_command_with_stream(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        assert_eq!(set_response, "+QUEUED\r\n");

        // Queue an INCR command
        let incr_response = send_command_with_stream(&mut stream, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assert_eq!(incr_response, "+QUEUED\r\n");
    }

    #[test]
    fn test_multi_nested_error() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // First MULTI should succeed
        let response1 = send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response1, "+OK\r\n");

        // Second MULTI should fail
        let response2 = send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response2, "-ERR MULTI calls can not be nested\r\n");
    }

    #[test]
    fn test_parse_exec() {
        let request = "*1\r\n$4\r\nEXEC\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], Command::Exec));
    }

    #[test]
    fn test_exec_without_multi() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let response = send_command_with_stream(&mut stream, "*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response, "-ERR EXEC without MULTI\r\n");
    }

    #[test]
    fn test_exec_executes_queued_commands() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Start transaction
        let multi_response = send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(multi_response, "+OK\r\n");

        // Queue SET command
        let set_response = send_command_with_stream(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        assert_eq!(set_response, "+QUEUED\r\n");

        // Queue GET command
        let get_response = send_command_with_stream(&mut stream, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assert_eq!(get_response, "+QUEUED\r\n");

        // Execute transaction
        let exec_response = send_command_with_stream(&mut stream, "*1\r\n$4\r\nEXEC\r\n");

        // Should return array with 2 results: +OK for SET and the value for GET
        assert!(exec_response.starts_with("*2\r\n"));
        assert!(exec_response.contains("+OK\r\n"));
        assert!(exec_response.contains("$5\r\nvalue\r\n"));
    }

    #[test]
    fn test_exec_with_incr() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Start transaction
        send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");

        // Queue INCR commands
        send_command_with_stream(&mut stream, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        send_command_with_stream(&mut stream, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        send_command_with_stream(&mut stream, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");

        // Execute transaction
        let exec_response = send_command_with_stream(&mut stream, "*1\r\n$4\r\nEXEC\r\n");

        // Should return array with 3 results: :1, :2, :3
        assert!(exec_response.starts_with("*3\r\n"));
        assert!(exec_response.contains(":1\r\n"));
        assert!(exec_response.contains(":2\r\n"));
        assert!(exec_response.contains(":3\r\n"));
    }

    #[test]
    fn test_parse_discard() {
        let request = "*1\r\n$7\r\nDISCARD\r\n";
        let commands = parse_commands(request);
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], Command::Discard));
    }

    #[test]
    fn test_discard_without_multi() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let response = send_command_with_stream(&mut stream, "*1\r\n$7\r\nDISCARD\r\n");
        assert_eq!(response, "-ERR DISCARD without MULTI\r\n");
    }

    #[test]
    fn test_discard_clears_queue() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Start transaction
        send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");

        // Queue some commands
        send_command_with_stream(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        send_command_with_stream(&mut stream, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");

        // Discard transaction
        let discard_response = send_command_with_stream(&mut stream, "*1\r\n$7\r\nDISCARD\r\n");
        assert_eq!(discard_response, "+OK\r\n");

        // Verify that key was not set
        let get_response = send_command_with_stream(&mut stream, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assert_eq!(get_response, "$-1\r\n");

        // Verify that counter was not incremented
        let get_counter_response = send_command_with_stream(&mut stream, "*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n");
        assert_eq!(get_counter_response, "$-1\r\n");
    }

    #[test]
    fn test_discard_resets_transaction_state() {
        let (_server, port) = start_test_server();
        thread::sleep(Duration::from_millis(100));

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Start transaction
        send_command_with_stream(&mut stream, "*1\r\n$5\r\nMULTI\r\n");

        // Queue a command
        send_command_with_stream(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");

        // Discard transaction
        send_command_with_stream(&mut stream, "*1\r\n$7\r\nDISCARD\r\n");

        // Should be able to execute commands normally now (not in transaction)
        let set_response = send_command_with_stream(&mut stream, "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n");
        assert_eq!(set_response, "+OK\r\n");

        // Verify key2 was set
        let get_response = send_command_with_stream(&mut stream, "*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n");
        assert_eq!(get_response, "$6\r\nvalue2\r\n");
    }
}
