#![crate_name = "hdn"]

//! HDN is a Hash Delivery network. It runs on a network
//! socket(default "127.0.0.1:7878") and accepts connections over TCP.
//! It uses JSON API and accepts two types of queries: "store" and "load".
extern crate chrono;
extern crate csv;
extern crate lazy_static;
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate threadpool;
mod db;
mod logger;

use log::LevelFilter;
use std::env;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufRead;
use std::net::TcpListener;
use std::net::TcpStream;

use lazy_static::lazy_static;
use std::collections::hash_map::Entry::Occupied;
use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::{json, Value};
use threadpool::ThreadPool;

lazy_static! {
    /// Mapping structure for keys and hashes.
    pub static ref MAPPING: RwLock<HashMap<String, String>> = RwLock::new(HashMap::new());
}

/// Program entry point.
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && args[1] == "--help" {
        println!("HDN is a multi threaded Hash Delivery network server.");
        println!("default network socket: 127.0.0.1:7878");
        println!("query examples:");
        println!("store a key: echo '{{ \"request_type\": \"store\", \"key\": \"some_key6\", \"hash\": \"0b672dd94fd3da6a8d404b66ee3f0c80\" }}' | nc -v 127.0.0.1 7878");
        println!("load a key: echo '{{ \"request_type\": \"load\", \"key\": \"some_key\" }}' | nc -v 127.0.0.1 7878");
        return;
    }
    env_logger::Builder::new()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .filter(None, LevelFilter::Info)
        .init();
    let n_workers = 4;

    initialize().unwrap();

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    let pool = ThreadPool::new(n_workers);

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        pool.execute(|| {
            handle_connection(stream);
        });
    }

    println!("Shutting down.");
}

/// Initializes DB. Creates a db file if it is absent or reads
/// the contents into memory at start.
fn initialize() -> std::io::Result<()> {
    let hashdb = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("hashdb.dat")
        .unwrap();
    if hashdb.metadata().unwrap().len() == 0 {
        Ok(())
    } else {
        db::read_db("hashdb.dat")
    }
}

/// Greets at every new connection.
fn greetings() -> serde_json::Value {
    json!({"student_name": "Anastasia Medvedeva"})
}

#[test]
fn test_greetings() {
    let greetings = greetings();
    assert_eq!(greetings, json!({"student_name": "Anastasia Medvedeva"}));
}

/// Generates response based on request type.
///
/// For "store" request will update the DB and the internal mapping and return
/// a confirming JSON to the user.
/// For "load" request will perform a lookup in the mapping and return the hash
/// for requested key. If key is not present in the mapping will response with
/// "key not found". Will return "unknown request" for illegal request.
fn generate_response(input_data: serde_json::Value, peer: String) -> serde_json::Value {
    let request = &input_data["request_type"];
    if request == "store" {
        let mut mapping = MAPPING
            .write()
            .map_err(|_| "Failed to acquire write lock")
            .unwrap();
        let request_key = input_data["key"].as_str().unwrap();
        let request_hash = input_data["hash"].as_str().unwrap();
        let log_message = "Received request to write new value ".to_string()
            + request_hash
            + " by key "
            + request_key
            + ". Storage size: "
            + mapping.len().to_string().as_str()
            + ".";
        logger::write_log(peer, log_message.as_str());

        if let Occupied(_) = mapping.entry(request_key.to_string()) {
            mapping.insert(request_key.to_string(), request_hash.to_string());
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_path("hashdb.dat")
                .unwrap();
            for (key, value) in mapping.iter() {
                if key == &request_key.to_string() {
                    writer
                        .write_record(&[request_key.to_string(), request_hash.to_string()])
                        .unwrap();
                    writer.flush().unwrap();
                } else {
                    writer
                        .write_record(&[key.to_string(), value.to_string()])
                        .unwrap();
                    writer.flush().unwrap();
                }
            }

            writer.flush().unwrap();
        } else {
            let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(
                OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open("hashdb.dat")
                    .unwrap(),
            );
            mapping
                .entry(request_key.to_string())
                .or_insert_with(|| request_hash.to_string());
            writer
                .write_record(&[request_key.to_string(), request_hash.to_string()])
                .unwrap();
            writer.flush().unwrap();
        }
        json!({"response_status": "success"})
    } else if request == "load" {
        let mapping = MAPPING
            .read()
            .map_err(|_| "Failed to acquire read lock")
            .unwrap();

        let request_key = input_data["key"].as_str().unwrap();
        let log_message = "Received request to get value by key ".to_string()
            + request_key
            + ". Storage size: "
            + mapping.len().to_string().as_str()
            + ".";
        logger::write_log(peer, log_message.as_str());

        if mapping.contains_key(&request_key.to_string()) {
            json!({"response_status": "success", "requested_key": request_key, "requested_hash": mapping[&request_key.to_string()]})
        } else {
            json!({"response_status": "key not found"})
        }
    } else {
        json!({"response_status": "unknown request"})
    }
}

#[test]
fn test_generate_response() {
    let store_request = json!({ "request_type": "store", "key": "some_key", "hash": "0b672dd94fd3da6a8d404b66ee3f0c80" });
    let load_request = json!({ "request_type": "load", "key": "some_key" });
    let load_request_absent = json!({ "request_type": "load", "key": "absent_key" });
    let load_request_unknown = json!({ "request_type": "unknown" });
    let store_response = generate_response(store_request, "127.0.0.1".to_string());
    let load_response = generate_response(load_request, "127.0.0.1".to_string());
    let load_response_absent = generate_response(load_request_absent, "127.0.0.1".to_string());
    let load_response_unknown = generate_response(load_request_unknown, "127.0.0.1".to_string());
    assert_eq!(store_response, json!({"response_status": "success"}));
    assert_eq!(
        load_response,
        json!({"requested_hash": "0b672dd94fd3da6a8d404b66ee3f0c80", "requested_key": "some_key", "response_status": "success"})
    );
    assert_eq!(
        load_response_absent,
        json!({"response_status": "key not found"})
    );
    assert_eq!(
        load_response_unknown,
        json!({"response_status": "unknown request"})
    );
}

/// Returns current storage size.
fn get_storage_size() -> usize {
    let mapping = MAPPING
        .read()
        .map_err(|_| "Failed to acquire read lock")
        .unwrap();
    mapping.len()
}

/// Handles TCP connection. Will read the input stream until meets the "}" character,
/// then will parse the JSON and generate response. Will gracefully respond
/// on malformed JSON.
fn handle_connection(mut stream: TcpStream) {
    let log_message = "Connection established. Storage size: ".to_string()
        + get_storage_size().to_string().as_str()
        + ".";
    logger::write_log(stream.peer_addr().unwrap().ip().to_string(), &log_message);
    let greet_message = greetings();

    serde_json::to_writer_pretty(&stream, &greet_message).unwrap();
    let _ = stream.write("\n".as_bytes()).unwrap();
    stream.flush().unwrap();

    let mut buffer: Vec<u8> = Vec::new();
    std::io::BufReader::new(&stream)
        .read_until(b'}', &mut buffer)
        .unwrap();

    let input_data = serde_json::from_slice(&buffer);

    let response: Value = match input_data {
        Ok(input_data) => {
            generate_response(input_data, stream.peer_addr().unwrap().ip().to_string())
        }
        Err(error) => json!({"response_status": "json parsing failed", "error": error.to_string()}),
    };

    serde_json::to_writer_pretty(&stream, &response).unwrap();
}
