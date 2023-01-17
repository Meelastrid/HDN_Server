//! DB module for HDN.

extern crate chrono;
extern crate csv;
extern crate lazy_static;
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate threadpool;

#[cfg(test)]
use std::fs::{self, File};
#[cfg(test)]
use std::io::Write;

/// Reads DB and updates the mapping.
pub fn read_db(file: &str) -> std::io::Result<()> {
    let mut mapping = crate::MAPPING
        .write()
        .map_err(|_| "Failed to acquire write lock")
        .unwrap();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(file)
        .unwrap();
    for result in reader.records() {
        let record = result.unwrap();
        mapping.insert(record[0].to_string(), record[1].to_string());
    }
    Ok(())
}

#[test]
fn test_read_db() {
    let mut test_file = File::create("/tmp/testdb").unwrap();
    let _ = test_file.write("some_key1,1111\n".as_bytes()).unwrap();
    _ = test_file.write("some_key2,2222\n".as_bytes()).unwrap();
    let result = read_db("/tmp/testdb");
    assert!(result.is_ok(), "should not result in error");
    fs::remove_file("/tmp/testdb").unwrap();
}
