//! Logging module for HDN.

extern crate env_logger;
extern crate log;
use chrono::{DateTime, Utc};
use log::info;

/// Writes log message in specified format.
/// Log message includes peer ip, time and message
/// itself.
pub fn write_log(peer_ip: String, message: &str) {
    let now: DateTime<Utc> = Utc::now();
    let time_prefix = now.format("[%d/%b/%Y:%H/%M/%S %z]");

    info!("{} {} {}", peer_ip, time_prefix, message);
}
