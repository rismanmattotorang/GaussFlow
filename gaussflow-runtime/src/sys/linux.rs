//! Linux-specific process information using procfs

use procfs::process::Process;
use std::collections::HashMap;

/// Get process CPU time on Linux
pub fn get_process_cputime(pid: i32) -> Option<(u64, u64)> {
    if let Ok(process) = Process::new(pid) {
        if let Ok(stat) = process.stat() {
            return Some((stat.utime, stat.stime));
        }
    }
    None
}

/// Get process status information on Linux.
///
/// `procfs::process::Status` is a typed struct (not a map), so we surface a small, stable
/// subset of fields as string key/value pairs.
pub fn get_process_status(pid: i32) -> Option<HashMap<String, String>> {
    let process = Process::new(pid).ok()?;
    let status = process.status().ok()?;
    let mut map = HashMap::new();
    map.insert("name".to_string(), status.name);
    map.insert("pid".to_string(), status.pid.to_string());
    map.insert("ppid".to_string(), status.ppid.to_string());
    Some(map)
}
