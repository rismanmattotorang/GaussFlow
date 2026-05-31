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

/// Get process status information on Linux
pub fn get_process_status(pid: i32) -> Option<HashMap<String, String>> {
    if let Ok(process) = Process::new(pid) {
        if let Ok(status) = process.status() {
            return Some(status.0);
        }
    }
    None
}
