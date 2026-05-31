//! Non-Linux implementation of process information

use std::collections::HashMap;

/// Get process CPU time (dummy implementation for non-Linux)
pub fn get_process_cputime(_pid: i32) -> Option<(u64, u64)> {
    // Return zeros for non-Linux platforms
    Some((0, 0))
}

/// Get process status information (dummy implementation for non-Linux)
pub fn get_process_status(_pid: i32) -> Option<HashMap<String, String>> {
    // Return an empty map for non-Linux platforms
    Some(HashMap::new())
}
