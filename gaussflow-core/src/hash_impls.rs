use crate::dag::EdgeMetadata;
use crate::model::{Backoff, EdgeSpec, RetrySpec, WorkflowSettings};
use crate::resource::ResourceSpec;
use bincode;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Helper function to hash any serializable type
#[allow(dead_code)] // scaffolding retained for a later phase (scheduler/executor/policy/planner wiring)
fn hash_value<T: serde::Serialize>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    let bytes = bincode::serialize(value).unwrap_or_default();
    bytes.hash(&mut hasher);
    hasher.finish()
}

impl Hash for EdgeSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.from.hash(state);
        self.to.hash(state);
        self.on.hash(state);
    }
}

impl Hash for EdgeMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.description.hash(state);
        self.priority.hash(state);
        self.retry_policy.hash(state);
        for (k, v) in &self.labels {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl Hash for RetrySpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.max_attempts.hash(state);
        self.backoff.hash(state);
        self.timeout.hash(state);
    }
}

impl Hash for Backoff {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Backoff::Fixed => 0.hash(state),
            Backoff::Exponential => 1.hash(state),
            Backoff::Linear => 2.hash(state),
        }
    }
}

impl Hash for WorkflowSettings {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.concurrency.hash(state);
        self.fail_fast.hash(state);
        self.resume.hash(state);
        self.enable_checkpoints.hash(state);
        self.checkpoint_interval_ms.hash(state);
        self.max_checkpoints.hash(state);
        self.version.hash(state);
    }
}

impl Hash for ResourceSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cpu_cores.hash(state);
        self.memory_mb.hash(state);
        self.gpu_count.hash(state);
        self.memory.hash(state);
        self.concurrency.hash(state);
        self.timeout_ms.hash(state);
        self.priority.hash(state);
        self.remote.hash(state);
        self.required_executor.hash(state);
        self.cpu_millicores.hash(state);
        self.retry_attempts.hash(state);
        self.retry_delay_ms.hash(state);
        self.affinity.hash(state);
        for (k, v) in &self.labels {
            k.hash(state);
            v.hash(state);
        }
    }
}

// Implement Hash for serde_json::Value
pub fn hash_json_value<H: Hasher>(value: &Value, state: &mut H) {
    use Value::*;
    match value {
        Null => 0u8.hash(state),
        Bool(b) => b.hash(state),
        Number(n) => {
            if let Some(i) = n.as_i64() {
                i.hash(state);
            } else if let Some(u) = n.as_u64() {
                u.hash(state);
            } else if let Some(f) = n.as_f64() {
                f.to_bits().hash(state);
            }
        }
        String(s) => s.hash(state),
        Array(a) => {
            a.len().hash(state);
            for v in a {
                hash_json_value(v, state);
            }
        }
        Object(o) => {
            let mut hash = 0u64;
            for (k, v) in o {
                let mut hasher = DefaultHasher::new();
                k.hash(&mut hasher);
                hash_json_value(v, &mut hasher);
                hash = hash.wrapping_add(hasher.finish());
            }
            hash.hash(state);
        }
    }
}

// Implement Hash for HashMap<String, Value>
pub fn hash_json_map<H: Hasher>(map: &std::collections::HashMap<String, Value>, state: &mut H) {
    let mut entries: Vec<_> = map.iter().collect();
    // Sort entries by key for consistent hashing
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (k, v) in entries {
        k.hash(state);
        hash_json_value(v, state);
    }
}
