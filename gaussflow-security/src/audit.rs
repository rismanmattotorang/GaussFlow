//! Append-only, tamper-evident audit log.
//!
//! Each event is chained to the previous one via a SHA-256 hash (`hash = H(seq | ts | actor |
//! action | target | prev_hash)`), so any insertion, deletion, reordering, or field edit breaks
//! the chain and is detected by [`AuditLog::verify`].

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const GENESIS: &str = "GENESIS";

/// A single audit event in the hash chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    /// Position in the log (0-based).
    pub seq: u64,
    /// Event time, seconds since the Unix epoch.
    pub timestamp_unix: u64,
    /// Who performed the action (e.g. a JWT subject).
    pub actor: String,
    /// What was done (e.g. `execute_workflow`).
    pub action: String,
    /// What it was done to (e.g. a workflow id).
    pub target: String,
    /// Hash of the previous event (or `GENESIS` for the first).
    pub prev_hash: String,
    /// Hash of this event.
    pub hash: String,
}

fn hash_entry(
    seq: u64,
    ts: u64,
    actor: &str,
    action: &str,
    target: &str,
    prev_hash: &str,
) -> String {
    let mut h = Sha256::new();
    h.update(format!("{seq}|{ts}|{actor}|{action}|{target}|{prev_hash}").as_bytes());
    format!("{:x}", h.finalize())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// An in-memory, append-only audit log with a verifiable hash chain.
#[derive(Debug, Default)]
pub struct AuditLog {
    events: Vec<AuditEvent>,
}

impl AuditLog {
    /// Create an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event, chaining it to the previous one. Returns the new event.
    pub fn append(&mut self, actor: &str, action: &str, target: &str) -> &AuditEvent {
        let seq = self.events.len() as u64;
        let prev_hash = self
            .events
            .last()
            .map(|e| e.hash.clone())
            .unwrap_or_else(|| GENESIS.to_string());
        let ts = now_unix();
        let hash = hash_entry(seq, ts, actor, action, target, &prev_hash);
        self.events.push(AuditEvent {
            seq,
            timestamp_unix: ts,
            actor: actor.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            prev_hash,
            hash,
        });
        self.events.last().expect("just pushed")
    }

    /// The recorded events.
    pub fn events(&self) -> &[AuditEvent] {
        &self.events
    }

    /// Verify the chain is intact: sequence, links, and recomputed hashes all match.
    pub fn verify(&self) -> bool {
        let mut prev = GENESIS.to_string();
        for (i, e) in self.events.iter().enumerate() {
            if e.seq != i as u64 || e.prev_hash != prev {
                return false;
            }
            let expected = hash_entry(
                e.seq,
                e.timestamp_unix,
                &e.actor,
                &e.action,
                &e.target,
                &e.prev_hash,
            );
            if expected != e.hash {
                return false;
            }
            prev = e.hash.clone();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_verifies_and_detects_tampering() {
        let mut log = AuditLog::new();
        log.append("alice", "create_workflow", "wf-1");
        log.append("alice", "execute_workflow", "wf-1");
        log.append("bob", "deploy", "wf-1");
        assert!(log.verify());
        assert_eq!(log.events().len(), 3);
        // Each event links to the prior one.
        assert_eq!(log.events()[0].prev_hash, "GENESIS");
        assert_eq!(log.events()[1].prev_hash, log.events()[0].hash);

        // Tamper with a recorded field → chain breaks.
        let mut tampered = AuditLog::new();
        tampered.append("alice", "create_workflow", "wf-1");
        tampered.append("alice", "execute_workflow", "wf-1");
        tampered.events[0].action = "delete_everything".to_string();
        assert!(!tampered.verify(), "editing a past event must be detected");
    }

    #[test]
    fn deleting_an_event_is_detected() {
        let mut log = AuditLog::new();
        log.append("a", "x", "t");
        log.append("b", "y", "t");
        log.append("c", "z", "t");
        // Remove the middle event → seq/links no longer match.
        log.events.remove(1);
        assert!(!log.verify());
    }
}
