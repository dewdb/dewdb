//! Durable term/vote state and the demotion transition.

use super::progress::Progress;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationMeta {
    pub term: u64,
    pub is_leader: bool,
    #[serde(default)]
    pub voted_for: Option<String>,
}

pub struct ReplicationState {
    pub term: u64,
    pub is_leader: bool,
    pub voted_for: Option<String>,
    pub last_heartbeat: Option<std::time::Instant>,
    pub was_receiving_replication: bool,
    pub last_replication: Option<std::time::Instant>,
    pub heartbeat_running: bool,
    pub primary_addr: Option<String>,
    pub replicas: Vec<String>,
    pub last_known_primary_position: Option<u64>,
    // Leader side: what each replica holds, and what a quorum has committed.
    pub progress: Progress,
    // Follower side: the commit watermark the leader last told us, per collection.
    pub leader_committed: HashMap<String, u64>,
}

impl ReplicationMeta {
    pub fn load(data_dir: &str) -> Option<Self> {
        let path = PathBuf::from(data_dir).join("replication.meta");
        let content = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&content).ok()
    }

    pub fn save(&self, data_dir: &str) -> io::Result<()> {
        let path = PathBuf::from(data_dir).join("replication.meta");
        let content = serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, content)
    }
}

// Only a strictly higher term demotes; on equal terms a peer could bounce the
// leader with every message.
pub fn apply_demotion(repl: &mut ReplicationState, new_term: u64) -> Option<bool> {
    if new_term <= repl.term {
        return None;
    }
    repl.term = new_term;
    repl.voted_for = None;
    repl.is_leader = false;
    repl.last_heartbeat = Some(std::time::Instant::now());
    repl.last_replication = None;
    repl.was_receiving_replication = false;
    let restart = !repl.heartbeat_running;
    repl.heartbeat_running = true;
    Some(restart)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leader_state(term: u64) -> ReplicationState {
        ReplicationState {
            term,
            is_leader: true,
            voted_for: None,
            last_heartbeat: None,
            was_receiving_replication: true,
            last_replication: Some(std::time::Instant::now()),
            heartbeat_running: false,
            primary_addr: None,
            replicas: vec![],
            last_known_primary_position: None,
            progress: Progress::new(),
            leader_committed: HashMap::new(),
        }
    }

    #[test]
    fn demotion_transitions_leader_to_follower() {
        let mut r = leader_state(2);

        assert_eq!(apply_demotion(&mut r, 2), None, "equal term is not a demotion");
        assert!(r.is_leader);
        assert_eq!(apply_demotion(&mut r, 1), None, "lower term is not a demotion");
        assert!(r.is_leader);

        assert_eq!(apply_demotion(&mut r, 5), Some(true), "higher term demotes and needs poll restart");
        assert!(!r.is_leader);
        assert_eq!(r.term, 5);
        assert!(r.heartbeat_running);
        assert!(!r.was_receiving_replication);

        assert_eq!(apply_demotion(&mut r, 7), Some(false), "already-following node adopts term without restarting poll");
        assert_eq!(r.term, 7);
        assert!(!r.is_leader);
    }
}
