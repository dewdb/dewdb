//! Durable term/vote state and the demotion transition.

use super::progress::Progress;
use crate::util::write_atomic;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const META_FILE: &str = "replication.meta";
const META_TMP: &str = "replication.meta.tmp";

// Guards the single staging path against interleaved writers, and orders the
// renames: last decided wins, not whichever fsync returned first.
static SAVE_LOCK: Mutex<()> = Mutex::new(());

// Raft persistent state. Written only as a pair: a term without its vote permits a double vote.
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
    /// `Ok(None)` is no consensus history; unreadable is an error, not a fresh start.
    /// Booting at term 0 would re-enable a vote this node has already cast.
    pub fn load(data_dir: &str) -> io::Result<Option<Self>> {
        let dir = Path::new(data_dir);
        let mut corrupt: Option<(String, String)> = None;

        // The staging file exists only if we died between its fsync and the rename: complete, and no older.
        for name in [META_FILE, META_TMP] {
            match fs::read_to_string(dir.join(name)) {
                Ok(content) => match serde_json::from_str::<Self>(&content) {
                    Ok(meta) => return Ok(Some(meta)),
                    Err(e) => corrupt = Some((name.to_string(), e.to_string())),
                },
                Err(e) if e.kind() == io::ErrorKind::NotFound => {},
                Err(e) => return Err(e),
            }
        }

        match corrupt {
            Some((name, why)) => Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                "{} is unreadable ({}); refusing to continue with a forgotten term and vote",
                name, why))),
            None => Ok(None),
        }
    }

    /// Durable on return. Callers must not act on a term or vote before it succeeds.
    pub fn save(&self, data_dir: &str) -> io::Result<()> {
        let dir = PathBuf::from(data_dir);
        let _guard = SAVE_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Terms are decided under the replication lock and persisted after releasing it;
        // saves can arrive out of order and the durable term must never rewind.
        if let Ok(Some(existing)) = Self::load(data_dir) {
            if self.term < existing.term {
                return Ok(());
            }
        }

        let content = serde_json::to_vec(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        write_atomic(&dir, META_FILE, &content)
    }
}

// Strictly higher only: on equal terms a peer bounces the leader with every message.
pub fn apply_demotion(repl: &mut ReplicationState, new_term: u64) -> Option<bool> {
    if new_term <= repl.term {
        return None;
    }
    repl.term = new_term;
    repl.voted_for = None;
    repl.is_leader = false;
    // Quorum evidence belongs to the term it was gathered in. Kept, it goes on being served as a
    // commit watermark by a node that no longer has the standing to have one.
    repl.progress.reset();
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
    use crate::test_support::temp_root;

    fn dir_of(root: &PathBuf) -> String {
        root.to_string_lossy().to_string()
    }

    #[test]
    fn term_and_vote_survive_a_reload() {
        let root = temp_root();
        let dir = dir_of(&root);

        ReplicationMeta { term: 7, is_leader: false, voted_for: Some("n2".into()) }.save(&dir).unwrap();
        let back = ReplicationMeta::load(&dir).unwrap().expect("record must be there");

        assert_eq!(back.term, 7);
        assert_eq!(back.voted_for.as_deref(), Some("n2"), "the vote is useless if only the term survives");
        assert!(!back.is_leader);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn an_absent_record_reads_as_a_fresh_node() {
        let root = temp_root();
        assert!(ReplicationMeta::load(&dir_of(&root)).unwrap().is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn an_unreadable_record_is_an_error_not_a_fresh_node() {
        let root = temp_root();
        let dir = dir_of(&root);

        ReplicationMeta { term: 9, is_leader: false, voted_for: Some("n1".into()) }.save(&dir).unwrap();
        fs::write(root.join(META_FILE), b"{\"term\": 9, \"is_lea").unwrap();

        let err = ReplicationMeta::load(&dir).expect_err(
            "a truncated record must not read as term 0; that would let this node \
             vote a second time in a term it already voted in");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_crash_before_the_rename_recovers_from_the_staging_file() {
        let root = temp_root();
        let dir = dir_of(&root);

        ReplicationMeta { term: 4, is_leader: false, voted_for: Some("n3".into()) }.save(&dir).unwrap();
        fs::rename(root.join(META_FILE), root.join(META_TMP)).unwrap();

        let back = ReplicationMeta::load(&dir).unwrap()
            .expect("a record fsynced but not yet renamed is still a complete record");
        assert_eq!(back.term, 4);
        assert_eq!(back.voted_for.as_deref(), Some("n3"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn the_durable_term_never_rewinds() {
        let root = temp_root();
        let dir = dir_of(&root);

        ReplicationMeta { term: 6, is_leader: false, voted_for: Some("n1".into()) }.save(&dir).unwrap();
        ReplicationMeta { term: 5, is_leader: false, voted_for: Some("n2".into()) }.save(&dir).unwrap();

        let back = ReplicationMeta::load(&dir).unwrap().unwrap();
        assert_eq!(back.term, 6, "a save decided at an older term must not overwrite a newer one");
        assert_eq!(back.voted_for.as_deref(), Some("n1"),
            "term and vote move together, so the stale vote must not land either");

        ReplicationMeta { term: 6, is_leader: true, voted_for: Some("n1".into()) }.save(&dir).unwrap();
        assert!(ReplicationMeta::load(&dir).unwrap().unwrap().is_leader,
            "the same term must still be updatable, or winning an election could not be recorded");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn saving_leaves_no_staging_file_behind() {
        let root = temp_root();
        let dir = dir_of(&root);

        ReplicationMeta { term: 1, is_leader: false, voted_for: None }.save(&dir).unwrap();
        ReplicationMeta { term: 2, is_leader: false, voted_for: Some("n1".into()) }.save(&dir).unwrap();

        assert!(!root.join(META_TMP).exists(),
            "a leftover staging file would be mistaken for an interrupted save on the next boot");
        assert_eq!(ReplicationMeta::load(&dir).unwrap().unwrap().term, 2);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_save_that_cannot_reach_disk_reports_the_failure() {
        let root = temp_root();
        let missing = root.join("gone");

        let err = ReplicationMeta { term: 1, is_leader: false, voted_for: Some("n1".into()) }
            .save(&missing.to_string_lossy());

        assert!(err.is_err(),
            "the vote path denies votes on this error, so it must surface rather than be swallowed");

        let _ = fs::remove_dir_all(&root);
    }

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
