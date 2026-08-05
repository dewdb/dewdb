//! Leader election and failover.

pub mod election;
pub mod failover;
pub mod progress;
pub mod state;

pub use election::{
    decide_vote, local_log_tails, seed_leader_progress, LogTail, VoteRequest, VoteResponse,
};
pub use failover::{demote, heartbeat_poll_task, progress_flush_task};
pub use progress::Progress;
pub use state::{ReplicationMeta, ReplicationState};
