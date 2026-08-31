//! Leader election and failover.

pub mod config;
pub mod election;
pub mod failover;
pub mod progress;
pub mod read_index;
pub mod reconfigure;
pub mod state;

pub use election::{
    decide_vote, local_log_tails, log_summary, publish_inherited_tails, seed_leader_progress,
    VoteRequest, VoteResponse,
};
pub use failover::{boot_resync, demote, heartbeat_poll_task, progress_flush_task};
pub use progress::Progress;
pub use state::{ReplicationMeta, ReplicationState};
