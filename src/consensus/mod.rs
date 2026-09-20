//! Leader election and failover.

pub mod config;
pub mod election;
pub mod failover;
pub mod lease;
pub mod progress;
pub mod read_index;
pub mod reconfigure;
pub mod recovery;
pub mod state;
pub mod transfer;

pub use election::{
    decide_pre_vote, decide_vote, log_summary, publish_inherited_tails,
    seed_leader_progress, VoteRequest, VoteResponse,
};
pub use failover::{
    boot_resync_if_replica, demote, heartbeat_poll_task, leader_contact_task, progress_flush_task,
};
pub use progress::Progress;
pub use state::{ReplicationMeta, ReplicationState};
pub use transfer::{transfer_leadership, TransferError};
