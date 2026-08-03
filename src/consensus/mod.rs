//! Leader election and failover.

pub mod election;
pub mod failover;
pub mod progress;
pub mod state;

pub use election::{decide_vote, VoteRequest, VoteResponse};
pub use failover::{demote, heartbeat_poll_task};
pub use progress::Progress;
pub use state::{ReplicationMeta, ReplicationState};
