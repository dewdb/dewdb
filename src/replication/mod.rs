//! Replication: ship frames to replicas, repair stragglers, gate write acks.

pub mod protocol;
pub mod snapshot;
pub mod stream;
pub mod write_concern;

pub use protocol::{DropRequest, ReplicateRequest, ResyncRequest};
pub use write_concern::{
    parse_write_concern, wc_query_string, WriteConcern, WriteConcernParams, DEFAULT_WTIMEOUT_MS,
};
