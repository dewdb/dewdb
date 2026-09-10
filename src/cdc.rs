//! Change data capture: one collection's committed changes, filtered once and delivered to any transport.
//! SSE, WebSocket and webhook delivery all consume this, so filtering and resume are stated once.

use crate::auth::Credential;
use crate::changefeed::{ChangeEvent, ChangeOp, FeedEnd, Subscription};
use crate::cluster::changestream::ClusterStream;
use crate::query::{parse_filter, Filter};
use crate::state::AppState;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

/// How often a leader-only stream re-checks that it still leads. A stream is one request that keeps
/// answering, so leadership is not a thing it can check once the way every other read does.
const LEADERSHIP_POLL: Duration = Duration::from_secs(1);
/// How often a connection re-checks the credential it opened with, for the same reason (IB-026).
const AUTH_RECHECK: Duration = Duration::from_secs(2);

/// Which ops a subscriber asked for. A list rather than a set: there are four.
pub struct OpFilter(Option<Vec<ChangeOp>>);

impl OpFilter {
    pub fn parse(spec: Option<&str>) -> Result<Self, String> {
        let Some(spec) = spec else { return Ok(Self(None)) };
        let ops = spec.split(',').map(str::trim).filter(|s| !s.is_empty())
            .map(ChangeOp::parse).collect::<Result<Vec<_>, _>>()?;
        if ops.is_empty() {
            return Err("ops must name at least one of insert, update, delete, drop".to_string());
        }
        Ok(Self(Some(ops)))
    }

    pub fn admits(&self, op: ChangeOp) -> bool {
        self.0.as_ref().is_none_or(|ops| ops.contains(&op))
    }
}

/// A document predicate and an op list, in the shapes the query engine and the feed already use.
pub struct CdcFilter {
    doc: Option<Filter>,
    ops: OpFilter,
}

impl CdcFilter {
    pub fn parse(filter: Option<&str>, ops: Option<&str>) -> Result<Self, String> {
        Ok(Self {
            doc: filter.map(parse_filter).transpose()?,
            ops: OpFilter::parse(ops)?,
        })
    }

    /// The document filter governs events that carry a document and nothing else: suppressing a delete it
    /// cannot test would leave a subscriber believing the match is still there. An op list still excludes.
    pub fn admits(&self, event: &ChangeEvent) -> bool {
        if !self.ops.admits(event.op) {
            return false;
        }
        match (&event.value, &self.doc) {
            (Some(value), Some(doc)) => doc.matches(value),
            _ => true,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CdcEnd {
    /// The consumer fell behind the feed's buffer. Carries the lowest position still resumable.
    Overrun(u64),
    Closed,
    /// The consumer asked for the leader and this node stopped being it.
    NotLeading,
}

impl CdcEnd {
    pub fn message(&self) -> &'static str {
        match self {
            Self::Overrun(_) =>
                "this subscriber fell behind the change buffer; resubscribe from `resume_floor`",
            Self::Closed =>
                "the collection was replaced or closed under this stream; resubscribe",
            Self::NotLeading =>
                "this node no longer leads the shard group; resubscribe from `position`",
        }
    }
}

/// A filtered view of one subscription. Holds no task of its own, so a consumer that goes away
/// drops the `Subscription` and the feed sees it leave.
pub struct CdcStream {
    sub: Subscription,
    filter: CdcFilter,
    queue: VecDeque<Arc<ChangeEvent>>,
    /// The consumer asked for the leader, so this stream owes it one for as long as it runs.
    leader_only: Option<AppState>,
}

impl CdcStream {
    pub fn new(sub: Subscription, filter: CdcFilter, leader_only: Option<AppState>) -> Self {
        Self { sub, filter, queue: VecDeque::new(), leader_only }
    }

    /// Where a resume picks up. It follows what was read from the feed, not what the filter let
    /// through: an event the filter dropped is one this consumer never needs again.
    pub fn position(&self) -> u64 {
        self.sub.position()
    }

    pub async fn next(&mut self) -> Result<Arc<ChangeEvent>, CdcEnd> {
        loop {
            while let Some(event) = self.queue.pop_front() {
                if self.filter.admits(&event) {
                    return Ok(event);
                }
            }
            // Ended rather than left quiet: a stepped-down leader publishes nothing more, and a
            // consumer that asked for the leader would sit on a stream that stopped saying so.
            if self.leader_only.as_ref().is_some_and(|state| !state.is_leader()) {
                return Err(CdcEnd::NotLeading);
            }
            // `next_batch` waits on a `watch`, which is cancel-safe, so a lapsed poll drops nothing.
            let batch = match &self.leader_only {
                Some(_) => match tokio::time::timeout(LEADERSHIP_POLL, self.sub.next_batch()).await {
                    Ok(batch) => batch,
                    Err(_) => continue,
                },
                None => self.sub.next_batch().await,
            };
            match batch {
                Ok(batch) => self.queue.extend(batch),
                Err(FeedEnd::Overrun(floor)) => return Err(CdcEnd::Overrun(floor)),
                Err(FeedEnd::Closed) => return Err(CdcEnd::Closed),
            }
        }
    }
}

/// One event as a transport hands it on: a name, the position a reconnect would carry, and a body.
/// SSE renders it as an event with an `id`, WebSocket as one JSON text message.
pub struct CdcFrame {
    pub name: &'static str,
    pub id: Option<String>,
    pub body: serde_json::Value,
}

impl CdcFrame {
    /// What a WebSocket sends. The name rides inside the object, since a text frame carries no
    /// envelope of its own the way an SSE block does.
    pub fn message(&self) -> serde_json::Value {
        let mut body = self.body.clone();
        if let Some(fields) = body.as_object_mut() {
            fields.insert("type".to_string(), serde_json::json!(self.name));
            if let Some(id) = &self.id {
                fields.insert("position".to_string(), serde_json::json!(id));
            }
        }
        body
    }
}

/// The frame sequence every transport delivers: one `open`, then `change`s, then at most one
/// `error` naming why it stopped.
pub struct CdcSession {
    stream: CdcStream,
    collection: String,
    opened: bool,
    ended: bool,
}

impl CdcSession {
    pub fn new(collection: String, stream: CdcStream) -> Self {
        Self { stream, collection, opened: false, ended: false }
    }

    pub async fn next_frame(&mut self) -> Option<CdcFrame> {
        if !self.opened {
            self.opened = true;
            return Some(CdcFrame {
                name: "open",
                id: None,
                body: serde_json::json!({
                    "collection": self.collection,
                    "position": self.stream.position(),
                }),
            });
        }
        if self.ended {
            return None;
        }
        match self.stream.next().await {
            Ok(event) => Some(CdcFrame {
                name: "change",
                // The id is the resume position, so a reconnect carrying it picks up exactly here.
                id: Some(event.lsn.to_string()),
                body: serde_json::to_value(&*event).unwrap_or(serde_json::Value::Null),
            }),
            Err(end) => {
                self.ended = true;
                let mut body = serde_json::json!({"error": end.message()});
                match end {
                    CdcEnd::Overrun(floor) => body["resume_floor"] = floor.into(),
                    CdcEnd::NotLeading => body["position"] = self.stream.position().into(),
                    CdcEnd::Closed => {},
                }
                Some(CdcFrame { name: "error", id: None, body })
            },
        }
    }
}

/// One collection's changes as a transport reads them, from whichever side of the cluster the node
/// answering sits on.
pub enum ChangeSource {
    Shard(CdcSession),
    Cluster(ClusterStream),
}

impl ChangeSource {
    async fn next_frame(&mut self) -> Option<CdcFrame> {
        match self {
            Self::Shard(session) => session.next_frame().await,
            Self::Cluster(stream) => stream.next_frame().await,
        }
    }
}

/// Ended in-band with the same `error` frame an overrun uses, so a subscriber learns why rather
/// than seeing a stream that went quiet.
pub const REVOKED: &str =
    "the credential this stream was opened with is no longer accepted; resubscribe";

/// A change stream and the re-authorization it needs because it answers past the request that opened
/// it: a revoked key has to end the stream rather than have been checked once.
pub struct ChangeStream {
    source: ChangeSource,
    /// Absent only where nothing authorized the stream in the first place, which is a test
    /// building one without going through the middleware.
    credential: Option<(AppState, Credential)>,
    revoked: bool,
}

impl ChangeStream {
    pub fn new(source: ChangeSource, state: &AppState, credential: Option<Credential>) -> Self {
        Self {
            source,
            credential: credential.map(|c| (state.clone(), c)),
            revoked: false,
        }
    }

    pub async fn next_frame(&mut self) -> Option<CdcFrame> {
        if self.revoked {
            return None;
        }
        if self.credential.is_none() {
            return self.source.next_frame().await;
        }
        loop {
            // `next_frame` waits on a `watch` or an `mpsc`, both cancel-safe, so a lapsed poll
            // drops no event.
            let polled = tokio::time::timeout(AUTH_RECHECK, self.source.next_frame()).await;
            if let Ok(frame) = polled {
                return frame;
            }
            let gone = self.credential.as_ref()
                .is_some_and(|(state, credential)| !credential.allowed(&state.auth()));
            if gone {
                self.revoked = true;
                return Some(CdcFrame {
                    name: "error",
                    id: None,
                    body: serde_json::json!({"error": REVOKED}),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(op: ChangeOp, value: Option<serde_json::Value>) -> ChangeEvent {
        ChangeEvent { lsn: 1, op, key: "k1".to_string(), value }
    }

    #[test]
    fn an_op_list_is_parsed_or_refused() {
        assert!(OpFilter::parse(Some("insert,delete")).unwrap().admits(ChangeOp::Insert));
        assert!(!OpFilter::parse(Some("insert,delete")).unwrap().admits(ChangeOp::Update));
        assert!(OpFilter::parse(None).unwrap().admits(ChangeOp::Drop), "absent is every op");
        assert!(OpFilter::parse(Some("upsert")).is_err());
        assert!(OpFilter::parse(Some(",")).is_err(), "an empty list would silence the stream");
    }

    /// A delete carries no document, so a document filter cannot judge it. Dropping it would leave
    /// a subscriber believing a match it was watching is still there.
    #[test]
    fn a_document_filter_governs_the_events_that_carry_a_document() {
        let f = CdcFilter::parse(Some(r#"{"status":"active"}"#), None).unwrap();

        assert!(f.admits(&event(ChangeOp::Insert, Some(serde_json::json!({"status": "active"})))));
        assert!(!f.admits(&event(ChangeOp::Update, Some(serde_json::json!({"status": "done"})))));
        assert!(f.admits(&event(ChangeOp::Delete, None)));
        assert!(f.admits(&event(ChangeOp::Drop, None)));

        let writes = CdcFilter::parse(None, Some("insert,update")).unwrap();
        assert!(!writes.admits(&event(ChangeOp::Delete, None)),
            "an op list still excludes a delete, because that one was asked for explicitly");
    }

    /// A WebSocket frame carries no envelope of its own, so the name and the resume position have
    /// to be inside the object a client parses.
    #[test]
    fn a_websocket_message_carries_its_own_name_and_position() {
        let frame = CdcFrame {
            name: "change",
            id: Some("7".to_string()),
            body: serde_json::json!({"lsn": 7, "op": "insert", "key": "k1"}),
        };
        let message = frame.message();
        assert_eq!(message["type"].as_str(), Some("change"));
        assert_eq!(message["position"].as_str(), Some("7"));
        assert_eq!(message["key"].as_str(), Some("k1"));
    }
}
