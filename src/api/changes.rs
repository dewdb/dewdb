//! The change-stream endpoint: one committed collection feed, delivered as SSE.

use crate::api::docs::not_the_primary;
use crate::api::middleware::{client_collection, CollectionPath};
use crate::changefeed::{ChangeEvent, ChangeOp, FeedEnd, SubscribeError, Subscription};
use crate::cluster::router::{parse_read_pref, ReadPreference};
use crate::model::err_json;
use crate::query::{parse_filter, Filter};
use crate::state::AppState;
use crate::storage::Collection;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// SSE's own reconnection hint. A client that reconnects sends `Last-Event-ID`, which is the LSN
/// this stream last delivered, so the default browser retry resumes rather than restarts.
const RETRY_HINT: Duration = Duration::from_secs(2);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
pub struct ChangeParams {
    /// Resume position: deliver committed changes above this LSN.
    pub after: Option<u64>,
    /// A document filter, in the `/query` syntax.
    pub filter: Option<String>,
    /// Comma-separated `insert`, `update`, `delete`, `drop`. Absent is all four.
    pub ops: Option<String>,
    pub read: Option<String>,
}

/// Which ops a subscriber asked for. A list rather than a set: there are four.
struct OpFilter(Option<Vec<ChangeOp>>);

impl OpFilter {
    fn parse(spec: Option<&str>) -> Result<Self, String> {
        let Some(spec) = spec else { return Ok(Self(None)) };
        let ops = spec.split(',').map(str::trim).filter(|s| !s.is_empty())
            .map(ChangeOp::parse).collect::<Result<Vec<_>, _>>()?;
        if ops.is_empty() {
            return Err("ops must name at least one of insert, update, delete, drop".to_string());
        }
        Ok(Self(Some(ops)))
    }

    fn admits(&self, op: ChangeOp) -> bool {
        self.0.as_ref().is_none_or(|ops| ops.contains(&op))
    }
}

/// The filter governs the events that carry a document. Suppressing a delete it cannot test would
/// leave a subscriber believing the match it was watching is still there.
fn passes(event: &ChangeEvent, filter: &Option<Filter>, ops: &OpFilter) -> bool {
    if !ops.admits(event.op) {
        return false;
    }
    match (&event.value, filter) {
        (Some(value), Some(filter)) => filter.matches(value),
        _ => true,
    }
}

fn data_event(name: &str, body: serde_json::Value) -> Event {
    Event::default().event(name).data(body.to_string())
}

/// The stream's own state. Held by the `unfold` rather than a spawned task, so a client that
/// disconnects drops the `Subscription` and the feed sees it leave.
struct Stream {
    sub: Subscription,
    queue: VecDeque<Arc<ChangeEvent>>,
    filter: Option<Filter>,
    ops: OpFilter,
    collection: String,
    opened: bool,
    ended: bool,
}

async fn next_event(mut s: Stream) -> Option<(Result<Event, Infallible>, Stream)> {
    if !s.opened {
        s.opened = true;
        let open = data_event("open", serde_json::json!({
            "collection": s.collection,
            "position": s.sub.position(),
        })).retry(RETRY_HINT);
        return Some((Ok(open), s));
    }
    if s.ended {
        return None;
    }
    loop {
        while let Some(event) = s.queue.pop_front() {
            if !passes(&event, &s.filter, &s.ops) {
                continue;
            }
            // The id is the resume position, so a reconnect carrying it in `Last-Event-ID` picks
            // up exactly where this one stopped.
            let out = Event::default().event("change").id(event.lsn.to_string())
                .data(serde_json::to_string(&*event).unwrap_or_default());
            return Some((Ok(out), s));
        }
        match s.sub.next_batch().await {
            Ok(batch) => s.queue.extend(batch),
            Err(end) => {
                s.ended = true;
                let (error, floor) = match end {
                    FeedEnd::Overrun(floor) => (
                        "this subscriber fell behind the change buffer; resubscribe from `resume_floor`",
                        Some(floor)),
                    FeedEnd::Closed => (
                        "the collection was replaced or closed under this stream; resubscribe", None),
                };
                let mut body = serde_json::json!({"error": error});
                if let Some(floor) = floor {
                    body["resume_floor"] = floor.into();
                }
                return Some((Ok(data_event("error", body)), s));
            },
        }
    }
}

fn refuse(err: SubscribeError, col: &Arc<Collection>) -> axum::response::Response {
    match err {
        // `410` rather than `400`: the position was valid and the server stopped being able to
        // honour it, which is the difference between "retry from here" and "fix your request".
        SubscribeError::Overrun(floor) => (StatusCode::GONE, Json(serde_json::json!({
            "error": "that position is older than the change buffer still holds",
            "resume_floor": floor,
        }))).into_response(),
        SubscribeError::Ahead(position) => err_json(StatusCode::BAD_REQUEST, format!(
            "position is above this node's committed log, which ends at {}", position)),
        SubscribeError::TooMany(max) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            Json(serde_json::json!({
                "error": format!("collection '{}' is at its ceiling of {} change subscribers",
                    col.name, max),
            })),
        ).into_response(),
        SubscribeError::Closed => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            Json(serde_json::json!({"error": "this collection handle was replaced; retry"})),
        ).into_response(),
    }
}

pub async fn stream_changes(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<ChangeParams>,
    headers: HeaderMap,
) -> axum::response::Response {
    if state.config.role == "router" {
        return err_json(StatusCode::NOT_IMPLEMENTED,
            "a change stream is served by a shard group; subscribe to the shards directly"
                .to_string());
    }

    let filter = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(f) => f,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let ops = match OpFilter::parse(params.ops.as_deref()) {
        Ok(o) => o,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    let pref = match parse_read_pref(params.read.as_deref()) {
        // A read index makes one answer linearizable, and a stream is not one answer. Honouring it
        // as `primary` would be claiming a guarantee this endpoint has no way to hold.
        Ok(ReadPreference::Quorum) => return err_json(StatusCode::BAD_REQUEST,
            "a change stream is not a point-in-time read; use `read=primary`".to_string()),
        Ok(p) => p,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e),
    };
    if let Some(refusal) = not_the_primary(&state, &pref) {
        return refusal;
    }

    // An explicit `after` wins: `Last-Event-ID` is what the browser resends on its own reconnect,
    // and a client that named a position meant that one.
    let after = match params.after {
        Some(after) => Some(after),
        None => match headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
            Some(raw) => match raw.trim().parse::<u64>() {
                Ok(lsn) => Some(lsn),
                Err(_) => return err_json(StatusCode::BAD_REQUEST,
                    "Last-Event-ID must be a change position".to_string()),
            },
            None => None,
        },
    };

    let col = match client_collection(&state, &col_name) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    // Sampled before subscribing, so a feed that was quiet learns how far the log moved while it
    // was not recording, and refuses a resume from under that rather than skipping it silently.
    let applied = col.applied_lsn();
    let sub = match col.changefeed.subscribe(after, applied) {
        Ok(sub) => sub,
        Err(e) => return refuse(e, &col),
    };

    let stream = futures::stream::unfold(
        Stream {
            sub,
            queue: VecDeque::new(),
            filter,
            ops,
            collection: col_name,
            opened: false,
            ended: false,
        },
        next_event,
    );

    Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEPALIVE_INTERVAL)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changefeed::ChangeOp;
    use crate::test_support::{next_test_port, single_node, temp_root, SseTap, TestNode};

    const SETTLE: Duration = Duration::from_secs(5);

    async fn changes(client: &reqwest::Client, base: &str, query: &str) -> reqwest::Response {
        client.get(format!("{}/collections/c/changes{}", base, query)).send().await.unwrap()
    }

    async fn put(client: &reqwest::Client, base: &str, key: &str, body: serde_json::Value) {
        let r = client.put(format!("{}/collections/c/docs/{}", base, key))
            .json(&serde_json::json!({"value": body})).send().await.unwrap();
        assert!(r.status().is_success(), "write failed: {}", r.status());
    }

    /// Subscribing is a read, so it needs the collection to exist; only a write makes one. Every
    /// test here seeds a key first for that reason, and the seed predates the stream.
    async fn watch(client: &reqwest::Client, base: &str, query: &str) -> SseTap {
        let r = changes(client, base, query).await;
        assert_eq!(r.status(), StatusCode::OK, "{}", r.text().await.unwrap());
        let tap = SseTap::open(r);
        assert_eq!(tap.wait_for_events("open", 1, SETTLE).await.len(), 1,
            "the stream must announce its starting position before anything happens");
        tap
    }

    fn ops(events: &[crate::test_support::SseEvent]) -> Vec<String> {
        events.iter().map(|e| e.data["op"].as_str().unwrap_or("?").to_string()).collect()
    }

    /// The three keyed shapes, in log order, each carrying the document it left behind. A delete
    /// carries none, which is the whole reason `value` is optional on the wire.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stream_publishes_committed_inserts_updates_and_deletes() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = watch(&c, &n.url(), "").await;

        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        put(&c, &n.url(), "k1", serde_json::json!({"v": 2})).await;
        c.delete(format!("{}/collections/c/docs/k1", n.url())).send().await.unwrap();
        // Nothing was there, so nothing changed, so nothing is published.
        c.delete(format!("{}/collections/c/docs/absent", n.url())).send().await.unwrap();
        put(&c, &n.url(), "k2", serde_json::json!({"v": 3})).await;

        let seen = tap.wait_for_events("change", 4, SETTLE).await;
        assert_eq!(ops(&seen), vec!["insert", "update", "delete", "insert"],
            "a delete of an absent key changes nothing and must not be published: {:?}", seen);
        assert_eq!(seen[1].data["value"]["v"].as_i64(), Some(2), "an update carries the new document");
        assert!(seen[2].data.get("value").is_none(), "a delete has no document to carry");

        let ids: Vec<u64> = seen.iter().map(|e| e.id.as_ref().unwrap().parse().unwrap()).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "positions must increase: {:?}", ids);
    }

    /// A subscriber that goes away and comes back sees what it missed, once, in order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reconnect_resumes_from_the_position_it_left() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = watch(&c, &n.url(), "").await;
        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        let first = tap.wait_for_events("change", 1, SETTLE).await;
        let resume: u64 = first[0].id.as_ref().unwrap().parse().unwrap();
        drop(tap);

        put(&c, &n.url(), "k2", serde_json::json!({"v": 2})).await;
        put(&c, &n.url(), "k3", serde_json::json!({"v": 3})).await;

        let resumed = watch(&c, &n.url(), &format!("?after={}", resume)).await;
        let seen = resumed.wait_for_events("change", 2, SETTLE).await;
        assert_eq!(seen.iter().map(|e| e.data["key"].as_str().unwrap()).collect::<Vec<_>>(),
            vec!["k2", "k3"], "a resume must not repeat what it had, nor skip what it missed");
    }

    /// The bound is the buffer, not a per-subscriber queue, so falling behind is refused with the
    /// position that still works rather than answered with a stream that has a hole in it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_position_the_buffer_no_longer_holds_is_refused_with_one_that_works() {
        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.changefeed = serde_json::json!({"buffer_events": 2});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = watch(&c, &n.url(), "").await;
        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        let first = tap.wait_for_events("change", 1, SETTLE).await;
        let stale: u64 = first[0].id.as_ref().unwrap().parse().unwrap();

        for key in ["k2", "k3", "k4"] {
            put(&c, &n.url(), key, serde_json::json!({"v": 1})).await;
        }

        let refused = changes(&c, &n.url(), &format!("?after={}", stale)).await;
        assert_eq!(refused.status(), StatusCode::GONE);
        let body = refused.json::<serde_json::Value>().await.unwrap();
        let floor = body["resume_floor"].as_u64().expect("a refusal must name a usable position");
        assert!(floor > stale, "{:?}", body);
        assert_eq!(changes(&c, &n.url(), &format!("?after={}", floor)).await.status(), StatusCode::OK,
            "the position the refusal named has to be one the server will accept");
    }

    /// A filter selects among documents. A delete has none, so filtering it out would leave a
    /// subscriber believing the match it was watching is still there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_filter_selects_documents_and_still_reports_every_delete() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"status": "seed"})).await;
        let tap = watch(&c, &n.url(), "?filter=%7B%22status%22%3A%22active%22%7D").await;

        put(&c, &n.url(), "k1", serde_json::json!({"status": "active"})).await;
        put(&c, &n.url(), "k2", serde_json::json!({"status": "archived"})).await;
        c.delete(format!("{}/collections/c/docs/k2", n.url())).send().await.unwrap();

        let seen = tap.wait_for_events("change", 2, SETTLE).await;
        assert_eq!(ops(&seen), vec!["insert", "delete"]);
        assert_eq!(seen[0].data["key"].as_str(), Some("k1"));
        assert_eq!(seen[1].data["key"].as_str(), Some("k2"),
            "the delete of a document the filter never matched still has to be reported");

        assert_eq!(changes(&c, &n.url(), "?filter=%7B%22%24nope%22%3A1%7D").await.status(),
            StatusCode::BAD_REQUEST, "a filter the engine cannot evaluate is refused, not ignored");
        assert_eq!(changes(&c, &n.url(), "?ops=upsert").await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(changes(&c, &n.url(), "?read=quorum").await.status(), StatusCode::BAD_REQUEST,
            "a stream cannot hold a read index, so it refuses rather than meaning `primary`");
        assert_eq!(changes(&c, &n.url(), "?read=primary").await.status(), StatusCode::OK,
            "a single node leads itself, so the leader-only stream is served");
    }

    /// A drop empties the collection in one entry, and the feed says so in one event rather than
    /// one delete per key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dropped_collection_is_one_event() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        put(&c, &n.url(), "k2", serde_json::json!({"v": 2})).await;

        let tap = watch(&c, &n.url(), "").await;
        assert!(c.delete(format!("{}/collections/c", n.url())).send().await.unwrap().status().is_success());

        let seen = tap.wait_for_events("change", 1, SETTLE).await;
        assert_eq!(ops(&seen), vec!["drop"], "{:?}", seen);
        assert!(seen[0].data.get("key").is_none(), "a drop names no key");
    }

    /// A cluster-wide feed is commit 57. Until then a router says where the feed lives rather than
    /// answering from one group and calling it the collection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_router_points_at_the_shard_groups() {
        let root = temp_root();
        let mut shard = TestNode::new("s1", next_test_port(), &root, "primary");
        shard.start();
        let mut router = TestNode::new("router", next_test_port(), &root, "primary");
        router.role = "router".to_string();
        router.shard_map = vec![(shard.url(), Vec::new())];
        router.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let c = reqwest::Client::new();
        assert_eq!(changes(&c, &router.url(), "").await.status(), StatusCode::NOT_IMPLEMENTED);
    }

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
        let filter = Some(parse_filter(r#"{"status":"active"}"#).unwrap());
        let all = OpFilter::parse(None).unwrap();

        assert!(passes(&event(ChangeOp::Insert, Some(serde_json::json!({"status": "active"}))), &filter, &all));
        assert!(!passes(&event(ChangeOp::Update, Some(serde_json::json!({"status": "done"}))), &filter, &all));
        assert!(passes(&event(ChangeOp::Delete, None), &filter, &all));
        assert!(passes(&event(ChangeOp::Drop, None), &filter, &all));

        let writes = OpFilter::parse(Some("insert,update")).unwrap();
        assert!(!passes(&event(ChangeOp::Delete, None), &None, &writes),
            "an op list still excludes a delete, because that one was asked for explicitly");
    }
}
