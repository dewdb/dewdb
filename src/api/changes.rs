//! The change-stream endpoint: one committed collection feed, delivered as SSE.

use crate::api::docs::not_the_primary;
use crate::api::middleware::{client_collection, CollectionPath};
use crate::auth::Credential;
use crate::cdc::{CdcFilter, CdcFrame, CdcSession, CdcStream, ChangeSource, ChangeStream};
use crate::changefeed::SubscribeError;
use crate::cluster::changestream::open_cluster_stream;
use crate::cluster::router::{parse_read_pref, ReadPreference};
use crate::model::err_json;
use crate::state::AppState;
use crate::storage::Collection;
use axum::extract::{Extension, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// SSE's own reconnection hint. A client that reconnects sends `Last-Event-ID`, which is the LSN
/// this stream last delivered, so the default browser retry resumes rather than restarts.
pub(crate) const RETRY_HINT: Duration = Duration::from_secs(2);
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
pub struct ChangeParams {
    /// Resume position. A shard reads it as an LSN; a router as the cluster position it issued.
    pub after: Option<String>,
    /// A document filter, in the `/query` syntax.
    pub filter: Option<String>,
    /// Comma-separated `insert`, `update`, `delete`, `drop`. Absent is all four.
    pub ops: Option<String>,
    pub read: Option<String>,
}

pub(crate) fn data_event(name: &str, body: serde_json::Value) -> Event {
    Event::default().event(name).data(body.to_string())
}

pub(crate) fn sse_event(frame: &CdcFrame) -> Event {
    let event = data_event(frame.name, frame.body.clone());
    match &frame.id {
        Some(id) => event.id(id),
        None => event,
    }
}

/// The parts of a change request every transport reads the same way. `read=quorum` is refused here
/// rather than downgraded: a read index makes one answer linearizable, and a stream is not one.
pub(crate) fn parse_request(
    params: &ChangeParams,
    headers: &HeaderMap,
) -> Result<(CdcFilter, ReadPreference, Option<String>), axum::response::Response> {
    let filter = CdcFilter::parse(params.filter.as_deref(), params.ops.as_deref())
        .map_err(|e| err_json(StatusCode::BAD_REQUEST, e))?;
    let pref = match parse_read_pref(params.read.as_deref()) {
        Ok(ReadPreference::Quorum) => return Err(err_json(StatusCode::BAD_REQUEST,
            "a change stream is not a point-in-time read; use `read=primary`".to_string())),
        Ok(p) => p,
        Err(e) => return Err(err_json(StatusCode::BAD_REQUEST, e)),
    };
    // An explicit `after` wins: `Last-Event-ID` is what the browser resends on its own reconnect,
    // and a client that named a position meant that one.
    let after = params.after.as_deref()
        .or_else(|| headers.get("last-event-id").and_then(|v| v.to_str().ok()))
        .map(|raw| raw.trim().to_string());
    Ok((filter, pref, after))
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

/// Subscribes to one collection's feed on this node. The refusals are the endpoint's, so SSE and
/// WebSocket answer an unusable position identically.
pub(crate) fn open_shard_session(
    state: &AppState,
    col_name: String,
    after: Option<&str>,
    filter: CdcFilter,
    pref: &ReadPreference,
) -> Result<CdcSession, axum::response::Response> {
    if let Some(refusal) = not_the_primary(state, pref) {
        return Err(refusal);
    }
    let after = match after {
        Some(raw) => match raw.parse::<u64>() {
            Ok(lsn) => Some(lsn),
            Err(_) => return Err(err_json(StatusCode::BAD_REQUEST,
                "a shard change position is the LSN of the event it follows".to_string())),
        },
        None => None,
    };

    let col = client_collection(state, &col_name)?;
    // Sampled before subscribing, so a feed that was quiet learns how far the log moved while it
    // was not recording, and refuses a resume from under that rather than skipping it silently.
    let applied = col.applied_lsn();
    let sub = col.changefeed.subscribe(after, applied).map_err(|e| refuse(e, &col))?;
    let leader_only = matches!(pref, ReadPreference::Primary).then(|| state.clone());
    Ok(CdcSession::new(col_name, CdcStream::new(sub, filter, leader_only)))
}

/// A cluster-wide stream is served by each group's leader, so it has no replica preference to
/// offer: a group's feed lags on a replica, and its positions are only honourable by a node whose
/// log the group agrees on.
pub(crate) fn router_rejects_replica_reads(pref: &ReadPreference) -> Option<axum::response::Response> {
    matches!(pref, ReadPreference::Replica).then(|| err_json(StatusCode::BAD_REQUEST,
        "a cluster-wide change stream is served by each group's leader; drop `read=replica`"
            .to_string()))
}

/// Everything both transports do before they start rendering frames: the request is parsed, the
/// feed is opened on whichever side of the cluster this node sits on, and the credential that
/// cleared the gate rides along so the connection can be judged again while it runs.
pub(crate) async fn open_change_stream(
    state: &AppState,
    col_name: String,
    params: &ChangeParams,
    headers: &HeaderMap,
    credential: Option<Credential>,
) -> Result<ChangeStream, axum::response::Response> {
    let (filter, pref, after) = parse_request(params, headers)?;

    let source = if state.config.role == "router" {
        if let Some(refusal) = router_rejects_replica_reads(&pref) {
            return Err(refusal);
        }
        ChangeSource::Cluster(
            open_cluster_stream(state, col_name, params, after.as_deref()).await?)
    } else {
        ChangeSource::Shard(
            open_shard_session(state, col_name, after.as_deref(), filter, &pref)?)
    };
    Ok(ChangeStream::new(source, state, credential))
}

pub async fn stream_changes(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<ChangeParams>,
    credential: Option<Extension<Credential>>,
    headers: HeaderMap,
) -> axum::response::Response {
    let stream = match open_change_stream(
        &state, col_name, &params, &headers, credential.map(|Extension(c)| c)).await {
        Ok(stream) => stream,
        Err(refusal) => return refusal,
    };

    // Held by the `unfold` rather than a spawned task, so a client that disconnects drops the
    // subscription and the feed sees it leave.
    let stream = futures::stream::unfold(stream, |mut stream| async move {
        let frame = stream.next_frame().await?;
        let event = match frame.name {
            "open" => sse_event(&frame).retry(RETRY_HINT),
            _ => sse_event(&frame),
        };
        Some((Ok::<Event, Infallible>(event), stream))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEPALIVE_INTERVAL)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::API_KEY_HEADER;
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

    /// A router serves the collection, not one group's share of it. The fan-out itself is covered
    /// in `cluster::changestream`; what this pins is that the endpoint no longer refuses there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_router_serves_the_collection_rather_than_refusing() {
        let root = temp_root();
        let mut shard = TestNode::new("s1", next_test_port(), &root, "primary");
        shard.start();
        let mut router = TestNode::new("router", next_test_port(), &root, "primary");
        router.role = "router".to_string();
        router.shard_map = vec![(shard.url(), Vec::new())];
        router.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let c = reqwest::Client::new();
        put(&c, &router.url(), "seed", serde_json::json!({"v": 0})).await;
        assert_eq!(changes(&c, &router.url(), "").await.status(), StatusCode::OK);
    }

    /// IB-026: the same holds for the credential. A stream was authorized when it opened and never
    /// again, so a key removed from the node's set kept reading the collection's writes until the
    /// process restarted or the peer went away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stream_ends_when_the_key_it_opened_with_stops_being_accepted() {
        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.auth = serde_json::json!({"api_keys": ["old", "new"]});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let c = reqwest::Client::new();
        let subscribe = format!("{}/collections/c/changes", n.url());
        assert!(c.put(format!("{}/collections/c/docs/seed", n.url()))
            .header(API_KEY_HEADER, "old").json(&serde_json::json!({"value": {"v": 0}}))
            .send().await.unwrap().status().is_success());

        let opened = c.get(&subscribe).header(API_KEY_HEADER, "old").send().await.unwrap();
        assert_eq!(opened.status(), StatusCode::OK);
        let tap = SseTap::open(opened);
        assert_eq!(tap.wait_for_events("open", 1, SETTLE).await.len(), 1);

        let state = n.state.as_ref().expect("the node is running");
        assert!(state.rotate_client_keys(vec!["new".to_string()], Vec::new()));

        let ended = tap.wait_for_events("error", 1, SETTLE).await;
        assert_eq!(ended.len(), 1, "the stream outlived the credential it was opened with");
        assert_eq!(ended[0].data["error"].as_str(), Some(crate::cdc::REVOKED),
            "the end has to say why, the way an overrun does: {:?}", ended[0]);

        assert_eq!(c.get(&subscribe).header(API_KEY_HEADER, "old").send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED, "the rotated key must not open a new stream either");
        assert_eq!(c.get(&subscribe).header(API_KEY_HEADER, "new").send().await.unwrap().status(),
            StatusCode::OK, "a key that is still held is untouched");
    }

    /// A stream is one request that keeps answering, so `read=primary` is a promise it has to keep
    /// past the moment it was checked. Ended in-band with the position, because a stepped-down
    /// leader publishes nothing further and silence is indistinguishable from a quiet collection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_only_stream_ends_when_the_node_stops_leading() {
        use crate::test_support::{leaders, three_node_cluster, wait_for};

        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let c = reqwest::Client::new();
        assert!(wait_for(SETTLE, || leaders(&[&n1, &n2, &n3]) == vec!["n1".to_string()]).await,
            "the test needs a settled leader to take office away from");

        put(&c, &n1.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = watch(&c, &n1.url(), "?read=primary").await;
        let opened = tap.named("open")[0].data["position"].as_u64().unwrap();

        let handover = c.post(format!("{}/cluster/transfer-leadership", n1.url()))
            .timeout(Duration::from_secs(30)).json(&serde_json::json!({"to": n2.url()}))
            .send().await.unwrap();
        assert!(handover.status().is_success(), "{}", handover.text().await.unwrap());

        let ended = tap.wait_for_events("error", 1, SETTLE).await;
        assert_eq!(ended.len(), 1, "the stream carried on without the leadership it asked for");
        assert!(ended[0].data["position"].as_u64().is_some_and(|p| p >= opened),
            "the refusal has to name a position the group can be resumed from: {:?}", ended[0]);
    }
}
