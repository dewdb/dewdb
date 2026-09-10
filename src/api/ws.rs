//! The change stream over WebSocket: the same feed as `/changes`, framed as JSON text messages. The
//! refusals happen before the upgrade, so an unusable position is a status rather than a closed socket.

use crate::api::changes::{open_change_stream, ChangeParams, KEEPALIVE_INTERVAL};
use crate::api::middleware::CollectionPath;
use crate::auth::Credential;
use crate::cdc::ChangeStream;
use crate::state::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Query, State};
use axum::http::HeaderMap;

pub async fn ws_changes(
    State(state): State<AppState>,
    CollectionPath(col_name): CollectionPath<String>,
    Query(params): Query<ChangeParams>,
    credential: Option<Extension<Credential>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> axum::response::Response {
    // Opened before the handshake: a client that learns its position is unusable from a `410` can
    // act on it, where one that learns it from a close frame has to parse the close reason.
    let feed = match open_change_stream(
        &state, col_name, &params, &headers, credential.map(|Extension(c)| c)).await {
        Ok(feed) => feed,
        Err(refusal) => return refusal,
    };

    upgrade.on_upgrade(move |socket| serve(socket, feed))
}

async fn serve(mut socket: WebSocket, mut feed: ChangeStream) {
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await;

    loop {
        tokio::select! {
            frame = feed.next_frame() => {
                let Some(frame) = frame else { break };
                let ended = frame.name == "error";
                if socket.send(Message::Text(frame.message().to_string())).await.is_err() {
                    return;
                }
                // The feed has nothing further to say, so the socket says so too rather than
                // leaving the client waiting on a stream that has stopped.
                if ended {
                    break;
                }
            },
            // A client that has gone away without closing is only discovered by writing to it, and
            // a quiet collection gives nothing to write.
            _ = keepalive.tick() => {
                if socket.send(Message::Ping(Vec::new())).await.is_err() {
                    return;
                }
            },
            // Server-push only. What matters is the close, and the disconnect that reads as one.
            inbound = socket.recv() => match inbound {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {},
            },
        }
    }
    let _ = socket.send(Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        next_test_port, put_value, single_node, temp_root, two_shard_cluster, TestNode, WsTap,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_secs(5);

    async fn put(client: &reqwest::Client, base: &str, key: &str, body: serde_json::Value) {
        assert!(put_value(client, base, "c", key, body, "").await.is_success(), "write failed");
    }

    async fn open(base: &str, query: &str) -> WsTap {
        let tap = WsTap::open(&format!("{}/collections/c/changes/ws{}", base, query)).await
            .expect("the socket must open");
        assert_eq!(tap.wait_for("open", 1, SETTLE).await.len(), 1,
            "the socket must announce its starting position before anything happens");
        tap
    }

    async fn refused(base: &str, query: &str) -> StatusCode {
        match WsTap::open(&format!("{}/collections/c/changes/ws{}", base, query)).await {
            Err(status) => status,
            Ok(_) => panic!("the handshake had to be refused"),
        }
    }

    /// The same frames as SSE, each one naming itself: a text frame carries no event name of its
    /// own, so a client that cannot read the `type` cannot tell a change from a refusal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_socket_delivers_the_same_frames_the_sse_stream_does() {
        let root = temp_root();
        let n = single_node(&root).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = open(&n.url(), "").await;

        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        put(&c, &n.url(), "k1", serde_json::json!({"v": 2})).await;
        c.delete(format!("{}/collections/c/docs/k1", n.url())).send().await.unwrap();

        let seen = tap.wait_for("change", 3, SETTLE).await;
        assert_eq!(seen.iter().map(|e| e["op"].as_str().unwrap_or("?")).collect::<Vec<_>>(),
            vec!["insert", "update", "delete"], "{:?}", seen);
        assert_eq!(seen[1]["value"]["v"].as_i64(), Some(2));
        assert!(seen[2].get("value").is_none(), "a delete has no document to carry");
        assert!(seen.iter().all(|e| e["position"].as_str().is_some()),
            "every change has to carry the position a reconnect resumes from: {:?}", seen);
    }

    /// A resume is the same position the frames carried, and the refusals are the endpoint's own,
    /// answered before the upgrade rather than as a socket that opens and shuts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_socket_resumes_from_a_position_and_refuses_before_upgrading() {
        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.changefeed = serde_json::json!({"buffer_events": 2});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let c = reqwest::Client::new();

        put(&c, &n.url(), "seed", serde_json::json!({"v": 0})).await;
        let tap = open(&n.url(), "").await;
        put(&c, &n.url(), "k1", serde_json::json!({"v": 1})).await;
        let stale = tap.wait_for("change", 1, SETTLE).await[0]["position"]
            .as_str().unwrap().to_string();
        drop(tap);

        put(&c, &n.url(), "k2", serde_json::json!({"v": 2})).await;
        let resumed = open(&n.url(), &format!("?after={}", stale)).await;
        let seen = resumed.wait_for("change", 1, SETTLE).await;
        assert_eq!(seen[0]["key"].as_str(), Some("k2"),
            "a resume must not repeat what it had, nor skip what it missed: {:?}", seen);
        drop(resumed);

        for key in ["k3", "k4", "k5"] {
            put(&c, &n.url(), key, serde_json::json!({"v": 3})).await;
        }
        assert_eq!(refused(&n.url(), &format!("?after={}", stale)).await, StatusCode::GONE,
            "a position the buffer lost is an HTTP refusal, not a socket that closes at once");
        assert_eq!(refused(&n.url(), "?ops=upsert").await, StatusCode::BAD_REQUEST);
        assert_eq!(refused(&n.url(), "?read=quorum").await, StatusCode::BAD_REQUEST);
    }

    /// IB-026: a socket is authorized on its handshake and never again, so the same re-check ends this
    /// one too -- in-band, because the frame is all a client has to tell a refusal from a change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_socket_ends_when_the_key_its_handshake_carried_stops_being_accepted() {
        use crate::auth::API_KEY_HEADER;

        let root = temp_root();
        let mut n = TestNode::new("solo", next_test_port(), &root, "primary");
        n.auth = serde_json::json!({"api_keys": ["old", "new"]});
        n.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let c = reqwest::Client::new();
        assert!(c.put(format!("{}/collections/c/docs/seed", n.url()))
            .header(API_KEY_HEADER, "old").json(&serde_json::json!({"value": {"v": 0}}))
            .send().await.unwrap().status().is_success());

        let socket = format!("{}/collections/c/changes/ws", n.url());
        let tap = WsTap::open_with_key(&socket, "old").await.expect("the socket must open");
        assert_eq!(tap.wait_for("open", 1, SETTLE).await.len(), 1);

        let state = n.state.as_ref().expect("the node is running");
        assert!(state.rotate_client_keys(vec!["new".to_string()], Vec::new()));

        let ended = tap.wait_for("error", 1, SETTLE).await;
        assert_eq!(ended.len(), 1, "the socket outlived the credential its handshake carried");
        assert_eq!(ended[0]["error"].as_str(), Some(crate::cdc::REVOKED), "{:?}", ended[0]);

        assert_eq!(WsTap::open_with_key(&socket, "old").await.err(),
            Some(StatusCode::UNAUTHORIZED), "the rotated key must not open a new socket either");
        assert!(WsTap::open_with_key(&socket, "new").await.is_ok());
    }

    /// A router serves the collection over a socket the same way it serves it over SSE: one
    /// subscription, every group, each event naming the log it was drawn from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_router_serves_the_whole_collection_over_a_socket() {
        use crate::ring::hash_key;
        use crate::util::same_endpoint;
        use std::collections::HashSet;

        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let state = router.state.as_ref().expect("the router is running");
        let key_on = |group: &str| (0..10_000).map(|i| format!("k{}", i))
            .find(|key| state.get_effective_shard_url(hash_key("c", key))
                .is_some_and(|(_, owner, _)| same_endpoint(&owner, group)))
            .expect("each group must own some keys");
        let (first, second) = (key_on(&s1.url()), key_on(&s2.url()));

        // Subscribing is a read, and a group holding none of the collection has nothing to read.
        put(&c, &router.url(), &first, serde_json::json!({"v": 0})).await;
        put(&c, &router.url(), &second, serde_json::json!({"v": 0})).await;

        let tap = open(&router.url(), "").await;
        put(&c, &router.url(), &first, serde_json::json!({"v": 1})).await;
        put(&c, &router.url(), &second, serde_json::json!({"v": 1})).await;

        let seen = tap.wait_for("change", 2, SETTLE).await;
        let shards: HashSet<String> = seen.iter()
            .map(|e| e["shard"].as_str().unwrap_or("?").to_string()).collect();
        assert_eq!(shards.len(), 2, "both groups have to reach one socket: {:?}", seen);
        assert_eq!(refused(&router.url(), "?read=replica").await, StatusCode::BAD_REQUEST,
            "a group's positions are only honourable by the node whose log it agrees on");
    }
}
