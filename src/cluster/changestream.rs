//! Cluster-wide change streams: one upstream subscription per shard group, merged behind a router.
//!
//! A group's feed is a position in that group's own log, so nothing about it is comparable across
//! groups. The subscriber therefore gets a position per group, stamped with the partitioning those
//! positions were taken against, and an ordering guarantee that is per group rather than global.

use crate::api::changes::ChangeParams;
use crate::cdc::CdcFrame;
use crate::cluster::router::{
    collection_absent_response, no_primary_response, read_targets, refusal_response,
    ReadPreference, ShardReply,
};
use crate::model::err_json;
use crate::query::{decode_cursor, encode_cursor};
use crate::state::AppState;
use crate::util::{encode_path_segment, same_endpoint};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

/// How long a group that holds none of the collection waits before asking again. It gains one on
/// whichever write first hashes there, and nothing tells the router when that happened.
const ABSENT_RETRY: Duration = Duration::from_secs(1);
/// Between reconnects to a group that has no reachable leader. Short, because the position is held
/// across the gap and a failover is over in about an election.
const RECONNECT_DELAY: Duration = Duration::from_millis(250);
/// How often the shard set is re-read. A change is operator-driven, so this is a liveness bound on
/// noticing one rather than something a request waits on.
const TOPOLOGY_POLL: Duration = Duration::from_millis(250);
/// Events held between the group readers and the subscriber. Full means the readers stop reading,
/// which is what pushes back on the shard feeds instead of growing this router's memory.
const MERGE_BUFFER: usize = 512;
/// Ceiling on one unparsed upstream event. A shard cannot publish a document above the record cap,
/// so anything larger is a peer that is not speaking SSE.
const MAX_UPSTREAM_EVENT: usize = 4 * crate::storage::frame::MAX_PUBLIC_BODY;

/// Where each shard group's feed stopped, and the partitioning those positions were taken against.
/// Encoded like the query cursors, and issued as every event's SSE `id`, so a browser's own
/// `Last-Event-ID` reconnect resumes each group exactly.
#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClusterChangeCursor {
    pub ring: u64,
    pub positions: BTreeMap<String, u64>,
}

/// 409, not 410: the positions were correct when they were issued and no position replaces them.
/// Ownership moved while nobody was watching the seam, so the changes that crossed it were
/// published on groups this stream had no subscription to, and no resume covers them.
fn stale_partitioning_response() -> axum::response::Response {
    err_json(
        StatusCode::CONFLICT,
        "position was issued against a different shard layout; resubscribe".to_string(),
    )
}

/// What one group answered the initial subscribe with.
enum Upstream {
    Live(reqwest::Response),
    /// The group holds no such collection. Not a failure: a collection narrower than the ring is
    /// normal, and the group may take its first key for it at any write.
    Absent,
    /// The group's own verdict on the request -- a position it cannot honour, or a filter it will
    /// not parse. Every candidate in the group reaches the same one, so it is the answer.
    Refused(ShardReply),
    NoPrimary(Option<ShardReply>),
    Failed,
}

/// One event, from one group. `Topology` and `Broken` travel the same channel as the changes so
/// they land in the subscriber's stream where they happened rather than ahead of it.
enum Merged {
    Change { shard: String, lsn: u64, payload: serde_json::Value },
    Topology { ring: u64, shards: Vec<String>, added: Vec<String>, removed: Vec<String> },
    /// A group's feed cannot be continued from where this stream left it. The whole stream ends: a
    /// cluster feed missing one group is a feed with a hole in it.
    Broken { shard: String, error: String, resume_floor: Option<u64> },
}

fn subscribe_url(target: &str, col: &str) -> String {
    format!("{}/collections/{}/changes", target, encode_path_segment(col))
}

/// The subscriber's `filter` and `ops` verbatim, evaluated by each shard rather than here: an event
/// the subscriber did not ask for is cheaper refused at its source than carried across the fan-in.
fn forwarded_params(params: &ChangeParams) -> Vec<(String, String)> {
    let mut q = vec![("read".to_string(), "primary".to_string())];
    if let Some(f) = &params.filter {
        q.push(("filter".to_string(), f.clone()));
    }
    if let Some(o) = &params.ops {
        q.push(("ops".to_string(), o.clone()));
    }
    q
}

/// Subscribes to one group, trying its leader first and its replicas after. `read=primary` is not
/// a freshness preference here but the thing that makes a position portable: every node in the
/// group numbers the same log, so a resume survives a failover only if whoever answers is the one
/// whose log the group agrees on.
async fn open_group(
    state: &AppState,
    col: &str,
    group: &str,
    replicas: &[String],
    after: Option<u64>,
    forwarded: &[(String, String)],
) -> Upstream {
    let effective = state.effective_primary(group);
    let rr = state.read_rr.fetch_add(1, Ordering::Relaxed);
    let loads = state.fresh_node_loads();
    let targets = read_targets(&ReadPreference::Primary, &effective, replicas, rr, &loads);

    let mut q = forwarded.to_vec();
    if let Some(a) = after {
        q.push(("after".to_string(), a.to_string()));
    }

    let mut no_primary = false;
    let mut primary_refusal: Option<ShardReply> = None;
    for target in targets {
        let Ok(res) = state.stream_client.get(subscribe_url(&target, col)).query(&q).send().await
        else {
            continue;
        };
        let status = res.status();
        if status.is_success() {
            if !same_endpoint(&target, group) {
                state.set_primary_override(group, &target);
            }
            return Upstream::Live(res);
        }
        match status {
            StatusCode::NOT_FOUND => return Upstream::Absent,
            StatusCode::SERVICE_UNAVAILABLE => {
                no_primary = true;
                if same_endpoint(&target, &effective) {
                    primary_refusal = Some(ShardReply::of(res).await);
                }
            },
            _ => return Upstream::Refused(ShardReply::of(res).await),
        }
    }

    match no_primary {
        true => Upstream::NoPrimary(primary_refusal),
        false => Upstream::Failed,
    }
}

/// Incremental SSE reader. Buffers bytes rather than a `String`, so a chunk that splits a
/// multi-byte character does not lose it to a lossy decode of the half that arrived.
#[derive(Default)]
struct SseReader {
    buf: Vec<u8>,
}

impl SseReader {
    fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    fn overflowed(&self) -> bool {
        self.buf.len() > MAX_UPSTREAM_EVENT
    }

    /// `None` while the tail is still partial. A keep-alive is a comment line carrying no `event:`,
    /// so it parses to nothing and the next block is tried instead.
    fn next_event(&mut self) -> Option<(String, serde_json::Value)> {
        loop {
            let end = self.buf.windows(2).position(|w| w == b"\n\n")?;
            let block: Vec<u8> = self.buf.drain(..end + 2).collect();
            let mut name = None;
            let mut data = String::new();
            for line in String::from_utf8_lossy(&block).lines() {
                match line.split_once(':') {
                    Some(("event", v)) => name = Some(v.trim().to_string()),
                    Some(("data", v)) => data.push_str(v.trim()),
                    _ => {},
                }
            }
            if let Some(name) = name {
                return Some((name, serde_json::from_str(&data).unwrap_or(serde_json::Value::Null)));
            }
        }
    }
}

enum Pump {
    /// The upstream ended for a reason a new subscription from the same position recovers from:
    /// a leader stepped down, a handle was replaced, or the connection dropped.
    Reconnect,
    /// The group can no longer serve this stream's position, and no reconnect changes that.
    Broken,
    Gone,
}

async fn pump(
    response: reqwest::Response,
    group: &str,
    position: &mut Option<u64>,
    tx: &mpsc::Sender<Merged>,
) -> Pump {
    use futures::StreamExt;
    let mut body = response.bytes_stream();
    let mut reader = SseReader::default();
    loop {
        while let Some((name, data)) = reader.next_event() {
            match name.as_str() {
                "change" => {
                    let Some(lsn) = data.get("lsn").and_then(|v| v.as_u64()) else { continue };
                    *position = Some(lsn);
                    let event = Merged::Change { shard: group.to_string(), lsn, payload: data };
                    if tx.send(event).await.is_err() {
                        return Pump::Gone;
                    }
                },
                // A floor is the shard saying the position is gone, which a reconnect cannot undo.
                // Anything else it ends on is a handle or a leader, and both come back.
                "error" => match data.get("resume_floor").and_then(|v| v.as_u64()) {
                    Some(floor) => {
                        let error = data.get("error").and_then(|e| e.as_str())
                            .unwrap_or("the shard feed ended").to_string();
                        let _ = tx.send(Merged::Broken {
                            shard: group.to_string(), error, resume_floor: Some(floor),
                        }).await;
                        return Pump::Broken;
                    },
                    None => return Pump::Reconnect,
                },
                _ => {},
            }
        }
        match body.next().await {
            Some(Ok(chunk)) => reader.push(&chunk),
            _ => return Pump::Reconnect,
        }
        if reader.overflowed() {
            let _ = tx.send(Merged::Broken {
                shard: group.to_string(),
                error: "the shard feed sent an event larger than this router will read".to_string(),
                resume_floor: None,
            }).await;
            return Pump::Broken;
        }
    }
}

/// One group's subscription, and everything needed to open it again. A failover, a replaced handle
/// and a dropped connection are the same event here, and `position` is what makes reopening exact.
struct GroupReader {
    state: AppState,
    col: String,
    group: String,
    replicas: Vec<String>,
    forwarded: Vec<(String, String)>,
    position: Option<u64>,
    tx: mpsc::Sender<Merged>,
}

impl GroupReader {
    fn spawn(self, initial: Option<reqwest::Response>) -> JoinHandle<()> {
        tokio::spawn(run_group(self, initial))
    }
}

async fn run_group(reader: GroupReader, initial: Option<reqwest::Response>) {
    let GroupReader { state, col, group, replicas, forwarded, mut position, tx } = reader;
    let mut current = initial;
    loop {
        let response = match current.take() {
            Some(r) => r,
            None => match open_group(&state, &col, &group, &replicas, position, &forwarded).await {
                Upstream::Live(r) => r,
                Upstream::Absent => {
                    tokio::time::sleep(ABSENT_RETRY).await;
                    continue;
                },
                Upstream::Refused(reply) => {
                    let body: serde_json::Value = serde_json::from_str(&reply.body)
                        .unwrap_or(serde_json::Value::Null);
                    let _ = tx.send(Merged::Broken {
                        shard: group.clone(),
                        error: body.get("error").and_then(|e| e.as_str())
                            .unwrap_or("the shard refused this position").to_string(),
                        resume_floor: body.get("resume_floor").and_then(|f| f.as_u64()),
                    }).await;
                    return;
                },
                Upstream::NoPrimary(_) | Upstream::Failed => {
                    state.clear_primary_override(&group);
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                },
            },
        };
        match pump(response, &group, &mut position, &tx).await {
            Pump::Reconnect => tokio::time::sleep(RECONNECT_DELAY).await,
            Pump::Broken | Pump::Gone => return,
        }
    }
}

/// Re-reads the shard set and keeps one reader per group in force. Owns the reader handles, so a
/// subscriber that disconnects takes every upstream subscription with it rather than leaving them
/// to notice on their own next keep-alive.
async fn supervise(
    state: AppState,
    col: String,
    forwarded: Vec<(String, String)>,
    mut ring: u64,
    mut readers: HashMap<String, JoinHandle<()>>,
    tx: mpsc::Sender<Merged>,
) {
    loop {
        tokio::time::sleep(TOPOLOGY_POLL).await;
        if tx.is_closed() {
            break;
        }
        let (next, owners) = state.partitioning();
        if next == ring {
            continue;
        }

        let live: HashMap<String, Vec<String>> = owners.into_iter().collect();
        let removed: Vec<String> = readers.keys()
            .filter(|group| !live.contains_key(*group)).cloned().collect();
        for group in &removed {
            if let Some(handle) = readers.remove(group) {
                handle.abort();
            }
        }
        let mut added = Vec::new();
        for (group, replicas) in &live {
            if readers.contains_key(group) {
                continue;
            }
            // From wherever the group is now: it owned nothing under the old partitioning, so no
            // client write reached it, and what it holds arrived by handover.
            readers.insert(group.clone(), GroupReader {
                state: state.clone(),
                col: col.clone(),
                group: group.clone(),
                replicas: replicas.clone(),
                forwarded: forwarded.clone(),
                position: None,
                tx: tx.clone(),
            }.spawn(None));
            added.push(group.clone());
        }

        let mut shards: Vec<String> = live.into_keys().collect();
        shards.sort();
        added.sort();
        info!(target: "changestream", collection = %col, ring = next,
            added = added.len(), removed = removed.len(), "Shard set moved under a cluster stream");
        ring = next;
        if tx.send(Merged::Topology { ring, shards, added, removed }).await.is_err() {
            break;
        }
    }

    for (_, handle) in readers {
        handle.abort();
    }
}

/// The merged feed as a transport consumes it. Frames rather than SSE events, so the WebSocket
/// endpoint delivers the same fan-out without a second merge.
pub struct ClusterStream {
    rx: mpsc::Receiver<Merged>,
    collection: String,
    cursor: ClusterChangeCursor,
    shards: Vec<String>,
    opened: bool,
    ended: bool,
}

impl ClusterStream {
    pub async fn next_frame(&mut self) -> Option<CdcFrame> {
        if !self.opened {
            self.opened = true;
            return Some(CdcFrame {
                name: "open",
                id: None,
                body: serde_json::json!({
                    "collection": self.collection,
                    "shards": self.shards,
                    "position": encode_cursor(&self.cursor),
                }),
            });
        }
        if self.ended {
            return None;
        }
        match self.rx.recv().await {
            Some(Merged::Change { shard, lsn, mut payload }) => {
                self.cursor.positions.insert(shard.clone(), lsn);
                if let Some(fields) = payload.as_object_mut() {
                    // Which group published it: the ordering guarantee is per group, and `lsn` only
                    // means anything alongside the log it was drawn from.
                    fields.insert("shard".to_string(), serde_json::json!(shard));
                }
                Some(CdcFrame {
                    name: "change",
                    id: Some(encode_cursor(&self.cursor)),
                    body: payload,
                })
            },
            Some(Merged::Topology { ring, shards, added, removed }) => {
                self.cursor.ring = ring;
                self.cursor.positions.retain(|group, _| shards.contains(group));
                self.shards = shards.clone();
                Some(CdcFrame {
                    name: "topology",
                    id: None,
                    body: serde_json::json!({
                        "ring": ring,
                        "shards": shards,
                        "added": added,
                        "removed": removed,
                        "position": encode_cursor(&self.cursor),
                    }),
                })
            },
            Some(Merged::Broken { shard, error, resume_floor }) => {
                self.ended = true;
                let mut body = serde_json::json!({"error": error, "shard": shard});
                if let Some(floor) = resume_floor {
                    body["resume_floor"] = floor.into();
                }
                Some(CdcFrame { name: "error", id: None, body })
            },
            // Only the supervisor closes the channel, and it does that when it is being dropped.
            None => {
                self.ended = true;
                Some(CdcFrame {
                    name: "error",
                    id: None,
                    body: serde_json::json!({
                        "error": "this router stopped coordinating the stream; resubscribe",
                    }),
                })
            },
        }
    }
}

/// One subscription per shard group, merged. `after` is a `ClusterChangeCursor`, not an LSN: an
/// LSN belongs to one group's log and says nothing about where the others are.
pub async fn open_cluster_stream(
    state: &AppState,
    col_name: String,
    params: &ChangeParams,
    after: Option<&str>,
) -> Result<ClusterStream, axum::response::Response> {
    let resume: Option<ClusterChangeCursor> = match after {
        Some(token) => match decode_cursor(token) {
            Some(c) => Some(c),
            None => return Err(err_json(StatusCode::BAD_REQUEST,
                "position does not belong to a cluster change stream".to_string())),
        },
        None => None,
    };

    let (ring, owners) = state.partitioning();
    if owners.is_empty() {
        return Err(no_primary_response());
    }
    // Before anything is opened: a position from another partitioning names groups that may not
    // exist and omits ones that do, and neither is recoverable by asking the shards.
    if resume.as_ref().is_some_and(|c| c.ring != ring) {
        return Err(stale_partitioning_response());
    }

    let forwarded = forwarded_params(params);
    let positions = resume.map(|c| c.positions).unwrap_or_default();
    // Borrowed as `Copy` handles, so each subscribe shares one collection name and one query
    // instead of cloning both per group.
    let (col, query) = (col_name.as_str(), forwarded.as_slice());
    let opened = futures::future::join_all(owners.iter().map(|(group, replicas)| {
        let after = positions.get(group).copied();
        async move {
            (group.clone(), replicas.clone(), after,
                open_group(state, col, group, replicas, after, query).await)
        }
    })).await;

    let mut live = Vec::new();
    let mut absent = Vec::new();
    for (group, replicas, after, upstream) in opened {
        match upstream {
            Upstream::Live(response) => live.push((group, replicas, after, response)),
            Upstream::Absent => absent.push((group, replicas)),
            // The group's own answer, passed through: it knows why the position or the filter is
            // unusable, and this router would only be guessing at it. Named, because a
            // `resume_floor` in it is a position in that group's log and in nobody else's.
            Upstream::Refused(reply) => {
                let mut body: serde_json::Value = serde_json::from_str(&reply.body)
                    .unwrap_or(serde_json::Value::String(reply.body));
                if let Some(fields) = body.as_object_mut() {
                    fields.insert("shard".to_string(), serde_json::json!(group));
                }
                return Err((reply.status, Json(body)).into_response());
            },
            // A cluster feed short one group would deliver a subset of the collection's changes
            // and call it the collection, so it refuses instead of opening.
            Upstream::NoPrimary(from_primary) => return Err(refusal_response(from_primary)),
            Upstream::Failed => return Err((StatusCode::BAD_GATEWAY,
                format!("shard group {} could not be reached for a change stream", group)).into_response()),
        }
    }
    if live.is_empty() && !absent.is_empty() {
        return Err(collection_absent_response(&col_name));
    }

    let (tx, rx) = mpsc::channel(MERGE_BUFFER);
    let mut readers = HashMap::new();
    let mut shards = Vec::new();
    // An absent group is watched too, on a retry loop: it takes its first key for the collection
    // on whichever write first hashes there, and nothing announces that.
    let opening = live.into_iter().map(|(group, replicas, after, response)| {
        (group, replicas, after, Some(response))
    }).chain(absent.into_iter().map(|(group, replicas)| (group, replicas, None, None)));
    for (group, replicas, after, response) in opening {
        shards.push(group.clone());
        readers.insert(group.clone(), GroupReader {
            state: state.clone(),
            col: col_name.clone(),
            group,
            replicas,
            forwarded: forwarded.clone(),
            position: after,
            tx: tx.clone(),
        }.spawn(response));
    }
    shards.sort();
    tokio::spawn(supervise(
        state.clone(), col_name.clone(), forwarded, ring, readers, tx));

    Ok(ClusterStream {
        rx,
        collection: col_name,
        cursor: ClusterChangeCursor { ring, positions },
        shards,
        opened: false,
        ended: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(ring: u64, positions: &[(&str, u64)]) -> ClusterChangeCursor {
        ClusterChangeCursor {
            ring,
            positions: positions.iter().map(|(g, p)| (g.to_string(), *p)).collect(),
        }
    }

    /// The position is the whole fan-out's, so it has to survive a round trip through an SSE `id`
    /// and come back naming the same groups at the same places.
    #[test]
    fn a_cluster_position_round_trips_through_its_event_id() {
        let token = encode_cursor(&cursor(77, &[("http://s1", 12), ("http://s2", 4)]));
        assert!(!token.contains('\n'), "an SSE id cannot carry a newline");

        let back: ClusterChangeCursor = decode_cursor(&token).expect("must decode");
        assert_eq!(back.ring, 77);
        assert_eq!(back.positions.get("http://s1"), Some(&12));
        assert_eq!(back.positions.get("http://s2"), Some(&4));
    }

    /// A shard's own single-log position is not one of these, and reading it as one would resume
    /// every group from nowhere and call the result a continuation.
    #[test]
    fn a_position_from_another_endpoint_is_not_read_as_this_one() {
        assert!(decode_cursor::<ClusterChangeCursor>("412").is_none());
        assert!(decode_cursor::<ClusterChangeCursor>("!!!not base64 json!!!").is_none());
        assert!(decode_cursor::<ClusterChangeCursor>(&encode_cursor(&crate::query::KeyCursor {
            key: "k1".to_string() })).is_none(), "a query cursor names no group positions");
    }

    fn feed(blocks: &[&str]) -> SseReader {
        let mut reader = SseReader::default();
        for block in blocks {
            reader.push(block.as_bytes());
        }
        reader
    }

    #[test]
    fn the_reader_yields_whole_events_and_skips_keep_alives() {
        let mut reader = feed(&[
            "event: open\ndata: {\"position\":4}\n\n",
            ": keep-alive\n\n",
            "event: change\ndata: {\"lsn\":5}\n\n",
        ]);

        assert_eq!(reader.next_event().map(|(n, _)| n), Some("open".to_string()));
        let (name, data) = reader.next_event().expect("a keep-alive must not hide the event behind it");
        assert_eq!(name, "change");
        assert_eq!(data["lsn"], 5);
        assert!(reader.next_event().is_none());
    }

    /// Chunk boundaries are the network's, not the protocol's: an event split across two reads is
    /// still one event, and a multi-byte character split across them is still that character.
    #[test]
    fn an_event_split_across_chunks_is_reassembled() {
        let mut reader = SseReader::default();
        let block = "event: change\ndata: {\"key\":\"café\"}\n\n".as_bytes();
        let (head, tail) = block.split_at(20);
        reader.push(head);
        assert!(reader.next_event().is_none(), "a partial block is not an event yet");

        reader.push(tail);
        let (name, data) = reader.next_event().expect("the whole block arrived");
        assert_eq!(name, "change");
        assert_eq!(data["key"], "café");
    }

    use crate::ring::hash_key;
    use crate::test_support::{put_value, temp_root, two_shard_cluster, SseTap, TestNode};
    use std::collections::HashSet;

    const SETTLE: Duration = Duration::from_secs(10);

    async fn changes(client: &reqwest::Client, base: &str, query: &str) -> reqwest::Response {
        client.get(format!("{}/collections/c/changes{}", base, query))
            .timeout(Duration::from_secs(10)).send().await.unwrap()
    }

    async fn watch(client: &reqwest::Client, base: &str, query: &str) -> SseTap {
        let r = changes(client, base, query).await;
        assert_eq!(r.status(), StatusCode::OK, "{}", r.text().await.unwrap());
        let tap = SseTap::open(r);
        assert_eq!(tap.wait_for_events("open", 1, SETTLE).await.len(), 1,
            "the stream must name the groups it is watching before anything happens");
        tap
    }

    async fn put(client: &reqwest::Client, base: &str, key: &str, v: i64) {
        assert!(put_value(client, base, "c", key, serde_json::json!({"v": v}), "").await.is_success(),
            "write of {} failed", key);
    }

    /// A key the router routes to `group`, found rather than assumed: which half of the ring a
    /// string lands in is not something a test should encode.
    fn key_on(router: &TestNode, group: &str) -> String {
        let state = router.state.as_ref().expect("the router is running");
        (0..10_000).map(|i| format!("k{}", i))
            .find(|key| state.get_effective_shard_url(hash_key("c", key))
                .is_some_and(|(_, owner, _)| same_endpoint(&owner, group)))
            .expect("each group must own some keys")
    }

    fn positions(event: &crate::test_support::SseEvent) -> ClusterChangeCursor {
        decode_cursor(event.id.as_ref().expect("every change carries a position"))
            .expect("an event id has to be a position the same endpoint accepts")
    }

    /// Swaps which group owns which range. Every group stays in the view, so what moves is the
    /// partitioning alone -- which is the half a stream in flight has to notice.
    fn reassign_ranges(router: &TestNode) {
        let state = router.state.as_ref().expect("the router is running");
        let mut view = state.cluster_view();
        assert_eq!(view.shards.len(), 2, "this router routes explicit ranges");
        let owner = view.shards[0].node_url.clone();
        view.shards[0].node_url = view.shards[1].node_url.clone();
        view.shards[1].node_url = owner;
        view.version += 1;
        view.seeded = false;
        view.updated_by = "test".to_string();
        state.adopt_cluster(view);
    }

    /// The point of the endpoint: one subscription, every group's committed changes, each one
    /// saying which log it came from -- because `lsn` means nothing without that.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cluster_stream_merges_every_shard_group() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let (first, second) = (key_on(&router, &s1.url()), key_on(&router, &s2.url()));

        // Subscribing is a read, and a group holding none of the collection has nothing to read.
        put(&c, &router.url(), &first, 0).await;
        put(&c, &router.url(), &second, 0).await;

        let tap = watch(&c, &router.url(), "").await;
        put(&c, &router.url(), &first, 1).await;
        put(&c, &router.url(), &second, 1).await;

        let seen = tap.wait_for_events("change", 2, SETTLE).await;
        let shards: HashSet<String> = seen.iter()
            .map(|e| e.data["shard"].as_str().unwrap_or("?").to_string()).collect();
        assert_eq!(shards.len(), 2, "both groups have to reach one subscriber: {:?}", seen);
        assert!(seen.iter().all(|e| e.data["op"] == "update"), "{:?}", seen);

        let last = positions(&seen[seen.len() - 1]);
        assert_eq!(last.positions.len(), 2, "a position covers every group, not the one that moved");
    }

    /// A reconnect resumes each group where that group stopped, which is the whole reason the
    /// position is a map rather than a number.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reconnect_resumes_every_group_where_it_left_it() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let (first, second) = (key_on(&router, &s1.url()), key_on(&router, &s2.url()));

        put(&c, &router.url(), &first, 0).await;
        put(&c, &router.url(), &second, 0).await;
        let tap = watch(&c, &router.url(), "").await;

        put(&c, &router.url(), &first, 1).await;
        put(&c, &router.url(), &second, 1).await;
        let seen = tap.wait_for_events("change", 2, SETTLE).await;
        let resume = seen[1].id.clone().expect("a position to come back with");
        drop(tap);

        put(&c, &router.url(), &first, 2).await;
        put(&c, &router.url(), &second, 2).await;

        let resumed = watch(&c, &router.url(), &format!("?after={}", resume)).await;
        let after = resumed.wait_for_events("change", 2, SETTLE).await;
        assert_eq!(after.len(), 2, "a resume must not repeat what it had: {:?}", after);
        assert!(after.iter().all(|e| e.data["value"]["v"] == 2),
            "nor skip what it missed: {:?}", after);
    }

    /// The seam a subscriber was not present for. Its positions were taken against a layout that no
    /// longer maps keys the same way, and the changes that crossed the seam were published on
    /// groups it had no subscription to, so the position is refused rather than resumed past them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_position_from_another_shard_layout_is_refused() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let (first, second) = (key_on(&router, &s1.url()), key_on(&router, &s2.url()));

        put(&c, &router.url(), &first, 0).await;
        put(&c, &router.url(), &second, 0).await;
        let tap = watch(&c, &router.url(), "").await;
        put(&c, &router.url(), &first, 1).await;
        let stale = tap.wait_for_events("change", 1, SETTLE).await[0].id.clone().unwrap();
        drop(tap);

        assert_eq!(changes(&c, &router.url(), &format!("?after={}", stale)).await.status(),
            StatusCode::OK, "the premise: the position works while the layout stands");

        reassign_ranges(&router);
        assert_eq!(changes(&c, &router.url(), &format!("?after={}", stale)).await.status(),
            StatusCode::CONFLICT);
        assert_eq!(changes(&c, &router.url(), "?after=not-a-position").await.status(),
            StatusCode::BAD_REQUEST);
    }

    /// A live stream is told in-band instead, because it never stopped watching: every group it
    /// holds is still a group, and the position it hands out from here carries the new layout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_layout_change_under_a_live_stream_is_announced_rather_than_hidden() {
        let root = temp_root();
        let (s1, s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        let (first, second) = (key_on(&router, &s1.url()), key_on(&router, &s2.url()));

        put(&c, &router.url(), &first, 0).await;
        put(&c, &router.url(), &second, 0).await;
        let tap = watch(&c, &router.url(), "").await;
        let opened: ClusterChangeCursor = decode_cursor(
            tap.named("open")[0].data["position"].as_str().unwrap()).unwrap();

        reassign_ranges(&router);

        let seen = tap.wait_for_events("topology", 1, SETTLE).await;
        assert_eq!(seen.len(), 1, "a stream must not carry on as though the layout had not moved");
        assert_eq!(seen[0].data["shards"].as_array().map(|s| s.len()), Some(2),
            "both groups are still in the view, so neither is added nor removed: {:?}", seen[0]);
        assert_eq!(seen[0].data["added"].as_array().map(|a| a.len()), Some(0));
        assert_eq!(seen[0].data["removed"].as_array().map(|r| r.len()), Some(0));

        let position = seen[0].data["position"].as_str().expect("a position to carry on with");
        let cursor: ClusterChangeCursor = decode_cursor(position).unwrap();
        assert_ne!(cursor.ring, opened.ring, "a position has to name the layout it was taken in");
        assert_eq!(changes(&c, &router.url(), &format!("?after={}", position)).await.status(),
            StatusCode::OK, "the position the announcement carried has to be one that works");
    }

    /// One group's history running out is the whole stream's problem, and the refusal has to name
    /// which group -- a `resume_floor` is a position in one log and in nobody else's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_group_that_lost_the_position_refuses_and_says_which_group() {
        use crate::test_support::{next_test_port, router_for};

        let root = temp_root();
        let mut s1 = TestNode::new("s1", next_test_port(), &root, "primary");
        let mut s2 = TestNode::new("s2", next_test_port(), &root, "primary");
        for shard in [&mut s1, &mut s2] {
            shard.changefeed = serde_json::json!({"buffer_events": 2});
            shard.start();
        }
        let router = router_for(&root, &[(s1.url(), Vec::new()), (s2.url(), Vec::new())]).await;
        let c = reqwest::Client::new();
        let (first, second) = (key_on(&router, &s1.url()), key_on(&router, &s2.url()));

        put(&c, &router.url(), &first, 0).await;
        put(&c, &router.url(), &second, 0).await;
        let tap = watch(&c, &router.url(), "").await;
        put(&c, &router.url(), &first, 1).await;
        let stale = tap.wait_for_events("change", 1, SETTLE).await[0].id.clone().unwrap();
        drop(tap);

        for v in 2..6 {
            put(&c, &router.url(), &first, v).await;
        }

        let refused = changes(&c, &router.url(), &format!("?after={}", stale)).await;
        assert_eq!(refused.status(), StatusCode::GONE);
        let body = refused.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body["shard"].as_str(), Some(s1.url().as_str()),
            "a floor without the log it belongs to is a number: {}", body);
        assert!(body["resume_floor"].as_u64().is_some(), "{}", body);

        assert_eq!(changes(&c, &router.url(), "").await.status(), StatusCode::OK,
            "resubscribing from now on is the recovery, and it has to work");
    }

    /// The refusals a router owes a subscriber before it opens anything, rather than after N shard
    /// round trips or as a stream that says nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cluster_stream_refuses_what_no_group_could_serve() {
        let root = temp_root();
        let (s1, _s2, router) = two_shard_cluster(&root).await;
        let c = reqwest::Client::new();
        put(&c, &router.url(), &key_on(&router, &s1.url()), 0).await;

        assert_eq!(c.get(format!("{}/collections/ghost/changes", router.url()))
            .send().await.unwrap().status(), StatusCode::NOT_FOUND,
            "a collection no group holds is the client's error, not an empty feed");
        assert_eq!(changes(&c, &router.url(), "?filter=%7B%22%24nope%22%3A1%7D").await.status(),
            StatusCode::BAD_REQUEST, "a filter wrong for every shard is refused once, here");
        assert_eq!(changes(&c, &router.url(), "?ops=upsert").await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(changes(&c, &router.url(), "?read=quorum").await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(changes(&c, &router.url(), "?read=replica").await.status(),
            StatusCode::BAD_REQUEST,
            "a group's positions are only honourable by the node whose log it agrees on");
        assert_eq!(changes(&c, &router.url(), "?read=primary").await.status(), StatusCode::OK);
    }

    #[test]
    fn a_subscribers_filter_and_ops_reach_the_shards_untouched() {
        let params = ChangeParams {
            after: None,
            filter: Some(r#"{"status":"active"}"#.to_string()),
            ops: Some("insert,update".to_string()),
            read: None,
        };
        let q = forwarded_params(&params);

        assert!(q.contains(&("read".to_string(), "primary".to_string())),
            "a position is portable across a failover only if the group's own leader issued it");
        assert!(q.contains(&("filter".to_string(), r#"{"status":"active"}"#.to_string())));
        assert!(q.contains(&("ops".to_string(), "insert,update".to_string())));

        let bare = forwarded_params(&ChangeParams {
            after: None, filter: None, ops: None, read: None });
        assert_eq!(bare.len(), 1, "an absent filter is not an empty one: {:?}", bare);
    }
}
