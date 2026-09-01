//! Runtime membership changes. Admitted nodes are learners; the quorum set is not touched here.

use crate::cluster::metadata::{
    plan_configuration, plan_join, plan_leave, Adoption, ClusterMetadata, JoinRequest,
};
use crate::consensus::reconfigure::{change_membership, pending_change, ChangeError};
use crate::model::err_json;
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use tracing::{info, warn};

#[derive(Deserialize)]
pub struct LeaveParams {
    pub url: String,
}

/// Only a leader may publish a membership change. The view converges by version rather than being
/// agreed, so two writers produce two versions of which one is discarded; one writer per shard
/// group is the strongest ordering available before joint consensus.
pub(crate) fn writable(state: &AppState) -> Result<(), axum::response::Response> {
    if !state.is_shard() {
        return Err(err_json(StatusCode::CONFLICT,
            "membership changes must be sent to a shard leader, not a router".to_string()));
    }
    if !state.is_leader() {
        let known = state.replication.as_ref()
            .and_then(|r| r.read().unwrap().primary_addr.clone());
        return Err((StatusCode::CONFLICT, Json(serde_json::json!({
            "error": "not the leader; send membership changes to the primary",
            "primary": known,
        }))).into_response());
    }
    Ok(())
}

pub(crate) async fn publish(state: &AppState, next: ClusterMetadata) -> Result<u64, axum::response::Response> {
    match state.adopt_cluster(next) {
        Adoption::Adopted { to, .. } => Ok(to),
        // Our own next version losing means someone else published concurrently.
        Adoption::Stale { current } => Err(err_json(StatusCode::CONFLICT, format!(
            "the view moved to v{} while this change was being applied; retry", current))),
        Adoption::Rejected(why) => Err(err_json(StatusCode::UNPROCESSABLE_ENTITY, why)),
    }
}

/// Pushes the new view straight at a node so it learns its role immediately rather than after a
/// poll. Spawned rather than awaited: the change is already published and durable, and a control
/// plane write must not hang on whether every node it names happens to be reachable.
pub(crate) fn nudge(state: &AppState, url: &str, view: &ClusterMetadata) {
    let client = state.client.clone();
    let endpoint = format!("{}/internal/cluster", url);
    let node = url.to_string();
    let view = view.clone();
    tokio::spawn(async move {
        if let Err(e) = client.post(&endpoint).json(&view).send().await {
            warn!(target: "membership", node = %node, error = %e,
                "Could not hand the new view to the node directly; it will pick it up by polling");
        }
    });
}

/// Broadcasts membership changes to the designated rebalance coordinator and all known nodes.
fn broadcast(state: &AppState, view: &ClusterMetadata, extra: Option<&str>) {
    let own = state.own_url();
    let mut seen = std::collections::HashSet::new();
    let targets: Vec<String> = view.members.iter().map(|m| m.url.clone())
        .chain(view.shard_owners().into_iter().map(|(url, _)| url))
        .chain(extra.into_iter().map(str::to_string))
        .filter(|url| !crate::util::same_endpoint(url, &own))
        .filter(|url| seen.insert(crate::util::endpoint_of(url).to_string()))
        .collect();
    for target in targets {
        nudge(state, &target, view);
    }
}

pub async fn join_handler(
    State(state): State<AppState>,
    Json(req): Json<JoinRequest>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }

    let own = state.own_url();
    let next = match plan_join(&state.cluster_view(), &state.config.node_id, &own, &req) {
        Ok(next) => next,
        Err(why) => return err_json(StatusCode::UNPROCESSABLE_ENTITY, why),
    };

    let version = match publish(&state, next).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let view = state.cluster_view();
    let follows = view.member(&req.url).and_then(|m| m.follows.clone());
    if follows.as_deref().map_or(false, |f| crate::util::same_endpoint(f, &own)) {
        state.begin_tracking_learner(&req.url);
    }
    broadcast(&state, &view, None);

    info!(target: "membership", node = %req.url, version, "Admitted as a learner");
    (StatusCode::OK, Json(serde_json::json!({
        "status": "joined",
        "url": req.url,
        "voting": false,
        "follows": follows,
        "version": version,
        "note": "learners replicate but are not counted in any quorum",
    }))).into_response()
}

/// What an operator asks for: the voting set they want, not the delta. Joint consensus makes an
/// arbitrary set change safe, so the set is the primitive and add/remove are the caller's
/// arithmetic over `GET /cluster/configuration`.
#[derive(Deserialize)]
pub struct ConfigurationRequest {
    pub voters: Vec<String>,
}

fn configuration_body(state: &AppState) -> serde_json::Value {
    let config = state.quorum_config();
    serde_json::json!({
        "voters": config.voters,
        "outgoing": config.outgoing,
        "joint": config.outgoing.is_some(),
        "uncommitted": pending_change(state).is_some(),
    })
}

pub async fn configuration_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    if !state.is_shard() {
        return err_json(StatusCode::CONFLICT,
            "a router has no quorum; ask a shard node".to_string());
    }
    (StatusCode::OK, Json(configuration_body(&state))).into_response()
}

pub async fn set_configuration_handler(
    State(state): State<AppState>,
    Json(req): Json<ConfigurationRequest>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }

    let installed = match change_membership(&state, req.voters).await {
        Ok(config) => config,
        Err(ChangeError::Refused(why)) => return err_json(StatusCode::UNPROCESSABLE_ENTITY, why),
        // 503, not 500: the entry is durable and in force, and retrying the same change finishes it.
        Err(ChangeError::Stalled(why)) => return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": why, "configuration": configuration_body(&state) })),
        ).into_response(),
    };

    // Only after the log committed it. The view records the outcome; it never decides it, so a
    // failure to publish costs routing accuracy and nothing in the quorum.
    let own = state.own_url();
    match plan_configuration(&state.cluster_view(), &state.config.node_id, &own, &installed.voters) {
        Ok(next) => {
            if publish(&state, next).await.is_ok() {
                broadcast(&state, &state.cluster_view(), None);
            }
        },
        Err(why) => warn!(target: "membership", error = %why,
            "Configuration committed but the view could not record it"),
    }

    info!(target: "membership", voters = ?installed.voters, "Configuration changed");
    (StatusCode::OK, Json(configuration_body(&state))).into_response()
}

pub async fn leave_handler(
    State(state): State<AppState>,
    Query(params): Query<LeaveParams>,
) -> impl axum::response::IntoResponse {
    if let Err(resp) = writable(&state) {
        return resp;
    }

    let next = match plan_leave(&state.cluster_view(), &state.config.node_id, &params.url) {
        Ok(next) => next,
        Err(why) => {
            let code = if why.contains("not a member") { StatusCode::NOT_FOUND }
                       else { StatusCode::UNPROCESSABLE_ENTITY };
            return err_json(code, why);
        },
    };

    let version = match publish(&state, next).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    broadcast(&state, &state.cluster_view(), Some(&params.url));

    info!(target: "membership", node = %params.url, version, "Removed from the cluster");
    (StatusCode::OK, Json(serde_json::json!({
        "status": "removed",
        "url": params.url,
        "version": version,
    }))).into_response()
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        put_doc_at, read_doc_http, temp_root, three_node_cluster, three_node_cluster_with_timeout, wait_for, TestNode,
    };
    use axum::http::StatusCode;
    use std::time::Duration;

    fn client() -> reqwest::Client {
        reqwest::Client::builder().timeout(Duration::from_secs(3)).build().unwrap()
    }

    /// A node nobody has heard of: no peers, no primary, exactly what a fresh box looks like.
    /// `membership_mode: learner` is what stops it electing itself; the heartbeat timeout is left
    /// short deliberately, so any test that passes here passes without a timing cushion.
    fn fresh_node(root: &std::path::Path, id: &str) -> TestNode {
        let port = crate::test_support::next_test_port();
        let mut n = TestNode::new(id, port, root, "replica");
        n.membership_mode = "learner".to_string();
        n.start();
        n
    }

    async fn join(c: &reqwest::Client, leader: &str, url: &str) -> (StatusCode, serde_json::Value) {
        let r = c.post(&format!("{}/cluster/members", leader))
            .json(&serde_json::json!({ "url": url, "node_id": "n4" }))
            .send().await.unwrap();
        let code = r.status();
        (code, r.json().await.unwrap_or(serde_json::Value::Null))
    }

    /// The window this closes: a node is running, nobody has admitted it, no leader is reachable,
    /// and it has no peers -- so a majority of one is arithmetically available to it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unadmitted_learner_never_campaigns_however_long_it_waits() {
        let root = temp_root();
        let c = client();

        // No cluster at all: nothing to find, nothing to follow, nothing to stop it but the mode.
        let mut n4 = fresh_node(&root, "n4");
        assert_eq!(n4.heartbeat_timeout_secs, 1, "the guard must hold without a timing cushion");
        assert!(n4.state.as_ref().unwrap().is_learner(),
            "the mode must apply from boot, before any cluster view exists");
        assert!(!n4.state.as_ref().unwrap().in_quorum());
        assert!(n4.state.as_ref().unwrap().cluster_view().seeded,
            "this node has never received a published view");

        // Several election timeouts, each of which would otherwise be a self-promotion.
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(!n4.is_leader(), "an unadmitted learner elected itself over an empty log");
        assert_eq!(n4.term(), 0, "it must not even have campaigned; the term should never move");

        let r = c.put(&format!("{}/collections/t/docs/k", n4.url()))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "an unadmitted node must refuse client writes");

        // Restart with no contact ever made: the restriction is config-borne, so it survives a
        // boot that reaches nothing and reads no cluster.meta from anyone.
        n4.kill();
        n4.start();
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(!n4.is_leader(), "the restriction did not survive a restart");
        assert_eq!(n4.term(), 0);

        let r = c.put(&format!("{}/collections/t/docs/k", n4.url()))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admission_gives_a_learner_data_but_never_a_vote() {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let c = client();

        assert_eq!(put_doc_at(&c, &n1.url(), "t", "early", 1, "?w=majority").await, StatusCode::CREATED);

        let mut n4 = fresh_node(&root, "n4");
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!n4.is_leader(), "it must survive being unadmitted next to a live cluster too");

        assert_eq!(join(&c, &n1.url(), &n4.url()).await.0, StatusCode::OK);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("early")).unwrap_or(false)
        }).await, "admission must still produce a working replica");

        // Admission populated who to follow and started replication tracking, and nothing else.
        assert!(n4.state.as_ref().unwrap().is_learner(), "admission must not promote to voter");
        assert!(!n4.state.as_ref().unwrap().in_quorum());
        assert_eq!(n1.state.as_ref().unwrap().voting_replicas().len(), 2,
            "the quorum set is the same size it was before the join");

        let vote = c.post(&format!("{}/internal/vote", n4.url()))
            .json(&serde_json::json!({
                "term": 99, "candidate_id": "someone", "last_lsn": 0, "last_term": 0}))
            .send().await.unwrap();
        assert_eq!(vote.status(), StatusCode::FORBIDDEN,
            "refusing to stand is half of it; a learner no threshold counts must not be able to              push a candidate over one either");

        // And it stays a learner across a restart even now that a view names it one.
        n4.kill();
        n4.start();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!n4.is_leader());
        assert!(n4.state.as_ref().unwrap().is_learner());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_admitted_at_runtime_catches_up_without_entering_the_quorum() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let c = client();

        // Written before the new node exists, so it can only arrive by catch-up.
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "early", 1, "?w=majority").await, StatusCode::CREATED);

        let n4 = fresh_node(&root, "n4");
        assert!(read_doc_http(&c, &n4.url(), "early").await.is_none(), "the new node starts empty");

        let (code, body) = join(&c, &n1.url(), &n4.url()).await;
        assert_eq!(code, StatusCode::OK, "join was refused: {}", body);
        assert_eq!(body["voting"], false, "a runtime join must never enter the quorum");
        assert_eq!(body["follows"].as_str(), Some(n1.url().as_str()));

        // No further writes: catch-up has to be driven by the leader, not by traffic.
        let caught_up = wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("early")).unwrap_or(false)
        }).await;
        assert!(caught_up, "the learner never received the backlog on an idle cluster");

        // A write at w=majority must be satisfied by the voters alone, and still reach the learner.
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "later", 2, "?w=majority").await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("later")).unwrap_or(false)
        }).await, "the learner stopped receiving writes after joining");

        let m = c.get(&format!("{}/metrics", n1.url())).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        assert_eq!(m["replication"]["voting_replicas"], 2, "the quorum set must not have grown");
        assert_eq!(m["replication"]["replica_count"], 3, "but the learner is a replication target");
        assert_eq!(m["replication"]["learners"][0].as_str(), Some(n4.url().as_str()));

        assert!(!n4.is_leader());
        assert_eq!(crate::test_support::leaders(&[&n1, &n2, &n3, &n4]), vec!["n1".to_string()]);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_learner_cannot_satisfy_a_write_concern_it_is_not_counted_in() {
        let root = temp_root();
        let (n1, mut n2, mut n3) = three_node_cluster_with_timeout(&root, 30).await;
        let c = client();

        let n4 = fresh_node(&root, "n4");
        assert_eq!(join(&c, &n1.url(), &n4.url()).await.0, StatusCode::OK);
        assert!(wait_for(Duration::from_secs(10),
            || n1.state.as_ref().unwrap().learner_replicas().len() == 1).await);

        // Both voters gone. w=majority needs 2 of {n1,n2,n3}; only the leader is left, plus a
        // learner that will happily acknowledge and must not be allowed to make up the difference.
        n2.kill();
        n3.kill();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let r = c.put(&format!("{}/collections/t/docs/lonely?w=majority&wtimeout=2000", n1.url()))
            .json(&serde_json::json!({"value": {"v": 1}}))
            .send().await.unwrap();
        let status = r.status();
        let body = r.json::<serde_json::Value>().await.unwrap();

        assert_eq!(status, StatusCode::ACCEPTED,
            "the write concern was reported as met with no quorum alive; a learner's ack was \
             counted as a vote. body={}", body);
        assert_eq!(body["acks"], 1, "only the leader itself may be counted: {}", body);
        assert_eq!(body["required"], 2, "majority of three is unchanged by a learner joining");
        assert_eq!(body["warning"], "write concern not met");

        // It is still shipped the data, which is the entire point of admitting it.
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.last_appended_lsn() > 0).unwrap_or(false)
        }).await, "the learner received nothing at all");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_learner_left_entirely_alone_still_refuses_to_elect_itself() {
        let root = temp_root();
        let (mut n1, mut n2, mut n3) = three_node_cluster(&root).await;
        let c = client();

        let mut n4 = fresh_node(&root, "n4");
        let (code, body) = join(&c, &n1.url(), &n4.url()).await;
        assert_eq!(code, StatusCode::OK, "join was refused: {}", body);
        assert!(wait_for(Duration::from_secs(10), || n4.state.as_ref().unwrap().is_learner()).await,
            "the learner never learned its own role");

        // Every voter gone. A node with no peers reaches a majority of one, so nothing but the
        // learner rule stops this one from promoting itself and serving writes off an empty log.
        n1.kill();
        n2.kill();
        n3.kill();

        // Comfortably past its election timeout, which is what a shorter one would race.
        n4.heartbeat_timeout_secs = 1;
        n4.kill();
        n4.start();
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(!n4.is_leader(),
            "a non-voting node elected itself; it would serve writes no quorum ever agreed to");
        assert!(n4.state.as_ref().unwrap().is_learner(),
            "the learner role must survive the restart that would otherwise let it stand");

        let _ = std::fs::remove_dir_all(&root);
    }

    async fn configuration(c: &reqwest::Client, node: &str) -> serde_json::Value {
        c.get(&format!("{}/cluster/configuration", node)).send().await.unwrap()
            .json().await.unwrap()
    }

    async fn set_voters(c: &reqwest::Client, leader: &str, voters: &[String])
        -> (StatusCode, serde_json::Value)
    {
        let r = c.post(&format!("{}/cluster/configuration", leader))
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "voters": voters }))
            .send().await.unwrap();
        let code = r.status();
        (code, r.json().await.unwrap_or(serde_json::Value::Null))
    }

    /// The point of commit 44: a learner becomes a voter without the cluster ever passing through
    /// a moment where the old and the new voting sets could each reach a majority on their own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_learner_is_promoted_into_the_quorum_through_a_joint_configuration() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let c = client();

        let n4 = fresh_node(&root, "n4");
        assert_eq!(join(&c, &n1.url(), &n4.url()).await.0, StatusCode::OK);
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "early", 1, "?w=majority").await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("early")).unwrap_or(false)
        }).await, "the learner has to catch up before it is worth promoting");

        let before = configuration(&c, &n1.url()).await;
        assert_eq!(before["joint"], false);
        assert_eq!(before["voters"].as_array().unwrap().len(), 3);

        let voters: Vec<String> = vec![n1.url(), n2.url(), n3.url(), n4.url()];
        let (code, body) = set_voters(&c, &n1.url(), &voters).await;
        assert_eq!(code, StatusCode::OK, "promotion failed: {}", body);
        assert_eq!(body["joint"], false, "the change must not stop in the joint entry: {}", body);
        assert_eq!(body["uncommitted"], false);
        assert_eq!(body["voters"].as_array().unwrap().len(), 4);

        // Every node decides against the same set, and it reached them through the log.
        for node in [&n1, &n2, &n3, &n4] {
            // Not `voters.len() == 4`: the joint entry's incoming half is also those four, so that
            // alone is satisfied before the target entry lands and the assert below then races it.
            assert!(wait_for(Duration::from_secs(15), || {
                let installed = node.state.as_ref().unwrap().quorum_config();
                !installed.is_joint() && installed.voters.len() == 4
            }).await, "{} never adopted the new configuration", node.node_id);
            let installed = node.state.as_ref().unwrap().quorum_config();
            assert!(!installed.is_joint(), "{} is stuck in joint consensus", node.node_id);
            assert!(installed.contains(&n4.url()));
        }
        assert!(n4.state.as_ref().unwrap().in_quorum(),
            "a promoted node must be able to stand, or the quorum counting it cannot elect");

        // A majority is 3 of 4 now, and writes still commit at it.
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "after", 2, "?w=majority").await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("after")).unwrap_or(false)
        }).await);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The other direction, and the reason a demoted node stays a member: it has to stop standing
    /// for election, and being told is the only way it finds out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_demoted_voter_leaves_the_quorum_and_stops_campaigning() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let c = client();

        assert_eq!(put_doc_at(&c, &n1.url(), "t", "k", 1, "?w=majority").await, StatusCode::CREATED);

        let voters: Vec<String> = vec![n1.url(), n2.url()];
        let (code, body) = set_voters(&c, &n1.url(), &voters).await;
        assert_eq!(code, StatusCode::OK, "demotion failed: {}", body);
        assert_eq!(body["voters"].as_array().unwrap().len(), 2);

        assert!(wait_for(Duration::from_secs(15), || {
            !n3.state.as_ref().unwrap().quorum_config().contains(&n3.url())
        }).await, "the demoted node still counts itself in the quorum");
        assert!(!n3.state.as_ref().unwrap().in_quorum(),
            "a node outside the configuration standing for election is what a removal must prevent");

        // A majority is 2 of 2 now, and the demoted node is still shipped frames.
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "after", 2, "?w=majority").await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(15), || {
            n3.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("after")).unwrap_or(false)
        }).await, "a demoted voter is a learner, not a node cut off");

        // And it may now be removed outright, which it could not be while it was voting.
        let r = c.delete(&format!("{}/cluster/members?url={}", n1.url(), n3.url()))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK, "demotion is what unblocks the removal");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A promotion that does not reach the election path is half a promotion: the node is counted
    /// in every majority and can neither be elected by one nor help elect anyone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_promoted_node_votes_and_can_be_elected() {
        let root = temp_root();
        let (mut n1, n2, n3) = three_node_cluster(&root).await;
        let c = client();

        let n4 = fresh_node(&root, "n4");
        assert_eq!(join(&c, &n1.url(), &n4.url()).await.0, StatusCode::OK);
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "k", 1, "?w=majority").await, StatusCode::CREATED);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists("k")).unwrap_or(false)
        }).await);

        let voters: Vec<String> = vec![n1.url(), n2.url(), n3.url(), n4.url()];
        assert_eq!(set_voters(&c, &n1.url(), &voters).await.0, StatusCode::OK);
        assert!(wait_for(Duration::from_secs(15), || {
            n4.state.as_ref().unwrap().quorum_config().contains(&n4.url())
        }).await);

        // A candidate now needs three of four, which it cannot reach without the promoted node.
        let vote = c.post(&format!("{}/internal/vote", n4.url()))
            .json(&serde_json::json!({
                "term": 99, "candidate_id": "someone", "last_lsn": 0, "last_term": 0}))
            .send().await.unwrap();
        assert_ne!(vote.status(), StatusCode::FORBIDDEN,
            "a promoted node refusing to vote leaves its own quorum unable to elect");

        n1.kill();
        let elected = crate::test_support::settle_leader(&[&n2, &n3, &n4], Duration::from_secs(30)).await;
        assert!(elected.is_some(), "the surviving three of four never settled on a leader");

        let leader = crate::test_support::node_by_id(&[&n2, &n3, &n4], elected.as_deref().unwrap());
        assert!(wait_for(Duration::from_secs(15),
            || read_doc_http_blocking(leader)).await, "the committed write did not survive");

        let _ = std::fs::remove_dir_all(&root);
    }

    fn read_doc_http_blocking(node: &TestNode) -> bool {
        node.state.as_ref().unwrap().db.as_ref().unwrap()
            .get_collection("t").map(|col| col.exists("k")).unwrap_or(false)
    }

    /// The configuration is a log entry, so a restart recovers it from the log, not from config.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_committed_configuration_survives_a_restart() {
        let root = temp_root();
        let (n1, n2, mut n3) = three_node_cluster(&root).await;
        let c = client();

        let voters: Vec<String> = vec![n1.url(), n2.url()];
        assert_eq!(set_voters(&c, &n1.url(), &voters).await.0, StatusCode::OK);
        assert!(wait_for(Duration::from_secs(15), || {
            n3.state.as_ref().unwrap().quorum_config().voters.len() == 2
        }).await);

        n3.kill();
        n3.start();
        tokio::time::sleep(Duration::from_secs(2)).await;

        let installed = n3.state.as_ref().unwrap().quorum_config();
        assert_eq!(installed.voters.len(), 2, "a restart fell back to the config-derived voter set");
        assert!(!installed.contains(&n3.url()));
        assert!(!n3.state.as_ref().unwrap().in_quorum(),
            "a demoted node that forgets it on restart campaigns against a quorum it is not in");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn configuration_changes_are_refused_when_they_cannot_be_made_safely() {
        let root = temp_root();
        let (n1, n2, n3) = three_node_cluster(&root).await;
        let c = client();

        let (code, body) = set_voters(&c, &n2.url(), &[n1.url()]).await;
        assert_eq!(code, StatusCode::CONFLICT, "only the leader may append a configuration: {}", body);

        let (code, body) = set_voters(&c, &n1.url(), &[]).await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{}", body);

        let (code, body) = set_voters(&c, &n1.url(), &[n2.url(), n3.url()]).await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{}", body);
        assert!(body["error"].as_str().unwrap().contains("cannot remove itself"),
            "nothing here transfers leadership, so a self-removal has to be refused: {}", body);

        let stranger = "http://127.0.0.1:1".to_string();
        let (code, body) = set_voters(&c, &n1.url(),
            &[n1.url(), n2.url(), n3.url(), stranger]).await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{}", body);
        assert!(body["error"].as_str().unwrap().contains("admit it as a learner first"),
            "a node holding nothing must not be counted in a majority: {}", body);

        // None of the refusals appended anything.
        let after = configuration(&c, &n1.url()).await;
        assert_eq!(after["joint"], false);
        assert_eq!(after["uncommitted"], false);
        assert_eq!(after["voters"].as_array().unwrap().len(), 3);
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "k", 1, "?w=majority").await, StatusCode::CREATED);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_config_log_is_not_reachable_as_a_collection() {
        let root = temp_root();
        let (n1, n2, _n3) = three_node_cluster(&root).await;
        let c = client();

        assert_eq!(set_voters(&c, &n1.url(), &[n1.url(), n2.url()]).await.0, StatusCode::OK);

        let r = c.put(&format!("{}/collections/_config/docs/k", n1.url()))
            .json(&serde_json::json!({"value": {"v": 1}})).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN,
            "a client writing the config log would move the quorum from the public API");

        let r = c.delete(&format!("{}/collections/_config", n1.url())).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);

        let listed = c.get(&format!("{}/collections", n1.url())).send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        assert!(!listed["collections"].as_array().unwrap().iter().any(|v| v == "_config"),
            "a system log is not a client collection: {}", listed);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn changes_that_would_move_the_quorum_are_refused_over_http() {
        let root = temp_root();
        let (n1, n2, _n3) = three_node_cluster(&root).await;
        let c = client();

        let r = c.post(&format!("{}/cluster/members", n2.url()))
            .json(&serde_json::json!({ "url": "http://127.0.0.1:1" })).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::CONFLICT, "only one writer per group may publish");
        let body = r.json::<serde_json::Value>().await.unwrap();
        assert!(body["primary"].as_str().is_some(), "a follower must name the primary: {}", body);

        let r = c.post(&format!("{}/cluster/members", n1.url()))
            .json(&serde_json::json!({ "url": "http://127.0.0.1:1", "voting": true }))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(r.json::<serde_json::Value>().await.unwrap()["error"].as_str().unwrap()
            .contains("/cluster/configuration"),
            "a join is still never a promotion; it points at the endpoint that is one");

        let r = c.delete(&format!("{}/cluster/members?url={}", n1.url(), n2.url()))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY,
            "a voter dropped from the view is one the log still counts and nothing can route to");
        assert!(r.json::<serde_json::Value>().await.unwrap()["error"].as_str().unwrap()
            .contains("demote it"));

        let r = c.delete(&format!("{}/cluster/members?url=http://nobody:1", n1.url()))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);

        // Writes are unaffected by any of the refusals.
        assert_eq!(put_doc_at(&c, &n1.url(), "t", "k", 1, "?w=majority").await, StatusCode::CREATED);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_removed_learner_stops_receiving_writes() {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let c = client();

        let n4 = fresh_node(&root, "n4");
        assert_eq!(join(&c, &n1.url(), &n4.url()).await.0, StatusCode::OK);

        assert_eq!(put_doc_at(&c, &n1.url(), "t", "before", 1, "?w=majority").await, StatusCode::CREATED);
        let holds = |key: &'static str| {
            n4.state.as_ref().unwrap().db.as_ref().unwrap()
                .get_collection("t").map(|col| col.exists(key)).unwrap_or(false)
        };
        assert!(wait_for(Duration::from_secs(15), || holds("before")).await,
            "the learner never received the write it was admitted for");

        let r = c.delete(&format!("{}/cluster/members?url={}", n1.url(), n4.url()))
            .send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK, "a learner may be removed");
        assert!(wait_for(Duration::from_secs(5),
            || n1.state.as_ref().unwrap().learner_replicas().is_empty()).await,
            "the leader still lists the removed node as a target");

        assert_eq!(put_doc_at(&c, &n1.url(), "t", "after", 2, "?w=majority").await, StatusCode::CREATED);
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!holds("after"), "a removed node must stop receiving data");
        assert!(holds("before"), "and keeps what it already had");

        let _ = std::fs::remove_dir_all(&root);
    }
}
