//! Webhook delivery: a CDC consumer that pushes to an endpoint instead of holding a connection.
//!
//! Registrations and counters are durable node-local state. An acknowledged position advances
//! through the group's `_webhooks` log before the local cursor, so failover remains at-least-once.
//!
//! Only the group's leader delivers. The stream is opened `read=primary` for that reason, and a
//! step-down ends it in place rather than leaving two nodes pushing the same events.

use crate::auth::digest_admitted;
use crate::cdc::{CdcEnd, CdcFilter, CdcStream};
use crate::changefeed::{ChangeEvent, Changefeed, FeedPin, SubscribeError};
use crate::replication::write_concern::DEFAULT_WTIMEOUT_MS;
use crate::replication::WriteConcern;
use crate::state::AppState;
use crate::storage::Database;
use crate::util::write_atomic;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn};

const WEBHOOK_FILE: &str = "webhooks.meta";
const WEBHOOK_PROGRESS_LOG: &str = "_webhooks";
/// How often the sender set is reconciled against the registrations and this node's leadership.
/// A registration change wakes it early, so this is a bound on noticing a failover rather than
/// something a request waits on.
const SUPERVISE_INTERVAL: Duration = Duration::from_millis(500);
/// Between attempts to open a feed that is not there yet: the collection was dropped, or the
/// handle was replaced under the sender.
const REOPEN_DELAY: Duration = Duration::from_secs(1);
pub const MAX_WEBHOOK_ID_LEN: usize = 64;
pub const MAX_WEBHOOK_URL_LEN: usize = 2048;

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    /// Off refuses registration outright rather than accepting one nothing will deliver.
    #[serde(default = "default_webhooks_enabled")]
    pub enabled: bool,
    #[serde(default = "default_max_subscriptions")]
    pub max_subscriptions: usize,
    /// Events per request. A batch is bounded by this or by `batch_window_ms`, whichever comes
    /// first, so a quiet collection is not held back waiting for the batch to fill.
    #[serde(default = "default_batch_max_events")]
    pub batch_max_events: usize,
    #[serde(default = "default_batch_window_ms")]
    pub batch_window_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_webhooks_enabled() -> bool { true }
fn default_max_subscriptions() -> usize { 32 }
fn default_batch_max_events() -> usize { 64 }
fn default_batch_window_ms() -> u64 { 200 }
fn default_request_timeout_ms() -> u64 { 10_000 }
fn default_initial_backoff_ms() -> u64 { 500 }
fn default_max_backoff_ms() -> u64 { 30_000 }

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: default_webhooks_enabled(),
            max_subscriptions: default_max_subscriptions(),
            batch_max_events: default_batch_max_events(),
            batch_window_ms: default_batch_window_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

impl WebhookConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_max_events == 0 {
            return Err("webhooks.batch_max_events must be at least 1".into());
        }
        if self.request_timeout_ms == 0 {
            return Err("webhooks.request_timeout_ms must be at least 1".into());
        }
        // A retry that does not wait is a busy loop against an endpoint that is already failing.
        if self.initial_backoff_ms == 0 {
            return Err("webhooks.initial_backoff_ms must be at least 1".into());
        }
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err("webhooks.max_backoff_ms must be at least initial_backoff_ms".into());
        }
        Ok(())
    }
}

/// What the operator registered. `filter` and `ops` are the change endpoint's own, kept as the
/// strings they arrived as so a reload parses them the same way the request did.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WebhookSpec {
    pub id: String,
    pub collection: String,
    pub url: String,
    /// Never returned by the API. It is the endpoint's proof that a delivery came from here, and
    /// an operator that lost it rotates it rather than reading it back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ops: Option<String>,
    /// SHA-256 of the API key the registration was created with, and absent when it was created
    /// against an open API. A registration outlives the process that accepted it, so the sender
    /// asks again whether a key the node still holds hashes to this (IB-026). The key itself is
    /// never written down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
}

/// The route a registration was accepted on, which is the gate its credential has to keep
/// clearing. `POST` rather than the delivery: nothing about pushing a batch is an HTTP request to
/// this node.
pub fn registration_route(collection: &str) -> String {
    format!("/collections/{}/webhooks", collection)
}

impl WebhookSpec {
    pub fn still_authorized(&self, auth: &crate::auth::AuthConfig) -> bool {
        digest_admitted(auth, self.creator.as_deref(),
            &registration_route(&self.collection), "POST")
    }
}

/// Where delivery got to, and what it has been doing. Durable, because the position is the only
/// thing that says which events the endpoint has already been told about.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Delivery {
    /// The LSN of the last event acknowledged with a `2xx`.
    pub position: u64,
    pub delivered: u64,
    pub attempts: u64,
    /// Consecutive failures for the batch in flight. Reset by an acknowledgement, and what the
    /// backoff is computed from.
    pub failures: u64,
    /// Events the feed dropped before this subscription read them, because delivery fell further
    /// behind than `changefeed.buffer_events` holds. Not a count of events -- the feed cannot say
    /// how many it evicted -- but of the times it happened.
    pub gaps: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Set when the endpoint answered `410`, which is it saying to stop rather than to retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Subscription {
    #[serde(flatten)]
    pub spec: WebhookSpec,
    #[serde(default)]
    pub delivery: Delivery,
}

impl Subscription {
    /// What the API returns: everything but the secret.
    pub fn public(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.spec.id,
            "collection": self.spec.collection,
            "url": self.spec.url,
            "filter": self.spec.filter,
            "ops": self.spec.ops,
            "signed": self.spec.secret.is_some(),
            "delivery": self.delivery,
        })
    }
}

#[derive(Serialize, Deserialize, Default)]
struct WebhookMeta {
    #[serde(default)]
    subscriptions: Vec<Subscription>,
}

/// A collection and an id. Two collections may use the same id, so neither alone is the identity.
pub type WebhookKey = (String, String);

/// Node-local registrations and delivery state, plus the feed pins a promotion needs beforehand.
pub struct WebhookStore {
    data_dir: String,
    subscriptions: std::sync::Mutex<BTreeMap<WebhookKey, Subscription>>,
    pinned: std::sync::Mutex<HashMap<String, (Arc<Changefeed>, FeedPin)>>,
    /// Wakes the supervisor, so a registration starts delivering at once rather than at the next
    /// poll -- the window between registering and subscribing is one the feed can move under.
    changed: Notify,
}

impl WebhookStore {
    pub fn restored(data_dir: &str) -> Self {
        let meta: WebhookMeta = std::fs::read(Path::new(data_dir).join(WEBHOOK_FILE))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let subscriptions = meta.subscriptions.into_iter()
            .map(|s| ((s.spec.collection.clone(), s.spec.id.clone()), s))
            .collect();
        Self {
            data_dir: data_dir.to_string(),
            subscriptions: std::sync::Mutex::new(subscriptions),
            pinned: std::sync::Mutex::new(HashMap::new()),
            changed: Notify::new(),
        }
    }

    fn persist(&self, held: &BTreeMap<WebhookKey, Subscription>) {
        let meta = WebhookMeta { subscriptions: held.values().cloned().collect() };
        let bytes = match serde_json::to_vec(&meta) {
            Ok(b) => b,
            Err(e) => {
                warn!(target: "webhook", error = %e, "Could not encode the webhook registrations");
                return;
            },
        };
        if let Err(e) = write_atomic(Path::new(&self.data_dir), WEBHOOK_FILE, &bytes) {
            // Delivery carries on: losing the record costs a redelivery after a restart, where
            // stopping would cost the events themselves.
            warn!(target: "webhook", error = %e, "Could not persist the webhook registrations");
        }
    }

    /// `Err` names the ceiling. Re-registering an id replaces the destination and restarts it from
    /// `position`, which is what makes rotating a secret possible without losing the cursor.
    pub fn upsert(&self, spec: WebhookSpec, position: u64, max: usize) -> Result<Subscription, usize> {
        let key = (spec.collection.clone(), spec.id.clone());
        let mut held = self.subscriptions.lock().unwrap();
        let existing = held.get(&key).map(|s| s.delivery.clone());
        if existing.is_none() && held.len() >= max {
            return Err(max);
        }
        let subscription = Subscription {
            spec,
            // A replaced registration keeps its position but loses whatever stopped it.
            delivery: Delivery {
                disabled: None,
                last_error: None,
                failures: 0,
                ..existing.unwrap_or(Delivery { position, ..Default::default() })
            },
        };
        held.insert(key, subscription.clone());
        self.persist(&held);
        self.changed.notify_waiters();
        Ok(subscription)
    }

    pub fn remove(&self, key: &WebhookKey) -> bool {
        let mut held = self.subscriptions.lock().unwrap();
        let removed = held.remove(key).is_some();
        if removed {
            self.persist(&held);
            self.changed.notify_waiters();
        }
        removed
    }

    pub fn get(&self, key: &WebhookKey) -> Option<Subscription> {
        self.subscriptions.lock().unwrap().get(key).cloned()
    }

    pub fn list(&self, collection: &str) -> Vec<Subscription> {
        self.subscriptions.lock().unwrap().values()
            .filter(|s| s.spec.collection == collection).cloned().collect()
    }

    fn collections(&self) -> HashSet<String> {
        self.subscriptions.lock().unwrap().values()
            .filter(|s| s.delivery.disabled.is_none())
            .map(|s| s.spec.collection.clone()).collect()
    }

    fn deliverable(&self) -> Vec<WebhookKey> {
        self.subscriptions.lock().unwrap().iter()
            .filter(|(_, s)| s.delivery.disabled.is_none())
            .map(|(key, _)| key.clone()).collect()
    }

    pub fn sync_pins(&self, db: &Database) {
        let wanted = self.collections();
        let mut pinned = self.pinned.lock().unwrap();
        pinned.retain(|collection, _| wanted.contains(collection));
        for collection in wanted {
            let Some(col) = db.existing_collection(&collection) else { continue };
            let stale = pinned.get(&collection)
                .is_none_or(|(feed, _)| !Arc::ptr_eq(feed, &col.changefeed));
            if stale {
                let feed = col.changefeed.clone();
                pinned.insert(collection, (feed.clone(), feed.pin_from(col.applied_lsn())));
            }
        }
    }

    /// Records progress against the registration as it stands. A subscription removed mid-delivery
    /// is not resurrected by the acknowledgement of its own last batch.
    fn note(&self, key: &WebhookKey, change: impl FnOnce(&mut Delivery)) {
        let mut held = self.subscriptions.lock().unwrap();
        let Some(subscription) = held.get_mut(key) else { return };
        change(&mut subscription.delivery);
        self.persist(&held);
    }
}

fn progress_key(key: &WebhookKey) -> String {
    serde_json::to_string(key).expect("webhook keys serialize")
}

fn replicated_position(state: &AppState, key: &WebhookKey) -> u64 {
    state.db.as_ref()
        .and_then(|db| db.existing_collection(WEBHOOK_PROGRESS_LOG))
        .and_then(|col| col.get(&progress_key(key)).ok().flatten())
        .and_then(|value| value.get("position").and_then(|p| p.as_u64()))
        .unwrap_or(0)
}

async fn commit_position(state: &AppState, key: &WebhookKey, position: u64) -> Result<(), Stop> {
    let timeout = Duration::from_millis(DEFAULT_WTIMEOUT_MS);
    loop {
        if replicated_position(state, key) >= position {
            return Ok(());
        }
        if state.replication.is_some() && !state.is_leader() {
            return Err(Stop::NotLeading);
        }
        match crate::api::write::local_write(
            state,
            WEBHOOK_PROGRESS_LOG,
            progress_key(key),
            Some(serde_json::json!({"position": position})),
            WriteConcern::Majority,
            timeout,
        ).await {
            Ok(outcome) if outcome.met && replicated_position(state, key) >= position =>
                return Ok(()),
            Ok(_) => warn!(target: "webhook", subscription = %key.1, collection = %key.0,
                position, "Webhook acknowledgement has not reached a quorum; retrying"),
            Err(response) => warn!(target: "webhook", subscription = %key.1,
                collection = %key.0, position, status = %response.status(),
                "Could not record webhook acknowledgement; retrying"),
        }
        tokio::time::sleep(REOPEN_DELAY).await;
    }
}

pub fn valid_webhook_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_WEBHOOK_ID_LEN
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Absolute and HTTP, because that is what the sender can post to. Nothing here checks that the
/// host resolves: an endpoint that is down is a retry, not a rejected registration.
pub fn valid_webhook_url(url: &str) -> bool {
    url.len() <= MAX_WEBHOOK_URL_LEN
        && (url.starts_with("http://") || url.starts_with("https://"))
        && url.len() > "https://".len()
        && !url.contains(['\n', '\r', ' '])
}

type HmacSha256 = Hmac<Sha256>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Over `<timestamp>.<body>` rather than the body alone, so a delivery captured off the wire
/// cannot be replayed later under its own signature.
pub fn sign(secret: &str, timestamp: u64, body: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());
    format!("sha256={}", hex(&mac.finalize().into_bytes()))
}

/// Doubling, capped, and spread by a per-subscription offset so two subscriptions that started
/// failing together do not retry in lockstep for the rest of the outage.
fn backoff(config: &WebhookConfig, failures: u64, id: &str) -> Duration {
    use std::hash::{Hash, Hasher};
    let step = config.initial_backoff_ms
        .saturating_mul(1u64 << failures.saturating_sub(1).min(20))
        .min(config.max_backoff_ms);
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    failures.hash(&mut h);
    Duration::from_millis(step + (h.finish() % (step / 4 + 1)))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn body_of(spec: &WebhookSpec, node: &str, delivery: &str, events: &[Arc<ChangeEvent>]) -> String {
    serde_json::json!({
        "subscription": spec.id,
        "collection": spec.collection,
        "delivery": delivery,
        "node": node,
        "position": events.last().map(|e| e.lsn).unwrap_or(0),
        "events": events.iter().map(|e| serde_json::to_value(&**e)
            .unwrap_or(serde_json::Value::Null)).collect::<Vec<_>>(),
    }).to_string()
}

/// Why a sender stopped. Everything else it retries in place.
enum Stop {
    /// The registration is gone, or the endpoint asked to be dropped.
    Done,
    /// This node no longer leads, so it must not be the one pushing.
    NotLeading,
}

/// Its own client, with no cluster credentials on it: the destination is an operator's endpoint,
/// and the internal secret is not something to hand to one.
fn sender_client(config: &WebhookConfig) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
        .unwrap_or_default()
}

/// Retries until the endpoint acknowledges, this node stops leading, or the registration goes.
/// There is no attempt ceiling: a bounded retry drops events, and dropping them silently is worse
/// than a subscription that is visibly behind.
async fn post_until_acknowledged(
    state: &AppState,
    client: &reqwest::Client,
    key: &WebhookKey,
    events: &[Arc<ChangeEvent>],
) -> Result<(), Stop> {
    let config = &state.config.webhooks;
    let delivery_id = uuid::Uuid::new_v4().to_string();
    loop {
        let Some(subscription) = state.webhooks.get(key) else { return Err(Stop::Done) };
        if !state.is_leader() {
            return Err(Stop::NotLeading);
        }
        // Alongside the leadership check and for the same reason: a retry can outlast the
        // credential, and the batch in hand is not owed to an endpoint that lost its registration.
        if revoked(state, key, &subscription.spec) {
            return Err(Stop::Done);
        }
        let timestamp = unix_now();
        let body = body_of(&subscription.spec, &state.config.node_id, &delivery_id, events);
        let mut request = client.post(&subscription.spec.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-Dew-Subscription", &subscription.spec.id)
            .header("X-Dew-Delivery", &delivery_id)
            .header("X-Dew-Timestamp", timestamp.to_string());
        if let Some(secret) = &subscription.spec.secret {
            request = request.header("X-Dew-Signature", sign(secret, timestamp, &body));
        }

        let outcome = match request.body(body).send().await {
            Ok(response) if response.status().is_success() => None,
            // The endpoint saying it is gone, not that it is busy. Retrying it forever would be
            // this node arguing with the answer it asked for.
            Ok(response) if response.status() == reqwest::StatusCode::GONE => {
                state.webhooks.note(key, |d| {
                    d.attempts += 1;
                    d.disabled = Some("the endpoint answered 410; register it again to resume"
                        .to_string());
                });
                info!(target: "webhook", subscription = %key.1, collection = %key.0,
                    "Endpoint answered 410; subscription disabled");
                return Err(Stop::Done);
            },
            Ok(response) => Some(format!("HTTP {}", response.status())),
            Err(e) => Some(e.to_string()),
        };

        let Some(error) = outcome else {
            let Some(last) = events.last() else { return Ok(()) };
            commit_position(state, key, last.lsn).await?;
            state.webhooks.note(key, |d| {
                d.attempts += 1;
                d.delivered += events.len() as u64;
                d.failures = 0;
                d.last_error = None;
                d.position = last.lsn;
            });
            return Ok(());
        };

        let failures = {
            let mut seen = 0;
            state.webhooks.note(key, |d| {
                d.attempts += 1;
                d.failures += 1;
                d.last_error = Some(error.clone());
                seen = d.failures;
            });
            seen
        };
        warn!(target: "webhook", subscription = %key.1, collection = %key.0, failures,
            error = %error, "Webhook delivery failed; retrying");
        tokio::time::sleep(backoff(config, failures, &key.1)).await;
    }
}

/// Whether the credential this registration was created with has stopped being accepted, and if
/// so disables it. A registration is durable, so a restart does not end one the way it ends a
/// connection: without this a key removed from the config keeps posting the collection's documents
/// to the endpoint it named, indefinitely (IB-026).
fn revoked(state: &AppState, key: &WebhookKey, spec: &WebhookSpec) -> bool {
    if spec.still_authorized(&state.auth()) {
        return false;
    }
    state.webhooks.note(key, |d| d.disabled = Some(
        "the credential this subscription was registered with is no longer accepted; \
         register it again".to_string()));
    warn!(target: "webhook", subscription = %key.1, collection = %key.0,
        "The registering credential is no longer accepted; subscription disabled");
    true
}

/// Opens this subscription's feed at its durable position. `None` means try again: the collection
/// is not there, or the handle was replaced.
fn open(state: &AppState, key: &WebhookKey) -> Option<CdcStream> {
    let subscription = state.webhooks.get(key)?;
    if revoked(state, key, &subscription.spec) {
        return None;
    }
    let db = state.db.as_ref()?;
    if db.existing_collection(WEBHOOK_PROGRESS_LOG)
        .is_some_and(|progress| progress.pending_len() > 0) {
        return None;
    }
    let col = db.lookup_collection(&subscription.spec.collection).ok().flatten()?;
    let filter = CdcFilter::parse(
        subscription.spec.filter.as_deref(), subscription.spec.ops.as_deref()).ok()?;

    let position = subscription.delivery.position.max(replicated_position(state, key));
    if position > subscription.delivery.position {
        state.webhooks.note(key, |d| d.position = d.position.max(position));
    }
    match col.changefeed.subscribe(Some(position), col.applied_lsn()) {
        Ok(sub) => Some(CdcStream::new(sub, filter, Some(state.clone()))),
        // Delivery fell further behind than the buffer holds. The events under the floor are gone,
        // so the honest move is to resume at it and record that it happened.
        Err(SubscribeError::Overrun(floor)) => {
            state.webhooks.note(key, |d| {
                d.gaps += 1;
                d.position = floor;
                d.last_error = Some(format!(
                    "fell behind the change buffer; resumed at {}", floor));
            });
            warn!(target: "webhook", subscription = %key.1, collection = %key.0, floor,
                "Webhook fell behind the change buffer; events before the floor were not delivered");
            None
        },
        // The position outlives the log it was taken against -- the collection was rebuilt under
        // this node -- so there is nothing below it left to deliver.
        Err(SubscribeError::Ahead(position)) => {
            state.webhooks.note(key, |d| {
                d.gaps += 1;
                d.position = position;
                d.last_error = Some(format!("position was above the log; resumed at {}", position));
            });
            None
        },
        Err(_) => None,
    }
}

async fn deliver(state: AppState, key: WebhookKey) {
    let config = state.config.webhooks.clone();
    let client = sender_client(&config);
    let window = Duration::from_millis(config.batch_window_ms);

    loop {
        let Some(mut stream) = open(&state, &key) else {
            if !state.is_leader() || state.webhooks.get(&key)
                .is_none_or(|s| s.delivery.disabled.is_some()) {
                return;
            }
            tokio::time::sleep(REOPEN_DELAY).await;
            continue;
        };

        loop {
            // Blocks until there is something to send: an empty batch is not a delivery.
            let mut batch = match stream.next().await {
                Ok(event) => vec![event],
                Err(CdcEnd::NotLeading) => return,
                Err(_) => break,
            };
            let mut ended = false;
            while batch.len() < config.batch_max_events {
                match tokio::time::timeout(window, stream.next()).await {
                    Ok(Ok(event)) => batch.push(event),
                    // The batch in hand is still owed to the endpoint, so it is sent before the
                    // stream is reopened.
                    Ok(Err(_)) => { ended = true; break },
                    Err(_) => break,
                }
            }
            match post_until_acknowledged(&state, &client, &key, &batch).await {
                Ok(()) if !ended => continue,
                Ok(()) => break,
                Err(Stop::Done) => return,
                Err(Stop::NotLeading) => return,
            }
        }
    }
}

/// Holds every registered feed open on followers as well as the node currently delivering.
/// Keeps one sender per deliverable registration while this node leads, and none when it does not.
pub fn webhook_task(state: AppState) {
    if state.db.is_none() || !state.config.webhooks.enabled {
        return;
    }
    // Before the caller binds its listener, not on the first tick: a change written while the
    // supervisor was still starting would otherwise never be built.
    state.webhooks.sync_pins(state.db.as_ref().unwrap());

    tokio::spawn(async move {
        let mut senders: HashMap<WebhookKey, JoinHandle<()>> = HashMap::new();
        loop {
            // A sender that returned is one whose subscription ended or whose leadership went;
            // reconciling against the registrations decides whether it comes back.
            senders.retain(|_, handle| !handle.is_finished());
            let wanted: Vec<WebhookKey> = match state.is_leader() {
                true => state.webhooks.deliverable(),
                false => Vec::new(),
            };
            for (key, handle) in senders.iter() {
                if !wanted.contains(key) {
                    handle.abort();
                }
            }
            senders.retain(|key, _| wanted.contains(key));
            state.webhooks.sync_pins(state.db.as_ref().unwrap());
            for key in wanted {
                senders.entry(key.clone())
                    .or_insert_with(|| tokio::spawn(deliver(state.clone(), key)));
            }

            tokio::select! {
                _ = tokio::time::sleep(SUPERVISE_INTERVAL) => {},
                _ = state.webhooks.changed.notified() => {},
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str) -> WebhookSpec {
        WebhookSpec {
            id: id.to_string(),
            collection: "c".to_string(),
            url: "http://127.0.0.1:1/hook".to_string(),
            secret: Some("s3cret".to_string()),
            filter: None,
            ops: None,
            creator: None,
        }
    }

    /// The signature is what an endpoint checks a delivery with, so it has to be reproducible from
    /// the three things the delivery carries and nothing else.
    #[test]
    fn a_signature_covers_the_timestamp_and_the_body() {
        let body = r#"{"events":[]}"#;
        let signed = sign("s3cret", 1_700_000_000, body);

        assert!(signed.starts_with("sha256="), "{}", signed);
        assert_eq!(signed, sign("s3cret", 1_700_000_000, body), "the same inputs have to agree");
        assert_ne!(signed, sign("s3cret", 1_700_000_001, body),
            "a replay at another time must not verify");
        assert_ne!(signed, sign("other", 1_700_000_000, body));
        assert_ne!(signed, sign("s3cret", 1_700_000_000, r#"{"events":[1]}"#));

        // The published construction: HMAC-SHA256 over `<timestamp>.<body>`, hex, `sha256=`.
        assert_eq!(signed.len(), "sha256=".len() + 64);
    }

    #[test]
    fn a_registration_is_checked_before_it_is_accepted() {
        assert!(valid_webhook_id("orders-sync_1"));
        assert!(!valid_webhook_id(""));
        assert!(!valid_webhook_id("has space"));
        assert!(!valid_webhook_id(&"x".repeat(MAX_WEBHOOK_ID_LEN + 1)));

        assert!(valid_webhook_url("https://example.test/hook"));
        assert!(valid_webhook_url("http://127.0.0.1:9000/hook"));
        assert!(!valid_webhook_url("example.test/hook"), "a relative target is not postable");
        assert!(!valid_webhook_url("ftp://example.test/hook"));
        assert!(!valid_webhook_url("https://"), "a scheme is not a destination");
        assert!(!valid_webhook_url("https://example.test/a\nb"),
            "a newline in a URL is a header this sender would be splitting");
    }

    /// The point of the backoff: it grows, it stops growing, and two subscriptions that failed
    /// together do not come back in lockstep.
    #[test]
    fn the_backoff_doubles_up_to_the_cap_and_is_spread_per_subscription() {
        let config = WebhookConfig {
            initial_backoff_ms: 100, max_backoff_ms: 1_000, ..Default::default() };

        let first = backoff(&config, 1, "a").as_millis() as u64;
        let second = backoff(&config, 2, "a").as_millis() as u64;
        assert!((100..=125).contains(&first), "{}", first);
        assert!(second > first && second <= 250, "{} then {}", first, second);

        let capped = backoff(&config, 40, "a").as_millis() as u64;
        assert!((1_000..=1_250).contains(&capped),
            "an outage must not push the retry beyond the cap: {}", capped);
        assert_ne!(backoff(&config, 40, "a"), backoff(&config, 40, "b"),
            "two subscriptions retrying in lockstep is what the spread is for");
    }

    /// A restart has to resume where the endpoint got to. That is the whole reason the position is
    /// on disk rather than in the sender.
    #[test]
    fn registrations_and_positions_survive_a_restart() {
        let root = crate::test_support::temp_root();
        let dir = root.to_string_lossy().to_string();
        let key = ("c".to_string(), "hook".to_string());

        let store = WebhookStore::restored(&dir);
        store.upsert(spec("hook"), 7, 4).unwrap();
        store.note(&key, |d| { d.position = 12; d.delivered = 3 });

        let reloaded = WebhookStore::restored(&dir);
        let back = reloaded.get(&key).expect("the registration has to come back");
        assert_eq!(back.delivery.position, 12, "a restart must not redeliver from the beginning");
        assert_eq!(back.delivery.delivered, 3);
        assert_eq!(back.spec.secret.as_deref(), Some("s3cret"),
            "the secret is what the endpoint verifies with, so it has to survive too");

        assert!(reloaded.remove(&key));
        assert!(WebhookStore::restored(&dir).get(&key).is_none(), "a removal has to be durable");
    }

    /// Re-registering an id is how a destination or a secret is rotated, and the position is the
    /// one thing that must not be reset by it.
    #[test]
    fn re_registering_keeps_the_position_and_clears_what_stopped_it() {
        let root = crate::test_support::temp_root();
        let dir = root.to_string_lossy().to_string();
        let key = ("c".to_string(), "hook".to_string());
        let store = WebhookStore::restored(&dir);

        store.upsert(spec("hook"), 0, 4).unwrap();
        store.note(&key, |d| {
            d.position = 20;
            d.failures = 5;
            d.disabled = Some("the endpoint answered 410".to_string());
        });

        let mut rotated = spec("hook");
        rotated.url = "https://elsewhere.test/hook".to_string();
        let back = store.upsert(rotated, 0, 4).unwrap();
        assert_eq!(back.delivery.position, 20);
        assert_eq!(back.delivery.failures, 0);
        assert!(back.delivery.disabled.is_none(), "a re-registration is how a stop is undone");
        assert_eq!(back.spec.url, "https://elsewhere.test/hook");
    }

    #[test]
    fn the_subscription_ceiling_refuses_rather_than_grows() {
        let root = crate::test_support::temp_root();
        let store = WebhookStore::restored(&root.to_string_lossy());

        assert!(store.upsert(spec("a"), 0, 2).is_ok());
        assert!(store.upsert(spec("b"), 0, 2).is_ok());
        assert_eq!(store.upsert(spec("c"), 0, 2).err(), Some(2));
        assert!(store.upsert(spec("a"), 0, 2).is_ok(), "replacing one adds nothing to the count");
    }

    #[test]
    fn a_secret_is_never_in_what_the_api_returns() {
        let subscription = Subscription { spec: spec("hook"), delivery: Delivery::default() };
        let shown = subscription.public().to_string();

        assert!(!shown.contains("s3cret"), "{}", shown);
        assert!(shown.contains("\"signed\":true"), "{}", shown);
    }
}
