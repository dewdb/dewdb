//! The committed change feed: what a collection publishes, and the buffer a subscriber resumes from.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Events handed to one subscriber per read. Bounds how long the ring lock is held, not how far
/// behind a subscriber may fall -- `buffer_events` does that.
const READ_BATCH: usize = 256;

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ChangefeedConfig {
    /// Events kept per collection. A subscriber that falls further behind than this is overrun and
    /// told to resubscribe rather than served a feed with a hole in it.
    #[serde(default = "default_buffer_events")]
    pub buffer_events: usize,
    /// How long after the last subscriber leaves the feed keeps recording, so a dropped connection
    /// can reconnect and resume. Past it the feed goes quiet and costs the write path nothing.
    #[serde(default = "default_idle_retention_ms")]
    pub idle_retention_ms: u64,
    #[serde(default = "default_max_subscribers")]
    pub max_subscribers: usize,
}

fn default_buffer_events() -> usize { 1024 }
fn default_idle_retention_ms() -> u64 { 30_000 }
fn default_max_subscribers() -> usize { 64 }

impl Default for ChangefeedConfig {
    fn default() -> Self {
        Self {
            buffer_events: default_buffer_events(),
            idle_retention_ms: default_idle_retention_ms(),
            max_subscribers: default_max_subscribers(),
        }
    }
}

impl ChangefeedConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_events == 0 {
            return Err("changefeed.buffer_events must be at least 1".into());
        }
        if self.max_subscribers == 0 {
            return Err("changefeed.max_subscribers must be at least 1".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
    /// The collection was dropped. One event rather than one delete per key: nothing bounds how
    /// many keys a drop removes, and the subscriber's next fact is that the collection is gone.
    Drop,
}

impl ChangeOp {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "drop" => Ok(Self::Drop),
            other => Err(format!(
                "unknown change op `{}`; supported: insert, update, delete, drop", other)),
        }
    }
}

/// One committed change. It carries no timestamp: a delete has no document to take one from, and
/// a wall clock only half the events could report would be worse than none.
#[derive(Serialize, Debug)]
pub struct ChangeEvent {
    pub lsn: u64,
    pub op: ChangeOp,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

struct Ring {
    events: VecDeque<Arc<ChangeEvent>>,
    /// Lowest position a subscriber may resume from. Rises when the buffer evicts, when a document
    /// could not be resolved, and when the feed skipped a commit because nobody was listening.
    resume_floor: u64,
    /// Highest commit this feed has accounted for, published or skipped. Where a subscriber that
    /// names no position starts.
    position: u64,
    subscribers: usize,
    /// When the last subscriber left. `None` means none has ever attached.
    idle_since: Option<Instant>,
    closed: bool,
}

pub struct Changefeed {
    config: ChangefeedConfig,
    ring: std::sync::Mutex<Ring>,
    /// Carries the published position. A `watch` rather than a `Notify` because its receiver tracks
    /// a version, so a publish between a subscriber's read and its wait cannot be missed.
    wake: watch::Sender<u64>,
    /// Mirrors `Ring::subscribers` for `active()`, which the apply path calls per commit and must
    /// not have to take the ring lock for in the common case of nobody listening.
    live: AtomicUsize,
    /// Consumers that keep the feed recording without holding a subscription: a webhook sender
    /// between one subscription and the next, and the window after a restart before it attaches.
    /// Without one, a change written in that window is never built, and no position recovers it.
    pins: AtomicUsize,
    published: AtomicU64,
    overruns: AtomicU64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubscribeError {
    /// The position is below what the buffer still covers. Carries the lowest one that works.
    Overrun(u64),
    /// A position this node's committed log has not reached, so it came from another log.
    Ahead(u64),
    TooMany(usize),
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FeedEnd {
    Overrun(u64),
    Closed,
}

enum Read {
    Events(Vec<Arc<ChangeEvent>>),
    Empty,
    /// The cursor fell below the floor and the subscriber is entitled to know.
    Overrun(u64),
    /// The same, for a subscriber that named no position and has read nothing: "from now on" has
    /// no position to be inconsistent with, so it starts at the floor instead of failing.
    Skipped(u64),
    Closed,
}

impl Changefeed {
    /// `position` is the collection's committed watermark at open. A feed starting at 0 would
    /// accept `after=0` on a collection with history and answer it with silence.
    pub fn new(config: ChangefeedConfig, position: u64) -> Self {
        Self {
            ring: std::sync::Mutex::new(Ring {
                events: VecDeque::new(),
                resume_floor: position,
                position,
                subscribers: 0,
                idle_since: None,
                closed: false,
            }),
            config,
            wake: watch::channel(position).0,
            live: AtomicUsize::new(0),
            pins: AtomicUsize::new(0),
            published: AtomicU64::new(0),
            overruns: AtomicU64::new(0),
        }
    }

    /// Whether the apply path should build events for this commit. Resolving a document that was
    /// not inlined is a WAL read, so a feed nobody watches costs one load and one uncontended lock.
    pub fn active(&self) -> bool {
        if self.live.load(Ordering::Relaxed) > 0 || self.pins.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let ring = self.ring.lock().unwrap();
        !ring.closed && ring.idle_since.is_some_and(|t| t.elapsed() < self.retention())
    }

    fn retention(&self) -> Duration {
        Duration::from_millis(self.config.idle_retention_ms)
    }

    pub fn publish(&self, events: Vec<ChangeEvent>, through: u64) {
        let count = events.len() as u64;
        let position = {
            let mut ring = self.ring.lock().unwrap();
            if ring.closed {
                return;
            }
            for event in events {
                ring.events.push_back(Arc::new(event));
                if ring.events.len() > self.config.buffer_events {
                    // Every event above the one just evicted is still held, so its LSN is exactly
                    // the oldest position the buffer can still answer.
                    if let Some(gone) = ring.events.pop_front() {
                        ring.resume_floor = ring.resume_floor.max(gone.lsn);
                    }
                }
            }
            ring.position = ring.position.max(through);
            ring.position
        };
        self.published.fetch_add(count, Ordering::Relaxed);
        self.wake.send_replace(position);
    }

    /// The feed did not record what happened up to `through`. Raises the floor so a resume from
    /// below it is refused rather than answered with a stream missing those changes.
    pub fn note_gap(&self, through: u64) {
        let position = {
            let mut ring = self.ring.lock().unwrap();
            if ring.closed {
                return;
            }
            ring.resume_floor = ring.resume_floor.max(through);
            ring.position = ring.position.max(through);
            // Everything still buffered is below the gap, and no resume may name a position there.
            ring.events.clear();
            ring.position
        };
        self.wake.send_replace(position);
    }

    /// The collection's directory was replaced under this handle, so no local position continues
    /// into what replaced it. Subscribers are ended rather than left waiting on a dead feed.
    pub fn close(&self) {
        let position = {
            let mut ring = self.ring.lock().unwrap();
            ring.closed = true;
            ring.events.clear();
            ring.position
        };
        self.wake.send_replace(position);
    }

    /// `applied` is the collection's committed watermark. Catching a quiet feed up to it here
    /// rather than on the write path is what keeps a feed nobody watches free.
    pub fn subscribe(self: &Arc<Self>, after: Option<u64>, applied: u64) -> Result<Subscription, SubscribeError> {
        let mut ring = self.ring.lock().unwrap();
        if ring.closed {
            return Err(SubscribeError::Closed);
        }
        if ring.subscribers >= self.config.max_subscribers {
            return Err(SubscribeError::TooMany(self.config.max_subscribers));
        }
        // Only while genuinely quiet: a feed with a subscriber, or inside its retention window, is
        // publishing, and its position trails `applied` for as long as one batch is in flight.
        let quiet = ring.subscribers == 0
            && self.pins.load(Ordering::Relaxed) == 0
            && !ring.idle_since.is_some_and(|t| t.elapsed() < self.retention());
        if quiet && applied > ring.position {
            ring.resume_floor = ring.resume_floor.max(applied);
            ring.position = applied;
            ring.events.clear();
        }
        let (cursor, strict) = match after {
            Some(a) if a > ring.position => return Err(SubscribeError::Ahead(ring.position)),
            Some(a) if a < ring.resume_floor => {
                self.overruns.fetch_add(1, Ordering::Relaxed);
                return Err(SubscribeError::Overrun(ring.resume_floor));
            },
            Some(a) => (a, true),
            None => (ring.position, false),
        };
        ring.subscribers += 1;
        ring.idle_since = None;
        self.live.store(ring.subscribers, Ordering::Relaxed);

        Ok(Subscription {
            feed: self.clone(),
            wake: self.wake.subscribe(),
            cursor,
            strict,
            delivered: false,
        })
    }

    /// Activates a quiet feed at the observed collection position and holds it for a consumer.
    pub fn pin_from(self: &Arc<Self>, applied: u64) -> FeedPin {
        let advanced = {
            let mut ring = self.ring.lock().unwrap();
            let quiet = ring.subscribers == 0
                && self.pins.load(Ordering::Relaxed) == 0
                && !ring.idle_since.is_some_and(|t| t.elapsed() < self.retention());
            self.pins.fetch_add(1, Ordering::Relaxed);
            if quiet && applied > ring.position {
                ring.resume_floor = ring.resume_floor.max(applied);
                ring.position = applied;
                ring.events.clear();
                Some(ring.position)
            } else {
                None
            }
        };
        if let Some(position) = advanced {
            self.wake.send_replace(position);
        }
        FeedPin { feed: self.clone() }
    }

    fn detach(&self) {
        let mut ring = self.ring.lock().unwrap();
        ring.subscribers = ring.subscribers.saturating_sub(1);
        if ring.subscribers == 0 {
            ring.idle_since = Some(Instant::now());
        }
        self.live.store(ring.subscribers, Ordering::Relaxed);
    }

    fn read_from(&self, cursor: u64, strict: bool) -> Read {
        let ring = self.ring.lock().unwrap();
        if cursor < ring.resume_floor {
            return if strict { Read::Overrun(ring.resume_floor) } else { Read::Skipped(ring.resume_floor) };
        }
        let start = ring.events.partition_point(|e| e.lsn <= cursor);
        let batch: Vec<Arc<ChangeEvent>> = ring.events.iter().skip(start).take(READ_BATCH).cloned().collect();
        match (batch.is_empty(), ring.closed) {
            (false, _) => Read::Events(batch),
            (true, true) => Read::Closed,
            (true, false) => Read::Empty,
        }
    }

    pub fn stats(&self) -> FeedStats {
        let ring = self.ring.lock().unwrap();
        FeedStats {
            position: ring.position,
            resume_floor: ring.resume_floor,
            subscribers: ring.subscribers,
            buffered: ring.events.len(),
            published: self.published.load(Ordering::Relaxed),
            overruns: self.overruns.load(Ordering::Relaxed),
        }
    }
}

pub struct FeedPin {
    feed: Arc<Changefeed>,
}

impl Drop for FeedPin {
    fn drop(&mut self) {
        let _ = self.feed.pins.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
            |n| Some(n.saturating_sub(1)));
    }
}

pub struct FeedStats {
    pub position: u64,
    pub resume_floor: u64,
    pub subscribers: usize,
    pub buffered: usize,
    pub published: u64,
    pub overruns: u64,
}

pub struct Subscription {
    feed: Arc<Changefeed>,
    wake: watch::Receiver<u64>,
    cursor: u64,
    /// The client named a resume position, so a floor above it is a refusal rather than a start.
    strict: bool,
    /// Whether it has been handed an event yet. Until it has, "from now on" is satisfiable at
    /// whatever position the feed reached, so a floor that moved under it is not a loss.
    delivered: bool,
}

impl Subscription {
    pub fn position(&self) -> u64 {
        self.cursor
    }

    pub async fn next_batch(&mut self) -> Result<Vec<Arc<ChangeEvent>>, FeedEnd> {
        loop {
            match self.feed.read_from(self.cursor, self.strict || self.delivered) {
                Read::Events(batch) => {
                    self.delivered = true;
                    self.cursor = batch.last().map_or(self.cursor, |e| e.lsn);
                    return Ok(batch);
                },
                // Only reachable on the first read, which is the connect race against a feed that
                // was skipping commits: "from now on" means from wherever the feed actually is.
                Read::Skipped(floor) => self.cursor = floor,
                Read::Overrun(floor) => {
                    self.feed.overruns.fetch_add(1, Ordering::Relaxed);
                    return Err(FeedEnd::Overrun(floor));
                },
                Read::Closed => return Err(FeedEnd::Closed),
                Read::Empty => {
                    if self.wake.changed().await.is_err() {
                        return Err(FeedEnd::Closed);
                    }
                },
            }
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.feed.detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(buffer_events: usize) -> Arc<Changefeed> {
        Arc::new(Changefeed::new(
            ChangefeedConfig { buffer_events, idle_retention_ms: 30_000, max_subscribers: 4 }, 0))
    }

    fn put(lsn: u64, key: &str) -> ChangeEvent {
        ChangeEvent {
            lsn,
            op: ChangeOp::Insert,
            key: key.to_string(),
            value: Some(serde_json::json!({"v": lsn})),
        }
    }

    fn lsns(batch: &[Arc<ChangeEvent>]) -> Vec<u64> {
        batch.iter().map(|e| e.lsn).collect()
    }

    #[tokio::test]
    async fn a_subscriber_reads_what_was_published_after_it_attached() {
        let feed = feed(16);
        let mut sub = feed.subscribe(None, 0).unwrap();

        feed.publish(vec![put(1, "a"), put(2, "b")], 2);

        assert_eq!(lsns(&sub.next_batch().await.unwrap()), vec![1, 2]);
        assert_eq!(sub.position(), 2);
    }

    /// The whole point of a resume position: a reconnect inside the buffer sees every event it
    /// missed, in order, and not the one it already had.
    #[tokio::test]
    async fn a_resume_replays_from_the_named_position() {
        let feed = feed(16);
        let _live = feed.subscribe(None, 0).unwrap();
        feed.publish(vec![put(1, "a"), put(2, "b"), put(3, "c")], 3);

        let mut resumed = feed.subscribe(Some(1), 0).unwrap();
        assert_eq!(lsns(&resumed.next_batch().await.unwrap()), vec![2, 3]);
    }

    #[test]
    fn an_unreachable_position_is_refused_rather_than_answered() {
        let feed = feed(2);
        let _live = feed.subscribe(None, 0).unwrap();
        feed.publish(vec![put(1, "a"), put(2, "b"), put(3, "c")], 3);

        // The buffer evicted 1, so 1 is the newest position it can still resume from exactly.
        assert_eq!(feed.subscribe(Some(0), 0).err(), Some(SubscribeError::Overrun(1)));
        assert!(feed.subscribe(Some(1), 0).is_ok());
        assert_eq!(feed.subscribe(Some(9), 0).err(), Some(SubscribeError::Ahead(3)),
            "a position above this log came from another one and cannot be honoured");
    }

    /// A subscriber that fell behind the ring is told so rather than handed a feed with a hole.
    #[tokio::test]
    async fn a_subscriber_that_falls_behind_the_ring_is_ended() {
        let feed = feed(2);
        let mut sub = feed.subscribe(None, 0).unwrap();
        feed.publish(vec![put(1, "a")], 1);
        assert_eq!(lsns(&sub.next_batch().await.unwrap()), vec![1]);

        feed.publish(vec![put(2, "b"), put(3, "c"), put(4, "d")], 4);
        assert_eq!(sub.next_batch().await.err(), Some(FeedEnd::Overrun(2)));
    }

    /// A skipped commit is not a silent one: the floor moves, so a later resume from under it is
    /// refused instead of resuming into a gap.
    #[test]
    fn a_gap_lifts_the_floor_past_what_was_never_recorded() {
        let feed = feed(16);
        let _live = feed.subscribe(None, 0).unwrap();
        feed.publish(vec![put(1, "a")], 1);
        feed.note_gap(7);

        assert_eq!(feed.subscribe(Some(1), 0).err(), Some(SubscribeError::Overrun(7)));
        assert!(feed.subscribe(Some(7), 0).is_ok(), "the gap's own position is resumable");
    }

    #[tokio::test]
    async fn closing_ends_every_subscriber() {
        let feed = feed(16);
        let mut sub = feed.subscribe(None, 0).unwrap();
        feed.close();

        assert_eq!(sub.next_batch().await.err(), Some(FeedEnd::Closed));
        assert_eq!(feed.subscribe(None, 0).err(), Some(SubscribeError::Closed));
    }

    #[test]
    fn the_feed_is_inactive_until_someone_subscribes_and_again_once_retention_closes() {
        let feed = Arc::new(Changefeed::new(
            ChangefeedConfig { buffer_events: 8, idle_retention_ms: 0, max_subscribers: 4 }, 0));
        assert!(!feed.active(), "a feed nobody has ever watched must cost the write path nothing");

        let sub = feed.subscribe(None, 0).unwrap();
        assert!(feed.active());
        drop(sub);
        assert!(!feed.active(), "a zero retention window closes as the last subscriber goes");
    }

    #[test]
    fn the_subscriber_ceiling_refuses_rather_than_queues() {
        let feed = Arc::new(Changefeed::new(
            ChangefeedConfig { buffer_events: 8, idle_retention_ms: 30_000, max_subscribers: 2 }, 0));
        let _a = feed.subscribe(None, 0).unwrap();
        let _b = feed.subscribe(None, 0).unwrap();
        assert_eq!(feed.subscribe(None, 0).err(), Some(SubscribeError::TooMany(2)));
    }

    /// A feed opens at the collection's committed position, so `after=0` on a collection with
    /// history is an overrun rather than a stream that silently begins in the middle.
    #[test]
    fn a_feed_opened_over_existing_history_refuses_a_position_from_before_it() {
        let feed = Arc::new(Changefeed::new(ChangefeedConfig::default(), 42));
        assert_eq!(feed.subscribe(Some(0), 0).err(), Some(SubscribeError::Overrun(42)));
        assert!(feed.subscribe(None, 0).is_ok());
    }

    /// A webhook sender is not a connection, so nothing about it keeps the feed recording on its
    /// own. Without the pin, a change written between a restart and the sender attaching is never
    /// built, and there is no position that recovers it.
    #[tokio::test]
    async fn a_pin_keeps_the_feed_recording_for_a_consumer_that_has_not_attached() {
        let feed = Arc::new(Changefeed::new(
            ChangefeedConfig { buffer_events: 8, idle_retention_ms: 0, max_subscribers: 4 }, 0));
        assert!(!feed.active());

        let pin = feed.pin_from(0);
        assert!(feed.active(), "a pinned feed records for a consumer that is not there yet");
        feed.publish(vec![put(1, "a")], 1);

        let mut sub = feed.subscribe(Some(0), 1).unwrap();
        assert_eq!(lsns(&sub.next_batch().await.unwrap()), vec![1],
            "the pin is what makes the change still there when the consumer arrives");

        drop(sub);
        drop(pin);
        assert!(!feed.active(), "an unpinned feed with no subscriber costs the write path nothing");
    }

    /// The connect race: a feed that was skipping commits moves its floor under a subscriber that
    /// named no position. That one wanted "from now on", so it starts at the floor.
    #[tokio::test]
    async fn a_subscriber_with_no_position_starts_wherever_the_feed_is() {
        let feed = feed(16);
        let mut sub = feed.subscribe(None, 0).unwrap();
        feed.note_gap(5);
        feed.publish(vec![put(6, "a")], 6);

        assert_eq!(lsns(&sub.next_batch().await.unwrap()), vec![6]);
    }
}
