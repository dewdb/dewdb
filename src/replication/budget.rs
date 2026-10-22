//! Outbound replication budget, one per peer.
//!
//! A single node-wide semaphore let one peer that never answers hold every slot, so the replicas
//! that did answer queued behind its timeouts: a group with one member down wrote at 8/s
//! (`compare_couchdb.md`, diagnostic 2). Each peer now spends only its own budget, and a peer whose
//! last request failed in transport gets one request at a time -- a probe, on a widening interval.
//! A send nobody waits on skips such a peer and leaves the frame to the cursor; a send a write
//! concern waits on waits for the probe instead, holding nothing, for as long as its write does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

// The first retry comes quickly, so a blip costs a peer little; the cap is how long a peer that is
// back can go unprobed, and so roughly how long it waits to be sent anything again.
const PROBE_BACKOFF_BASE: Duration = Duration::from_millis(100);
const PROBE_BACKOFF_CAP: Duration = Duration::from_secs(2);

fn probe_backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    PROBE_BACKOFF_BASE.saturating_mul(1u32 << shift).min(PROBE_BACKOFF_CAP)
}

/// Closes when the write a send is for has been answered, met or not. A send holding one waits on
/// a peer only that long; a send without one is fire-and-forget and never waits on a dead peer.
pub type WriteDone = tokio::sync::watch::Receiver<()>;

pub(crate) async fn answered(done: &mut WriteDone) {
    // Nothing is ever sent on it, so this resolves only when the sender is dropped.
    let _ = done.changed().await;
}

#[derive(Default)]
struct Health {
    unreachable: bool,
    /// Failed probes in a row, which is what widens the interval. Requests already in flight when
    /// the peer went away fail too, and counting them would jump straight to the cap.
    failures: u32,
    retry_at: Option<Instant>,
    probing: bool,
}

enum Admission {
    Normal,
    Probe,
    Skip,
}

struct Peer {
    slots: Arc<Semaphore>,
    health: Mutex<Health>,
    /// Woken on every settle, for the sends waiting out a probe.
    settled: Notify,
}

impl Peer {
    fn admission(&self, now: Instant) -> Admission {
        let mut h = self.health.lock().unwrap();
        if !h.unreachable {
            return Admission::Normal;
        }
        if h.probing || h.retry_at.is_some_and(|at| now < at) {
            return Admission::Skip;
        }
        h.probing = true;
        Admission::Probe
    }

    fn admits(&self, now: Instant) -> bool {
        let h = self.health.lock().unwrap();
        !h.unreachable || (!h.probing && h.retry_at.map_or(true, |at| now >= at))
    }

    /// Until something could have changed the answer -- a settle, or the probe falling due -- and
    /// `false` if `done` closed first.
    async fn wait_for_change(&self, done: &mut WriteDone) -> bool {
        let notified = self.settled.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Read after enabling, so a settle between the caller's check and here is not missed. With
        // a probe out only its settle can change anything: its due time has passed, and sleeping
        // until then would spin for as long as the probe takes.
        let due = {
            let h = self.health.lock().unwrap();
            if !h.unreachable {
                return true;
            }
            match h.retry_at {
                _ if h.probing => None,
                Some(at) if at > Instant::now() => Some(tokio::time::Instant::from_std(at)),
                _ => return true,
            }
        };
        let falls_due = async {
            match due {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = notified => true,
            _ = falls_due => true,
            _ = answered(done) => false,
        }
    }
}

pub struct PeerBudgets {
    per_peer: usize,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
}

impl PeerBudgets {
    pub fn new(per_peer: usize) -> Self {
        Self { per_peer: per_peer.max(1), peers: Mutex::new(HashMap::new()) }
    }

    fn peer(&self, url: &str) -> Arc<Peer> {
        let mut peers = self.peers.lock().unwrap();
        peers.entry(crate::util::node_key(url))
            .or_insert_with(|| Arc::new(Peer {
                slots: Arc::new(Semaphore::new(self.per_peer)),
                health: Mutex::new(Health::default()),
                settled: Notify::new(),
            }))
            .clone()
    }

    /// A slot for one request to `url`. `None` is not a failure to report: the cursor did not move,
    /// so the backlog path resends. Without `done` that is any peer unreachable and not due a probe;
    /// with it, only a write answered before the peer could be sent anything.
    pub async fn acquire(&self, url: &str, mut done: Option<&mut WriteDone>) -> Option<PeerSlot> {
        let peer = self.peer(url);
        loop {
            match peer.admission(Instant::now()) {
                Admission::Normal => {},
                // Outside the slots: a peer that went dark with requests in flight holds all of
                // them until they time out, and the probe that finds it back must not queue there.
                Admission::Probe => return Some(PeerSlot { peer, permit: None, probe: true }),
                Admission::Skip => match done.as_deref_mut() {
                    Some(d) => {
                        if peer.wait_for_change(d).await {
                            continue;
                        }
                        return None;
                    },
                    None => return None,
                },
            }
            let permit = match done.as_deref_mut() {
                Some(d) => tokio::select! {
                    biased;
                    permit = peer.slots.clone().acquire_owned() => permit.ok()?,
                    _ = answered(d) => return None,
                },
                None => peer.slots.clone().acquire_owned().await.ok()?,
            };
            // Re-asked, because the peer may have gone dark while this waited, and a queue that
            // formed behind its timeouts should drain at once rather than one timeout at a time.
            match peer.admission(Instant::now()) {
                Admission::Normal => return Some(PeerSlot { peer, permit: Some(permit), probe: false }),
                Admission::Probe => return Some(PeerSlot { peer, permit: Some(permit), probe: true }),
                Admission::Skip if done.is_some() => continue,
                Admission::Skip => return None,
            }
        }
    }

    /// Whether a send to `url` would be attempted now. For callers with work to do before the send,
    /// such as a repair reading the backlog, so that a peer known to be down costs them nothing.
    pub fn admits(&self, url: &str) -> bool {
        self.peer(url).admits(Instant::now())
    }

    /// Until a send to `url` would be attempted, or `done` closes; `false` is the latter.
    pub async fn wait_admitted(&self, url: &str, done: &mut WriteDone) -> bool {
        let peer = self.peer(url);
        loop {
            if peer.admits(Instant::now()) {
                return true;
            }
            if !peer.wait_for_change(done).await {
                return false;
            }
        }
    }

    /// `(peer, free slots, reachable)` for every peer this node has sent to.
    pub fn snapshot(&self) -> Vec<(String, usize, bool)> {
        let peers = self.peers.lock().unwrap();
        let mut out: Vec<_> = peers.iter()
            .map(|(k, p)| (k.clone(), p.slots.available_permits(), !p.health.lock().unwrap().unreachable))
            .collect();
        out.sort();
        out
    }

    pub fn per_peer(&self) -> usize {
        self.per_peer
    }
}

/// Held for the duration of one request. `settle` records whether the peer answered at all: any
/// HTTP status is an answer, and only a transport error says the peer is not there.
pub struct PeerSlot {
    peer: Arc<Peer>,
    permit: Option<OwnedSemaphorePermit>,
    probe: bool,
}

impl PeerSlot {
    pub fn settle(mut self, answered: bool) {
        let mut h = self.peer.health.lock().unwrap();
        if answered {
            *h = Health::default();
        } else if self.probe || !h.unreachable {
            h.unreachable = true;
            h.failures = h.failures.saturating_add(1);
            h.retry_at = Some(Instant::now() + probe_backoff(h.failures));
        }
        if self.probe {
            h.probing = false;
            self.probe = false;
        }
        drop(h);
        self.permit.take();
        self.peer.settled.notify_waiters();
    }
}

impl Drop for PeerSlot {
    // A probe dropped unsettled -- its task cancelled mid-request -- must not leave the peer marked
    // as being probed, or nothing would ever probe it again.
    fn drop(&mut self) {
        if self.probe {
            self.peer.health.lock().unwrap().probing = false;
            self.peer.settled.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEER: &str = "http://127.0.0.1:59101";

    #[tokio::test]
    async fn one_peer_exhausting_its_budget_leaves_another_peers_untouched() {
        let budgets = PeerBudgets::new(2);
        let _a = budgets.acquire(PEER, None).await.unwrap();
        let _b = budgets.acquire(PEER, None).await.unwrap();

        let other = tokio::time::timeout(Duration::from_millis(200),
            budgets.acquire("http://127.0.0.1:59102", None)).await;
        assert!(matches!(other, Ok(Some(_))),
            "the node-wide semaphore made this wait on the first peer's requests: the defect itself");

        let same = tokio::time::timeout(Duration::from_millis(100), budgets.acquire(PEER, None)).await;
        assert!(same.is_err(), "a peer's own budget still bounds it");
    }

    #[tokio::test]
    async fn an_unreachable_peer_gets_one_probe_at_a_time_and_nothing_else() {
        let budgets = PeerBudgets::new(4);
        budgets.acquire(PEER, None).await.unwrap().settle(false);

        assert!(budgets.acquire(PEER, None).await.is_none(), "inside the backoff nothing is sent");
        assert!(!budgets.admits(PEER));

        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(20)).await;
        assert!(budgets.admits(PEER), "due a probe once the backoff has passed");
        let probe = budgets.acquire(PEER, None).await.expect("the probe");
        assert!(budgets.acquire(PEER, None).await.is_none(), "a second request while the probe is out");
        assert!(!budgets.admits(PEER));

        probe.settle(false);
        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(20)).await;
        assert!(!budgets.admits(PEER), "a failed probe widens the interval");
        tokio::time::sleep(probe_backoff(2)).await;
        budgets.acquire(PEER, None).await.expect("the next probe").settle(true);

        let all: Vec<_> = futures::future::join_all((0..4).map(|_| budgets.acquire(PEER, None))).await;
        assert!(all.iter().all(Option::is_some), "an answered probe restores the whole budget");
    }

    #[tokio::test]
    async fn requests_already_in_flight_do_not_widen_the_backoff() {
        let budgets = PeerBudgets::new(8);
        let in_flight: Vec<_> = futures::future::join_all((0..8).map(|_| budgets.acquire(PEER, None))).await;
        for slot in in_flight {
            slot.unwrap().settle(false);
        }
        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(20)).await;
        assert!(budgets.admits(PEER),
            "eight requests that were out when the peer went away are one failure, not eight");
    }

    #[tokio::test]
    async fn a_dropped_probe_does_not_wedge_the_peer() {
        let budgets = PeerBudgets::new(1);
        budgets.acquire(PEER, None).await.unwrap().settle(false);
        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(20)).await;
        drop(budgets.acquire(PEER, None).await.expect("the probe"));
        assert!(budgets.admits(PEER), "a cancelled probe has to free the probe for the next one");
    }

    #[tokio::test]
    async fn a_queue_behind_a_peer_that_went_dark_drains_at_once() {
        let budgets = Arc::new(PeerBudgets::new(1));
        let held = budgets.acquire(PEER, None).await.unwrap();
        let waiter = tokio::spawn({
            let budgets = budgets.clone();
            async move { budgets.acquire(PEER, None).await.is_some() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        held.settle(false);
        assert!(!waiter.await.unwrap(),
            "a send that queued while the peer was reachable must not go out once it is not");
    }

    /// The other side of skipping: a write concern that a returning peer could still meet has to
    /// be sent there, not answered early because the peer was down when the write began.
    #[tokio::test]
    async fn a_send_a_write_waits_on_rides_out_the_probe() {
        let budgets = Arc::new(PeerBudgets::new(2));
        budgets.acquire(PEER, None).await.unwrap().settle(false);
        let (write, done) = tokio::sync::watch::channel(());

        let waiter = tokio::spawn({
            let (budgets, mut done) = (budgets.clone(), done.clone());
            async move { budgets.acquire(PEER, Some(&mut done)).await.map(|s| s.settle(true)).is_some() }
        });
        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(50)).await;
        assert!(waiter.await.unwrap(), "the write was still waiting when the probe fell due");
        assert!(budgets.admits(PEER));

        budgets.acquire(PEER, None).await.unwrap().settle(false);
        let gave_up = tokio::spawn({
            let (budgets, mut done) = (budgets.clone(), done.clone());
            async move { budgets.acquire(PEER, Some(&mut done)).await.is_none() }
        });
        drop(write);
        let gave_up = tokio::time::timeout(Duration::from_millis(50), gave_up).await;
        assert!(matches!(gave_up, Ok(Ok(true))), "an answered write stops waiting at once");
    }

    /// A probe out past its due time used to send the waiters round a loop that slept until a
    /// moment already gone, for as long as the probe took.
    #[tokio::test]
    async fn a_send_waiting_on_a_probe_in_flight_sleeps_until_it_settles() {
        let budgets = Arc::new(PeerBudgets::new(1));
        budgets.acquire(PEER, None).await.unwrap().settle(false);
        tokio::time::sleep(probe_backoff(1) + Duration::from_millis(20)).await;
        let probe = budgets.acquire(PEER, None).await.expect("the probe");
        let (_write, done) = tokio::sync::watch::channel(());

        let peer = budgets.peer(PEER);
        let mut waiting = done.clone();
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = polls.clone();
        let waiter = tokio::spawn(async move {
            loop {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if !peer.wait_for_change(&mut waiting).await || peer.admits(Instant::now()) {
                    return;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(polls.load(std::sync::atomic::Ordering::SeqCst) <= 2,
            "woke {} times with nothing changed", polls.load(std::sync::atomic::Ordering::SeqCst));
        probe.settle(true);
        tokio::time::timeout(Duration::from_millis(200), waiter).await
            .expect("the settle has to wake it").unwrap();
    }

    #[test]
    fn the_probe_interval_doubles_to_its_cap() {
        assert_eq!(probe_backoff(1), PROBE_BACKOFF_BASE);
        assert_eq!(probe_backoff(2), PROBE_BACKOFF_BASE * 2);
        assert_eq!(probe_backoff(40), PROBE_BACKOFF_CAP);
    }
}
