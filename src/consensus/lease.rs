//! Leader leases: what a voter promises when it polls, and the round it saves the leader.
//!
//! Commit 47 confirmed leadership per read, with a heartbeat round. The round asks whether any
//! voter could have elected someone else; a voter that has heard from a leader recently answers
//! that in advance by refusing to grant a vote, and its poll already carries the answer. A majority
//! of those promises is a window in which no election can complete, which is a lease.

use super::failover::HEARTBEAT_POLL_INTERVAL_MS;
use crate::storage::frame::Configuration;
use crate::util::same_endpoint;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

/// How far the wall clock may run ahead of the monotonic one before a promise is retired. Well above
/// NTP slew over a window this short (a few ms), well below any suspend, which is seconds at least.
const STALL_TOLERANCE: Duration = Duration::from_millis(250);

/// A voter withholds its vote while its leader contact is newer than this, and promises the leader
/// whatever is left of the window. Half of what the contact timeout has after one poll interval of
/// skew, so every voter is free to vote well before any of them times out and stands.
pub fn refusal_window(heartbeat_timeout: Duration) -> Duration {
    heartbeat_timeout.saturating_sub(Duration::from_millis(HEARTBEAT_POLL_INTERVAL_MS)) / 2
}

/// Age of the newest evidence that a leader this node accepts is alive. `None` is a node that has
/// heard from nobody, which promises nothing.
pub fn contact_age(
    last_heartbeat: Option<Instant>,
    last_replication: Option<Instant>,
) -> Option<Duration> {
    [last_heartbeat, last_replication].into_iter().flatten().max().map(|t| t.elapsed())
}

/// The other half of the lease, and sound only because it is kept: everything below rests on a voter
/// with fresh contact not voting, whatever term the candidate offers.
///
/// Boot counts the same as contact. A restart destroys the knowledge of whether this node was
/// vote-eligible a moment ago, while a leader can still be counting the promise the process before
/// it made, so a node that has just come up has to assume it owes that silence. Free at a cold
/// start: the window is below the contact timeout, so it has lapsed before any candidate asks.
pub fn withholds_vote(
    contact: Option<Duration>,
    since_boot: Duration,
    heartbeat_timeout: Duration,
) -> bool {
    let window = refusal_window(heartbeat_timeout);
    since_boot < window || contact.is_some_and(|age| age < window)
}

/// What a poller may promise: the rest of its refusal window, less a margin for the request's
/// flight. The leader starts the window when the request lands, which is after the age was read, so
/// the margin is what keeps starting it late conservative rather than short. See bugs.md M16.
pub fn promise(age: Option<Duration>, heartbeat_timeout: Duration) -> Duration {
    let window = refusal_window(heartbeat_timeout);
    match age {
        Some(age) => window.saturating_sub(age).saturating_sub(window / 4),
        None => Duration::ZERO,
    }
}

/// A promise is worth no more than our own window: nothing an honest voter with the same contact
/// timeout computes exceeds it, and a misconfigured one must not be able to hand out an unbounded
/// lease. A voter with a longer timeout loses some of its promise, which costs a round, not a read.
pub fn accept_promise(claimed: Duration, heartbeat_timeout: Duration) -> Duration {
    claimed.min(refusal_window(heartbeat_timeout))
}

struct Promise {
    until: Instant,
    at: Instant,
    at_wall: SystemTime,
}

impl Promise {
    /// A lease is a claim about real time and `Instant` is not: `CLOCK_MONOTONIC` does not advance
    /// while the host is suspended, so a resumed leader would read a deadline measured in a clock
    /// that stopped. The wall clock keeps time across that, so the two diverging retires the
    /// promise -- which costs a round. A wall clock stepped backwards proves nothing here.
    fn live(&self, now: Instant, wall_now: SystemTime) -> bool {
        if self.until <= now {
            return false;
        }
        let monotonic = now.saturating_duration_since(self.at);
        let wall = wall_now.duration_since(self.at_wall).unwrap_or(Duration::ZERO);
        wall <= monotonic + STALL_TOLERANCE
    }
}

/// Promises a leader is holding, by voter. Quorum evidence like match state, so it is dropped on
/// every leadership transition rather than reasoned about across one.
#[derive(Default)]
pub struct Leases {
    promises: HashMap<String, Promise>,
}

impl Leases {
    pub fn clear(&mut self) {
        self.promises.clear();
    }

    /// `until` extends only: a poll promising less than one still outstanding says the voter's
    /// contact aged, not that it withdrew anything. The clock pair is always the newest poll's --
    /// the request arriving is fresh evidence that both clocks are running.
    pub fn note_promise(
        &mut self,
        voter: &str,
        now: Instant,
        wall_now: SystemTime,
        promise: Duration,
    ) {
        let until = match self.promises.get(voter) {
            Some(held) => held.until.max(now + promise),
            None => now + promise,
        };
        self.promises.insert(voter.to_string(), Promise { until, at: now, at_wall: wall_now });
    }

    /// Whether a majority still owes this node silence, and so whether a read needs no round.
    /// Counting itself is sound because granting a vote steps a leader down, which the read path
    /// checks on its own. A promise from a node the configuration has dropped counts for nothing.
    pub fn held(
        &self,
        config: &Configuration,
        own_url: &str,
        now: Instant,
        wall_now: SystemTime,
    ) -> bool {
        let mut promised = vec![own_url.to_string()];
        for (voter, promise) in &self.promises {
            if promise.live(now, wall_now) && !same_endpoint(voter, own_url) {
                promised.push(voter.clone());
            }
        }
        config.has_quorum(&promised)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: &str = "http://own";
    const TIMEOUT: Duration = Duration::from_secs(10);

    fn config(others: &[&str]) -> Configuration {
        let mut voters = vec![OWN.to_string()];
        voters.extend(others.iter().map(|s| s.to_string()));
        Configuration::simple(voters)
    }

    /// Up long enough to be out of every window under test.
    fn booted() -> Duration {
        TIMEOUT * 10
    }

    #[test]
    fn a_promise_is_what_is_left_of_the_window_and_never_all_of_it() {
        let window = refusal_window(TIMEOUT);
        assert!(window < TIMEOUT, "a voter must be free to vote before it stands for election");

        let fresh = promise(Some(Duration::ZERO), TIMEOUT);
        assert!(fresh > Duration::ZERO && fresh < window,
            "the margin covers the flight the leader cannot measure, so it is never the whole window");
        assert!(promise(Some(window), TIMEOUT).is_zero(), "a voter at the edge owes nothing");
        assert!(promise(None, TIMEOUT).is_zero());
    }

    #[test]
    fn anything_a_voter_promises_it_is_still_withholding_a_vote_over() {
        for ms in [0, 100, 1000, 4000, 4500, 20_000] {
            let age = Some(Duration::from_millis(ms));
            assert!(promise(age, TIMEOUT).is_zero() || withholds_vote(age, booted(), TIMEOUT),
                "a promise the voter would not honour at age {}ms is a stale read", ms);
        }
    }

    #[test]
    fn a_promise_longer_than_our_own_window_is_taken_as_our_own_window() {
        let window = refusal_window(TIMEOUT);
        assert_eq!(accept_promise(Duration::from_secs(3600), TIMEOUT), window,
            "a voter with a broken timeout must not be able to hand out an unbounded lease");
        assert_eq!(accept_promise(window / 2, TIMEOUT), window / 2);
    }

    /// The restart hole: a leader can still be counting a promise the process before this one made,
    /// and nothing on disk says whether it did.
    #[test]
    fn a_node_that_has_just_booted_withholds_even_though_it_has_heard_from_nobody() {
        let window = refusal_window(TIMEOUT);
        assert!(withholds_vote(None, Duration::ZERO, TIMEOUT),
            "the promise a restart forgot is one a leader is entitled to still be holding");
        assert!(withholds_vote(None, window - Duration::from_millis(1), TIMEOUT));
        assert!(!withholds_vote(None, window, TIMEOUT), "and it is a window, not a state");
        assert!(window < TIMEOUT,
            "a candidate only asks after the whole contact timeout, so a cold start waits on nothing");
    }

    #[test]
    fn contact_is_the_newer_of_the_two_signals() {
        let old = Instant::now() - Duration::from_secs(5);
        let recent = Instant::now();
        assert!(contact_age(Some(old), Some(recent)).unwrap() < Duration::from_secs(1));
        assert!(contact_age(Some(recent), Some(old)).unwrap() < Duration::from_secs(1));
        assert!(contact_age(None, None).is_none());
    }

    #[test]
    fn a_majority_of_promises_is_a_lease_and_a_minority_is_not() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        let config = config(&["http://a", "http://b", "http://c", "http://d"]);

        assert!(!leases.held(&config, OWN, now, wall), "one of five is not a majority");
        leases.note_promise("http://a", now, wall, Duration::from_secs(3));
        assert!(!leases.held(&config, OWN, now, wall));
        leases.note_promise("http://b", now, wall, Duration::from_secs(3));
        assert!(leases.held(&config, OWN, now, wall), "three of five, one of them itself");

        let later = Duration::from_secs(4);
        assert!(!leases.held(&config, OWN, now + later, wall + later),
            "an expired promise is not a promise");
        leases.clear();
        assert!(!leases.held(&config, OWN, now, wall));
    }

    #[test]
    fn a_promise_from_a_node_the_configuration_dropped_counts_for_nothing() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        leases.note_promise("http://gone", now, wall, Duration::from_secs(30));
        leases.note_promise("http://a", now, wall, Duration::from_secs(30));

        // Three of four, if the dropped node counted. Two of four otherwise.
        assert!(!leases.held(&config(&["http://a", "http://b", "http://c"]), OWN, now, wall),
            "a removed member cannot go on voting, so it cannot go on promising not to either");
    }

    #[test]
    fn a_lease_over_one_half_of_a_joint_configuration_is_not_a_lease() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        let joint = Configuration::joint(
            vec![OWN.to_string(), "http://a".into(), "http://b".into()],
            vec![OWN.to_string(), "http://c".into(), "http://d".into()],
        );

        leases.note_promise("http://c", now, wall, Duration::from_secs(30));
        assert!(!leases.held(&joint, OWN, now, wall),
            "the outgoing half can still elect a leader, so it still has to promise not to");
        leases.note_promise("http://a", now, wall, Duration::from_secs(30));
        assert!(leases.held(&joint, OWN, now, wall));
    }

    #[test]
    fn a_later_poll_promising_less_does_not_shorten_the_lease() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        let config = config(&["http://a", "http://b"]);

        leases.note_promise("http://a", now, wall, Duration::from_secs(4));
        leases.note_promise("http://a", now, wall, Duration::from_secs(1));
        let later = Duration::from_secs(3);
        assert!(leases.held(&config, OWN, now + later, wall + later));
    }

    /// A suspended host, from the leader's side: the wall clock kept time across it and `Instant`
    /// did not, so `until` is a deadline in a clock that stopped.
    #[test]
    fn a_promise_whose_monotonic_clock_stalled_is_retired_rather_than_trusted() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        let config = config(&["http://a", "http://b"]);

        leases.note_promise("http://a", now, wall - Duration::from_secs(3600),
            Duration::from_secs(30));
        assert!(!leases.held(&config, OWN, now, wall),
            "an hour of wall time against no monotonic time is a stall, and the lease goes with it");

        leases.note_promise("http://a", now, wall, Duration::from_secs(30));
        assert!(leases.held(&config, OWN, now, wall), "and the same promise stands on a live clock");
    }

    #[test]
    fn a_wall_clock_stepped_backwards_does_not_retire_a_promise() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let mut leases = Leases::default();
        let config = config(&["http://a", "http://b"]);

        leases.note_promise("http://a", now, wall + Duration::from_secs(600),
            Duration::from_secs(30));
        assert!(leases.held(&config, OWN, now, wall),
            "a backwards step says nothing about the monotonic clock, which is what times the lease");
    }
}
