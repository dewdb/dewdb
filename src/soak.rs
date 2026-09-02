//! Long-running durability and recovery scenarios. Not part of the suite; each is `#[ignore]`d and
//! run explicitly: `cargo test --release -- --ignored --nocapture soak`
//!
//! Each has a fixed seed so a failure replays. `DEWDB_SOAK_SEED=0x...` overrides it, which is how a
//! run that found nothing is asked for a different schedule.
//!
//! All of them assert one thing: a write the cluster answered 200 or 201 is still readable, with a
//! value no older than the one acknowledged, after whatever happened in between. A 202 is staged
//! and promises nothing, so it is counted and never asserted on. Anything the client never saw an
//! answer for is not evidence either way and is recorded as attempted only.
//!
//! `TestNode::kill` drops the process's state without flushing anything, so it is a process crash.
//! It is not a machine crash: writes that reached the OS but were never fsynced still survive,
//! because the page cache does. `truncated_tail` is what covers that half.

use crate::test_support::{cleanup, next_test_port, temp_root, three_node_cluster, TestNode};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// Seeded so a failure can be replayed, and overridable with `DEWDB_SOAK_SEED` so a run that found
/// nothing can be told to try a different schedule without an edit.
struct Rng(u64);

impl Rng {
    fn seeded(scenario: &str, default: u64) -> Self {
        let seed = std::env::var("DEWDB_SOAK_SEED").ok()
            .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .unwrap_or(default);
        println!("[{}] seed 0x{:X}", scenario, seed);
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// What the client was told, which is the only thing a durability claim can rest on. Per key: the
/// highest value acknowledged, and the highest attempted. A later attempt that was never
/// acknowledged is why the check is a range and not an equality -- it may or may not have landed,
/// and both outcomes are correct.
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<String, (Option<i64>, i64)>,
    acked: usize,
    staged: usize,
    failed: usize,
}

impl Ledger {
    fn record(&mut self, key: &str, value: i64, status: Option<u16>) {
        let slot = self.entries.entry(key.to_string()).or_insert((None, value));
        slot.1 = slot.1.max(value);
        match status {
            Some(200) | Some(201) => {
                slot.0 = Some(slot.0.map_or(value, |v: i64| v.max(value)));
                self.acked += 1;
            },
            Some(202) => self.staged += 1,
            _ => self.failed += 1,
        }
    }

    fn acked_keys(&self) -> usize {
        self.entries.values().filter(|(a, _)| a.is_some()).count()
    }

    /// Every complaint, not the first: one lost key and two thousand say different things about
    /// what broke.
    fn check(&self, actual: &BTreeMap<String, i64>) -> Vec<String> {
        let mut problems = Vec::new();
        for (key, (acked, attempted)) in &self.entries {
            let acked = match acked {
                Some(v) => *v,
                None => continue,
            };
            match actual.get(key) {
                None => problems.push(format!("{}: acknowledged at {} and gone", key, acked)),
                Some(&got) if got < acked => problems
                    .push(format!("{}: acknowledged at {} and read back {}", key, acked, got)),
                Some(&got) if got > *attempted => problems
                    .push(format!("{}: read back {}, higher than anything sent ({})", key, got, attempted)),
                Some(_) => {},
            }
        }
        for key in actual.keys() {
            if !self.entries.contains_key(key) {
                problems.push(format!("{}: present and never written", key));
            }
        }
        problems
    }
}

fn report(scenario: &str, ledger: &Ledger, actual: &BTreeMap<String, i64>) {
    println!(
        "[{}] {} acked / {} staged / {} failed, {} acked keys, {} present",
        scenario, ledger.acked, ledger.staged, ledger.failed, ledger.acked_keys(), actual.len());
}

fn assert_durable(scenario: &str, ledger: &Ledger, actual: &BTreeMap<String, i64>) {
    let problems = ledger.check(actual);
    if problems.is_empty() {
        return;
    }
    let shown: Vec<&String> = problems.iter().take(10).collect();
    panic!("[{}] {} durability violations, first {}: {:#?}",
        scenario, problems.len(), shown.len(), shown);
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap()
}

async fn write_one(c: &reqwest::Client, base: &str, key: &str, value: i64, query: &str) -> Option<u16> {
    let url = format!("{}/collections/t/docs/{}{}", base, key, query);
    let body = serde_json::json!({"value": {"k": key, "v": value}});
    c.put(&url).json(&body).send().await.ok().map(|r| r.status().as_u16())
}

/// The whole visible state in one request, which also catches a document that is present and was
/// never written -- a per-key probe cannot.
async fn contents(c: &reqwest::Client, base: &str) -> Option<BTreeMap<String, i64>> {
    let url = format!("{}/collections/t/docs", base);
    let docs = c.get(&url).send().await.ok()?.json::<Vec<serde_json::Value>>().await.ok()?;
    Some(docs.iter()
        .filter_map(|d| Some((d.get("k")?.as_str()?.to_string(), d.get("v")?.as_i64()?)))
        .collect())
}

/// Writes to whichever node answers, because the leader moves under every scenario here.
async fn write_somewhere(
    c: &reqwest::Client,
    bases: &[String],
    key: &str,
    value: i64,
    query: &str,
) -> Option<u16> {
    let mut last = None;
    for base in bases {
        match write_one(c, base, key, value, query).await {
            Some(s) if s < 400 => return Some(s),
            other => last = other.or(last),
        }
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn soak_a_single_node_survives_repeated_crashes() {
    const CYCLES: usize = 30;
    const BATCH: i64 = 60;
    const IN_FLIGHT: i64 = 12;

    let mut rng = Rng::seeded("crash-loop", 0x5EED_0051);

    let root = temp_root();
    let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
    node.start();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let c = client();
    let mut ledger = Ledger::default();
    let mut counter: i64 = 0;

    for cycle in 0..CYCLES {
        for _ in 0..BATCH {
            counter += 1;
            // A quarter of the writes land on a key already written, so recovery is asked to keep
            // an ordering and not just a set.
            let key = if counter % 4 == 0 && counter > 8 {
                format!("k{:06}", counter - 8)
            } else {
                format!("k{:06}", counter)
            };
            let status = write_one(&c, &node.url(), &key, counter, "").await;
            ledger.record(&key, counter, status);
        }

        // Left unawaited and killed underneath: a write whose answer the client never saw is
        // exactly the case the ledger has to stay silent about, and the case a crash produces.
        let mut inflight = Vec::new();
        for _ in 0..IN_FLIGHT {
            counter += 1;
            let key = format!("k{:06}", counter);
            let (client, base, sent) = (c.clone(), node.url(), key.clone());
            inflight.push((key, counter, tokio::spawn(async move {
                write_one(&client, &base, &sent, counter, "").await
            })));
        }
        tokio::time::sleep(Duration::from_millis(rng.below(25))).await;

        node.kill();
        for (key, value, handle) in inflight {
            ledger.record(&key, value, handle.await.ok().flatten());
        }
        node.start();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let actual = contents(&c, &node.url()).await.expect("node unreadable after restart");
        if cycle + 1 == CYCLES {
            report("crash-loop", &ledger, &actual);
        }
        assert_durable("crash-loop", &ledger, &actual);
    }

    node.kill();
    cleanup(&root).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn soak_a_cluster_under_churn_loses_no_acknowledged_write() {
    const ROUNDS: usize = 24;
    const PER_ROUND: i64 = 40;
    const CONCURRENCY: i64 = 8;

    let mut rng = Rng::seeded("churn", 0x5EED_0052);

    let root = temp_root();
    let (mut n1, mut n2, mut n3) = three_node_cluster(&root).await;
    let bases: Vec<String> = vec![n1.url(), n2.url(), n3.url()];
    let c = client();
    let mut ledger = Ledger::default();
    let mut counter: i64 = 0;

    for round in 0..ROUNDS {
        // Concurrent, in batches: writes serialised one at a time never overlap a failover, which
        // is where an acknowledged write would go missing if one could.
        for chunk in 0..PER_ROUND / CONCURRENCY {
            let _ = chunk;
            let mut batch = Vec::new();
            for _ in 0..CONCURRENCY {
                counter += 1;
                let key = format!("k{:06}", counter);
                let (client, targets, sent) = (c.clone(), bases.clone(), key.clone());
                batch.push((key, counter, tokio::spawn(async move {
                    write_somewhere(&client, &targets, &sent, counter, "?w=majority&wtimeout=3000").await
                })));
            }
            for (key, value, handle) in batch {
                ledger.record(&key, value, handle.await.ok().flatten());
            }
        }

        // Weighted at the leader: killing a follower costs the group nothing to recover from, and
        // it is the handover that an acknowledged write has to survive.
        let leader = [n1.is_leader(), n2.is_leader(), n3.is_leader()].iter().position(|l| *l);
        let victim = match leader {
            Some(i) if rng.below(3) > 0 => i as u64,
            _ => rng.below(3),
        };
        // One at a time, so a majority always survives and every acknowledged write is one a
        // quorum held. Two would make loss legal and the assertion meaningless.
        let node: &mut TestNode = match victim {
            0 => &mut n1,
            1 => &mut n2,
            _ => &mut n3,
        };
        node.kill();
        if round % 5 == 4 {
            // Compacted while it is down, so catching up from the WAL is no longer possible and
            // the returning node has to take a snapshot instead. Short outages never reach that
            // path, and it is the one recovery step with the most moving parts.
            for base in &bases {
                let _ = c.post(format!("{}/collections/t/compact", base)).send().await;
            }
            tokio::time::sleep(Duration::from_millis(1500)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(400 + rng.below(1200))).await;
        }
        node.start();
        tokio::time::sleep(Duration::from_millis(500)).await;

        if round % 6 == 0 {
            println!("[churn] round {}: {} acked, {} staged, {} failed",
                round, ledger.acked, ledger.staged, ledger.failed);
        }
    }

    // Waited for rather than sampled: a replica still catching up is not a durability failure
    // until it stops catching up.
    let mut converged = false;
    for _ in 0..60 {
        let mut all = Vec::new();
        for base in &bases {
            match contents(&c, base).await {
                Some(m) => all.push(m),
                None => break,
            }
        }
        if all.len() == bases.len() && all.iter().all(|m| m == &all[0]) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let leader_view = contents(&c, &bases[0]).await.unwrap_or_default();
    report("churn", &ledger, &leader_view);
    assert_durable("churn", &ledger, &leader_view);

    for (i, base) in bases.iter().enumerate() {
        let view = contents(&c, base).await.expect("node unreadable at the end");
        assert_durable(&format!("churn/node{}", i + 1), &ledger, &view);
    }
    assert!(converged, "[churn] the three nodes never agreed on the same contents");

    n1.kill();
    n2.kill();
    n3.kill();
    cleanup(&root).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn soak_compaction_and_crashes_interleave_without_losing_writes() {
    const CYCLES: usize = 20;
    const BATCH: i64 = 80;

    let mut rng = Rng::seeded("compaction", 0x5EED_0053);

    let root = temp_root();
    let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
    node.start();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let c = client();
    let mut ledger = Ledger::default();
    let mut counter: i64 = 0;

    for cycle in 0..CYCLES {
        for _ in 0..BATCH {
            counter += 1;
            // Overwrites are what give compaction dead bytes to reclaim; a set of unique keys
            // compacts to itself and tests nothing.
            let key = format!("k{:05}", counter % 120);
            let status = write_one(&c, &node.url(), &key, counter, "").await;
            ledger.record(&key, counter, status);
        }

        let compact = format!("{}/collections/t/compact", node.url());
        let _ = c.post(&compact).send().await;

        // Half the cycles crash on top of the compaction rather than after it settles.
        if rng.below(2) == 0 {
            node.kill();
            node.start();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        let actual = contents(&c, &node.url()).await.expect("node unreadable after compaction");
        if cycle + 1 == CYCLES {
            report("compaction", &ledger, &actual);
        }
        assert_durable("compaction", &ledger, &actual);
    }

    node.kill();
    node.start();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let final_view = contents(&c, &node.url()).await.expect("node unreadable after the last restart");
    assert_durable("compaction/final", &ledger, &final_view);

    node.kill();
    cleanup(&root).await;
}

/// The half `kill` cannot reach: a machine losing power drops whatever the page cache still held,
/// so recovery meets a WAL whose last frame is cut off mid-record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn soak_a_truncated_wal_tail_costs_only_the_frames_it_cuts() {
    const CYCLES: usize = 15;
    const BATCH: i64 = 40;

    let mut rng = Rng::seeded("truncation", 0x5EED_0054);

    let root = temp_root();
    let mut node = TestNode::new("solo", next_test_port(), &root, "primary");
    node.start();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let c = client();
    let mut ledger = Ledger::default();
    let mut counter: i64 = 0;

    for cycle in 0..CYCLES {
        let mut written = Vec::new();
        for _ in 0..BATCH {
            counter += 1;
            let key = format!("k{:06}", counter);
            let status = write_one(&c, &node.url(), &key, counter, "").await;
            written.push((key, counter, status));
        }

        let before = contents(&c, &node.url()).await.expect("node unreadable before the cut");
        node.kill();
        // A third of the cycles are a clean crash, so the ledger has acknowledged writes to hold
        // the run to and not only a shrinking prefix.
        let lost = match rng.below(3) {
            0 => 0,
            _ => truncate_active_wal(&node.data_dir, 1 + rng.below(400) as u64),
        };

        // Only where nothing was cut is the client's answer still a promise: a frame the disk no
        // longer holds was never durable, whatever it was told before the power went.
        for (key, value, status) in written {
            ledger.record(&key, value, if lost == 0 { status } else { None });
        }

        node.start();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let actual = contents(&c, &node.url()).await.expect("node unreadable after truncation");
        assert_durable("truncation", &ledger, &actual);
        assert_lost_only_the_tail(cycle, &before, &actual);
    }

    let actual = contents(&c, &node.url()).await.unwrap_or_default();
    report("truncation", &ledger, &actual);

    counter += 1;
    let key = format!("k{:06}", counter);
    let status = write_one(&c, &node.url(), &key, counter, "").await;
    assert!(matches!(status, Some(200) | Some(201)),
        "writes did not resume after the last truncation: {:?}", status);

    node.kill();
    cleanup(&root).await;
}

/// What a cut tail may cost: entries the WAL no longer ends with. Keys are written in value order
/// here, so the ones a truncation can take are those with the highest values -- a surviving entry
/// above a lost one means recovery kept a frame it should not have, or dropped one it should have.
fn assert_lost_only_the_tail(
    cycle: usize,
    before: &BTreeMap<String, i64>,
    after: &BTreeMap<String, i64>,
) {
    let mut kept_high = 0;
    let mut lost_low = i64::MAX;
    for (key, &was) in before {
        match after.get(key) {
            None => lost_low = lost_low.min(was),
            Some(&now) => {
                assert_eq!(now, was, "[truncation] cycle {}: {} changed value under a cut tail", cycle, key);
                kept_high = kept_high.max(was);
            },
        }
    }
    assert!(lost_low == i64::MAX || kept_high < lost_low,
        "[truncation] cycle {}: kept an entry at {} written after one lost at {}, so the loss was          not a tail", cycle, kept_high, lost_low);
    for key in after.keys() {
        assert!(before.contains_key(key),
            "[truncation] cycle {}: {} appeared out of a truncated WAL", cycle, key);
    }
}

/// Cuts `bytes` off the newest WAL, returning how many it actually removed. A file already shorter
/// than that is left alone rather than emptied: an empty WAL is a different scenario.
fn truncate_active_wal(data_dir: &Path, bytes: u64) -> u64 {
    let mut newest: Option<(std::ffi::OsString, std::path::PathBuf)> = None;
    let collection = data_dir.join("t");
    let entries = match std::fs::read_dir(&collection) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let as_str = name.to_string_lossy().to_string();
        if as_str.starts_with("wal-") && as_str.ends_with(".log") {
            if newest.as_ref().is_none_or(|(seen, _)| name > *seen) {
                newest = Some((name, entry.path()));
            }
        }
    }

    let path = match newest {
        Some((_, p)) => p,
        None => return 0,
    };
    let len = match std::fs::metadata(&path) {
        Ok(m) => m.len(),
        Err(_) => return 0,
    };
    if len <= bytes {
        return 0;
    }
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - bytes).unwrap();
    file.sync_all().unwrap();
    bytes
}
