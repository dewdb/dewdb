//! Read/write benchmarks. Not part of the suite; each is `#[ignore]`d and run explicitly:
//! `cargo test --release -- --ignored --nocapture bench`
//!
//! Every scenario is sampled several times and reported by median. A single run on a loopback
//! cluster varies by more than most of the effects being measured, so one number proves nothing.

use crate::test_support::{
    cleanup, get_raw, put_doc_at, put_value, sharded_cluster, single_node, temp_root, three_node_cluster,
    voter_group, TestNode,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SAMPLES: usize = 5;

struct Stats {
    n: usize,
    tput: f64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn summarize(mut latencies: Vec<f64>, wall_s: f64) -> Stats {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = latencies.len();
    let pick = |q: f64| latencies[((n as f64 * q) as usize).min(n.saturating_sub(1))];
    Stats {
        n,
        tput: n as f64 / wall_s,
        avg_ms: latencies.iter().sum::<f64>() / n as f64,
        p50_ms: pick(0.50),
        p95_ms: pick(0.95),
        p99_ms: pick(0.99),
    }
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

/// Reports the median across samples for every figure, so a single slow run cannot flatter or
/// damage the result. `spread` is the min..max of throughput, which is the honest error bar.
fn report(label: &str, samples: Vec<Stats>) {
    let tputs: Vec<f64> = samples.iter().map(|s| s.tput).collect();
    let lo = tputs.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = tputs.iter().cloned().fold(0.0, f64::max);
    println!(
        "{:<26} n={:<6} {:>7.0} ops/s [{:.0}-{:.0}]  avg={:>6.2}  p50={:>6.2}  p95={:>7.2}  p99={:>7.2} ms",
        label,
        samples[0].n,
        median(tputs),
        lo,
        hi,
        median(samples.iter().map(|s| s.avg_ms).collect()),
        median(samples.iter().map(|s| s.p50_ms).collect()),
        median(samples.iter().map(|s| s.p95_ms).collect()),
        median(samples.iter().map(|s| s.p99_ms).collect()),
    );
}

fn bench_client() -> Arc<reqwest::Client> {
    Arc::new(
        reqwest::Client::builder()
            .pool_max_idle_per_host(128)
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap(),
    )
}

/// Runs `op` across `concurrency` workers, `per_worker` times each, timing every operation.
/// Failures are counted separately rather than folded into the latency distribution.
async fn drive<F, Fut>(concurrency: usize, per_worker: usize, op: F) -> (Vec<f64>, usize)
where
    F: Fn(usize, usize) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = bool> + Send,
{
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let op = op.clone();
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::with_capacity(per_worker);
            for i in 0..per_worker {
                let t = Instant::now();
                let ok = op(w, i).await;
                out.push((t.elapsed().as_secs_f64() * 1000.0, ok));
            }
            out
        }));
    }

    let mut latencies = Vec::new();
    let mut failures = 0;
    for t in tasks {
        for (ms, ok) in t.await.unwrap() {
            if ok {
                latencies.push(ms)
            } else {
                failures += 1
            }
        }
    }
    let _ = started;
    (latencies, failures)
}

async fn write_sample(node: &TestNode, client: Arc<reqwest::Client>, query: &'static str,
                      concurrency: usize, per_worker: usize, tag: &'static str) -> Stats {
    let url = node.url();
    let started = Instant::now();
    let (lat, fails) = drive(concurrency, per_worker, move |w, i| {
        let client = client.clone();
        let url = url.clone();
        async move {
            // 202 is a write that did not meet its concern, and counting it as completed is how a
            // replication path that stopped acknowledging would show up here as faster.
            let status = put_doc_at(&client, &url, "bench", &format!("{}-{}-{}", tag, w, i), i as i64, query).await;
            status.is_success() && status != axum::http::StatusCode::ACCEPTED
        }
    }).await;
    assert_eq!(fails, 0, "{}: {} writes failed", tag, fails);
    summarize(lat, started.elapsed().as_secs_f64())
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_write_concern() {
    println!("\n=== writes by concern, 3 nodes, 16 concurrent ===");
    for (label, query, tag) in [
        ("w=1 (leader durable)", "?w=1", "w1"),
        ("w=majority (2 of 3)", "?w=majority&wtimeout=15000", "wm"),
        ("w=all (3 of 3)", "?w=all&wtimeout=15000", "wa"),
    ] {
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let (n1, _n2, _n3) = three_node_cluster(&root).await;
            let client = bench_client();
            let _ = put_doc_at(&client, &n1.url(), "bench", "warm", 0, "?w=all&wtimeout=15000").await;
            samples.push(write_sample(&n1, client, query, 16, 40, tag).await);
            drop((n1, _n2, _n3));
            cleanup(&root).await;
        }
        report(label, samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_replication_cost() {
    println!("\n=== what replication costs, w=1, 16 concurrent ===");

    let mut solo = Vec::new();
    for _ in 0..SAMPLES {
        let root = temp_root();
        let n = single_node(&root).await;
        let client = bench_client();
        let _ = put_doc_at(&client, &n.url(), "bench", "warm", 0, "?w=1").await;
        solo.push(write_sample(&n, client, "?w=1", 16, 40, "solo").await);
        drop(n);
        cleanup(&root).await;
    }
    report("single node", solo);

    let mut clustered = Vec::new();
    for _ in 0..SAMPLES {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let client = bench_client();
        let _ = put_doc_at(&client, &n1.url(), "bench", "warm", 0, "?w=1").await;
        clustered.push(write_sample(&n1, client, "?w=1", 16, 40, "clus").await);
        drop((n1, _n2, _n3));
        cleanup(&root).await;
    }
    report("3 nodes (2 replicas)", clustered);
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_concurrency_scaling() {
    println!("\n=== concurrency scaling, w=majority, 3 nodes ===");
    // Group commit amortises one fsync over a whole batch of waiters, so throughput should climb
    // faster than concurrency until the disk or the round trip saturates.
    for conc in [1usize, 4, 16, 64] {
        let per = (640 / conc).max(4);
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let (n1, _n2, _n3) = three_node_cluster(&root).await;
            let client = bench_client();
            let _ = put_doc_at(&client, &n1.url(), "bench", "warm", 0, "?w=all&wtimeout=15000").await;
            samples.push(write_sample(&n1, client, "?w=majority&wtimeout=15000", conc, per, "cs").await);
            drop((n1, _n2, _n3));
            cleanup(&root).await;
        }
        report(&format!("concurrency {}", conc), samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_batch_writes() {
    println!("\n=== single writes vs bulk, w=majority, 3 nodes ===");

    let mut singles = Vec::new();
    for _ in 0..SAMPLES {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let client = bench_client();
        let _ = put_doc_at(&client, &n1.url(), "bench", "warm", 0, "?w=all&wtimeout=15000").await;
        singles.push(write_sample(&n1, client, "?w=majority&wtimeout=15000", 8, 40, "sg").await);
        drop((n1, _n2, _n3));
        cleanup(&root).await;
    }
    report("320 single writes", singles);

    // Driven one bulk at a time, so its ops/s is a single-stream figure while the row above uses
    // eight. Compare the per-document latency, not the throughput.
    let batch_of = 32usize;
    let mut batched = Vec::new();
    for _ in 0..SAMPLES {
        let root = temp_root();
        let (n1, _n2, _n3) = three_node_cluster(&root).await;
        let client = bench_client();
        // The row above warms the collection before timing; without the same warm-up this one
        // measures a first write to a collection the replicas have never seen instead of a bulk.
        let _ = put_doc_at(&client, &n1.url(), "bench", "warm", 0, "?w=all&wtimeout=15000").await;
        let url = n1.url();
        let started = Instant::now();
        let mut lat = Vec::new();
        for b in 0..10 {
            let docs: Vec<serde_json::Value> = (0..batch_of)
                .map(|i| serde_json::json!({"id": format!("b{}-{}", b, i), "value": {"v": i}}))
                .collect();
            let t = Instant::now();
            let resp = client
                .post(format!("{}/collections/bench/docs/bulk?w=majority&wtimeout=15000", url))
                .json(&docs)
                .send().await;
            let ok = match resp {
                Ok(r) if r.status().is_success() => true,
                Ok(r) => panic!("bulk write returned {}: {}", r.status(),
                    r.text().await.unwrap_or_default()),
                Err(e) => panic!("bulk write failed: {}", e),
            };
            assert!(ok);
            // Charged per document so the number is comparable with the single-write row.
            let per_doc = t.elapsed().as_secs_f64() * 1000.0 / batch_of as f64;
            for _ in 0..batch_of {
                lat.push(per_doc);
            }
        }
        batched.push(summarize(lat, started.elapsed().as_secs_f64()));
        drop((n1, _n2, _n3));
        cleanup(&root).await;
    }
    report(&format!("320 writes, bulks of {}", batch_of), batched);
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_reads() {
    println!("\n=== reads, 3 nodes, 32 concurrent ===");
    // 64 bytes stays under inline_max_value_bytes and is served from the index; 2 KiB exceeds it
    // and costs a WAL seek per read.
    for (label, filler, tag) in [
        ("point read, cached inline", 64usize, "hot"),
        ("point read, from the WAL", 2048usize, "cold"),
    ] {
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let (n1, _n2, _n3) = three_node_cluster(&root).await;
            let client = bench_client();
            let keys = 200usize;
            let payload = serde_json::json!({"pad": "x".repeat(filler)});
            for k in 0..keys {
                let st = put_value(&client, &n1.url(), "bench", &format!("{}-{}", tag, k),
                    payload.clone(), "?w=majority&wtimeout=15000").await;
                assert!(st.is_success(), "seed write failed: {}", st);
            }

            let url = n1.url();
            for k in 0..20 {
                let _ = get_raw(&client, &url, "bench", &format!("{}-{}", tag, k)).await;
            }
            let c2 = client.clone();
            let started = Instant::now();
            let (lat, fails) = drive(32, 40, move |w, i| {
                let client = c2.clone();
                let url = url.clone();
                async move {
                    get_raw(&client, &url, "bench", &format!("{}-{}", tag, (w * 40 + i) % keys)).await
                }
            }).await;
            assert_eq!(fails, 0, "{} reads failed", fails);
            samples.push(summarize(lat, started.elapsed().as_secs_f64()));
            drop((n1, _n2, _n3));
            cleanup(&root).await;
        }
        report(label, samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_queries() {
    println!("\n=== scans, 3 nodes, 8 concurrent ===");
    for (label, limit) in [("query limit=10", 10usize), ("query limit=100", 100)] {
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let (n1, _n2, _n3) = three_node_cluster(&root).await;
            let client = bench_client();
            for k in 0..500 {
                let st = put_doc_at(&client, &n1.url(), "bench", &format!("q{:04}", k), k as i64,
                    "?w=majority&wtimeout=15000").await;
                assert!(st.is_success(), "seed write failed: {}", st);
            }
            let url = n1.url();
            // Drains the seeding traffic, or whichever scenario runs first pays for it.
            for _ in 0..20 {
                let _ = client.get(format!("{}/collections/bench/query?limit={}", url, limit))
                    .send().await;
            }

            let c2 = client.clone();
            let started = Instant::now();
            let (lat, fails) = drive(8, 25, move |_w, _i| {
                let client = c2.clone();
                let url = url.clone();
                async move {
                    client.get(format!("{}/collections/bench/query?limit={}", url, limit))
                        .send().await.map(|r| r.status().is_success()).unwrap_or(false)
                }
            }).await;
            assert_eq!(fails, 0, "{} queries failed", fails);
            samples.push(summarize(lat, started.elapsed().as_secs_f64()));
            drop((n1, _n2, _n3));
            cleanup(&root).await;
        }
        report(label, samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_quorum_width() {
    println!("\n=== w=majority by group size, 16 concurrent ===");
    // A majority is still whichever ceil(n/2) replicas answer first, so what widening costs is the
    // leader's outbound fan-out, not the latency it waits on.
    for voters in [3usize, 5, 7] {
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let nodes = voter_group(&root, voters, 5).await;
            let client = bench_client();
            let _ = put_doc_at(&client, &nodes[0].url(), "bench", "warm", 0, "?w=majority&wtimeout=15000").await;
            samples.push(write_sample(&nodes[0], client, "?w=majority&wtimeout=15000", 16, 40, "qw").await);
            drop(nodes);
            cleanup(&root).await;
        }
        report(&format!("{} voters (majority {})", voters, voters / 2 + 1), samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_shard_scaling() {
    println!("\n=== routed writes by shard count, 32 concurrent ===");
    // Each shard is one node, so nothing replicates and the only thing changing is how many
    // independent write paths the router spreads a key space over.
    for shards in [1usize, 2, 4, 8] {
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let root = temp_root();
            let (owners, router) = sharded_cluster(&root, shards).await;
            let client = bench_client();
            let _ = put_doc_at(&client, &router.url(), "bench", "warm", 0, "").await;
            samples.push(write_sample(&router, client, "", 32, 20, "sh").await);
            drop(owners);
            drop(router);
            cleanup(&root).await;
        }
        report(&format!("{} shards", shards), samples);
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bench_cross_shard_query() {
    println!("\n=== cross-shard query by shard count, 1000 docs, 8 concurrent ===");
    // The unsorted fan-out concatenates whatever each shard returns; the sorted one has to merge
    // on the sort key, which is the cost this measures against it.
    for shards in [1usize, 2, 4, 8] {
        for (label, suffix) in [("plain", ""), ("sorted", "&sort=v:asc")] {
            let mut samples = Vec::new();
            for _ in 0..SAMPLES {
                let root = temp_root();
                let (owners, router) = sharded_cluster(&root, shards).await;
                let client = bench_client();
                for k in 0..1000 {
                    let st = put_value(&client, &router.url(), "bench", &format!("q{:05}", k),
                        serde_json::json!({"v": k}), "").await;
                    assert!(st.is_success(), "seed write failed: {}", st);
                }

                let url = router.url();
                for _ in 0..10 {
                    let _ = client.get(format!("{}/collections/bench/query?limit=100{}", url, suffix))
                        .send().await;
                }

                let c2 = client.clone();
                let started = Instant::now();
                let (lat, fails) = drive(8, 20, move |_w, _i| {
                    let client = c2.clone();
                    let url = url.clone();
                    async move {
                        client.get(format!("{}/collections/bench/query?limit=100{}", url, suffix))
                            .send().await.map(|r| r.status().is_success()).unwrap_or(false)
                    }
                }).await;
                assert_eq!(fails, 0, "{} queries failed", fails);
                samples.push(summarize(lat, started.elapsed().as_secs_f64()));
                drop(owners);
                drop(router);
                cleanup(&root).await;
            }
            report(&format!("{} shards, {}", shards, label), samples);
        }
    }
}
