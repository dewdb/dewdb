//! Latency histograms, replica ack watermarks, and repair counters.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub const LATENCY_BUCKETS_MS: [f64; 11] = [0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeLoad {
    pub inflight: u64,
    pub latency_ewma_us: u64,
}

#[derive(Clone, Default)]
pub struct RouteStats {
    pub count: u64,
    pub errors: u64,
    pub nanos_total: u64,
    pub buckets: [u64; 12],
}

impl RouteStats {
    pub fn observe(&mut self, nanos: u64, is_error: bool) {
        self.count += 1;
        self.nanos_total += nanos;
        if is_error {
            self.errors += 1;
        }
        let ms = nanos as f64 / 1_000_000.0;
        let slot = LATENCY_BUCKETS_MS.iter().position(|b| ms <= *b).unwrap_or(LATENCY_BUCKETS_MS.len());
        self.buckets[slot] += 1;
    }

    pub fn quantile_ms(&self, q: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (self.count as f64 * q).ceil() as u64;
        let mut seen = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= target {
                return LATENCY_BUCKETS_MS.get(i).copied().unwrap_or(f64::INFINITY);
            }
        }
        f64::INFINITY
    }

    pub fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.nanos_total as f64 / self.count as f64 / 1_000_000.0
    }
}

pub struct Metrics {
    pub started_at: std::time::Instant,
    pub routes: std::sync::Mutex<BTreeMap<String, RouteStats>>,
    pub gaps: AtomicU64,
    pub divergences: AtomicU64,
    pub resyncs: AtomicU64,
    pub batches_sent: AtomicU64,
    pub frames_sent: AtomicU64,
    pub max_batch_frames: AtomicU64,
    pub writes_rejected: AtomicU64,
    active_requests: AtomicU64,
    latency_ewma_us: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started_at: std::time::Instant::now(),
            routes: std::sync::Mutex::new(BTreeMap::new()),
            gaps: AtomicU64::new(0),
            divergences: AtomicU64::new(0),
            resyncs: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            max_batch_frames: AtomicU64::new(0),
            writes_rejected: AtomicU64::new(0),
            active_requests: AtomicU64::new(0),
            latency_ewma_us: AtomicU64::new(0),
        }
    }

    pub fn begin_request(&self) {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_request(&self) {
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn node_load(&self) -> NodeLoad {
        NodeLoad {
            inflight: self.active_requests.load(Ordering::Relaxed),
            latency_ewma_us: self.latency_ewma_us.load(Ordering::Relaxed),
        }
    }

    pub fn note_write_rejected(&self) {
        self.writes_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn writes_rejected(&self) -> u64 {
        self.writes_rejected.load(Ordering::Relaxed)
    }

    pub fn note_batch(&self, frames: usize) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.frames_sent.fetch_add(frames as u64, Ordering::Relaxed);
        self.max_batch_frames.fetch_max(frames as u64, Ordering::Relaxed);
    }

    /// `(batches, frames)`. Frames above batches is the whole point of pipelining.
    pub fn batch_counts(&self) -> (u64, u64) {
        (
            self.batches_sent.load(Ordering::Relaxed),
            self.frames_sent.load(Ordering::Relaxed),
        )
    }

    /// High-water mark rather than a windowed delta: repair can begin before the last write is
    /// appended, so totals over an interval are racy while this is not.
    pub fn max_batch_frames(&self) -> u64 {
        self.max_batch_frames.load(Ordering::Relaxed)
    }

    pub fn note_gap(&self) {
        self.gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_divergence(&self) {
        self.divergences.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_resync(&self) {
        self.resyncs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn repair_counts(&self) -> (u64, u64, u64) {
        (
            self.gaps.load(Ordering::Relaxed),
            self.divergences.load(Ordering::Relaxed),
            self.resyncs.load(Ordering::Relaxed),
        )
    }

    pub fn observe(&self, key: String, nanos: u64, is_error: bool) {
        self.routes.lock().unwrap().entry(key).or_default().observe(nanos, is_error);
        let sample = (nanos / 1_000).max(1);
        let mut previous = self.latency_ewma_us.load(Ordering::Relaxed);
        loop {
            let next = if previous == 0 {
                sample
            } else {
                previous.saturating_mul(7).saturating_add(sample) / 8
            };
            match self.latency_ewma_us.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => previous = actual,
            }
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn routes_snapshot(&self) -> Vec<(String, RouteStats)> {
        self.routes.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_histogram_buckets_and_quantiles() {
        let mut st = RouteStats::default();
        for _ in 0..90 { st.observe(400_000, false); }
        for _ in 0..9 { st.observe(30_000_000, false); }
        st.observe(900_000_000, true);

        assert_eq!(st.count, 100);
        assert_eq!(st.errors, 1);
        assert_eq!(st.buckets[0], 90, "0.4ms lands in the 0.5ms bucket");
        assert_eq!(st.quantile_ms(0.50), 0.5, "p50 sits in the fast bucket");
        assert_eq!(st.quantile_ms(0.95), 50.0, "p95 reflects the slow tail");
        assert_eq!(st.quantile_ms(0.99), 50.0);
        assert!((st.avg_ms() - 12.06).abs() < 0.01, "mean of 90x0.4ms + 9x30ms + 900ms, got {}", st.avg_ms());

        let empty = RouteStats::default();
        assert_eq!(empty.quantile_ms(0.99), 0.0, "an unused route must not divide by zero");
        assert_eq!(empty.avg_ms(), 0.0);
    }

    #[test]
    fn slow_requests_fall_into_the_overflow_bucket() {
        let mut st = RouteStats::default();
        st.observe(5_000_000_000, false);
        assert_eq!(*st.buckets.last().unwrap(), 1, "5s must land in +Inf, not be dropped");
        assert_eq!(st.quantile_ms(0.99), f64::INFINITY);
    }

    #[test]
    fn metrics_records_errors_separately_from_traffic() {
        let m = Metrics::new();
        m.observe("GET /x".to_string(), 1_000_000, false);
        m.observe("GET /x".to_string(), 1_000_000, true);
        m.observe("POST /y".to_string(), 1_000_000, false);

        let snap = m.routes_snapshot();
        assert_eq!(snap.len(), 2, "routes are tracked by template, not by URL");
        let x = &snap.iter().find(|(k, _)| k == "GET /x").unwrap().1;
        assert_eq!(x.count, 2);
        assert_eq!(x.errors, 1);
    }

    #[test]
    fn node_load_tracks_active_requests() {
        let m = Metrics::new();
        m.begin_request();
        m.begin_request();
        m.observe("GET /x".to_string(), 8_000_000, false);

        assert_eq!(m.node_load(), NodeLoad { inflight: 2, latency_ewma_us: 8_000 });

        m.end_request();
        m.end_request();
        assert_eq!(m.node_load().inflight, 0);
    }

    #[test]
    fn load_latency_uses_an_ewma() {
        let m = Metrics::new();
        m.observe("GET /x".to_string(), 8_000_000, false);
        m.observe("GET /x".to_string(), 16_000_000, false);
        assert_eq!(m.node_load().latency_ewma_us, 9_000);
    }
}
