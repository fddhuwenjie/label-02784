use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

/// Token-bucket rate limiter keyed by client IP address.
///
/// Thread-safe via interior `Mutex`. Uses `std::sync::Mutex` (not tokio)
/// because the critical section is extremely short (sub-microsecond).
pub struct RateLimiter {
    state: Mutex<HashMap<IpAddr, Bucket>>,
    capacity: f64,
    refill_per_sec: f64,
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// * `capacity` – maximum burst size (tokens).
    /// * `refill_per_sec` – sustained request rate (tokens/second).
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            capacity: f64::from(capacity),
            refill_per_sec,
        }
    }

    /// Returns `true` if the request is allowed, `false` if rate-limited.
    pub fn allow(&self, addr: IpAddr) -> bool {
        let now = Instant::now();
        let mut map = self.state.lock().unwrap_or_else(|e| e.into_inner());

        let bucket = map.entry(addr).or_insert(Bucket {
            tokens: self.capacity,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Remove entries idle for longer than `max_idle_secs` to bound memory.
    pub fn evict_idle(&self, max_idle_secs: u64) {
        let now = Instant::now();
        let mut map = self.state.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, b| now.duration_since(b.last_refill).as_secs() < max_idle_secs);
    }
}
