//! Monotonic clock abstraction for MarketForge Live.
//!
//! Deadlines and internal durations use monotonic time.
//! Wall-clock timestamps are handled separately.

use std::time::Instant;

/// Monotonic clock used for deadlines and elapsed-time measurement.
pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Production monotonic clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
