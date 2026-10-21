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

/// Wall time is for external timestamps, never deadlines or elapsed time.
pub trait WallClock: Send + Sync {
    fn unix_time_ns(&self) -> Result<i64, crate::live::model::error::ErrorCode>;
}
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;
impl WallClock for SystemWallClock {
    fn unix_time_ns(&self) -> Result<i64, crate::live::model::error::ErrorCode> {
        let value = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => i128::try_from(d.as_nanos()).ok(),
            Err(e) => i128::try_from(e.duration().as_nanos()).ok().map(|n| -n),
        };
        value
            .and_then(|n| i64::try_from(n).ok())
            .ok_or(crate::live::model::error::ErrorCode::Limit)
    }
}

/// Explicit deterministic adapter. Advancing wall time does not advance deadlines.
#[derive(Debug)]
pub struct ManualClock {
    monotonic: std::sync::Mutex<Instant>,
    wall: std::sync::atomic::AtomicI64,
}
impl ManualClock {
    pub fn new(monotonic: Instant, wall_ns: i64) -> Self {
        Self {
            monotonic: std::sync::Mutex::new(monotonic),
            wall: std::sync::atomic::AtomicI64::new(wall_ns),
        }
    }
    pub fn advance(
        &self,
        duration: std::time::Duration,
    ) -> Result<(), crate::live::model::error::ErrorCode> {
        let mut now = self.monotonic.lock().unwrap_or_else(|e| e.into_inner());
        *now = now
            .checked_add(duration)
            .ok_or(crate::live::model::error::ErrorCode::Limit)?;
        Ok(())
    }
    pub fn set_wall_ns(&self, value: i64) {
        self.wall.store(value, std::sync::atomic::Ordering::Release);
    }
}
impl MonotonicClock for ManualClock {
    fn now(&self) -> Instant {
        *self.monotonic.lock().unwrap_or_else(|e| e.into_inner())
    }
}
impl WallClock for ManualClock {
    fn unix_time_ns(&self) -> Result<i64, crate::live::model::error::ErrorCode> {
        Ok(self.wall.load(std::sync::atomic::Ordering::Acquire))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wall_clock_jumps_do_not_change_deadlines() {
        let start = Instant::now();
        let clock = ManualClock::new(start, 100);
        clock.set_wall_ns(-100);
        assert_eq!(clock.now(), start);
        clock.advance(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(
            clock.now().duration_since(start),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(clock.unix_time_ns(), Ok(-100));
    }
}
