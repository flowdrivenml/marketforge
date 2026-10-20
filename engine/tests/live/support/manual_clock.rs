use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use marketforge_engine::live::foundation::clock::MonotonicClock;

/// Deterministic monotonic clock for Live contract tests.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<Instant>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            now: Mutex::new(Instant::now()),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap();

        *now = now.checked_add(duration).expect("test clock overflow");
    }
}

impl MonotonicClock for ManualClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}
