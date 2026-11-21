use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const WINDOW: Duration = Duration::from_secs(4);

#[derive(Default)]
pub(super) struct Rate {
    samples: VecDeque<(Instant, u64)>,
    pub(super) history: VecDeque<u64>,
    pub(super) current: Option<f64>,
}

impl Rate {
    pub(super) fn sample(&mut self, now: Instant, count: u64) {
        if let Some(&(last, previous)) = self.samples.back() {
            if now < last || previous > count {
                *self = Self::default();
            } else if now - last < SAMPLE_INTERVAL {
                return;
            }
        }
        self.samples.push_back((now, count));
        while self
            .samples
            .front()
            .is_some_and(|&(time, _)| now - time > WINDOW)
        {
            self.samples.pop_front();
        }
        self.current = self.samples.front().and_then(|&(first, initial)| {
            let elapsed = now - first;
            (elapsed >= Duration::from_secs(1))
                .then(|| (count - initial) as f64 / elapsed.as_secs_f64())
        });
        if let Some(rate) = self.current {
            if self.history.len() == 30 {
                self.history.pop_front();
            }
            self.history.push_back(rate as u64);
        }
    }

    pub(super) fn eta(&self, remaining: u64) -> Option<Duration> {
        let rate = self.current.filter(|rate| *rate > 0.0)?;
        (remaining > 0)
            .then(|| Duration::try_from_secs_f64((remaining as f64 / rate).ceil()).ok())
            .flatten()
    }
}
