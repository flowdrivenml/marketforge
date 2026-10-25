use std::time::Duration;

use crate::live::config::general::RecoveryDefaultsConfig;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackoffError {
    #[error("initial backoff must be greater than zero")]
    ZeroInitialDelay,

    #[error("maximum backoff must be greater than zero")]
    ZeroMaximumDelay,

    #[error("initial backoff must not exceed maximum backoff")]
    InitialExceedsMaximum,

    #[error("backoff duration exceeds the supported millisecond range")]
    DurationOverflow,
}

// ============================================================
// CONFIGURATION
// ============================================================

/// Immutable parameters for exponential backoff.
///
/// Jitter uses the equal-jitter strategy:
///
///     actual_delay = base_delay / 2 + random(0..=base_delay / 2)
///
/// This avoids synchronized retry storms while ensuring that
/// the delay does not become arbitrarily close to zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffConfig {
    pub initial_delay: Duration,
    pub maximum_delay: Duration,
    pub jitter: bool,
}

impl BackoffConfig {
    pub fn new(
        initial_delay: Duration,
        maximum_delay: Duration,
        jitter: bool,
    ) -> Result<Self, BackoffError> {
        if initial_delay.is_zero() {
            return Err(BackoffError::ZeroInitialDelay);
        }

        if maximum_delay.is_zero() {
            return Err(BackoffError::ZeroMaximumDelay);
        }

        if initial_delay > maximum_delay {
            return Err(BackoffError::InitialExceedsMaximum);
        }

        let initial_ms = initial_delay.as_millis();
        let maximum_ms = maximum_delay.as_millis();

        if initial_ms == 0 || maximum_ms == 0 {
            return Err(BackoffError::ZeroInitialDelay);
        }

        if initial_ms > u64::MAX as u128 || maximum_ms > u64::MAX as u128 {
            return Err(BackoffError::DurationOverflow);
        }

        Ok(Self {
            initial_delay,
            maximum_delay,
            jitter,
        })
    }

    fn initial_ms(self) -> u64 {
        self.initial_delay.as_millis() as u64
    }

    fn maximum_ms(self) -> u64 {
        self.maximum_delay.as_millis() as u64
    }
}

// ============================================================
// GENERAL CONFIGURATION INTEGRATION
// ============================================================

impl TryFrom<&RecoveryDefaultsConfig> for BackoffConfig {
    type Error = BackoffError;

    fn try_from(defaults: &RecoveryDefaultsConfig) -> Result<Self, Self::Error> {
        let initial_delay = Duration::from_millis(defaults.initial_backoff_ms);

        let maximum_delay = Duration::from_secs(defaults.max_backoff_seconds);

        Self::new(initial_delay, maximum_delay, defaults.jitter)
    }
}

// ============================================================
// EXPONENTIAL BACKOFF
// ============================================================

/// Independent backoff state for one retrying operation.
///
/// A REST operation and a WebSocket connection must never
/// share the same ExponentialBackoff instance.
///
/// This type calculates delays only.
///
/// The caller owns:
/// - Sleeping
/// - Cancellation
/// - Retry classification
/// - Maximum attempts
/// - Recovery deadlines
/// - Success/failure lifecycle
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    config: BackoffConfig,

    next_base_ms: u64,

    consecutive_failures: u64,
}

impl ExponentialBackoff {
    pub fn new(config: BackoffConfig) -> Self {
        Self {
            next_base_ms: config.initial_ms(),
            consecutive_failures: 0,
            config,
        }
    }

    // ========================================================
    // DELAY CALCULATION
    // ========================================================

    /// Calculate the delay following another failure.
    ///
    /// The first failure uses initial_delay.
    ///
    /// Subsequent failures double the base delay until
    /// maximum_delay is reached.
    ///
    /// When jitter is enabled, the returned delay falls
    /// between approximately half the base and the base.
    pub fn next_delay(&mut self) -> Duration {
        let base_ms = self.next_base_ms;

        self.consecutive_failures = self.consecutive_failures.saturating_add(1);

        self.next_base_ms = base_ms.saturating_mul(2).min(self.config.maximum_ms());

        let actual_ms = if self.config.jitter {
            Self::equal_jitter(base_ms)
        } else {
            base_ms
        };

        Duration::from_millis(actual_ms)
    }

    /// Apply equal jitter to an exponential delay.
    ///
    /// The resulting delay never exceeds the base delay.
    fn equal_jitter(base_ms: u64) -> u64 {
        let minimum_ms = base_ms / 2 + base_ms % 2;

        if minimum_ms == base_ms {
            return base_ms;
        }

        rand::random_range(minimum_ms..=base_ms)
    }

    // ========================================================
    // STATE MANAGEMENT
    // ========================================================

    /// Reset backoff to its initial state.
    ///
    /// The caller decides when recovery is sufficiently
    /// successful to justify resetting the failure history.
    pub fn reset(&mut self) {
        self.next_base_ms = self.config.initial_ms();
        self.consecutive_failures = 0;
    }

    /// Number of failures registered since the last reset.
    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures
    }

    /// Base delay that would be used following the next failure.
    ///
    /// This is the unjittered delay.
    pub fn next_base_delay(&self) -> Duration {
        Duration::from_millis(self.next_base_ms)
    }

    /// Immutable backoff configuration.
    pub fn config(&self) -> BackoffConfig {
        self.config
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn config(jitter: bool) -> BackoffConfig {
        BackoffConfig::new(Duration::from_millis(500), Duration::from_secs(4), jitter).unwrap()
    }

    // ========================================================
    // CONFIGURATION VALIDATION
    // ========================================================

    #[test]
    fn rejects_zero_initial_delay() {
        let result = BackoffConfig::new(Duration::ZERO, Duration::from_secs(10), false);

        assert_eq!(result, Err(BackoffError::ZeroInitialDelay));
    }

    #[test]
    fn rejects_zero_maximum_delay() {
        let result = BackoffConfig::new(Duration::from_millis(500), Duration::ZERO, false);

        assert_eq!(result, Err(BackoffError::ZeroMaximumDelay));
    }

    #[test]
    fn rejects_initial_delay_above_maximum() {
        let result = BackoffConfig::new(Duration::from_secs(10), Duration::from_secs(2), false);

        assert_eq!(result, Err(BackoffError::InitialExceedsMaximum));
    }

    // ========================================================
    // EXPONENTIAL GROWTH
    // ========================================================

    #[test]
    fn doubles_delay_until_maximum() {
        let mut backoff = ExponentialBackoff::new(config(false));

        let expected = [500, 1_000, 2_000, 4_000, 4_000, 4_000];

        for milliseconds in expected {
            assert_eq!(backoff.next_delay(), Duration::from_millis(milliseconds),);
        }

        assert_eq!(backoff.consecutive_failures(), expected.len() as u64,);
    }

    #[test]
    fn does_not_overflow_after_many_failures() {
        let mut backoff = ExponentialBackoff::new(config(false));

        for _ in 0..1_000 {
            let delay = backoff.next_delay();

            assert!(delay <= Duration::from_secs(4));
        }

        assert_eq!(backoff.next_base_delay(), Duration::from_secs(4),);
    }

    // ========================================================
    // JITTER
    // ========================================================

    #[test]
    fn jitter_remains_within_expected_bounds() {
        let mut backoff = ExponentialBackoff::new(config(true));

        for (minimum, maximum) in [
            (250, 500),
            (500, 1_000),
            (1_000, 2_000),
            (2_000, 4_000),
            (2_000, 4_000),
        ] {
            let delay = backoff.next_delay();

            assert!(
                delay >= Duration::from_millis(minimum),
                "delay {delay:?} is below {minimum} ms",
            );

            assert!(
                delay <= Duration::from_millis(maximum),
                "delay {delay:?} exceeds {maximum} ms",
            );
        }
    }

    // ========================================================
    // RESET
    // ========================================================

    #[test]
    fn reset_restores_initial_state() {
        let mut backoff = ExponentialBackoff::new(config(false));

        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay();

        assert_eq!(backoff.consecutive_failures(), 3);

        backoff.reset();

        assert_eq!(backoff.consecutive_failures(), 0);

        assert_eq!(backoff.next_base_delay(), Duration::from_millis(500),);

        assert_eq!(backoff.next_delay(), Duration::from_millis(500),);
    }

    // ========================================================
    // INDEPENDENT OPERATIONS
    // ========================================================

    #[test]
    fn separate_instances_have_independent_state() {
        let mut rest = ExponentialBackoff::new(config(false));

        let mut websocket = ExponentialBackoff::new(config(false));

        rest.next_delay();
        rest.next_delay();
        rest.next_delay();

        assert_eq!(rest.next_base_delay(), Duration::from_secs(4),);

        assert_eq!(websocket.next_base_delay(), Duration::from_millis(500),);

        assert_eq!(websocket.next_delay(), Duration::from_millis(500),);
    }

    // ========================================================
    // GENERAL CONFIG INTEGRATION
    // ========================================================

    #[test]
    fn creates_config_from_recovery_defaults() {
        let defaults = RecoveryDefaultsConfig {
            initial_backoff_ms: 500,
            max_backoff_seconds: 60,
            jitter: true,
        };

        let config = BackoffConfig::try_from(&defaults).unwrap();

        assert_eq!(config.initial_delay, Duration::from_millis(500),);

        assert_eq!(config.maximum_delay, Duration::from_secs(60),);

        assert!(config.jitter);
    }
}
