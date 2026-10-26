use std::mem::size_of;
use std::time::Duration;

use crate::{canonical::L2LevelUpdate, live::infrastructure::normalization::NormalizedDepth};

// ============================================================
// RESOURCE LIMITS
// ============================================================

/// Resource limits for depth integrity processing.
///
/// These are admission limits, not process-wide RSS limits.
///
/// max_levels_per_message:
///     Maximum canonical level updates in one normalized
///     depth message.
///
/// max_pending_snapshot_levels:
///     Maximum retained price levels in a privately staged
///     Bitget snapshot.
///
/// max_pending_snapshot_age:
///     Maximum time a Bitget snapshot may wait for its
///     first valid bridging delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthResourceLimits {
    pub max_levels_per_message: usize,
    pub max_pending_snapshot_levels: usize,
    pub max_pending_snapshot_age: Duration,
}

impl Default for DepthResourceLimits {
    fn default() -> Self {
        Self {
            max_levels_per_message: 20_000,
            max_pending_snapshot_levels: 20_000,
            max_pending_snapshot_age: Duration::from_secs(30),
        }
    }
}

impl DepthResourceLimits {
    pub fn validate(self) -> Result<Self, DepthResourceError> {
        if self.max_levels_per_message == 0
            || self.max_pending_snapshot_levels == 0
            || self.max_pending_snapshot_age.is_zero()
        {
            return Err(DepthResourceError::InvalidLimits);
        }

        Ok(self)
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthResourceError {
    InvalidLimits,

    TooManyLevels { received: usize, maximum: usize },

    PendingSnapshotTooLarge { received: usize, maximum: usize },

    SizeOverflow,
}

// ============================================================
// RESOURCE ACCOUNTING
// ============================================================

pub struct DepthResourceAccounting;

impl DepthResourceAccounting {
    /// Validate the normalized message's level count.
    pub fn validate_message(
        message: &NormalizedDepth,
        limits: DepthResourceLimits,
    ) -> Result<(), DepthResourceError> {
        if message.level_count() > limits.max_levels_per_message {
            return Err(DepthResourceError::TooManyLevels {
                received: message.level_count(),
                maximum: limits.max_levels_per_message,
            });
        }

        Ok(())
    }

    /// Validate the number of levels retained in a
    /// staged snapshot.
    pub fn validate_pending_snapshot(
        retained_levels: usize,
        limits: DepthResourceLimits,
    ) -> Result<(), DepthResourceError> {
        if retained_levels > limits.max_pending_snapshot_levels {
            return Err(DepthResourceError::PendingSnapshotTooLarge {
                received: retained_levels,
                maximum: limits.max_pending_snapshot_levels,
            });
        }

        Ok(())
    }

    /// Estimate a conservative admission charge for one
    /// owned NormalizedDepth message.
    ///
    /// We account for:
    /// - The NormalizedDepth structure.
    /// - The complete vector of canonical levels.
    /// - Dynamically allocated envelope strings.
    /// - The top-level stream identifier.
    /// - Additional allocation overhead/headroom.
    ///
    /// The multiplier is a conservative accounting policy,
    /// not an exact Rust allocator measurement.
    ///
    /// This method deliberately rejects arithmetic overflow.
    ///
    /// It cannot account for allocations held outside this
    /// message or establish a hard RSS guarantee.
    pub fn estimate_retained_bytes(message: &NormalizedDepth) -> Result<usize, DepthResourceError> {
        let level_storage = message
            .level_count()
            .checked_mul(size_of::<L2LevelUpdate>())
            .ok_or(DepthResourceError::SizeOverflow)?;

        let mut bytes = size_of::<NormalizedDepth>()
            .checked_add(level_storage)
            .and_then(|value| value.checked_add(message.stream_id().len()))
            .ok_or(DepthResourceError::SizeOverflow)?;

        for level in message.levels() {
            bytes = bytes
                .checked_add(level.envelope.stream_id.len())
                .and_then(|value| value.checked_add(level.envelope.symbol.len()))
                .ok_or(DepthResourceError::SizeOverflow)?;
        }

        // Charge twice the directly estimated footprint
        // to allow for spare capacities and allocation
        // overhead in an admission-control context.
        //
        // A stricter allocator-aware bound can replace
        // this policy without changing the buffer API.
        bytes = bytes
            .checked_mul(2)
            .ok_or(DepthResourceError::SizeOverflow)?;

        // Empty messages still consume memory.
        Ok(bytes.max(1))
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::{
        canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
        live::infrastructure::normalization::{DepthMessageKind, NormalizationSource},
    };

    fn message(count: usize) -> NormalizedDepth {
        let levels = (0..count)
            .map(|index| L2LevelUpdate {
                envelope: EventEnvelope {
                    event_timestamp_ns: 1,
                    system_timestamp_ns: None,
                    exchange: Exchange::Binance,
                    instrument_id: 42,
                    symbol: "BTCUSDT".into(),
                    stream_id: "stream-42".into(),
                },

                side: BookSide::Bid,

                price: Decimal::from(80_000 + index as i64),

                quantity_base: Some(Decimal::ONE),
                quantity_quote: None,
                quantity_contracts: None,
                order_count: None,
            })
            .collect();

        NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            SequenceMetadata::default(),
            levels,
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_resource_limits() {
        let limits = DepthResourceLimits {
            max_levels_per_message: 0,
            ..DepthResourceLimits::default()
        };

        assert_eq!(limits.validate(), Err(DepthResourceError::InvalidLimits),);
    }

    #[test]
    fn rejects_oversized_depth_message() {
        let limits = DepthResourceLimits {
            max_levels_per_message: 2,
            ..DepthResourceLimits::default()
        };

        assert!(matches!(
            DepthResourceAccounting::validate_message(&message(3), limits,),
            Err(DepthResourceError::TooManyLevels {
                received: 3,
                maximum: 2,
            })
        ));
    }

    #[test]
    fn accepts_exact_level_limit() {
        let limits = DepthResourceLimits {
            max_levels_per_message: 2,
            ..DepthResourceLimits::default()
        };

        assert!(DepthResourceAccounting::validate_message(&message(2), limits,).is_ok());
    }

    #[test]
    fn pending_snapshot_has_separate_limit() {
        let limits = DepthResourceLimits {
            max_pending_snapshot_levels: 2,
            ..DepthResourceLimits::default()
        };

        assert!(matches!(
            DepthResourceAccounting::validate_pending_snapshot(3, limits,),
            Err(DepthResourceError::PendingSnapshotTooLarge {
                received: 3,
                maximum: 2,
            })
        ));
    }

    #[test]
    fn memory_charge_increases_with_message_size() {
        let small = DepthResourceAccounting::estimate_retained_bytes(&message(1)).unwrap();

        let large = DepthResourceAccounting::estimate_retained_bytes(&message(10)).unwrap();

        assert!(large > small);
        assert!(small > 0);
    }

    #[test]
    fn empty_message_has_nonzero_charge() {
        let estimated = DepthResourceAccounting::estimate_retained_bytes(&message(0)).unwrap();

        assert!(estimated > 0);
    }
}
