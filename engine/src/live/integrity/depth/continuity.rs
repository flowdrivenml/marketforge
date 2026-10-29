use crate::shared::domain::canonical::SequenceMetadata;

use crate::live::integrity::depth::continuity_compiler::{
    CompiledDepthContinuityPlan, DepthContinuityFamily,
};

// ============================================================
// DECISIONS
// ============================================================

/// Decision for one incoming depth delta.
///
/// Accept:
///     The delta satisfies the protocol's continuity rule.
///     The caller may proceed to atomic book application.
///
/// Ignore:
///     The delta is stale or an accepted no-op.
///
/// Resynchronize:
///     The delta cannot safely extend the current book.
///     The owning controller must invalidate the book
///     and initiate the configured recovery procedure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthContinuityDecision {
    Accept { next_sequence: u64 },

    Ignore { reason: DepthIgnoreReason },

    Resynchronize { reason: DepthViolationReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthIgnoreReason {
    Stale,
    Keepalive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthViolationReason {
    MissingSequence,
    InvalidRange,
    SequenceGap,
    PreviousSequenceMismatch,
    SequenceRegression,
    UnexpectedEqualSequence,
    ResetRequiresSnapshot,
    UnsupportedMetadata,
    SequenceOverflow,
}

// ============================================================
// BASELINE
// ============================================================

/// Accepted sequence state of the current synchronized book.
///
/// This is supplied by the stream controller.
///
/// The validator does not own or mutate it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthSequenceBaseline {
    pub last_sequence: u64,

    /// Applicable to Bybit's secondary cross-sequence.
    pub cross_sequence: Option<u64>,

    /// True only after the required snapshot/bridge
    /// synchronization procedure has completed.
    pub synchronized: bool,
}

// ============================================================
// VALIDATOR
// ============================================================

pub struct DepthContinuityValidator;

impl DepthContinuityValidator {
    /// Evaluate one normalized depth delta.
    ///
    /// This method never mutates the baseline.
    ///
    /// `has_levels` indicates whether the normalized
    /// message contains any bid or ask level updates.
    ///
    /// Snapshot messages must be handled separately by
    /// the synchronization controller.
    pub fn evaluate(
        plan: &CompiledDepthContinuityPlan,
        baseline: DepthSequenceBaseline,
        sequence: &SequenceMetadata,
        has_levels: bool,
    ) -> DepthContinuityDecision {
        if !baseline.synchronized {
            return reject(DepthViolationReason::ResetRequiresSnapshot);
        }

        match plan.family {
            DepthContinuityFamily::BinanceSpotRange => {
                evaluate_range(baseline.last_sequence, sequence)
            }

            DepthContinuityFamily::BinanceFuturesPrevious => {
                evaluate_binance_futures(baseline.last_sequence, sequence)
            }

            DepthContinuityFamily::GateRangeCover => {
                evaluate_range(baseline.last_sequence, sequence)
            }

            DepthContinuityFamily::BybitV5 => evaluate_bybit(baseline, sequence),

            DepthContinuityFamily::BitgetV3Previous => evaluate_bitget(
                baseline.last_sequence,
                sequence,
                plan.allows_linked_sequence_decrease,
            ),

            DepthContinuityFamily::OkxV5Previous => evaluate_okx(
                baseline.last_sequence,
                sequence,
                has_levels,
                plan.allows_equal_sequence_keepalive,
                plan.allows_linked_sequence_decrease,
            ),
        }
    }
}

// ============================================================
// RANGE-COVERAGE CONTINUITY
// ============================================================

/// Used by Binance Spot and Gate.io.
///
/// The next required ID is:
///
/// ```
/// use marketforge_engine::live::{config::format::DepthFormatConfig, integrity::{DepthContinuityCompiler, DepthContinuityValidator, DepthContinuityDecision, DepthSequenceBaseline}};
/// use marketforge_engine::shared::domain::canonical::SequenceMetadata;
/// let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats/depth/binance_spot.toml");
/// let plan = DepthContinuityCompiler::compile(&DepthFormatConfig::load(path).unwrap()).unwrap();
/// let baseline = DepthSequenceBaseline {last_sequence:100, cross_sequence:None, synchronized:true};
/// let sequence = SequenceMetadata {sequence_first:Some(101), sequence_last:Some(101), sequence_previous:None, cross_sequence:None};
/// assert_eq!(DepthContinuityValidator::evaluate(&plan, baseline, &sequence, true), DepthContinuityDecision::Accept {next_sequence:101});
/// let unsynchronized = DepthSequenceBaseline {synchronized:false, ..baseline};
/// assert!(matches!(DepthContinuityValidator::evaluate(&plan, unsynchronized, &sequence, true), DepthContinuityDecision::Resynchronize {..}));
/// ```
///
/// An incoming range [first, last] must cover it.
///
/// Overlapping ranges are permitted.
/// Completely stale ranges are ignored.
/// Ranges beginning after the required ID indicate a gap.
fn evaluate_range(previous: u64, sequence: &SequenceMetadata) -> DepthContinuityDecision {
    if sequence.sequence_previous.is_some() || sequence.cross_sequence.is_some() {
        return reject(DepthViolationReason::UnsupportedMetadata);
    }

    let (Some(first), Some(last)) = (sequence.sequence_first, sequence.sequence_last) else {
        return reject(DepthViolationReason::MissingSequence);
    };

    if first > last {
        return reject(DepthViolationReason::InvalidRange);
    }

    if last <= previous {
        return DepthContinuityDecision::Ignore {
            reason: DepthIgnoreReason::Stale,
        };
    }

    let Some(required) = previous.checked_add(1) else {
        return reject(DepthViolationReason::SequenceOverflow);
    };

    if first > required {
        return reject(DepthViolationReason::SequenceGap);
    }

    DepthContinuityDecision::Accept {
        next_sequence: last,
    }
}

// ============================================================
// BINANCE FUTURES
// ============================================================

/// Binance Futures requires:
///
/// ```
/// use marketforge_engine::live::{config::format::DepthFormatConfig, integrity::{DepthContinuityCompiler, DepthContinuityValidator, DepthContinuityDecision, DepthSequenceBaseline}};
/// use marketforge_engine::shared::domain::canonical::SequenceMetadata;
/// let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats/depth/binance_linear.toml");
/// let plan = DepthContinuityCompiler::compile(&DepthFormatConfig::load(path).unwrap()).unwrap();
/// let baseline = DepthSequenceBaseline {last_sequence:100, cross_sequence:None, synchronized:true};
/// let sequence = SequenceMetadata {sequence_first:Some(101), sequence_last:Some(101), sequence_previous:Some(100), cross_sequence:None};
/// assert_eq!(DepthContinuityValidator::evaluate(&plan, baseline, &sequence, true), DepthContinuityDecision::Accept {next_sequence:101});
/// let unsynchronized = DepthSequenceBaseline {synchronized:false, ..baseline};
/// assert!(matches!(DepthContinuityValidator::evaluate(&plan, unsynchronized, &sequence, true), DepthContinuityDecision::Resynchronize {..}));
/// ```
///
/// Native [U, u] range consistency is also checked.
///
/// A sequence range can contain multiple native update IDs.
fn evaluate_binance_futures(previous: u64, sequence: &SequenceMetadata) -> DepthContinuityDecision {
    if sequence.cross_sequence.is_some() {
        return reject(DepthViolationReason::UnsupportedMetadata);
    }

    let (Some(first), Some(last), Some(link)) = (
        sequence.sequence_first,
        sequence.sequence_last,
        sequence.sequence_previous,
    ) else {
        return reject(DepthViolationReason::MissingSequence);
    };

    if first > last {
        return reject(DepthViolationReason::InvalidRange);
    }

    if last <= previous {
        return DepthContinuityDecision::Ignore {
            reason: DepthIgnoreReason::Stale,
        };
    }

    if link != previous {
        return reject(DepthViolationReason::PreviousSequenceMismatch);
    }

    let Some(required) = previous.checked_add(1) else {
        return reject(DepthViolationReason::SequenceOverflow);
    };

    if first > required || last < required {
        return reject(DepthViolationReason::SequenceGap);
    }

    DepthContinuityDecision::Accept {
        next_sequence: last,
    }
}

// ============================================================
// BITGET V3
// ============================================================

/// Bitget requires:
///
/// ```
/// use marketforge_engine::live::{config::format::DepthFormatConfig, integrity::{DepthContinuityCompiler, DepthContinuityValidator, DepthContinuityDecision, DepthSequenceBaseline}};
/// use marketforge_engine::shared::domain::canonical::SequenceMetadata;
/// let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats/depth/bitget_spot.toml");
/// let plan = DepthContinuityCompiler::compile(&DepthFormatConfig::load(path).unwrap()).unwrap();
/// let baseline = DepthSequenceBaseline {last_sequence:100, cross_sequence:None, synchronized:true};
/// let sequence = SequenceMetadata {sequence_first:None, sequence_last:Some(101), sequence_previous:Some(100), cross_sequence:None};
/// assert_eq!(DepthContinuityValidator::evaluate(&plan, baseline, &sequence, true), DepthContinuityDecision::Accept {next_sequence:101});
/// let unsynchronized = DepthSequenceBaseline {synchronized:false, ..baseline};
/// assert!(matches!(DepthContinuityValidator::evaluate(&plan, unsynchronized, &sequence, true), DepthContinuityDecision::Resynchronize {..}));
/// ```
///
/// Sequence IDs are not universally consecutive.
///
/// A pseq=0 reset requires a new authoritative snapshot,
/// rather than extending the previous book.
fn evaluate_bitget(
    previous: u64,
    sequence: &SequenceMetadata,
    allow_linked_decrease: bool,
) -> DepthContinuityDecision {
    if sequence.sequence_first.is_some() || sequence.cross_sequence.is_some() {
        return reject(DepthViolationReason::UnsupportedMetadata);
    }

    let (Some(current), Some(link)) = (sequence.sequence_last, sequence.sequence_previous) else {
        return reject(DepthViolationReason::MissingSequence);
    };

    if link == 0 {
        return reject(DepthViolationReason::ResetRequiresSnapshot);
    }

    if link != previous {
        return reject(DepthViolationReason::PreviousSequenceMismatch);
    }

    if current == previous {
        return reject(DepthViolationReason::UnexpectedEqualSequence);
    }

    if current < previous && !allow_linked_decrease {
        return reject(DepthViolationReason::SequenceRegression);
    }

    DepthContinuityDecision::Accept {
        next_sequence: current,
    }
}

// ============================================================
// OKX V5
// ============================================================

/// OKX requires:
///
/// ```
/// use marketforge_engine::live::{config::format::DepthFormatConfig, integrity::{DepthContinuityCompiler, DepthContinuityValidator, DepthContinuityDecision, DepthSequenceBaseline}};
/// use marketforge_engine::shared::domain::canonical::SequenceMetadata;
/// let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats/depth/okx_spot.toml");
/// let plan = DepthContinuityCompiler::compile(&DepthFormatConfig::load(path).unwrap()).unwrap();
/// let baseline = DepthSequenceBaseline {last_sequence:100, cross_sequence:None, synchronized:true};
/// let sequence = SequenceMetadata {sequence_first:None, sequence_last:Some(101), sequence_previous:Some(100), cross_sequence:None};
/// assert_eq!(DepthContinuityValidator::evaluate(&plan, baseline, &sequence, true), DepthContinuityDecision::Accept {next_sequence:101});
/// let unsynchronized = DepthSequenceBaseline {synchronized:false, ..baseline};
/// assert!(matches!(DepthContinuityValidator::evaluate(&plan, unsynchronized, &sequence, true), DepthContinuityDecision::Resynchronize {..}));
/// ```
///
/// Documented exceptions:
///
/// 1. Empty keepalive with an unchanged sequence.
/// 2. Linked sequence decrease during maintenance.
///
/// These exceptions must be explicitly enabled in the
/// compiled configuration plan.
fn evaluate_okx(
    previous: u64,
    sequence: &SequenceMetadata,
    has_levels: bool,
    allow_keepalive: bool,
    allow_linked_decrease: bool,
) -> DepthContinuityDecision {
    if sequence.sequence_first.is_some() || sequence.cross_sequence.is_some() {
        return reject(DepthViolationReason::UnsupportedMetadata);
    }

    let (Some(current), Some(link)) = (sequence.sequence_last, sequence.sequence_previous) else {
        return reject(DepthViolationReason::MissingSequence);
    };

    if link != previous {
        return reject(DepthViolationReason::PreviousSequenceMismatch);
    }

    if current == previous {
        if allow_keepalive && !has_levels {
            return DepthContinuityDecision::Ignore {
                reason: DepthIgnoreReason::Keepalive,
            };
        }

        return reject(DepthViolationReason::UnexpectedEqualSequence);
    }

    if current < previous && !allow_linked_decrease {
        return reject(DepthViolationReason::SequenceRegression);
    }

    DepthContinuityDecision::Accept {
        next_sequence: current,
    }
}

// ============================================================
// BYBIT V5
// ============================================================

/// Bybit standard depth does not guarantee consecutive IDs.
///
/// We check ordering/regression, not arithmetic gaps.
///
/// Snapshot resets and depth-1 snapshot-only behavior
/// remain responsibilities of the synchronization controller.
fn evaluate_bybit(
    baseline: DepthSequenceBaseline,
    sequence: &SequenceMetadata,
) -> DepthContinuityDecision {
    if sequence.sequence_first.is_some() || sequence.sequence_previous.is_some() {
        return reject(DepthViolationReason::UnsupportedMetadata);
    }

    let Some(current) = sequence.sequence_last else {
        return reject(DepthViolationReason::MissingSequence);
    };

    let Some(cross) = sequence.cross_sequence else {
        return reject(DepthViolationReason::MissingSequence);
    };

    if current == 1 && baseline.last_sequence != 1 {
        return reject(DepthViolationReason::ResetRequiresSnapshot);
    }

    if current <= baseline.last_sequence {
        return DepthContinuityDecision::Ignore {
            reason: DepthIgnoreReason::Stale,
        };
    }

    if let Some(previous_cross) = baseline.cross_sequence {
        if cross <= previous_cross {
            return reject(DepthViolationReason::SequenceRegression);
        }
    } else {
        return reject(DepthViolationReason::MissingSequence);
    }

    DepthContinuityDecision::Accept {
        next_sequence: current,
    }
}

// ============================================================
// HELPERS
// ============================================================

fn reject(reason: DepthViolationReason) -> DepthContinuityDecision {
    DepthContinuityDecision::Resynchronize { reason }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::integrity::depth::continuity_compiler::{
        DepthBridgeRule, DepthSnapshotSource,
    };

    fn plan(family: DepthContinuityFamily) -> CompiledDepthContinuityPlan {
        CompiledDepthContinuityPlan {
            format_id: "test".into(),
            family,
            snapshot_source: DepthSnapshotSource::WebSocket,
            initial_bridge: DepthBridgeRule::WebSocketSnapshotBaseline,
            requires_update_buffer: false,
            accepts_websocket_snapshots: true,
            allows_overlapping_ranges: true,
            allows_equal_sequence_keepalive: true,
            allows_linked_sequence_decrease: true,
            requires_generation_fencing: true,
        }
    }

    fn baseline(last: u64) -> DepthSequenceBaseline {
        DepthSequenceBaseline {
            last_sequence: last,
            cross_sequence: Some(last),
            synchronized: true,
        }
    }

    fn sequence(
        first: Option<u64>,
        last: Option<u64>,
        previous: Option<u64>,
        cross: Option<u64>,
    ) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: first,
            sequence_last: last,
            sequence_previous: previous,
            cross_sequence: cross,
        }
    }

    #[test]
    fn binance_spot_accepts_contiguous_range() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceSpotRange),
            baseline(100),
            &sequence(Some(101), Some(105), None, None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 105 }
        );
    }

    #[test]
    fn binance_spot_accepts_overlapping_range() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceSpotRange),
            baseline(100),
            &sequence(Some(98), Some(103), None, None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 103 }
        );
    }

    #[test]
    fn binance_spot_detects_gap() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceSpotRange),
            baseline(100),
            &sequence(Some(103), Some(105), None, None),
            true,
        );

        assert_eq!(result, reject(DepthViolationReason::SequenceGap));
    }

    #[test]
    fn gate_ignores_stale_range() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::GateRangeCover),
            baseline(100),
            &sequence(Some(95), Some(100), None, None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Ignore {
                reason: DepthIgnoreReason::Stale
            }
        );
    }

    #[test]
    fn binance_futures_accepts_linked_update() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceFuturesPrevious),
            baseline(100),
            &sequence(Some(101), Some(105), Some(100), None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 105 }
        );
    }

    #[test]
    fn binance_futures_rejects_wrong_previous_id() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceFuturesPrevious),
            baseline(100),
            &sequence(Some(101), Some(105), Some(99), None),
            true,
        );

        assert_eq!(
            result,
            reject(DepthViolationReason::PreviousSequenceMismatch)
        );
    }

    #[test]
    fn bitget_accepts_nonconsecutive_sequence() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BitgetV3Previous),
            baseline(100),
            &sequence(None, Some(120), Some(100), None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 120 }
        );
    }

    #[test]
    fn bitget_reset_requires_snapshot() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BitgetV3Previous),
            baseline(100),
            &sequence(None, Some(101), Some(0), None),
            true,
        );

        assert_eq!(result, reject(DepthViolationReason::ResetRequiresSnapshot));
    }

    #[test]
    fn okx_accepts_empty_keepalive() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::OkxV5Previous),
            baseline(100),
            &sequence(None, Some(100), Some(100), None),
            false,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Ignore {
                reason: DepthIgnoreReason::Keepalive
            }
        );
    }

    #[test]
    fn okx_accepts_linked_maintenance_reset() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::OkxV5Previous),
            baseline(100),
            &sequence(None, Some(10), Some(100), None),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 10 }
        );
    }

    #[test]
    fn okx_rejects_unlinked_update() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::OkxV5Previous),
            baseline(100),
            &sequence(None, Some(110), Some(99), None),
            true,
        );

        assert_eq!(
            result,
            reject(DepthViolationReason::PreviousSequenceMismatch)
        );
    }

    #[test]
    fn bybit_accepts_monotonic_sequences() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BybitV5),
            baseline(100),
            &sequence(None, Some(105), None, Some(110)),
            true,
        );

        assert_eq!(
            result,
            DepthContinuityDecision::Accept { next_sequence: 105 }
        );
    }

    #[test]
    fn bybit_rejects_cross_sequence_regression() {
        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BybitV5),
            baseline(100),
            &sequence(None, Some(105), None, Some(99)),
            true,
        );

        assert_eq!(result, reject(DepthViolationReason::SequenceRegression));
    }

    #[test]
    fn cannot_accept_without_synchronized_baseline() {
        let mut state = baseline(100);
        state.synchronized = false;

        let result = DepthContinuityValidator::evaluate(
            &plan(DepthContinuityFamily::BinanceSpotRange),
            state,
            &sequence(Some(101), Some(102), None, None),
            true,
        );

        assert_eq!(result, reject(DepthViolationReason::ResetRequiresSnapshot));
    }
}
