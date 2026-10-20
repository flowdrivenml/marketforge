/// Instrument identity strategy for archive processing.
///
/// This module contains pure data types and the identity-policy resolver,
/// extracted from [`identity`](super::identity) so they are available without
/// the `historical` (CSV) feature.
///
/// The concrete [`InstrumentMatcher`](super::identity::InstrumentMatcher)
/// remains historical-only because it depends on `csv::ByteRecord`.
use crate::canonical::Exchange;
use crate::job::{InstrumentKind, WorkTask};

/// Policy for instrument identity in archive processing.
///
/// Used when `InstrumentMatcher` must determine whether each record
/// in a multi-instrument archive belongs to the requested instrument.
///
/// **Note**: this type is unconditional and available to the live path.
/// The live path will use it together with `identity_policy()` and
/// `IdentityDecision` for instrument identity, but the matching
/// machinery itself is historical-only (WP-02D).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityPolicy {
    /// Archive is expected to contain only the requested instrument.
    Strict,

    /// Archive may contain multiple instruments.
    Filter,

    /// Source does not expose a native instrument identifier.
    TrustSource,
}

/// Outcome of an instrument identity check.
///
/// **Note**: unconditional — live path will use this for deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityDecision {
    Match,
    Skip,
}

/// Accumulated identity-processing statistics.
///
/// **Note**: unconditional — live path will use this for sequence /
/// deduplication metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityMetrics {
    /// Total records examined.
    pub records_read: u64,
    /// Records determined to belong to the requested instrument.
    pub records_matched: u64,
    /// Records skipped because the source contained another instrument
    /// from the same futures family.
    pub records_skipped_instrument: u64,
}

/// Resolve the identity-policy applicable to a download / processing task.
///
/// This function is **unconditional** — it operates only on
/// `WorkTask` / `Exchange` / `InstrumentKind` which are always compiled.
///
/// The concrete matcher that consumes this policy resides in the
/// historical-only `InstrumentMatcher` type.
pub fn identity_policy(task: &WorkTask) -> IdentityPolicy {
    match (task.exchange, task.instrument.instrument_kind) {
        (Exchange::Okx, InstrumentKind::Future) => IdentityPolicy::Filter,

        (Exchange::Okx, _) => IdentityPolicy::Strict,

        (Exchange::Bybit, InstrumentKind::Perpetual) => IdentityPolicy::Strict,

        _ => IdentityPolicy::TrustSource,
    }
}
