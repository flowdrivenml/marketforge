use crate::shared::domain::canonical::Exchange;
use crate::shared::domain::instrument::InstrumentKind;

/// Instrument matching strategy, independent of task configuration or record decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityPolicy {
    /// Every record must identify the requested instrument.
    Strict,

    /// A futures-family archive may contain other contracts in the same family.
    Filter,

    /// The source does not expose a native instrument identifier.
    TrustSource,
}

/// Whether the current record belongs to the requested instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityDecision {
    Match,
    Skip,
}

/// Identity matching counters accumulated while decoding records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityMetrics {
    pub records_read: u64,
    pub records_matched: u64,
    pub records_skipped_instrument: u64,
}

/// Resolve archive matching policy from explicit exchange and instrument metadata.
pub fn resolve_identity_policy(
    exchange: Exchange,
    instrument_kind: InstrumentKind,
) -> IdentityPolicy {
    match (exchange, instrument_kind) {
        (Exchange::Okx, InstrumentKind::Future) => IdentityPolicy::Filter,

        (Exchange::Okx, _) => IdentityPolicy::Strict,

        (Exchange::Bybit, InstrumentKind::Perpetual) => IdentityPolicy::Strict,

        _ => IdentityPolicy::TrustSource,
    }
}
