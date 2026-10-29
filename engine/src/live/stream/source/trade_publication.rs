use crate::{live::integrity::IntegrityOutcome, shared::domain::canonical::Trade};

/// Trade publication payload produced by a Source Stream.
///
/// Ordinary outcomes preserve the existing integrity result.
///
/// Recovered trades have already passed numeric reconciliation
/// and integrity preflight, but their identities are not
/// committed until the publication callback succeeds.
#[derive(Debug)]
pub enum SourceTradePublication<'a> {
    /// Result of ordinary WebSocket trade processing.
    Integrity(&'a IntegrityOutcome),

    /// Reconciled trades awaiting acknowledged publication.
    Recovery(&'a [Trade]),
}
