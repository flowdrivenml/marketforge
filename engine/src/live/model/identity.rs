//! Foundational identity types required by MarketForge Live.
//!
//! Authoritative reference: 02 — Domain Models and Contracts.md, Sections 3–5.

use crate::canonical::Exchange;
use crate::instrument::InstrumentIdentity;
use serde::{Deserialize, Serialize};

/// Exchange identity — venue-level enum.
///
// Prefers the canonical Exchange enum to avoid identity duplication across
// subsystems. Re-exported here as ExchangeId for live-module clarity.
pub type ExchangeId = Exchange;

/// Market category: Spot, Linear perpetuals, or Inverse futures.
///
/// Market categories identify the relevant instrument/API family.
/// Specific contract semantics remain defined by the existing instrument catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Spot,
    Linear,
    Inverse,
}

/// Semantic channel kind — identifies a stream's market-data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Trades,
    AggregateTrades,
    L2,
    Liquidations,
}

/// Channel acquisition parameters.
///
// This is illustrative. The final representation must support validated
// exchange-specific parameters without treating arbitrary configuration
// strings as canonical identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelSpec {
    pub kind: ChannelKind,
    pub depth: Option<u32>,
    pub interval_ms: Option<u32>,
}

/// Source stream identity — one unique logical exchange acquisition.
///
// Required properties:
// - Hashable and comparable.
// - Stable semantic equality.
// - Independent of connection assignment.
// - Independent of subscribers and recording configuration.
// - Includes all acquisition parameters affecting feed semantics.
// - Compatible with configuration serialization.
// - Uses resolved instrument identity from the instrument catalog.
//
// Two equivalent keys must resolve to one logical source acquisition.
// Different acquisition semantics must produce different keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceStreamKey {
    pub exchange: ExchangeId,
    pub market: MarketKind,
    pub instrument: InstrumentIdentity,
    pub channel: ChannelSpec,
}

/// Opaque identifier assigned to a registered logical stream.
///
// The representation is provisional.
// StreamId must not be used as a substitute for SourceStreamKey when
// determining acquisition equivalence.
// A stream identifier may represent either a source stream or a derived stream.
// Internal APIs should distinguish these categories where required.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId(pub String);

/// Opaque identifier for a derived stream.
///
// Derived streams have independent identities and lifecycle state.
// Their identities must incorporate or reference a validated transformation
// specification.
// Two semantically equivalent derived-stream requests may reuse an existing
// processor.
// A source stream and a derived stream must never accidentally share the
// same registry identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DerivedStreamId(pub String);
