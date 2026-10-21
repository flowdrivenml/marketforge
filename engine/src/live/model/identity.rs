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

/// Validation errors collected from a single SourceStreamKey check.
///
/// Consumers inspect this to determine whether definite failures exist
/// (caller must reject) versus unresolved classifications (escalate to
/// the configuration compiler).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssues {
    pub issues: Vec<ValidationIssue>,
}

impl ValidationIssues {
    /// Whether any **definite** validation failures exist.
    ///
    /// Unresolved classifications (e.g., perpetuals without contract_kind)
    /// do not count; they are informational for the configuration compiler.
    pub fn has_definite_failures(&self) -> bool {
        self.issues
            .iter()
            .any(|issue| issue.kind.is_definite_failure())
    }

    /// Definite validation failures only (unresolved classifications excluded).
    pub fn definite_failures(&self) -> impl Iterator<Item = &ValidationIssue> + '_ {
        self.issues
            .iter()
            .filter(|issue| issue.kind.is_definite_failure())
    }

    /// Unresolved classification requirements — metadata gaps the
    /// configuration compiler must resolve.
    pub fn unresolved_classifications(&self) -> impl Iterator<Item = &ValidationIssue> + '_ {
        self.issues
            .iter()
            .filter(|issue| !issue.kind.is_definite_failure())
    }
}

/// A single identity-consistency validation issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub kind: ValidationIssueKind,
    pub detail: String,
}

/// Classification of a validated identity constraint.
///
// Public because configuration-compiler consumers need to discriminate
// definite failures from unresolved classification requirements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationIssueKind {
    /// SourceStreamKey.exchange and InstrumentIdentity.exchange do not match.
    ExchangeMismatch,
    /// MarketKind is incompatible with the instrument identity metadata.
    MarketKindMismatch,
    /// MarketKind is ambiguous with respect to the instrument identity
    /// metadata. The catalog does not distinguish linear from inverse
    /// for this instrument kind; the ambiguity must be resolved manually.
    ///
    /// Examples: `InstrumentKind::Perpetual` (family provisioning remains unresolved)
    /// or `InstrumentKind::Future` with contract_kind missing.
    /// This is an **unresolved classification**, not a definite user error.
    MarketKindAmbiguous,
    /// The instrument kind is not yet supported by the validation rules.
    UnsupportedInstrumentKind,
    /// A channel parameter violates a basic consistency rule.
    InvalidChannelSpec,
}

impl ValidationIssueKind {
    /// Whether this issue represents a definite validation failure
    /// versus an unresolved classification requirement.
    ///
    /// Definite failures must be addressed before the key can be used.
    /// Unresolved classifications indicate missing metadata (e.g., perpetuals
    /// without contract_kind) that the caller should escalate to the
    /// configuration compiler for resolution.
    pub fn is_definite_failure(&self) -> bool {
        !matches!(self, ValidationIssueKind::MarketKindAmbiguous)
    }
}

impl SourceStreamKey {
    /// Validate identity consistency without catalog lookup or runtime state.
    ///
    /// Returns all collected issues. The caller may treat one or more issues
    /// as hard errors or proceed with warnings.
    ///
    /// # Validation rules
    ///
    /// 1. **Exchange consistency** — the canonical ExchangeId variant is
    ///    converted to its snake_case string and compared against
    ///    `InstrumentIdentity.exchange`.
    ///
    /// 2. **MarketKind against instrument metadata** — where
    ///    `InstrumentIdentity` provides enough information to determine the
    ///    market family unambiguously, the MarketKind is checked for
    ///    compatibiity. Where the metadata is insufficient the constraint
    ///    is reported as an ambiguity (not silently accepted).
    ///
    /// 3. **Unsupported instrument kinds** — instrument kinds outside
    ///    {Spot, Perpetual, Future} are rejected unconditionally.
    ///
    /// 4. **Channel parameter consistency** — depth must be > 0 when
    ///    present; interval_ms must be > 0 when present.
    ///
    /// # Design constraints observed
    ///
    /// - No catalog lookup (belongs to configuration compiler).
    /// - No exchange capability validation.
    /// - No WebSocket, subscriber, recording, or metrics coupling.
    /// - No new instrument model invented.
    /// - No mappings invented for ambiguous instrument categories:
    ///   when `InstrumentIdentity` cannot determine the market family
    ///   (e.g., perpetuals without contract_kind), the ambiguity is
    ///   reported rather than silently assumed.
    pub fn validate(&self) -> ValidationIssues {
        let mut issues: Vec<ValidationIssue> = Vec::new();

        // 1. Exchange consistency
        let exchange_str = match self.exchange {
            Exchange::Bybit => "bybit",
            Exchange::Binance => "binance",
            Exchange::Okx => "okx",
            Exchange::Bitget => "bitget",
            Exchange::GateIo => "gateio",
        };
        if !exchange_str.eq_ignore_ascii_case(self.instrument.exchange.as_str()) {
            issues.push(ValidationIssue {
                kind: ValidationIssueKind::ExchangeMismatch,
                detail: format!(
                    "exchange {:?} resolves to \"{}\" but instrument.exchange is \"{}\"",
                    self.exchange, exchange_str, self.instrument.exchange
                ),
            });
        }

        // 2. MarketKind against instrument identity metadata
        match self.instrument.instrument_kind {
            crate::job::InstrumentKind::Spot => {
                if self.market != MarketKind::Spot {
                    issues.push(ValidationIssue {
                        kind: ValidationIssueKind::MarketKindMismatch,
                        detail: format!(
                            "instrument_kind=Spot requires MarketKind::Spot, got {:?}",
                            self.market
                        ),
                    });
                }
            }
            crate::job::InstrumentKind::Perpetual => {
                if self.market == MarketKind::Spot {
                    issues.push(ValidationIssue {
                        kind: ValidationIssueKind::MarketKindMismatch,
                        detail: "a perpetual instrument cannot use the spot market family".into(),
                    });
                } else {
                    // Family provisioning remains the compiler's OPEN-001
                    // responsibility. Do not infer linear versus inverse.
                    issues.push(ValidationIssue {
                        kind: ValidationIssueKind::MarketKindAmbiguous,
                        detail:
                            "perpetual market family requires authoritative compiler resolution"
                                .into(),
                    });
                }
            }
            crate::job::InstrumentKind::Future => {
                if let Some(contract_kind) = self.instrument.contract_kind {
                    let expected = match contract_kind {
                        crate::job::ContractKind::Linear => MarketKind::Linear,
                        crate::job::ContractKind::Inverse => MarketKind::Inverse,
                    };
                    if self.market != expected {
                        issues.push(ValidationIssue {
                            kind: ValidationIssueKind::MarketKindMismatch,
                            detail: format!(
                                "instrument_kind=Future(contract_kind={:?}) \
                                 requires MarketKind::{:?}, got {:?}",
                                contract_kind, expected, self.market
                            ),
                        });
                    }
                } else if self.market == MarketKind::Spot {
                    issues.push(ValidationIssue {
                        kind: ValidationIssueKind::MarketKindMismatch,
                        detail: "a futures instrument cannot use the spot market family".into(),
                    });
                } else {
                    // Future without contract_kind — ambiguous.
                    issues.push(ValidationIssue {
                        kind: ValidationIssueKind::MarketKindAmbiguous,
                        detail: format!(
                            "instrument_kind=Future with contract_kind=None: \
                             market family cannot be determined. \
                             market={:?}",
                            self.market
                        ),
                    });
                }
            }
            crate::job::InstrumentKind::Option => {
                issues.push(ValidationIssue {
                    kind: ValidationIssueKind::UnsupportedInstrumentKind,
                    detail: "instrument_kind=Option is not yet supported".to_owned(),
                });
            }
        }

        // 3. Channel parameter consistency
        if let Some(depth) = self.channel.depth {
            if depth == 0 {
                issues.push(ValidationIssue {
                    kind: ValidationIssueKind::InvalidChannelSpec,
                    detail: "channel depth must be > 0 when present".to_owned(),
                });
            }
        }
        if let Some(interval_ms) = self.channel.interval_ms {
            if interval_ms == 0 {
                issues.push(ValidationIssue {
                    kind: ValidationIssueKind::InvalidChannelSpec,
                    detail: "channel interval_ms must be > 0 when present".to_owned(),
                });
            }
        }

        ValidationIssues { issues }
    }

    /// Construct a SourceStreamKey candidate without validating catalog or
    /// exchange-capability correctness.
    ///
    /// **This constructor does NOT establish** that:
    /// - `exchange` matches `InstrumentIdentity.exchange`
    /// - `market` is the correct category for the instrument
    /// - the exchange supports acquisition for this instrument type
    ///
    /// Callers **must** run `validate()` and inspect the result before
    /// using the key: use `has_definite_failures()` to reject outright
    /// failures and `unresolved_classifications()` to escalate metadata
    /// gaps to the configuration compiler.
    pub fn new(
        exchange: ExchangeId,
        market: MarketKind,
        instrument: InstrumentIdentity,
        channel: ChannelSpec,
    ) -> Self {
        Self {
            exchange,
            market,
            instrument,
            channel,
        }
    }
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
