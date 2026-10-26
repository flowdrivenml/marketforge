use crate::live::config::format::TradeFormatConfig;

// ============================================================
// COMPILED POLICY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeIdentityKind {
    Numeric,
    Opaque,
    MarketScoped,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSequenceKind {
    None,
    CrossSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeCoverageAction {
    RequestRecovery,
    ReportUnverifiedCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTradeIntegrityPlan {
    pub format_id: String,

    pub identity_kind: TradeIdentityKind,

    pub sequence_kind: TradeSequenceKind,

    pub detect_duplicates: bool,

    pub detect_id_regression: bool,

    pub detect_id_gaps: bool,

    pub detect_cross_sequence_regression: bool,

    pub allow_equal_cross_sequence: bool,

    pub coverage_action: TradeCoverageAction,

    /// Whether this plan can be used by the existing
    /// LiveTradeController for individual executions.
    pub supports_individual_trades: bool,
}

// ============================================================
// COMPILATION ERROR
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIntegrityCompileError {
    pub field: &'static str,
    pub reason: String,
}

impl TradeIntegrityCompileError {
    fn new(field: &'static str, reason: impl Into<String>) -> Self {
        Self {
            field,
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for TradeIntegrityCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "trade integrity {}: {}", self.field, self.reason)
    }
}

impl std::error::Error for TradeIntegrityCompileError {}

// ============================================================
// COMPILER
// ============================================================

pub struct TradeIntegrityCompiler;

impl TradeIntegrityCompiler {
    pub fn compile(
        config: &TradeFormatConfig,
    ) -> Result<CompiledTradeIntegrityPlan, TradeIntegrityCompileError> {
        let identity_kind = match config.identity.strategy.as_str() {
            "native_trade_id" | "native_numeric_string_trade_id" => TradeIdentityKind::Numeric,

            "native_string_trade_id" => TradeIdentityKind::Opaque,

            "native_market_trade_id" => TradeIdentityKind::MarketScoped,

            "aggregate_trade_id" => TradeIdentityKind::Aggregate,

            other => {
                return Err(TradeIntegrityCompileError::new(
                    "identity.strategy",
                    format!("unsupported strategy: {other}"),
                ));
            }
        };

        // Every supported strategy requires a native
        // identity source.
        if config.identity.source.trim().is_empty() {
            return Err(TradeIntegrityCompileError::new(
                "identity.source",
                "identity source must not be empty",
            ));
        }

        let sequence_kind = match &config.sequence {
            None => TradeSequenceKind::None,

            Some(sequence) => match sequence.strategy.as_str() {
                "cross_sequence_metadata" => {
                    if sequence.source.trim().is_empty() {
                        return Err(TradeIntegrityCompileError::new(
                            "sequence.source",
                            "cross-sequence source is empty",
                        ));
                    }

                    if sequence.require_consecutive == Some(true) {
                        return Err(TradeIntegrityCompileError::new(
                            "sequence.require_consecutive",
                            "cross-sequence IDs cannot \
                                     be assumed consecutive",
                        ));
                    }

                    TradeSequenceKind::CrossSequence
                }

                other => {
                    return Err(TradeIntegrityCompileError::new(
                        "sequence.strategy",
                        format!("unsupported strategy: {other}"),
                    ));
                }
            },
        };

        let integrity = &config.integrity;

        let is_aggregate = identity_kind == TradeIdentityKind::Aggregate;

        let detect_duplicates = if is_aggregate {
            integrity.detect_duplicate_aggregate_ids.unwrap_or(false)
        } else {
            integrity.detect_duplicate_trade_ids.unwrap_or(false)
        };

        let detect_id_regression = if is_aggregate {
            integrity.detect_aggregate_id_regression.unwrap_or(false)
        } else if identity_kind == TradeIdentityKind::MarketScoped {
            integrity.detect_market_trade_id_regression.unwrap_or(false)
        } else {
            integrity.detect_trade_id_regression.unwrap_or(false)
        };

        let detect_id_gaps = if is_aggregate {
            integrity.detect_aggregate_id_gaps.unwrap_or(false)
        } else if identity_kind == TradeIdentityKind::MarketScoped {
            integrity.detect_market_trade_id_gaps.unwrap_or(false)
        } else {
            integrity.detect_trade_id_gaps.unwrap_or(false)
        };

        // Numeric comparisons are meaningless for
        // opaque exchange-native identifiers.
        if identity_kind == TradeIdentityKind::Opaque && (detect_id_regression || detect_id_gaps) {
            return Err(TradeIntegrityCompileError::new(
                "integrity",
                "opaque trade IDs cannot use numeric \
                 regression or gap checks",
            ));
        }

        let detect_cross_sequence_regression =
            integrity.detect_cross_sequence_regression.unwrap_or(false);

        if detect_cross_sequence_regression && sequence_kind != TradeSequenceKind::CrossSequence {
            return Err(TradeIntegrityCompileError::new(
                "integrity.detect_cross_sequence_regression",
                "cross-sequence checking requires \
                 cross_sequence_metadata",
            ));
        }

        let coverage_action = match integrity.gap_action.as_str() {
            "request_recovery" => TradeCoverageAction::RequestRecovery,

            "report_unverified_coverage" => TradeCoverageAction::ReportUnverifiedCoverage,

            other => {
                return Err(TradeIntegrityCompileError::new(
                    "integrity.gap_action",
                    format!("unsupported action: {other}"),
                ));
            }
        };

        Ok(CompiledTradeIntegrityPlan {
            format_id: config.format.id.clone(),

            identity_kind,

            sequence_kind,

            detect_duplicates,

            detect_id_regression,

            detect_id_gaps,

            detect_cross_sequence_regression,

            allow_equal_cross_sequence: config
                .sequence
                .as_ref()
                .and_then(|sequence| sequence.allow_equal_values)
                .unwrap_or(false),

            coverage_action,

            supports_individual_trades: !is_aggregate,
        })
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    fn profile(name: &str) -> TradeFormatConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/trades")
            .join(format!("{name}.toml"));

        TradeFormatConfig::load(path).unwrap()
    }

    #[test]
    fn compiles_all_fifteen_trade_profiles() {
        let exchanges = ["binance", "bybit", "bitget", "gateio", "okx"];

        let markets = ["spot", "linear", "inverse"];

        for exchange in exchanges {
            for market in markets {
                let name = format!("{exchange}_{market}");

                let config = profile(&name);

                let result = TradeIntegrityCompiler::compile(&config);

                assert!(result.is_ok(), "{name}: {result:?}");
            }
        }
    }

    #[test]
    fn binance_spot_uses_numeric_identity() {
        let plan = TradeIntegrityCompiler::compile(&profile("binance_spot")).unwrap();

        assert_eq!(plan.identity_kind, TradeIdentityKind::Numeric);

        assert!(plan.detect_duplicates);
        assert!(plan.detect_id_regression);
        assert!(plan.detect_id_gaps);
    }

    #[test]
    fn bybit_uses_opaque_ids_and_cross_sequence() {
        let plan = TradeIntegrityCompiler::compile(&profile("bybit_spot")).unwrap();

        assert_eq!(plan.identity_kind, TradeIdentityKind::Opaque);

        assert_eq!(plan.sequence_kind, TradeSequenceKind::CrossSequence);

        assert!(!plan.detect_id_gaps);

        assert!(plan.detect_cross_sequence_regression);
        assert!(plan.allow_equal_cross_sequence);
    }

    #[test]
    fn rejects_numeric_gap_checks_for_opaque_ids() {
        let mut config = profile("bybit_spot");

        config.integrity.detect_trade_id_gaps = Some(true);

        assert!(TradeIntegrityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_unsupported_identity_strategy() {
        let mut config = profile("binance_spot");

        config.identity.strategy = "unknown_strategy".into();

        assert!(TradeIntegrityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_consecutive_cross_sequence_assumption() {
        let mut config = profile("bybit_spot");

        config.sequence.as_mut().unwrap().require_consecutive = Some(true);

        assert!(TradeIntegrityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_missing_cross_sequence_contract() {
        let mut config = profile("bybit_spot");

        config.sequence = None;

        assert!(TradeIntegrityCompiler::compile(&config).is_err());
    }
}
