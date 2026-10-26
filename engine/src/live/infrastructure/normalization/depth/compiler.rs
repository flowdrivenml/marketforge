use thiserror::Error;

use crate::live::config::format::{DepthFormatConfig, DepthLevelSideConfig};

use crate::live::infrastructure::normalization::depth::plan::{
    DepthLevelPlan, DepthSidePlan, NativeDepthLevelLayout,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum DepthCompileError {
    #[error("invalid depth configuration: {0}")]
    InvalidConfig(String),

    #[error("unsupported depth configuration: {0}")]
    Unsupported(String),

    #[error("missing depth configuration field: {0}")]
    MissingField(String),
}

// ============================================================
// COMPILED LEVEL PLAN
// ============================================================

/// Immutable extraction instructions derived from one
/// native exchange depth format.
///
/// This is the level-extraction portion of the eventual
/// complete depth normalization plan.
///
/// Sequence, timestamp, envelope, and source semantics
/// will be compiled separately.
#[derive(Debug, Clone)]
pub struct CompiledDepthLevelPlan {
    pub format_id: String,
    pub levels: DepthLevelPlan,
}

// ============================================================
// DEPTH NORMALIZATION COMPILER
// ============================================================

pub struct DepthNormalizationCompiler;

impl DepthNormalizationCompiler {
    // ========================================================
    // COMPILE NATIVE LEVEL EXTRACTION
    // ========================================================

    pub fn compile_levels(
        config: &DepthFormatConfig,
        max_levels: usize,
    ) -> Result<CompiledDepthLevelPlan, DepthCompileError> {
        if max_levels == 0 {
            return Err(DepthCompileError::InvalidConfig(
                "max_levels must be positive".into(),
            ));
        }

        let bids = compile_side(&config.levels.bids, "bid")?;

        let asks = compile_side(&config.levels.asks, "ask")?;

        let levels = DepthLevelPlan::new(bids, asks, max_levels)
            .map_err(|error| DepthCompileError::InvalidConfig(error.to_string()))?;

        Ok(CompiledDepthLevelPlan {
            format_id: config.format.id.clone(),
            levels,
        })
    }
}

// ============================================================
// COMPILE ONE SIDE
// ============================================================

fn compile_side(
    config: &DepthLevelSideConfig,
    expected_side: &'static str,
) -> Result<DepthSidePlan, DepthCompileError> {
    // ========================================================
    // SIDE IDENTITY
    // ========================================================

    if config.side != expected_side {
        return Err(DepthCompileError::InvalidConfig(format!(
            "expected {expected_side} side, got '{}'",
            config.side,
        )));
    }

    // ========================================================
    // COLLECTION REPRESENTATION
    // ========================================================

    if config.representation != "array" {
        return Err(DepthCompileError::Unsupported(format!(
            "{expected_side}: unsupported level collection \
                 representation '{}'",
            config.representation,
        )));
    }

    // ========================================================
    // PRICE / QUANTITY TYPES
    // ========================================================

    // Native exact decimal strings are common, but
    // Gate.io futures may send an integer JSON quantity.
    //
    // NumericConverter already supports exact strings
    // and native JSON integers.
    //
    // We validate declared representations here and
    // enforce their precise runtime types in a later
    // typed-field decoding pass.

    for (name, value) in [
        ("price_type", config.price_type.as_str()),
        ("quantity_type", config.quantity_type.as_str()),
    ] {
        match value {
            "decimal_string" | "integer_string" | "integer" | "u64" | "i64" => {}

            other => {
                return Err(DepthCompileError::Unsupported(format!(
                    "{expected_side}.{name}: '{other}'",
                )));
            }
        }
    }

    // ========================================================
    // LEVEL REPRESENTATION
    // ========================================================

    let layout = match config.level_type.as_str() {
        // ====================================================
        // POSITIONAL ARRAYS
        // ====================================================
        "array" => {
            let price_index = config
                .price_index
                .ok_or_else(|| missing(expected_side, "price_index"))?;

            let quantity_index = config
                .quantity_index
                .ok_or_else(|| missing(expected_side, "quantity_index"))?;

            if config.price_field.is_some() || config.quantity_field.is_some() {
                return Err(DepthCompileError::InvalidConfig(format!(
                    "{expected_side}: array levels must not \
                         declare object fields"
                )));
            }

            if let Some(order_count_index) = config.order_count_index {
                if config.order_count_type.is_none() {
                    return Err(DepthCompileError::MissingField(format!(
                        "{expected_side}.order_count_type"
                    )));
                }

                if config.ignored_indices.contains(&order_count_index) {
                    return Err(DepthCompileError::InvalidConfig(format!(
                        "{expected_side}: order count index \
                             cannot be ignored"
                    )));
                }
            } else if config.order_count_type.is_some() {
                return Err(DepthCompileError::InvalidConfig(format!(
                    "{expected_side}: order_count_type \
                         requires order_count_index"
                )));
            }

            if config.ignored_indices.contains(&price_index)
                || config.ignored_indices.contains(&quantity_index)
            {
                return Err(DepthCompileError::InvalidConfig(format!(
                    "{expected_side}: a required field \
                         cannot be ignored"
                )));
            }

            NativeDepthLevelLayout::Array {
                price_index,
                quantity_index,
                order_count_index: config.order_count_index,
            }
        }

        // ====================================================
        // OBJECT LEVELS
        // ====================================================
        "object" => {
            let price_field = config
                .price_field
                .clone()
                .ok_or_else(|| missing(expected_side, "price_field"))?;

            let quantity_field = config
                .quantity_field
                .clone()
                .ok_or_else(|| missing(expected_side, "quantity_field"))?;

            if config.price_index.is_some()
                || config.quantity_index.is_some()
                || config.order_count_index.is_some()
            {
                return Err(DepthCompileError::InvalidConfig(format!(
                    "{expected_side}: object levels must not \
                         declare positional indices"
                )));
            }

            if !config.ignored_indices.is_empty() {
                return Err(DepthCompileError::InvalidConfig(format!(
                    "{expected_side}: ignored_indices are \
                         only valid for array levels"
                )));
            }

            // The current DepthLevelSideConfig schema has
            // no order_count_field member for object levels.
            //
            // Reject a declared count type rather than
            // silently discarding that configuration.

            if config.order_count_type.is_some() {
                return Err(DepthCompileError::Unsupported(format!(
                    "{expected_side}: object order-count \
                         extraction is not configured"
                )));
            }

            NativeDepthLevelLayout::Object {
                price_field,
                quantity_field,
                order_count_field: None,
            }
        }

        other => {
            return Err(DepthCompileError::Unsupported(format!(
                "{expected_side}: unsupported level_type '{other}'"
            )));
        }
    };

    // ========================================================
    // ORDER COUNT TYPE
    // ========================================================

    if let Some(value) = config.order_count_type.as_deref() {
        match value {
            "integer_string" | "u64" | "integer" => {}

            other => {
                return Err(DepthCompileError::Unsupported(format!(
                    "{expected_side}: unsupported \
                         order_count_type '{other}'"
                )));
            }
        }
    }

    Ok(DepthSidePlan {
        path: config.path.clone(),
        layout,
    })
}

// ============================================================
// HELPERS
// ============================================================

fn missing(side: &str, field: &str) -> DepthCompileError {
    DepthCompileError::MissingField(format!("{side}.{field}"))
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    use crate::live::infrastructure::normalization::extraction::JsonExtractor;

    // ========================================================
    // CONFIGURATION FACTORY
    // ========================================================

    fn load(profile: &str) -> DepthFormatConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{profile}.toml"));

        DepthFormatConfig::load(path).unwrap()
    }

    // ========================================================
    // ALL 15 PROFILES
    // ========================================================

    #[test]
    fn compiles_all_fifteen_depth_profiles() {
        let exchanges = ["binance", "bybit", "bitget", "gateio", "okx"];

        let markets = ["spot", "linear", "inverse"];

        for exchange in exchanges {
            for market in markets {
                let name = format!("{exchange}_{market}");

                let config = load(&name);

                let result = DepthNormalizationCompiler::compile_levels(&config, 20_000);

                assert!(result.is_ok(), "{name}: {result:?}",);
            }
        }
    }

    // ========================================================
    // BINANCE ARRAY REPRESENTATION
    // ========================================================

    #[test]
    fn compiles_binance_spot_array_levels() {
        let config = load("binance_spot");

        let compiled = DepthNormalizationCompiler::compile_levels(&config, 1000).unwrap();

        assert!(matches!(
            compiled.levels.bids().layout,
            NativeDepthLevelLayout::Array {
                price_index: 0,
                quantity_index: 1,
                order_count_index: None,
            }
        ));
    }

    // ========================================================
    // GATE.IO FUTURES OBJECT REPRESENTATION
    // ========================================================

    #[test]
    fn compiles_gateio_futures_object_levels() {
        let config = load("gateio_linear");

        let compiled = DepthNormalizationCompiler::compile_levels(&config, 1000).unwrap();

        assert!(matches!(
            &compiled.levels.bids().layout,
            NativeDepthLevelLayout::Object {
                price_field,
                quantity_field,
                order_count_field: None,
            }
            if price_field == "p"
                && quantity_field == "s"
        ));
    }

    // ========================================================
    // OKX ORDER COUNT
    // ========================================================

    #[test]
    fn compiles_okx_order_count_position() {
        let config = load("okx_spot");

        let compiled = DepthNormalizationCompiler::compile_levels(&config, 1000).unwrap();

        assert!(matches!(
            compiled.levels.bids().layout,
            NativeDepthLevelLayout::Array {
                price_index: 0,
                quantity_index: 1,
                order_count_index: Some(3),
            }
        ));
    }

    // ========================================================
    // CONFIGURATION ERRORS
    // ========================================================

    #[test]
    fn rejects_missing_required_index() {
        let mut config = load("binance_spot");

        config.levels.bids.price_index = None;

        assert!(DepthNormalizationCompiler::compile_levels(&config, 1000,).is_err());
    }

    #[test]
    fn rejects_wrong_side_declaration() {
        let mut config = load("binance_spot");

        config.levels.bids.side = "ask".into();

        assert!(DepthNormalizationCompiler::compile_levels(&config, 1000,).is_err());
    }

    #[test]
    fn rejects_ignored_required_index() {
        let mut config = load("okx_spot");

        config.levels.bids.ignored_indices.push(0);

        assert!(DepthNormalizationCompiler::compile_levels(&config, 1000,).is_err());
    }

    #[test]
    fn rejects_missing_order_count_type() {
        let mut config = load("okx_spot");

        config.levels.bids.order_count_type = None;

        assert!(DepthNormalizationCompiler::compile_levels(&config, 1000,).is_err());
    }

    #[test]
    fn rejects_zero_level_limit() {
        let config = load("binance_spot");

        assert!(DepthNormalizationCompiler::compile_levels(&config, 0,).is_err());
    }

    // ========================================================
    // REAL FIXTURE — BINANCE
    // ========================================================

    #[test]
    fn compiled_plan_extracts_real_binance_fixture() {
        let config = load("binance_spot");

        let compiled = DepthNormalizationCompiler::compile_levels(&config, 20_000).unwrap();

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.cache/live/websocket-fixtures/depth")
            .join("binance_spot.json");

        let fixtures: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();

        let message = &fixtures[0];

        let levels = compiled.levels.extract(message).unwrap();

        assert!(!levels.bids.is_empty());
        assert!(!levels.asks.is_empty());
    }

    // ========================================================
    // NO HARDCODED EXCHANGE BEHAVIOR
    // ========================================================

    #[test]
    fn compiled_plan_uses_declared_paths() {
        let mut config = load("binance_spot");

        config.levels.bids.path = "custom.bids".into();
        config.levels.asks.path = "custom.asks".into();

        let compiled = DepthNormalizationCompiler::compile_levels(&config, 100).unwrap();

        let message = serde_json::json!({
            "custom": {
                "bids": [["80000", "1"]],
                "asks": [["80001", "2"]],
            }
        });

        let levels = compiled.levels.extract(&message).unwrap();

        assert_eq!(levels.bids.len(), 1);
        assert_eq!(levels.asks.len(), 1);

        // The parser must use configured paths,
        // not assume exchange-specific JSON keys.
        assert!(JsonExtractor::required(&message, compiled.levels.bids().path.as_str(),).is_ok());
    }
}
