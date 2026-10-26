use crate::live::config::exchange::{ExchangeId, MarketType};
use crate::live::config::format::DepthFormatConfig;
use crate::live::infrastructure::normalization::depth::compiler::DepthNormalizationCompiler;
use crate::live::infrastructure::normalization::depth::plan::DepthLevelPlan;
use crate::live::infrastructure::normalization::depth::rest::plan::{
    RestDepthLevelMapping, compile_rest_depth_levels,
};
use crate::live::infrastructure::normalization::numeric::TimestampUnit;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum RestDepthCompileError {
    #[error("invalid REST endpoint: {0}")]
    Endpoint(String),

    #[error("invalid REST depth mapping: {0}")]
    Mapping(String),

    #[error("REST depth level compilation failed: {0}")]
    Levels(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestDepthRecordContainer {
    Object,
    SingleObjectArray,
}
// ============================================================
// SNAPSHOT SEQUENCE SEMANTICS
// ============================================================

/// These are native fields, not synchronization decisions.
///
/// Integrity determines whether a particular snapshot is
/// suitable for establishing a new book generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestDepthSequencePlan {
    pub sequence_path: Option<String>,
    pub update_id_path: Option<String>,
    pub cross_sequence_path: Option<String>,
}

impl RestDepthSequencePlan {
    pub fn has_baseline(&self) -> bool {
        self.sequence_path.is_some() || self.update_id_path.is_some()
    }
}

// ============================================================
// COMPILED REST SNAPSHOT PLAN
// ============================================================

#[derive(Debug, Clone)]
pub struct RestDepthNormalizationPlan {
    pub format_id: String,

    /// Root containing the native snapshot.
    ///
    /// "$" means the entire REST response.
    pub snapshot_root: String,

    pub levels: DepthLevelPlan,

    pub sequence: RestDepthSequencePlan,

    /// Native response field containing the instrument.
    /// None means the REST response does not repeat it.
    pub symbol_path: Option<String>,

    /// Exchange-provided timestamp, if explicitly mapped.
    /// A transport receive timestamp is not equivalent.
    pub timestamp_path: Option<String>,

    /// Maximum number of levels permitted in one snapshot.
    pub max_levels: usize,
    pub record_container: RestDepthRecordContainer,
    pub exchange: ExchangeId,
    pub market: MarketType,
    /// Unit of the native REST response timestamp.
    ///
    /// None means this endpoint does not expose a mapped
    /// exchange timestamp.
    pub timestamp_unit: Option<TimestampUnit>,
}

// ============================================================
// COMPILER
// ============================================================

pub struct RestDepthNormalizationCompiler;

impl RestDepthNormalizationCompiler {
    pub fn compile<E: Serialize>(
        depth_format: &DepthFormatConfig,
        rest_endpoint: &E,
        max_levels: usize,
    ) -> Result<RestDepthNormalizationPlan, RestDepthCompileError> {
        if max_levels == 0 {
            return Err(RestDepthCompileError::Mapping(
                "max_levels must be positive".into(),
            ));
        }

        // Reuse the real endpoint configuration.
        // Do not maintain another independent REST schema.
        let endpoint = serde_json::to_value(rest_endpoint)
            .map_err(|error| RestDepthCompileError::Endpoint(error.to_string()))?;

        if endpoint.get("method").and_then(Value::as_str) != Some("GET") {
            return Err(RestDepthCompileError::Endpoint(
                "depth_snapshot must use GET".into(),
            ));
        }

        let response_root = optional_string(&endpoint, "response_root")?.unwrap_or("$");

        let snapshot_root = normalize_root(response_root)?;
        let record_container = match optional_string(&endpoint, "response_record_container")? {
            Some("object") => RestDepthRecordContainer::Object,

            Some("single_object_array") => RestDepthRecordContainer::SingleObjectArray,

            None if snapshot_root == "$" => RestDepthRecordContainer::Object,

            None => {
                return Err(RestDepthCompileError::Mapping(
                    "nested REST snapshot requires \
             response_record_container"
                        .into(),
                ));
            }

            Some(other) => {
                return Err(RestDepthCompileError::Mapping(format!(
                    "unsupported REST record container '{other}'"
                )));
            }
        };
        let snapshot_object_path = match record_container {
            RestDepthRecordContainer::Object => snapshot_root.clone(),

            RestDepthRecordContainer::SingleObjectArray => {
                if snapshot_root == "$" {
                    "0".to_owned()
                } else {
                    format!("{snapshot_root}.0")
                }
            }
        };
        // Compile and reuse the WebSocket level layouts.
        //
        // At this stage we reuse level element positions and
        // object field names, not WebSocket collection paths.
        let websocket = DepthNormalizationCompiler::compile_levels(depth_format, max_levels)
            .map_err(|error| RestDepthCompileError::Levels(error.to_string()))?;

        // REST endpoint declarations take precedence.
        //
        // For endpoints without explicit bid/ask mappings,
        // use the documented response convention:
        //
        // - Root snapshots: bids / asks
        // - Nested snapshots: retain native WS field names
        //
        // These defaults are checked against the real
        // REST fixtures in the integration suite.
        let default_bids = if snapshot_root == "$" {
            "bids"
        } else {
            trailing_field(&depth_format.levels.bids.path)?
        };

        let default_asks = if snapshot_root == "$" {
            "asks"
        } else {
            trailing_field(&depth_format.levels.asks.path)?
        };

        let bids_field = optional_string(&endpoint, "response_bids_field")?.unwrap_or(default_bids);

        let asks_field = optional_string(&endpoint, "response_asks_field")?.unwrap_or(default_asks);

        let bids_path = join_path(&snapshot_object_path, bids_field)?;

        let asks_path = join_path(&snapshot_object_path, asks_field)?;

        if bids_path == asks_path {
            return Err(RestDepthCompileError::Mapping(
                "bid and ask paths cannot be identical".into(),
            ));
        }

        // Reject explicitly declared REST level layouts
        // that disagree with the reusable native layout.
        if let Some(level_type) = optional_string(&endpoint, "response_level_type")? {
            let bids_layout = &websocket.levels.bids().layout;
            let asks_layout = &websocket.levels.asks().layout;

            let compatible = match level_type {
                "array" => {
                    matches!(bids_layout, crate::live::infrastructure::normalization::depth::plan::NativeDepthLevelLayout::Array { .. })
                        && matches!(asks_layout, crate::live::infrastructure::normalization::depth::plan::NativeDepthLevelLayout::Array { .. })
                }

                "object" => {
                    matches!(bids_layout, crate::live::infrastructure::normalization::depth::plan::NativeDepthLevelLayout::Object { .. })
                        && matches!(asks_layout, crate::live::infrastructure::normalization::depth::plan::NativeDepthLevelLayout::Object { .. })
                }

                _ => false,
            };

            if !compatible {
                return Err(RestDepthCompileError::Mapping(format!(
                    "REST level_type '{level_type}' \
                         conflicts with compiled level layout"
                )));
            }

            // When object field names are explicitly
            // declared for REST, require exact agreement.
            if level_type == "object" {
                for layout in [bids_layout, asks_layout] {
                    if let crate::live::infrastructure::normalization::depth::plan::NativeDepthLevelLayout::Object {
                        price_field,
                        quantity_field,
                        ..
                    } = layout
                    {
                        if let Some(expected) = optional_string(&endpoint, "response_price_field")?
                        {
                            if price_field != expected {
                                return Err(RestDepthCompileError::Mapping(
                                    "REST price field mismatch".into(),
                                ));
                            }
                        }

                        if let Some(expected) =
                            optional_string(&endpoint, "response_quantity_field")?
                        {
                            if quantity_field != expected {
                                return Err(RestDepthCompileError::Mapping(
                                    "REST quantity field mismatch".into(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        let levels = compile_rest_depth_levels(
            &websocket.levels,
            RestDepthLevelMapping {
                bids_path,
                asks_path,
            },
            max_levels,
        )
        .map_err(|error| RestDepthCompileError::Levels(error.to_string()))?;

        let sequence = RestDepthSequencePlan {
            sequence_path: mapped_path(
                &endpoint,
                "response_sequence_field",
                &snapshot_object_path,
            )?,

            update_id_path: mapped_path(
                &endpoint,
                "response_update_id_field",
                &snapshot_object_path,
            )?,

            cross_sequence_path: mapped_path(
                &endpoint,
                "response_cross_sequence_field",
                &snapshot_object_path,
            )?,
        };

        if sequence.sequence_path.is_some() && sequence.update_id_path.is_some() {
            return Err(RestDepthCompileError::Mapping(
                "ambiguous primary snapshot sequence mapping".into(),
            ));
        }

        let symbol_path = mapped_path(&endpoint, "response_symbol_field", &snapshot_object_path)?;

        let timestamp_path =
            mapped_path(&endpoint, "response_timestamp_field", &snapshot_object_path)?;
        let timestamp_unit = match optional_string(&endpoint, "response_timestamp_unit")? {
            Some("seconds") => Some(TimestampUnit::Seconds),

            Some("milliseconds") => Some(TimestampUnit::Milliseconds),

            Some("microseconds") => Some(TimestampUnit::Microseconds),

            Some("nanoseconds") => Some(TimestampUnit::Nanoseconds),

            Some(other) => {
                return Err(RestDepthCompileError::Mapping(format!(
                    "unsupported REST timestamp unit '{other}'"
                )));
            }

            None => None,
        };

        // The timestamp field and its unit form one
        // inseparable configuration contract.
        match (&timestamp_path, timestamp_unit) {
            (Some(_), None) => {
                return Err(RestDepthCompileError::Mapping(
                    "REST timestamp field requires an explicit unit".into(),
                ));
            }

            (None, Some(_)) => {
                return Err(RestDepthCompileError::Mapping(
                    "REST timestamp unit declared without a field".into(),
                ));
            }

            _ => {}
        }
        Ok(RestDepthNormalizationPlan {
            format_id: depth_format.format.id.clone(),

            exchange: depth_format.format.exchange.clone(),
            market: depth_format.format.market.clone(),

            snapshot_root,
            record_container,

            levels,
            sequence,
            symbol_path,
            timestamp_path,
            timestamp_unit,

            max_levels,
        })
    }
}

// ============================================================
// CONFIGURATION FIELD HELPERS
// ============================================================

fn optional_string<'a>(
    endpoint: &'a Value,
    key: &str,
) -> Result<Option<&'a str>, RestDepthCompileError> {
    match endpoint.get(key) {
        None | Some(Value::Null) => Ok(None),

        Some(Value::String(value)) if !value.trim().is_empty() && value.trim() == value => {
            Ok(Some(value))
        }

        _ => Err(RestDepthCompileError::Endpoint(format!(
            "{key} must be a nonempty string"
        ))),
    }
}

fn normalize_root(root: &str) -> Result<String, RestDepthCompileError> {
    let root = root.strip_prefix("$.").unwrap_or(root);

    if root.is_empty() {
        return Err(RestDepthCompileError::Mapping("empty response root".into()));
    }

    // The current REST snapshot contract expects exactly
    // one snapshot record when response_root points to
    // a native array (OKX data[0]).
    //
    // Do not guess between arbitrary multiple snapshots.
    let root = if root == "data" {
        // The caller must distinguish object-vs-array
        // response roots. The known Bitget/OKX schemas
        // have different record container types, so
        // this is intentionally left unchanged here.
        root.to_owned()
    } else {
        root.to_owned()
    };

    Ok(root)
}

fn trailing_field(path: &str) -> Result<&str, RestDepthCompileError> {
    path.rsplit('.')
        .next()
        .filter(|field| !field.is_empty())
        .ok_or_else(|| {
            RestDepthCompileError::Mapping(format!("invalid native level path '{path}'"))
        })
}

fn join_path(root: &str, field: &str) -> Result<String, RestDepthCompileError> {
    if field.trim().is_empty() || field.starts_with('.') {
        return Err(RestDepthCompileError::Mapping(format!(
            "invalid response field '{field}'"
        )));
    }

    if field == "$" {
        return Err(RestDepthCompileError::Mapping(
            "response field cannot refer to entire root".into(),
        ));
    }

    if field.starts_with("$.") {
        return Ok(field.to_owned());
    }

    if root == "$" {
        Ok(field.to_owned())
    } else {
        Ok(format!("{root}.{field}"))
    }
}

fn mapped_path(
    endpoint: &Value,
    key: &str,
    root: &str,
) -> Result<Option<String>, RestDepthCompileError> {
    optional_string(endpoint, key)?
        .map(|field| join_path(root, field))
        .transpose()
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn project_data() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live")
    }

    fn load_depth(profile: &str) -> DepthFormatConfig {
        let path = project_data()
            .join("formats/depth")
            .join(format!("{profile}.toml"));

        DepthFormatConfig::load(path).unwrap()
    }

    fn load_rest_endpoint(profile: &str) -> Value {
        let path = project_data()
            .join("exchanges")
            .join(format!("{profile}.toml"));

        let contents = std::fs::read_to_string(path).unwrap();

        let config: toml::Value = toml::from_str(&contents).unwrap();

        serde_json::to_value(&config["rest"]["endpoints"]["depth_snapshot"]).unwrap()
    }

    fn compile(profile: &str) -> RestDepthNormalizationPlan {
        RestDepthNormalizationCompiler::compile(
            &load_depth(profile),
            &load_rest_endpoint(profile),
            20_000,
        )
        .unwrap()
    }

    #[test]
    fn compiles_all_fifteen_rest_depth_profiles() {
        for exchange in ["binance", "bybit", "bitget", "gateio", "okx"] {
            for market in ["spot", "linear", "inverse"] {
                let profile = format!("{exchange}_{market}");
                let plan = compile(&profile);

                assert_eq!(plan.format_id, profile);

                assert!(!plan.levels.bids().path.is_empty());
                assert!(!plan.levels.asks().path.is_empty());
            }
        }
    }

    #[test]
    fn binance_uses_last_update_id() {
        let plan = compile("binance_spot");

        assert_eq!(plan.sequence.sequence_path.as_deref(), Some("lastUpdateId"),);

        assert_eq!(plan.levels.bids().path, "bids");
        assert_eq!(plan.levels.asks().path, "asks");
    }

    #[test]
    fn bybit_preserves_update_and_cross_sequence() {
        let plan = compile("bybit_linear");

        assert_eq!(plan.sequence.update_id_path.as_deref(), Some("result.u"),);

        assert_eq!(
            plan.sequence.cross_sequence_path.as_deref(),
            Some("result.seq"),
        );

        assert_eq!(plan.symbol_path.as_deref(), Some("result.s"),);
    }

    #[test]
    fn bitget_snapshot_has_no_sequence_baseline() {
        for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
            let plan = compile(profile);

            assert!(
                !plan.sequence.has_baseline(),
                "{profile}: invented REST sequence baseline"
            );
        }
    }

    #[test]
    fn gateio_uses_snapshot_id() {
        let plan = compile("gateio_inverse");

        assert_eq!(plan.sequence.sequence_path.as_deref(), Some("id"),);

        assert_eq!(plan.levels.bids().path, "bids");
        assert_eq!(plan.levels.asks().path, "asks");
    }

    #[test]
    fn rejects_non_get_depth_endpoint() {
        let mut endpoint = load_rest_endpoint("binance_spot");

        endpoint["method"] = Value::String("POST".into());

        assert!(
            RestDepthNormalizationCompiler::compile(&load_depth("binance_spot"), &endpoint, 1000,)
                .is_err()
        );
    }

    #[test]
    fn rejects_zero_level_limit() {
        assert!(
            RestDepthNormalizationCompiler::compile(
                &load_depth("binance_spot"),
                &load_rest_endpoint("binance_spot"),
                0,
            )
            .is_err()
        );
    }
    #[test]
    fn okx_compiles_single_object_array_paths() {
        for profile in ["okx_spot", "okx_linear", "okx_inverse"] {
            let plan = compile(profile);

            assert_eq!(
                plan.record_container,
                RestDepthRecordContainer::SingleObjectArray,
            );

            assert_eq!(plan.levels.bids().path, "data.0.bids",);

            assert_eq!(plan.levels.asks().path, "data.0.asks",);

            assert_eq!(plan.sequence.sequence_path.as_deref(), Some("data.0.seqId"),);
        }
    }

    #[test]
    fn bitget_compiles_object_paths_without_sequence() {
        for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
            let plan = compile(profile);

            assert_eq!(plan.record_container, RestDepthRecordContainer::Object,);

            assert_eq!(plan.levels.bids().path, "data.b");
            assert_eq!(plan.levels.asks().path, "data.a");

            assert!(!plan.sequence.has_baseline());

            assert_eq!(plan.timestamp_path.as_deref(), Some("data.ts"),);
        }
    }

    #[test]
    fn rejects_undeclared_nested_record_container() {
        let mut endpoint = load_rest_endpoint("okx_spot");

        endpoint
            .as_object_mut()
            .unwrap()
            .remove("response_record_container");

        let result =
            RestDepthNormalizationCompiler::compile(&load_depth("okx_spot"), &endpoint, 1000);

        assert!(result.is_err());
    }
    #[test]
    fn bitget_compiles_explicit_millisecond_timestamps() {
        for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
            let plan = compile(profile);

            assert_eq!(plan.timestamp_path.as_deref(), Some("data.ts"),);

            assert_eq!(plan.timestamp_unit, Some(TimestampUnit::Milliseconds),);
        }
    }

    #[test]
    fn timestamp_field_without_unit_is_rejected() {
        let mut endpoint = load_rest_endpoint("bitget_spot");

        endpoint
            .as_object_mut()
            .unwrap()
            .remove("response_timestamp_unit");

        let result =
            RestDepthNormalizationCompiler::compile(&load_depth("bitget_spot"), &endpoint, 1000);

        assert!(result.is_err(), "timestamp field without unit was accepted");
    }

    #[test]
    fn timestamp_unit_without_field_is_rejected() {
        let mut endpoint = load_rest_endpoint("binance_spot");

        endpoint["response_timestamp_unit"] = Value::String("milliseconds".into());

        let result =
            RestDepthNormalizationCompiler::compile(&load_depth("binance_spot"), &endpoint, 1000);

        assert!(result.is_err(), "orphaned timestamp unit was accepted");
    }

    #[test]
    fn unsupported_timestamp_unit_is_rejected() {
        let mut endpoint = load_rest_endpoint("bitget_spot");

        endpoint["response_timestamp_unit"] = Value::String("minutes".into());

        let result =
            RestDepthNormalizationCompiler::compile(&load_depth("bitget_spot"), &endpoint, 1000);

        assert!(result.is_err());
    }

    #[test]
    fn endpoints_without_timestamps_remain_explicitly_unmapped() {
        for profile in ["binance_spot", "bybit_spot", "gateio_spot", "okx_spot"] {
            let plan = compile(profile);

            assert_eq!(plan.timestamp_path, None);
            assert_eq!(plan.timestamp_unit, None);
        }
    }
}
