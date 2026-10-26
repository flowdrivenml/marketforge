use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::live::config::{error::ConfigError, loader::load_toml};

use super::{
    common::{
        EnvelopeMappings, FieldMappings, FieldTypes, InstrumentConfig, MessageConfig,
        NormalizationConfig, RecordsConfig, TimestampConfig,
    },
    identity::{FormatIdentity, FormatKind},
};

// ============================================================
// COMPLETE DEPTH FORMAT CONFIGURATION
// ============================================================

/// Describes an exchange-native L2 order-book format.
///
/// The configuration defines extraction, normalization,
/// sequencing, synchronization, and recovery requirements.
///
/// It does not maintain an order book or decode messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthFormatConfig {
    pub schema_version: u32,

    pub format: FormatIdentity,

    pub message: MessageConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<RecordsConfig>,

    pub fields: FieldMappings,

    pub types: FieldTypes,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EnvelopeMappings>,

    pub timestamps: TimestampConfig,

    pub levels: DepthLevelsConfig,

    pub normalization: NormalizationConfig,

    pub instrument: InstrumentConfig,

    pub sequence: DepthSequenceConfig,

    pub synchronization: DepthSynchronizationConfig,

    pub book: DepthBookConfig,

    pub integrity: DepthIntegrityConfig,

    pub recovery: DepthRecoveryConfig,

    pub output: DepthOutputConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<DepthClassificationConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<DepthProtocolConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_type: Option<DepthSymbolTypeConfig>,
}

// ============================================================
// BID / ASK LEVEL EXTRACTION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthLevelsConfig {
    pub bids: DepthLevelSideConfig,
    pub asks: DepthLevelSideConfig,
}

/// Describes how one side of an L2 update is represented.
///
/// Supports:
/// - Array levels: ["price", "quantity"]
/// - Object levels: {"p": "price", "s": "quantity"}
/// - Optional order counts and ignored indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthLevelSideConfig {
    pub path: String,

    pub representation: String,

    pub level_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_index: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_index: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_count_index: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_field: Option<String>,

    pub price_type: String,

    pub quantity_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_count_type: Option<String>,

    pub side: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignored_indices: Vec<usize>,
}

// ============================================================
// NATIVE SEQUENCING
// ============================================================

/// Native exchange sequencing and continuity rules.
///
/// The strategy determines how sequence fields are interpreted.
/// Binance, Bybit, OKX, Bitget, and Gate.io have different
/// continuity guarantees.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceConfig {
    pub strategy: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_field_required: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_update_id_required: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_first_lte_last: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_sequence_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_sequence_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_previous_sequence_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_cross_sequence: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_consecutive_integer_ids: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_consecutive_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_sequence_requires_consecutive: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_previous_sequence_id: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bridge: Option<DepthSequenceBridgeConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset: Option<DepthSequenceResetConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<DepthSequenceMaintenanceConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive: Option<DepthSequenceKeepaliveConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<DepthSequenceIncrementalConfig>,
}

// ============================================================
// SEQUENCE SNAPSHOT BRIDGE
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceBridgeConfig {
    pub enabled: bool,

    pub strategy: String,

    pub on_bridge_failure: String,
}

// ============================================================
// SEQUENCE RESET
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceResetConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_previous_sequence_id: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_sequence_decrease: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_previous_sequence_match: Option<bool>,

    pub action: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_initial_snapshot: Option<bool>,
}

// ============================================================
// MAINTENANCE EXCEPTIONS
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceMaintenanceConfig {
    pub allow_sequence_decrease: bool,

    pub require_previous_sequence_match: bool,
}

// ============================================================
// KEEPALIVE EXCEPTIONS
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceKeepaliveConfig {
    pub enabled: bool,

    pub require_empty_bids: bool,

    pub require_empty_asks: bool,

    pub allow_equal_sequence_id: bool,

    pub book_action: String,
}

// ============================================================
// INCREMENTAL CONTINUITY
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSequenceIncrementalConfig {
    pub stale_action: String,

    pub gap_action: String,

    pub allow_overlapping_ranges: bool,
}

// ============================================================
// SNAPSHOT / DELTA SYNCHRONIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSynchronizationConfig {
    pub strategy: String,

    pub snapshot_source: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_updates_before_snapshot: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_snapshot_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_initial_snapshot: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_message_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_message_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_book_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_before_snapshot_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_sequence_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_retry_condition: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discard_condition: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_discard_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_bridge_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_update_id: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_action: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_only_depths: Vec<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_bridge: Option<DepthSynchronizationBridgeConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_snapshot: Option<DepthWebSocketSnapshotConfig>,
}

// ============================================================
// SYNCHRONIZATION SNAPSHOT BRIDGE
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSynchronizationBridgeConfig {
    pub enabled: bool,

    pub strategy: String,

    pub discard_strategy: String,

    pub on_bridge_failure: String,
}

// ============================================================
// WEBSOCKET SNAPSHOT
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthWebSocketSnapshotConfig {
    pub enabled: bool,

    pub indicator_field: String,

    pub indicator_value: bool,

    pub snapshot_action: String,

    pub sequence_baseline_field: String,

    pub depth_field: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_book_generation: Option<bool>,

    pub require_coverage_tracking: bool,
}

// ============================================================
// DEPTH BOOK STATE
// ============================================================

/// Declares how canonical L2 book state should be maintained.
///
/// These settings describe required behavior.
/// Actual book storage and update application belong to
/// the runtime order-book subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthBookConfig {
    pub storage: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_update_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_action: Option<String>,

    pub zero_quantity_action: String,

    pub atomic_message_updates: bool,

    pub require_valid_generation: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<DepthCoverageConfig>,
}

// ============================================================
// DEPTH COVERAGE
// ============================================================

/// Describes the depth coverage available from an exchange.
///
/// A synchronized book is only authoritative for the
/// depth actually provided by its source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthCoverageConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    pub strategy: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_documented_depth: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_depths: Vec<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_snapshot_source: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_snapshot_depth: Option<bool>,
}

// ============================================================
// MESSAGE CLASSIFICATION
// ============================================================

/// Classifies exchange depth messages as snapshots or deltas.
///
/// Gate.io uses the native `full` indicator to distinguish
/// authoritative book replacements from incremental updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthClassificationConfig {
    pub strategy: String,

    pub full_snapshot_field: String,

    pub full_snapshot_value: bool,

    pub missing_full_means_delta: bool,
}

// ============================================================
// PROTOCOL REQUIREMENTS
// ============================================================

/// Physical WebSocket requirements imposed by a depth format.
///
/// Gate.io Futures requires decimal-size negotiation to
/// preserve fractional native contract quantities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthProtocolConfig {
    pub required_header: String,

    pub required_header_value: String,

    pub require_decimal_size_support: bool,
}

// ============================================================
// DEPTH INTEGRITY
// ============================================================

/// Declarative L2 integrity requirements.
///
/// The runtime decoder and synchronization strategy must
/// enforce these rules before publishing valid book state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthIntegrityConfig {
    pub reject_malformed_json: bool,

    pub reject_missing_required_fields: bool,

    pub reject_invalid_numeric_values: bool,

    pub reject_nonpositive_price: bool,

    pub reject_negative_quantity: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_negative_order_count: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_precision_loss: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_sequence_gaps: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_stale_updates: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_update_id_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_cross_sequence_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_cross_sequence_gaps: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_exact_missing_update_count: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_previous_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_previous_sequence_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_sequence_ranges: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_snapshot_bridge: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_documented_sequence_exceptions: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_authoritative_snapshot_reset: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_violation_action: Option<String>,

    pub checksum_strategy: String,
}

// ============================================================
// DEPTH RECOVERY
// ============================================================

/// Describes the recovery policy for an L2 stream.
///
/// Recovery is executed by a separate runtime component.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthRecoveryConfig {
    pub strategy: String,

    pub on_connection_loss: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_sequence_gap: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_sequence_violation: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_new_snapshot: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_snapshot_fallback: Option<bool>,

    pub require_generation_fencing: bool,
}

// ============================================================
// CANONICAL DEPTH OUTPUT
// ============================================================

/// Declares which native L2 metadata should be retained
/// when publishing canonical depth events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthOutputConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_event_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_event_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_sequence_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_previous_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_sequence_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_previous_sequence_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_cross_sequence: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_event_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_exchange_event_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_message_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_order_count: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_snapshot_depth: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_source_provenance: Option<bool>,

    pub atomic_updates: bool,
}

// ============================================================
// SYMBOL TYPE
// ============================================================

/// Optional Binance Futures symbol-type validation.
///
/// The exchange may include a symbol-type discriminator
/// distinguishing USD-M and COIN-M instruments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSymbolTypeConfig {
    pub source: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_values: Vec<u64>,

    pub missing_policy: String,

    pub mismatch_policy: String,
}

// ============================================================
// DEPTH FORMAT LOADING AND VALIDATION
// ============================================================

impl DepthFormatConfig {
    /// Load a depth format from a TOML file.
    ///
    /// Reuses the generic MarketForge configuration loader.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let config: Self = load_toml(path)?;

        config
            .validate()
            .map_err(|reason| ConfigError::Validation {
                path: path.to_path_buf(),
                reason,
            })?;

        Ok(config)
    }

    /// Returns the fully qualified format identifier.
    ///
    /// Example: depth/binance_spot
    pub fn qualified_id(&self) -> String {
        self.format.qualified_id(FormatKind::Depth)
    }

    /// Performs basic semantic validation.
    ///
    /// Detailed cross-field validation belongs in
    /// format/validation.rs.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported depth format schema version {}",
                self.schema_version
            ));
        }

        self.format.validate(FormatKind::Depth)?;

        if self.fields.is_empty() {
            return Err("depth format requires field mappings".into());
        }

        if self.types.is_empty() {
            return Err("depth format requires field types".into());
        }

        if self.timestamps.canonical_unit != "nanoseconds" {
            return Err("depth format canonical timestamp unit must be nanoseconds".into());
        }

        if self.instrument.exchange != self.format.exchange {
            return Err("depth format instrument exchange does not match format identity".into());
        }

        if self.instrument.market != self.format.market {
            return Err("depth format instrument market does not match format identity".into());
        }

        // ----------------------------------------------------
        // LEVEL EXTRACTION
        // ----------------------------------------------------

        self.validate_level_side(&self.levels.bids, "bid")?;
        self.validate_level_side(&self.levels.asks, "ask")?;

        // ----------------------------------------------------
        // BOOK STATE
        // ----------------------------------------------------

        if self.book.storage.trim().is_empty() {
            return Err("depth book storage cannot be empty".into());
        }

        if self.book.zero_quantity_action != "delete" {
            return Err(format!(
                "unsupported depth zero_quantity_action '{}'",
                self.book.zero_quantity_action
            ));
        }

        if !self.book.atomic_message_updates {
            return Err("depth format requires atomic message updates".into());
        }

        // ----------------------------------------------------
        // OUTPUT
        // ----------------------------------------------------

        if !self.output.atomic_updates {
            return Err("depth format output requires atomic updates".into());
        }

        if let Some(event_type) = &self.output.event_type {
            if event_type != "l2_update" {
                return Err(format!(
                    "unsupported depth output event_type '{}'",
                    event_type
                ));
            }
        }

        if let Some(snapshot_type) = &self.output.snapshot_event_type {
            if snapshot_type != "l2_snapshot" {
                return Err(format!(
                    "unsupported depth snapshot_event_type '{}'",
                    snapshot_type
                ));
            }
        }

        if let Some(delta_type) = &self.output.delta_event_type {
            if delta_type != "l2_update" {
                return Err(format!(
                    "unsupported depth delta_event_type '{}'",
                    delta_type
                ));
            }
        }

        Ok(())
    }

    // ========================================================
    // LEVEL VALIDATION
    // ========================================================

    fn validate_level_side(
        &self,
        side: &DepthLevelSideConfig,
        expected_side: &str,
    ) -> Result<(), String> {
        if side.side != expected_side {
            return Err(format!(
                "depth {} levels declare side '{}'",
                expected_side, side.side
            ));
        }

        if side.path.trim().is_empty() {
            return Err(format!(
                "depth {} level path cannot be empty",
                expected_side
            ));
        }

        if side.representation != "array" {
            return Err(format!(
                "unsupported depth {} level representation '{}'",
                expected_side, side.representation
            ));
        }

        match side.level_type.as_str() {
            "array" => {
                if side.price_index.is_none() {
                    return Err(format!(
                        "depth {} array levels require price_index",
                        expected_side
                    ));
                }

                if side.quantity_index.is_none() {
                    return Err(format!(
                        "depth {} array levels require quantity_index",
                        expected_side
                    ));
                }
            }

            "object" => {
                if side.price_field.as_deref().is_none_or(str::is_empty) {
                    return Err(format!(
                        "depth {} object levels require price_field",
                        expected_side
                    ));
                }

                if side.quantity_field.as_deref().is_none_or(str::is_empty) {
                    return Err(format!(
                        "depth {} object levels require quantity_field",
                        expected_side
                    ));
                }
            }

            other => {
                return Err(format!(
                    "unsupported depth {} level_type '{}'",
                    expected_side, other
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::live::config::format::{
        DepthBookConfig, DepthClassificationConfig, DepthCoverageConfig, DepthFormatConfig,
        DepthIntegrityConfig, DepthLevelSideConfig, DepthLevelsConfig, DepthOutputConfig,
        DepthProtocolConfig, DepthRecoveryConfig, DepthSequenceBridgeConfig, DepthSequenceConfig,
        DepthSequenceIncrementalConfig, DepthSequenceKeepaliveConfig,
        DepthSequenceMaintenanceConfig, DepthSequenceResetConfig, DepthSymbolTypeConfig,
        DepthSynchronizationBridgeConfig, DepthSynchronizationConfig, DepthWebSocketSnapshotConfig,
    };

    // ============================================================
    // TEST HELPERS
    // ============================================================

    fn depth_directory() -> &'static Path {
        Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../data/.config/live/formats/depth"
        ))
    }

    fn load_depth(profile: &str) -> DepthFormatConfig {
        let path = depth_directory().join(format!("{profile}.toml"));

        DepthFormatConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()))
    }

    fn parse<T: serde::de::DeserializeOwned>(input: &str) -> T {
        toml::from_str(input).expect("Failed to deserialize test TOML")
    }

    // ============================================================
    // LEVEL EXTRACTION
    // ============================================================

    #[test]
    fn deserialize_array_level_side() {
        let input = r#"
path = "b"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "bid"
"#;

        let config: DepthLevelSideConfig = parse(input);

        assert_eq!(config.path, "b");
        assert_eq!(config.representation, "array");
        assert_eq!(config.level_type, "array");
        assert_eq!(config.price_index, Some(0));
        assert_eq!(config.quantity_index, Some(1));
        assert_eq!(config.side, "bid");
    }

    #[test]
    fn deserialize_object_level_side() {
        let input = r#"
path = "bids"
representation = "array"
level_type = "object"
price_field = "price"
quantity_field = "size"
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "bid"
"#;

        let config: DepthLevelSideConfig = parse(input);

        assert_eq!(config.level_type, "object");
        assert_eq!(config.price_field.as_deref(), Some("price"));
        assert_eq!(config.quantity_field.as_deref(), Some("size"));
    }

    #[test]
    fn deserialize_level_side_with_order_count() {
        let input = r#"
path = "b"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
order_count_index = 2
price_type = "decimal_string"
quantity_type = "decimal_string"
order_count_type = "u64"
side = "bid"
ignored_indices = [3]
"#;

        let config: DepthLevelSideConfig = parse(input);

        assert_eq!(config.order_count_index, Some(2));
        assert_eq!(config.order_count_type.as_deref(), Some("u64"));
        assert_eq!(config.ignored_indices, vec![3]);
    }

    #[test]
    fn reject_missing_level_path() {
        let input = r#"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "bid"
"#;

        assert!(toml::from_str::<DepthLevelSideConfig>(input).is_err());
    }

    #[test]
    fn reject_unknown_level_field() {
        let input = r#"
path = "b"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "bid"
unknown = true
"#;

        assert!(toml::from_str::<DepthLevelSideConfig>(input).is_err());
    }

    #[test]
    fn deserialize_complete_levels() {
        let input = r#"
[bids]
path = "b"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "bid"

[asks]
path = "a"
representation = "array"
level_type = "array"
price_index = 0
quantity_index = 1
price_type = "decimal_string"
quantity_type = "decimal_string"
side = "ask"
"#;

        let config: DepthLevelsConfig = parse(input);

        assert_eq!(config.bids.side, "bid");
        assert_eq!(config.asks.side, "ask");
    }

    // ============================================================
    // SEQUENCING
    // ============================================================

    #[test]
    fn deserialize_minimal_sequence() {
        let config: DepthSequenceConfig = parse(
            r#"
strategy = "monotonic"
"#,
        );

        assert_eq!(config.strategy, "monotonic");
        assert!(config.first_field.is_none());
        assert!(config.snapshot_bridge.is_none());
    }

    #[test]
    fn deserialize_binance_sequence() {
        let input = r#"
strategy = "binance_update_range"
first_field = "first_update_id"
last_field = "last_update_id"
previous_field = "previous_update_id"
validate_range = true
require_first_lte_last = true
preserve_native_sequence_range = true
"#;

        let config: DepthSequenceConfig = parse(input);

        assert_eq!(config.first_field.as_deref(), Some("first_update_id"));
        assert_eq!(config.last_field.as_deref(), Some("last_update_id"));
        assert_eq!(config.previous_field.as_deref(), Some("previous_update_id"));
        assert_eq!(config.validate_range, Some(true));
    }

    #[test]
    fn deserialize_sequence_bridge() {
        let input = r#"
enabled = true
strategy = "binance_snapshot_bridge"
on_bridge_failure = "restart"
"#;

        let config: DepthSequenceBridgeConfig = parse(input);

        assert!(config.enabled);
        assert_eq!(config.strategy, "binance_snapshot_bridge");
        assert_eq!(config.on_bridge_failure, "restart");
    }

    #[test]
    fn deserialize_sequence_reset() {
        let input = r#"
reset_previous_sequence_id = -1
allow_sequence_decrease = true
require_previous_sequence_match = false
action = "replace_book"
allow_initial_snapshot = true
"#;

        let config: DepthSequenceResetConfig = parse(input);

        assert_eq!(config.reset_previous_sequence_id, Some(-1));
        assert_eq!(config.allow_sequence_decrease, Some(true));
        assert_eq!(config.action, "replace_book");
    }

    #[test]
    fn deserialize_sequence_maintenance() {
        let input = r#"
allow_sequence_decrease = true
require_previous_sequence_match = false
"#;

        let config: DepthSequenceMaintenanceConfig = parse(input);

        assert!(config.allow_sequence_decrease);
        assert!(!config.require_previous_sequence_match);
    }

    #[test]
    fn deserialize_sequence_keepalive() {
        let input = r#"
enabled = true
require_empty_bids = true
require_empty_asks = true
allow_equal_sequence_id = true
book_action = "no_change"
"#;

        let config: DepthSequenceKeepaliveConfig = parse(input);

        assert!(config.enabled);
        assert!(config.require_empty_bids);
        assert!(config.require_empty_asks);
        assert!(config.allow_equal_sequence_id);
        assert_eq!(config.book_action, "no_change");
    }

    #[test]
    fn deserialize_incremental_continuity() {
        let input = r#"
stale_action = "discard"
gap_action = "resynchronize"
allow_overlapping_ranges = true
"#;

        let config: DepthSequenceIncrementalConfig = parse(input);

        assert_eq!(config.stale_action, "discard");
        assert_eq!(config.gap_action, "resynchronize");
        assert!(config.allow_overlapping_ranges);
    }

    #[test]
    fn reject_missing_sequence_strategy() {
        assert!(
            toml::from_str::<DepthSequenceConfig>("first_field = \"first_update_id\"").is_err()
        );
    }

    // ============================================================
    // SYNCHRONIZATION
    // ============================================================

    #[test]
    fn deserialize_rest_synchronization() {
        let input = r#"
strategy = "rest_snapshot_bridge"
snapshot_source = "rest"
snapshot_endpoint = "depth_snapshot"
snapshot_sequence_field = "lastUpdateId"
buffer_updates_before_snapshot = true
require_snapshot_id = true
"#;

        let config: DepthSynchronizationConfig = parse(input);

        assert_eq!(config.snapshot_source, "rest");
        assert_eq!(config.snapshot_endpoint.as_deref(), Some("depth_snapshot"));
        assert_eq!(config.buffer_updates_before_snapshot, Some(true));
    }

    #[test]
    fn deserialize_websocket_synchronization() {
        let input = r#"
strategy = "websocket_snapshot_delta"
snapshot_source = "websocket"
require_initial_snapshot = true
snapshot_action = "snapshot"
delta_action = "update"
"#;

        let config: DepthSynchronizationConfig = parse(input);

        assert_eq!(config.snapshot_source, "websocket");
        assert_eq!(config.require_initial_snapshot, Some(true));
        assert_eq!(config.snapshot_action.as_deref(), Some("snapshot"));
    }

    #[test]
    fn deserialize_synchronization_bridge() {
        let input = r#"
enabled = true
strategy = "sequence_bridge"
discard_strategy = "discard_old"
on_bridge_failure = "restart"
"#;

        let config: DepthSynchronizationBridgeConfig = parse(input);

        assert!(config.enabled);
        assert_eq!(config.strategy, "sequence_bridge");
        assert_eq!(config.discard_strategy, "discard_old");
    }

    #[test]
    fn deserialize_websocket_snapshot() {
        let input = r#"
enabled = true
indicator_field = "full"
indicator_value = true
snapshot_action = "replace_book"
sequence_baseline_field = "last_update_id"
depth_field = "depth"
new_book_generation = true
require_coverage_tracking = true
"#;

        let config: DepthWebSocketSnapshotConfig = parse(input);

        assert!(config.enabled);
        assert_eq!(config.indicator_field, "full");
        assert!(config.indicator_value);
        assert_eq!(config.new_book_generation, Some(true));
        assert!(config.require_coverage_tracking);
    }

    #[test]
    fn reject_missing_synchronization_strategy() {
        let input = r#"
snapshot_source = "rest"
snapshot_endpoint = "depth_snapshot"
"#;

        assert!(toml::from_str::<DepthSynchronizationConfig>(input).is_err());
    }

    // ============================================================
    // BOOK STATE
    // ============================================================

    #[test]
    fn deserialize_minimal_book() {
        let input = r#"
storage = "price_level_map"
zero_quantity_action = "delete"
atomic_message_updates = true
require_valid_generation = true
"#;

        let config: DepthBookConfig = parse(input);

        assert_eq!(config.storage, "price_level_map");
        assert_eq!(config.zero_quantity_action, "delete");
        assert!(config.atomic_message_updates);
        assert!(config.require_valid_generation);
        assert!(config.depth.is_none());
    }

    #[test]
    fn deserialize_depth_coverage() {
        let input = r#"
source = "snapshot"
strategy = "bounded"
maximum_documented_depth = 5000
supported_depths = [50, 200, 1000]
track_snapshot_source = true
track_snapshot_depth = true
"#;

        let config: DepthCoverageConfig = parse(input);

        assert_eq!(config.strategy, "bounded");
        assert_eq!(config.maximum_documented_depth, Some(5000));
        assert_eq!(config.supported_depths, vec![50, 200, 1000]);
        assert_eq!(config.track_snapshot_source, Some(true));
    }

    #[test]
    fn reject_missing_book_storage() {
        let input = r#"
zero_quantity_action = "delete"
atomic_message_updates = true
require_valid_generation = true
"#;

        assert!(toml::from_str::<DepthBookConfig>(input).is_err());
    }

    // ============================================================
    // CLASSIFICATION
    // ============================================================

    #[test]
    fn deserialize_depth_classification() {
        let input = r#"
strategy = "full_flag"
full_snapshot_field = "full"
full_snapshot_value = true
missing_full_means_delta = true
"#;

        let config: DepthClassificationConfig = parse(input);

        assert_eq!(config.strategy, "full_flag");
        assert_eq!(config.full_snapshot_field, "full");
        assert!(config.full_snapshot_value);
        assert!(config.missing_full_means_delta);
    }

    // ============================================================
    // PROTOCOL
    // ============================================================

    #[test]
    fn deserialize_depth_protocol() {
        let input = r#"
required_header = "X-Gate-Size-Decimal"
required_header_value = "1"
require_decimal_size_support = true
"#;

        let config: DepthProtocolConfig = parse(input);

        assert_eq!(config.required_header, "X-Gate-Size-Decimal");
        assert_eq!(config.required_header_value, "1");
        assert!(config.require_decimal_size_support);
    }

    // ============================================================
    // INTEGRITY
    // ============================================================

    #[test]
    fn deserialize_depth_integrity() {
        let input = r#"
reject_malformed_json = true
reject_missing_required_fields = true
reject_invalid_numeric_values = true
reject_nonpositive_price = true
reject_negative_quantity = true
detect_sequence_gaps = true
detect_stale_updates = true
gap_action = "resynchronize"
checksum_strategy = "none"
"#;

        let config: DepthIntegrityConfig = parse(input);

        assert!(config.reject_malformed_json);
        assert!(config.reject_negative_quantity);
        assert_eq!(config.detect_sequence_gaps, Some(true));
        assert_eq!(config.gap_action.as_deref(), Some("resynchronize"));
        assert_eq!(config.checksum_strategy, "none");
    }

    #[test]
    fn reject_missing_depth_checksum_strategy() {
        let input = r#"
reject_malformed_json = true
reject_missing_required_fields = true
reject_invalid_numeric_values = true
reject_nonpositive_price = true
reject_negative_quantity = true
"#;

        assert!(toml::from_str::<DepthIntegrityConfig>(input).is_err());
    }

    // ============================================================
    // RECOVERY
    // ============================================================

    #[test]
    fn deserialize_depth_recovery() {
        let input = r#"
strategy = "resynchronize"
on_connection_loss = "invalidate"
on_sequence_gap = "restart"
on_sequence_violation = "restart"
rest_snapshot_fallback = true
require_generation_fencing = true
"#;

        let config: DepthRecoveryConfig = parse(input);

        assert_eq!(config.strategy, "resynchronize");
        assert_eq!(config.on_connection_loss, "invalidate");
        assert_eq!(config.on_sequence_gap.as_deref(), Some("restart"));
        assert_eq!(config.rest_snapshot_fallback, Some(true));
        assert!(config.require_generation_fencing);
    }

    #[test]
    fn reject_missing_recovery_generation_fencing() {
        let input = r#"
strategy = "resynchronize"
on_connection_loss = "invalidate"
"#;

        assert!(toml::from_str::<DepthRecoveryConfig>(input).is_err());
    }

    // ============================================================
    // OUTPUT
    // ============================================================

    #[test]
    fn deserialize_depth_output() {
        let input = r#"
event_type = "l2_update"
snapshot_event_type = "l2_snapshot"
delta_event_type = "l2_update"
preserve_native_sequence_range = true
preserve_event_timestamp = true
atomic_updates = true
"#;

        let config: DepthOutputConfig = parse(input);

        assert_eq!(config.event_type.as_deref(), Some("l2_update"));
        assert_eq!(config.snapshot_event_type.as_deref(), Some("l2_snapshot"));
        assert_eq!(config.preserve_native_sequence_range, Some(true));
        assert!(config.atomic_updates);
    }

    #[test]
    fn reject_missing_output_atomic_updates() {
        let input = r#"
event_type = "l2_update"
"#;

        assert!(toml::from_str::<DepthOutputConfig>(input).is_err());
    }

    // ============================================================
    // SYMBOL TYPE
    // ============================================================

    #[test]
    fn deserialize_depth_symbol_type() {
        let input = r#"
source = "symbol_type"
expected_values = [1, 2]
missing_policy = "reject"
mismatch_policy = "reject"
"#;

        let config: DepthSymbolTypeConfig = parse(input);

        assert_eq!(config.source, "symbol_type");
        assert_eq!(config.expected_values, vec![1, 2]);
    }

    // ============================================================
    // REAL DEPTH FORMAT CONFIGURATIONS
    // ============================================================

    #[test]
    fn depth_format_rejects_wrong_schema_version() {
        let mut config = load_depth("binance_spot");

        config.schema_version = 999;

        assert!(config.validate().is_err());
    }

    #[test]
    fn depth_format_rejects_wrong_instrument_exchange() {
        use crate::live::config::exchange::ExchangeId;

        let mut config = load_depth("binance_spot");

        config.instrument.exchange = ExchangeId::Bybit;

        assert!(config.validate().is_err());
    }

    #[test]
    fn depth_format_rejects_wrong_instrument_market() {
        use crate::live::config::exchange::MarketType;

        let mut config = load_depth("binance_spot");

        config.instrument.market = MarketType::Linear;

        assert!(config.validate().is_err());
    }

    #[test]
    fn depth_format_rejects_wrong_canonical_timestamp_unit() {
        let mut config = load_depth("binance_spot");

        config.timestamps.canonical_unit = "milliseconds".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("canonical timestamp unit"),
            "Unexpected validation error: {error}"
        );
    }

    // ============================================================
    // LEVEL VALIDATION
    // ============================================================

    #[test]
    fn depth_format_rejects_wrong_bid_side() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.side = "ask".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("bid"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_wrong_ask_side() {
        let mut config = load_depth("binance_spot");

        config.levels.asks.side = "bid".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("ask"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_empty_bid_path() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.path.clear();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("bid level path"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_empty_ask_path() {
        let mut config = load_depth("binance_spot");

        config.levels.asks.path.clear();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("ask level path"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_unsupported_level_representation() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.representation = "binary".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("representation"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_missing_array_price_index() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.level_type = "array".into();
        config.levels.bids.price_index = None;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("price_index"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_missing_array_quantity_index() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.level_type = "array".into();
        config.levels.bids.quantity_index = None;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("quantity_index"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_missing_object_price_field() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.level_type = "object".into();
        config.levels.bids.price_field = None;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("price_field"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_missing_object_quantity_field() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.level_type = "object".into();

        // Satisfy the price-field requirement so that
        // validation reaches the missing quantity field.
        config.levels.bids.price_field = Some("price".into());

        // Intentionally leave quantity_field missing.
        config.levels.bids.quantity_field = None;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("quantity_field"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_unsupported_level_type() {
        let mut config = load_depth("binance_spot");

        config.levels.bids.level_type = "unknown".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("level_type"),
            "Unexpected validation error: {error}"
        );
    }

    // ============================================================
    // BOOK VALIDATION
    // ============================================================

    #[test]
    fn depth_format_rejects_empty_book_storage() {
        let mut config = load_depth("binance_spot");

        config.book.storage.clear();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("book storage"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_unsupported_zero_quantity_action() {
        let mut config = load_depth("binance_spot");

        config.book.zero_quantity_action = "ignore".into();

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("zero_quantity_action"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_requires_atomic_message_updates() {
        let mut config = load_depth("binance_spot");

        config.book.atomic_message_updates = false;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("atomic message updates"),
            "Unexpected validation error: {error}"
        );
    }

    // ============================================================
    // OUTPUT VALIDATION
    // ============================================================

    #[test]
    fn depth_format_requires_atomic_output() {
        let mut config = load_depth("binance_spot");

        config.output.atomic_updates = false;

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("atomic updates"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_wrong_output_event_type() {
        let mut config = load_depth("binance_spot");

        config.output.event_type = Some("trade".into());

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("event_type"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_wrong_snapshot_event_type() {
        let mut config = load_depth("binance_spot");

        config.output.snapshot_event_type = Some("trade".into());

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("snapshot_event_type"),
            "Unexpected validation error: {error}"
        );
    }

    #[test]
    fn depth_format_rejects_wrong_delta_event_type() {
        let mut config = load_depth("binance_spot");

        config.output.delta_event_type = Some("trade".into());

        let error = config.validate().unwrap_err();

        assert!(
            error.contains("delta_event_type"),
            "Unexpected validation error: {error}"
        );
    }

    // ============================================================
    // STRICT DESERIALIZATION
    // ============================================================

    #[test]
    fn reject_unknown_depth_sequence_field() {
        let input = r#"
strategy = "monotonic"
unexpected = true
"#;

        assert!(toml::from_str::<DepthSequenceConfig>(input).is_err());
    }

    #[test]
    fn reject_unknown_depth_synchronization_field() {
        let input = r#"
strategy = "websocket_snapshot_delta"
snapshot_source = "websocket"
unexpected = true
"#;

        assert!(toml::from_str::<DepthSynchronizationConfig>(input).is_err());
    }

    #[test]
    fn reject_unknown_depth_book_field() {
        let input = r#"
storage = "price_level_map"
zero_quantity_action = "delete"
atomic_message_updates = true
require_valid_generation = true
unexpected = true
"#;

        assert!(toml::from_str::<DepthBookConfig>(input).is_err());
    }

    #[test]
    fn reject_unknown_depth_recovery_field() {
        let input = r#"
strategy = "resynchronize"
on_connection_loss = "invalidate"
require_generation_fencing = true
unexpected = true
"#;

        assert!(toml::from_str::<DepthRecoveryConfig>(input).is_err());
    }

    #[test]
    fn reject_unknown_depth_output_field() {
        let input = r#"
atomic_updates = true
unexpected = true
"#;

        assert!(toml::from_str::<DepthOutputConfig>(input).is_err());
    }
}
