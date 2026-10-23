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
