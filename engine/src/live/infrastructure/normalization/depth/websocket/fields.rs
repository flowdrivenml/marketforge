use serde_json::Value;
use thiserror::Error;

use crate::{live::config::format::DepthFormatConfig, shared::domain::canonical::SequenceMetadata};

use crate::live::infrastructure::normalization::extraction::JsonExtractor;
use crate::live::infrastructure::normalization::message::DepthMessageKind;
use crate::live::infrastructure::normalization::numeric::{NumericConverter, TimestampUnit};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum DepthMetadataError {
    #[error("invalid depth metadata configuration: {0}")]
    Configuration(String),

    #[error("depth metadata extraction failed at '{path}': {reason}")]
    Extraction { path: String, reason: String },

    #[error("invalid depth metadata at '{path}': {reason}")]
    InvalidValue { path: String, reason: String },
}

// ============================================================
// COMPILED FIELD REFERENCES
// ============================================================

#[derive(Debug, Clone)]
struct SequenceField {
    path: String,

    // A missing field is only permitted when the native
    // format explicitly declares it optional.
    required: bool,

    // Used for the OKX snapshot previous-sequence sentinel.
    snapshot_sentinel: Option<i64>,
}

// ============================================================
// DECODED NATIVE METADATA
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDepthMetadata {
    pub sequence: SequenceMetadata,
    pub event_timestamp_ns: i64,
}

// ============================================================
// COMPILED METADATA PLAN
// ============================================================

#[derive(Debug, Clone)]
pub struct DepthMetadataPlan {
    first: Option<SequenceField>,
    last: Option<SequenceField>,
    previous: Option<SequenceField>,
    cross: Option<SequenceField>,

    timestamp_path: String,
    timestamp_unit: TimestampUnit,
}

impl DepthMetadataPlan {
    // ========================================================
    // COMPILE FROM EXISTING DEPTH TOML
    // ========================================================

    pub fn compile(config: &DepthFormatConfig) -> Result<Self, DepthMetadataError> {
        let sequence = &config.sequence;

        // The native sequence strategy determines which
        // logical fields represent the canonical identifiers.
        //
        // The actual JSON paths still come from TOML.
        let (first_name, last_name, previous_name, cross_name) = match sequence.strategy.as_str() {
            "binance_spot_update_range"
            | "binance_futures_previous_update"
            | "gate_spot_update_range"
            | "gate_futures_update_range" => (
                sequence.first_field.as_deref(),
                sequence.last_field.as_deref(),
                sequence.previous_field.as_deref(),
                None,
            ),

            "bybit_v5_orderbook" => (
                None,
                sequence.update_id_field.as_deref(),
                None,
                sequence.cross_sequence_field.as_deref(),
            ),

            "bitget_v3_books" | "okx_v5_books" => (
                None,
                sequence.sequence_field.as_deref(),
                sequence.previous_sequence_field.as_deref(),
                None,
            ),

            other => {
                return Err(DepthMetadataError::Configuration(format!(
                    "unsupported sequence strategy '{other}'"
                )));
            }
        };

        if last_name.is_none() {
            return Err(DepthMetadataError::Configuration(
                "native last/primary sequence field is required".into(),
            ));
        }

        if matches!(
            sequence.strategy.as_str(),
            "binance_spot_update_range"
                | "binance_futures_previous_update"
                | "gate_spot_update_range"
                | "gate_futures_update_range"
        ) && first_name.is_none()
        {
            return Err(DepthMetadataError::Configuration(
                "native first sequence field is required".into(),
            ));
        }

        if matches!(
            sequence.strategy.as_str(),
            "binance_futures_previous_update" | "bitget_v3_books" | "okx_v5_books"
        ) && previous_name.is_none()
        {
            return Err(DepthMetadataError::Configuration(
                "native previous sequence field is required".into(),
            ));
        }

        if sequence.strategy == "bybit_v5_orderbook" && cross_name.is_none() {
            return Err(DepthMetadataError::Configuration(
                "Bybit cross-sequence field is required".into(),
            ));
        }

        // ========================================================
        // RECORD-RELATIVE FIELD RESOLUTION
        // ========================================================
        //
        // When [records] is declared, [fields] paths are relative
        // to one extracted record.
        //
        // Array record collections use index zero because the
        // current depth normalization contract expects exactly
        // one native depth record per message.
        //
        // Without [records], [fields] paths are relative to the
        // complete native message.
        //
        // Examples:
        //
        // Binance:
        //     No [records]
        //     "u" -> "u"
        //
        // Bybit:
        //     No [records]
        //     "data.u" -> "data.u"
        //     "cts" -> "cts"
        //
        // Gate.io:
        //     [records] path="result", representation="object"
        //     "u" -> "result.u"
        //
        // Bitget / OKX:
        //     [records] path="data", representation="array"
        //     "seq" -> "data.0.seq"

        let record_prefix: Option<String> = match &config.records {
            Some(records) => {
                let path = records.path.strip_prefix("$.").unwrap_or(&records.path);

                match records.representation.as_str() {
                    "object" => {
                        if path == "$" || path.is_empty() {
                            None
                        } else {
                            Some(path.to_owned())
                        }
                    }

                    "array" => {
                        if path == "$" || path.is_empty() {
                            Some("0".into())
                        } else {
                            Some(format!("{path}.0"))
                        }
                    }

                    other => {
                        return Err(DepthMetadataError::Configuration(format!(
                            "unsupported depth record representation '{other}'"
                        )));
                    }
                }
            }

            None => None,
        };

        // Message field mappings may be rooted at the
        // full envelope (Bybit), while fields belonging
        // to array/object depth records are relative
        // to the extracted record (Bitget, OKX, Gate).
        let resolve = |logical_name: Option<&str>,
                       required: bool|
         -> Result<Option<SequenceField>, DepthMetadataError> {
            let Some(logical_name) = logical_name else {
                return Ok(None);
            };

            let path = config.fields.get(logical_name).ok_or_else(|| {
                DepthMetadataError::Configuration(format!("missing fields.{logical_name}"))
            })?;

            // If the declaration already starts with the
            // configured payload path, don't prefix twice.
            let resolved = resolve_path(record_prefix.as_deref(), path);

            Ok(Some(SequenceField {
                path: resolved,
                required,
                snapshot_sentinel: None,
            }))
        };

        let first = resolve(first_name, true)?;
        let last = resolve(last_name, true)?;

        let previous_required = matches!(
            sequence.strategy.as_str(),
            "binance_futures_previous_update" | "bitget_v3_books" | "okx_v5_books"
        );

        let mut previous = resolve(previous_name, previous_required)?;

        if sequence.strategy == "okx_v5_books" {
            // The supported OKX books format explicitly
            // declares -1 as its snapshot sentinel.
            //
            // The declaration is verified here rather than
            // silently assuming every negative ID is valid.
            let sentinel = sequence.snapshot_previous_sequence_id.ok_or_else(|| {
                DepthMetadataError::Configuration("OKX snapshot sentinel is not configured".into())
            })?;

            if sentinel != -1 {
                return Err(DepthMetadataError::Configuration(format!(
                    "unsupported OKX snapshot sentinel {sentinel}"
                )));
            }

            if let Some(field) = previous.as_mut() {
                field.snapshot_sentinel = Some(sentinel);
            }
        }

        let cross = resolve(cross_name, true)?;

        let timestamp_name = &config.timestamps.event_source;

        let timestamp_field = config.fields.get(timestamp_name).ok_or_else(|| {
            DepthMetadataError::Configuration(format!("missing timestamp field '{timestamp_name}'"))
        })?;

        let timestamp_path = resolve_path(record_prefix.as_deref(), timestamp_field);

        let timestamp_unit = TimestampUnit::parse(&config.timestamps.source_unit)
            .map_err(DepthMetadataError::Configuration)?;

        Ok(Self {
            first,
            last,
            previous,
            cross,
            timestamp_path,
            timestamp_unit,
        })
    }

    // ========================================================
    // DECODE MESSAGE METADATA
    // ========================================================

    pub fn decode(
        &self,
        message: &Value,
        kind: DepthMessageKind,
    ) -> Result<DecodedDepthMetadata, DepthMetadataError> {
        let sequence_first = read_sequence(message, self.first.as_ref(), kind)?;

        let sequence_last = read_sequence(message, self.last.as_ref(), kind)?;

        let sequence_previous = read_sequence(message, self.previous.as_ref(), kind)?;

        let cross_sequence = read_sequence(message, self.cross.as_ref(), kind)?;

        if let (Some(first), Some(last)) = (sequence_first, sequence_last) {
            if first > last {
                return Err(DepthMetadataError::InvalidValue {
                    path: "sequence".into(),
                    reason: format!("first sequence {first} exceeds last {last}"),
                });
            }
        }

        let timestamp_value = read_required(message, &self.timestamp_path)?;

        let event_timestamp_ns =
            NumericConverter::timestamp_ns_nonnegative(timestamp_value, self.timestamp_unit)
                .map_err(|error| DepthMetadataError::InvalidValue {
                    path: self.timestamp_path.clone(),
                    reason: error.to_string(),
                })?;

        Ok(DecodedDepthMetadata {
            sequence: SequenceMetadata {
                sequence_first,
                sequence_last,
                sequence_previous,
                cross_sequence,
            },

            event_timestamp_ns,
        })
    }
}

// ============================================================
// JSON PATH RESOLUTION
// ============================================================
fn resolve_path(record_prefix: Option<&str>, field: &str) -> String {
    // Explicit root-relative paths must not receive
    // the extracted-record prefix.
    if field == "$" {
        return field.to_owned();
    }

    if field.starts_with("$.") {
        return field.to_owned();
    }

    let Some(prefix) = record_prefix else {
        return field.to_owned();
    };

    if prefix.is_empty() || prefix == "$" {
        return field.to_owned();
    }

    format!("{prefix}.{field}")
}

// ============================================================
// FIELD READING
// ============================================================

fn read_required<'a>(message: &'a Value, path: &str) -> Result<&'a Value, DepthMetadataError> {
    JsonExtractor::required(message, path).map_err(|error| DepthMetadataError::Extraction {
        path: path.to_owned(),
        reason: error.to_string(),
    })
}

fn read_sequence(
    message: &Value,
    field: Option<&SequenceField>,
    kind: DepthMessageKind,
) -> Result<Option<u64>, DepthMetadataError> {
    let Some(field) = field else {
        return Ok(None);
    };

    let value = if field.required {
        Some(read_required(message, &field.path)?)
    } else {
        JsonExtractor::optional(message, &field.path).map_err(|error| {
            DepthMetadataError::Extraction {
                path: field.path.clone(),
                reason: error.to_string(),
            }
        })?
    };

    let Some(value) = value else {
        return Ok(None);
    };

    // Only a snapshot may use the configured
    // negative previous-sequence sentinel.
    if kind == DepthMessageKind::Snapshot {
        if let Some(sentinel) = field.snapshot_sentinel {
            if value.as_i64() == Some(sentinel) {
                return Ok(None);
            }
        }
    }

    let parsed =
        NumericConverter::u64(value).map_err(|error| DepthMetadataError::InvalidValue {
            path: field.path.clone(),
            reason: error.to_string(),
        })?;

    Ok(Some(parsed))
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn config(profile: &str) -> DepthFormatConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{profile}.toml"));

        DepthFormatConfig::load(path).unwrap()
    }

    fn fixtures(profile: &str) -> Vec<Value> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.cache/live/websocket-fixtures/depth")
            .join(format!("{profile}.json"));

        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    fn kind(profile: &str, message: &Value) -> DepthMessageKind {
        if profile.starts_with("binance_") || profile.starts_with("gateio_") {
            return DepthMessageKind::Delta;
        }

        let native = if profile.starts_with("bybit_") {
            message["type"].as_str()
        } else {
            message["action"].as_str()
        };

        match native {
            Some("snapshot") => DepthMessageKind::Snapshot,
            Some("delta" | "update") => DepthMessageKind::Delta,
            other => panic!("unexpected fixture kind: {other:?}"),
        }
    }

    #[test]
    fn decodes_metadata_from_all_fifteen_profiles() {
        for exchange in ["binance", "bybit", "bitget", "gateio", "okx"] {
            for market in ["spot", "linear", "inverse"] {
                let profile = format!("{exchange}_{market}");
                let plan = DepthMetadataPlan::compile(&config(&profile)).unwrap();

                for (index, message) in fixtures(&profile).iter().enumerate() {
                    let result = plan.decode(message, kind(&profile, message));

                    assert!(result.is_ok(), "{profile}, message {index}: {result:?}");

                    let result = result.unwrap();

                    assert!(
                        result.sequence.sequence_last.is_some(),
                        "{profile}, message {index}"
                    );

                    assert!(result.event_timestamp_ns > 0);
                }
            }
        }
    }

    #[test]
    fn okx_snapshot_sentinel_becomes_none() {
        let plan = DepthMetadataPlan::compile(&config("okx_spot")).unwrap();

        let message = fixtures("okx_spot").remove(0);

        let metadata = plan.decode(&message, DepthMessageKind::Snapshot).unwrap();

        assert_eq!(metadata.sequence.sequence_previous, None,);

        assert!(metadata.sequence.sequence_last.is_some());
    }

    #[test]
    fn okx_negative_sequence_in_delta_is_rejected() {
        let plan = DepthMetadataPlan::compile(&config("okx_spot")).unwrap();

        let mut message = fixtures("okx_spot").remove(1);

        message["data"][0]["prevSeqId"] = serde_json::json!(-1);

        assert!(plan.decode(&message, DepthMessageKind::Delta).is_err());
    }

    #[test]
    fn binance_futures_preserves_previous_id() {
        let plan = DepthMetadataPlan::compile(&config("binance_linear")).unwrap();

        let message = fixtures("binance_linear").remove(0);

        let metadata = plan.decode(&message, DepthMessageKind::Delta).unwrap();

        assert_eq!(metadata.sequence.sequence_previous, message["pu"].as_u64(),);
    }

    #[test]
    fn bybit_preserves_cross_sequence() {
        let plan = DepthMetadataPlan::compile(&config("bybit_spot")).unwrap();

        let message = fixtures("bybit_spot").remove(0);

        let metadata = plan.decode(&message, DepthMessageKind::Snapshot).unwrap();

        assert_eq!(
            metadata.sequence.cross_sequence,
            message["data"]["seq"].as_u64(),
        );
    }

    #[test]
    fn rejects_missing_required_sequence() {
        let plan = DepthMetadataPlan::compile(&config("binance_spot")).unwrap();

        let mut message = fixtures("binance_spot").remove(0);

        message.as_object_mut().unwrap().remove("u");

        assert!(plan.decode(&message, DepthMessageKind::Delta).is_err());
    }
}
