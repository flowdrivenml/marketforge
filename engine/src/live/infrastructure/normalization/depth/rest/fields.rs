use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::canonical::SequenceMetadata;

use crate::live::infrastructure::normalization::depth::rest::compiler::{
    RestDepthNormalizationPlan, RestDepthRecordContainer,
};
use crate::live::infrastructure::normalization::extraction::JsonExtractor;
use crate::live::infrastructure::normalization::numeric::NumericConverter;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum RestDepthFieldsError {
    #[error("invalid REST depth configuration: {0}")]
    Configuration(String),

    #[error("REST application response rejected: {0}")]
    Application(String),

    #[error("invalid REST snapshot structure: {0}")]
    Structure(String),

    #[error("REST snapshot instrument mismatch: {0}")]
    Instrument(String),

    #[error("invalid REST snapshot metadata: {0}")]
    Metadata(String),
}

// ============================================================
// APPLICATION STATUS CONTRACT
// ============================================================

#[derive(Debug, Clone)]
enum ApplicationStatus {
    Raw,

    Envelope {
        code_path: String,
        success_codes: Vec<String>,
    },
}

// ============================================================
// VALIDATED SNAPSHOT METADATA
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestDepthSnapshotMetadata {
    pub sequence: SequenceMetadata,

    /// Exchange timestamp in nanoseconds, if available.
    pub timestamp_ns: Option<i64>,

    /// True only if the native response supplied a
    /// primary sequence/update identifier.
    ///
    /// This does NOT mean Integrity has synchronized
    /// the order book.
    pub has_native_sequence_baseline: bool,
}

// ============================================================
// COMPILED SNAPSHOT READER
// ============================================================

#[derive(Debug, Clone)]
pub struct RestDepthSnapshotReader {
    application: ApplicationStatus,

    plan: RestDepthNormalizationPlan,
}

impl RestDepthSnapshotReader {
    // ========================================================
    // COMPILATION
    // ========================================================

    pub fn compile<E: Serialize>(
        plan: RestDepthNormalizationPlan,
        endpoint: &E,
    ) -> Result<Self, RestDepthFieldsError> {
        let endpoint = serde_json::to_value(endpoint)
            .map_err(|e| RestDepthFieldsError::Configuration(e.to_string()))?;

        let app = endpoint.get("application_response").ok_or_else(|| {
            RestDepthFieldsError::Configuration("missing application_response".into())
        })?;

        let strategy = app.get("strategy").and_then(Value::as_str).ok_or_else(|| {
            RestDepthFieldsError::Configuration("missing application response strategy".into())
        })?;

        let application = match strategy {
            "raw" => ApplicationStatus::Raw,

            "envelope" => {
                let code_path = app
                    .get("code_field")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        RestDepthFieldsError::Configuration("missing application code field".into())
                    })?
                    .to_owned();

                let success_codes = app
                    .get("success_codes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        RestDepthFieldsError::Configuration(
                            "missing application success codes".into(),
                        )
                    })?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .map(str::to_owned)
                            .ok_or_else(|| {
                                RestDepthFieldsError::Configuration(
                                    "invalid application success code".into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if success_codes.is_empty() {
                    return Err(RestDepthFieldsError::Configuration(
                        "empty application success codes".into(),
                    ));
                }

                ApplicationStatus::Envelope {
                    code_path,
                    success_codes,
                }
            }

            other => {
                return Err(RestDepthFieldsError::Configuration(format!(
                    "unsupported application strategy '{other}'"
                )));
            }
        };

        Ok(Self { application, plan })
    }

    // ========================================================
    // VALIDATE AND EXTRACT
    // ========================================================

    pub fn read(
        &self,
        response: &Value,
        expected_symbol: &str,
    ) -> Result<RestDepthSnapshotMetadata, RestDepthFieldsError> {
        if expected_symbol.trim().is_empty() {
            return Err(RestDepthFieldsError::Instrument(
                "expected instrument cannot be empty".into(),
            ));
        }

        self.validate_application(response)?;

        let _record = self.snapshot_record(response)?;

        self.validate_symbol(response, expected_symbol)?;

        let sequence_last = match (
            self.plan.sequence.sequence_path.as_deref(),
            self.plan.sequence.update_id_path.as_deref(),
        ) {
            (Some(path), None) | (None, Some(path)) => Some(read_u64(response, path)?),

            (None, None) => None,

            (Some(_), Some(_)) => {
                return Err(RestDepthFieldsError::Configuration(
                    "ambiguous snapshot sequence mapping".into(),
                ));
            }
        };

        let cross_sequence = self
            .plan
            .sequence
            .cross_sequence_path
            .as_deref()
            .map(|path| read_u64(response, path))
            .transpose()?;

        let timestamp_ns = self
            .plan
            .timestamp_path
            .as_deref()
            .map(|path| {
                let unit = self.plan.timestamp_unit.ok_or_else(|| {
                    RestDepthFieldsError::Configuration(
                        "compiled REST timestamp field has no unit".into(),
                    )
                })?;

                let value = required(response, path)?;

                NumericConverter::timestamp_ns_nonnegative(value, unit)
                    .map_err(|error| RestDepthFieldsError::Metadata(format!("{path}: {error}")))
            })
            .transpose()?;

        Ok(RestDepthSnapshotMetadata {
            sequence: SequenceMetadata {
                sequence_first: None,
                sequence_last,
                sequence_previous: None,
                cross_sequence,
            },

            timestamp_ns,

            has_native_sequence_baseline: sequence_last.is_some(),
        })
    }

    // ========================================================
    // APPLICATION STATUS
    // ========================================================

    fn validate_application(&self, response: &Value) -> Result<(), RestDepthFieldsError> {
        match &self.application {
            ApplicationStatus::Raw => Ok(()),

            ApplicationStatus::Envelope {
                code_path,
                success_codes,
            } => {
                let value = required(response, code_path)
                    .map_err(|e| RestDepthFieldsError::Application(e.to_string()))?;

                // Numeric JSON success codes and their
                // string forms can be equivalent.
                //
                // String codes retain their original
                // representation, including leading zeros.
                let native_code = match value {
                    Value::String(s) => s.clone(),

                    Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),

                    _ => {
                        return Err(RestDepthFieldsError::Application(
                            "application code has invalid type".into(),
                        ));
                    }
                };

                if success_codes.iter().any(|code| code == &native_code) {
                    Ok(())
                } else {
                    Err(RestDepthFieldsError::Application(format!(
                        "exchange returned application code \
                             '{native_code}'"
                    )))
                }
            }
        }
    }

    // ========================================================
    // SNAPSHOT STRUCTURE
    // ========================================================

    fn snapshot_record<'a>(&self, response: &'a Value) -> Result<&'a Value, RestDepthFieldsError> {
        let root = required(response, &self.plan.snapshot_root)?;

        match self.plan.record_container {
            RestDepthRecordContainer::Object => {
                if !root.is_object() {
                    return Err(RestDepthFieldsError::Structure(
                        "expected snapshot object".into(),
                    ));
                }

                Ok(root)
            }

            RestDepthRecordContainer::SingleObjectArray => {
                let array = root.as_array().ok_or_else(|| {
                    RestDepthFieldsError::Structure("expected snapshot array".into())
                })?;

                if array.len() != 1 {
                    return Err(RestDepthFieldsError::Structure(format!(
                        "expected one snapshot record, got {}",
                        array.len(),
                    )));
                }

                if !array[0].is_object() {
                    return Err(RestDepthFieldsError::Structure(
                        "snapshot record is not an object".into(),
                    ));
                }

                Ok(&array[0])
            }
        }
    }

    // ========================================================
    // INSTRUMENT IDENTITY
    // ========================================================

    fn validate_symbol(
        &self,
        response: &Value,
        expected_symbol: &str,
    ) -> Result<(), RestDepthFieldsError> {
        let Some(path) = self.plan.symbol_path.as_deref() else {
            // Some REST responses omit the instrument.
            // Runtime request binding remains mandatory.
            return Ok(());
        };

        let value = required(response, path)?;

        let symbol = value.as_str().ok_or_else(|| {
            RestDepthFieldsError::Instrument(format!("{path}: expected string instrument"))
        })?;

        if symbol != expected_symbol {
            return Err(RestDepthFieldsError::Instrument(format!(
                "received '{symbol}', expected '{expected_symbol}'"
            )));
        }

        Ok(())
    }
}

// ============================================================
// FIELD HELPERS
// ============================================================

fn required<'a>(value: &'a Value, path: &str) -> Result<&'a Value, RestDepthFieldsError> {
    JsonExtractor::required(value, path)
        .map_err(|e| RestDepthFieldsError::Metadata(format!("{path}: {e}")))
}

fn read_u64(response: &Value, path: &str) -> Result<u64, RestDepthFieldsError> {
    let value = required(response, path)?;

    NumericConverter::u64(value).map_err(|e| RestDepthFieldsError::Metadata(format!("{path}: {e}")))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    use crate::live::config::format::DepthFormatConfig;

    use crate::live::infrastructure::normalization::depth::rest::compiler::RestDepthNormalizationCompiler;

    fn root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data")
    }

    fn setup(profile: &str) -> (RestDepthSnapshotReader, Value) {
        let root = root();

        let depth = DepthFormatConfig::load(
            root.join(".config/live/formats/depth")
                .join(format!("{profile}.toml")),
        )
        .unwrap();

        let endpoint_text = std::fs::read_to_string(
            root.join(".config/live/exchanges")
                .join(format!("{profile}.toml")),
        )
        .unwrap();

        let exchange: toml::Value = toml::from_str(&endpoint_text).unwrap();

        let endpoint =
            serde_json::to_value(&exchange["rest"]["endpoints"]["depth_snapshot"]).unwrap();

        let plan = RestDepthNormalizationCompiler::compile(&depth, &endpoint, 20_000).unwrap();

        let reader = RestDepthSnapshotReader::compile(plan, &endpoint).unwrap();

        let fixture_path = root
            .join(".cache/live/rest-fixtures")
            .join(format!("{profile}__depth_snapshot.json"));

        let fixture: Value = serde_json::from_slice(&std::fs::read(fixture_path).unwrap()).unwrap();

        (reader, fixture)
    }

    fn symbol(profile: &str) -> &'static str {
        match profile {
            "binance_inverse" => "BTCUSD_PERP",
            "bybit_inverse" => "BTCUSD",
            "bitget_inverse" => "BTCUSD_CM",
            "gateio_spot" | "gateio_linear" => "BTC_USDT",
            "gateio_inverse" => "BTC_USD",
            "okx_spot" => "BTC-USDT",
            "okx_linear" => "BTC-USDT-SWAP",
            "okx_inverse" => "BTC-USD-SWAP",
            _ => "BTCUSDT",
        }
    }

    #[test]
    fn reads_all_fifteen_real_rest_snapshots() {
        for exchange in ["binance", "bybit", "bitget", "gateio", "okx"] {
            for market in ["spot", "linear", "inverse"] {
                let profile = format!("{exchange}_{market}");

                let (reader, fixture) = setup(&profile);

                let metadata = reader
                    .read(&fixture, symbol(&profile))
                    .unwrap_or_else(|e| panic!("{profile}: {e:?}"));

                if exchange == "bitget" {
                    assert!(!metadata.has_native_sequence_baseline);
                    assert_eq!(metadata.sequence.sequence_last, None,);
                } else {
                    assert!(
                        metadata.has_native_sequence_baseline,
                        "{profile}: expected native sequence"
                    );
                }
            }
        }
    }

    #[test]
    fn rejects_bybit_application_failure() {
        let (reader, mut fixture) = setup("bybit_spot");

        fixture["retCode"] = serde_json::json!(10001);

        assert!(reader.read(&fixture, "BTCUSDT").is_err());
    }

    #[test]
    fn rejects_bitget_application_failure() {
        let (reader, mut fixture) = setup("bitget_spot");

        fixture["code"] = serde_json::json!("10001");

        assert!(reader.read(&fixture, "BTCUSDT").is_err());
    }

    #[test]
    fn rejects_okx_multiple_snapshot_records() {
        let (reader, mut fixture) = setup("okx_spot");

        let duplicate = fixture["data"][0].clone();

        fixture["data"].as_array_mut().unwrap().push(duplicate);

        assert!(reader.read(&fixture, "BTC-USDT").is_err());
    }

    #[test]
    fn rejects_bybit_foreign_instrument() {
        let (reader, mut fixture) = setup("bybit_spot");

        fixture["result"]["s"] = serde_json::json!("ETHUSDT");

        assert!(reader.read(&fixture, "BTCUSDT").is_err());
    }

    #[test]
    fn rejects_missing_required_snapshot_sequence() {
        let (reader, mut fixture) = setup("binance_spot");

        fixture.as_object_mut().unwrap().remove("lastUpdateId");

        assert!(reader.read(&fixture, "BTCUSDT").is_err());
    }
}
