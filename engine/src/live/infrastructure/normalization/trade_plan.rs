use serde_json::Value;
use thiserror::Error;

use super::{ExtractionError, JsonExtractor};

// ============================================================
// RECORD REPRESENTATION
// ============================================================

/// How trade records are represented at the configured
/// JSON extraction path.
///
/// Array:
///     One JSON array containing zero or more object records.
///
/// Object:
///     One JSON object representing one trade record.
///
/// Both layouts produce the same borrowed record interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeRecordRepresentation {
    Array,
    Object,
}

impl TradeRecordRepresentation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Array => "array",
            Self::Object => "object",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "array" => Ok(Self::Array),
            "object" => Ok(Self::Object),
            other => Err(format!("unsupported trade record representation '{other}'")),
        }
    }
}

// ============================================================
// EXTRACTION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TradeExtractionError {
    #[error("invalid trade extraction configuration: {0}")]
    Configuration(String),

    #[error(transparent)]
    Extraction(#[from] ExtractionError),

    #[error("trade record at index {index} must be an object, got {actual}")]
    InvalidRecord { index: usize, actual: &'static str },
}

// ============================================================
// BORROWED TRADE RECORDS
// ============================================================

/// Borrowed view of the native trade records.
///
/// No native JSON values are copied.
///
/// Keeping an explicit single-record variant avoids
/// allocating a temporary Vec for object messages.
#[derive(Debug, Clone, Copy)]
pub enum TradeRecordView<'a> {
    Single(&'a Value),
    Batch(&'a [Value]),
}

impl<'a> TradeRecordView<'a> {
    pub fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Batch(records) => records.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&'a Value> {
        match self {
            Self::Single(record) => {
                if index == 0 {
                    Some(record)
                } else {
                    None
                }
            }

            Self::Batch(records) => records.get(index),
        }
    }

    pub fn iter(&self) -> TradeRecordIterator<'a> {
        match self {
            Self::Single(record) => TradeRecordIterator {
                single: Some(record),
                batch: None,
            },

            Self::Batch(records) => TradeRecordIterator {
                single: None,
                batch: Some(records.iter()),
            },
        }
    }
}

// ============================================================
// BORROWED RECORD ITERATOR
// ============================================================

pub struct TradeRecordIterator<'a> {
    single: Option<&'a Value>,
    batch: Option<std::slice::Iter<'a, Value>>,
}

impl<'a> Iterator for TradeRecordIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(record) = self.single.take() {
            return Some(record);
        }

        self.batch.as_mut().and_then(Iterator::next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = if self.single.is_some() {
            1
        } else {
            self.batch.as_ref().map(ExactSizeIterator::len).unwrap_or(0)
        };

        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TradeRecordIterator<'_> {}

// ============================================================
// TRADE EXTRACTION PLAN
// ============================================================

/// Immutable extraction plan for one native trade format.
///
/// The plan describes:
///
/// - Where records are located.
/// - Whether the location contains an array or object.
/// - Maximum accepted records per message.
///
/// REST and WebSocket may use different plans while
/// sharing the same downstream canonical trade decoder.
///
/// This plan does not interpret trade fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeExtractionPlan {
    path: String,

    representation: TradeRecordRepresentation,

    max_records: usize,
}

impl TradeExtractionPlan {
    /// Compile a native trade-record extraction plan.
    ///
    /// Supported paths:
    ///
    /// "$"             root
    /// ""              root
    /// "result.list"    nested object field
    /// "$.result.list"  explicitly rooted path
    /// "data.0.trades"  nested array indexing
    pub fn new(
        path: impl Into<String>,
        representation: TradeRecordRepresentation,
        max_records: usize,
    ) -> Result<Self, TradeExtractionError> {
        let path = path.into();

        if max_records == 0 {
            return Err(TradeExtractionError::Configuration(
                "max_records must be positive".into(),
            ));
        }

        validate_record_path(&path)?;

        Ok(Self {
            path,
            representation,
            max_records,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn representation(&self) -> TradeRecordRepresentation {
        self.representation
    }

    pub fn max_records(&self) -> usize {
        self.max_records
    }

    // ========================================================
    // NATIVE RECORD EXTRACTION
    // ========================================================

    /// Extract native trade records without copying JSON.
    ///
    /// The returned records borrow from `root`.
    ///
    /// All records are validated as JSON objects.
    ///
    /// The extraction is stateless and deterministic.
    pub fn extract<'a>(
        &self,
        root: &'a Value,
    ) -> Result<TradeRecordView<'a>, TradeExtractionError> {
        match self.representation {
            // =================================================
            // ARRAY OF RECORDS
            // =================================================
            TradeRecordRepresentation::Array => {
                let records = JsonExtractor::object_records(root, &self.path, self.max_records)?;

                Ok(TradeRecordView::Batch(records))
            }

            // =================================================
            // ONE OBJECT RECORD
            // =================================================
            TradeRecordRepresentation::Object => {
                let record = JsonExtractor::required(root, &self.path)?;

                if !record.is_object() {
                    return Err(TradeExtractionError::InvalidRecord {
                        index: 0,
                        actual: super::json_type(record),
                    });
                }

                Ok(TradeRecordView::Single(record))
            }
        }
    }
}

// ============================================================
// PATH CONFIGURATION VALIDATION
// ============================================================

/// Validate the path syntax supported by JsonExtractor.
///
/// This checks syntax only.
///
/// The existence and runtime types of fields are verified
/// during extraction against actual native JSON.
fn validate_record_path(path: &str) -> Result<(), TradeExtractionError> {
    if path.is_empty() || path == "$" {
        return Ok(());
    }

    if path.trim() != path {
        return Err(TradeExtractionError::Configuration(
            "record path contains surrounding whitespace".into(),
        ));
    }

    let normalized = path.strip_prefix("$.").unwrap_or(path.as_ref());

    if normalized.is_empty() {
        return Err(TradeExtractionError::Configuration(
            "record path is empty after root prefix".into(),
        ));
    }

    for segment in normalized.split('.') {
        if segment.is_empty() {
            return Err(TradeExtractionError::Configuration(format!(
                "empty segment in record path '{path}'"
            )));
        }

        if segment == "$" {
            return Err(TradeExtractionError::Configuration(
                "root token may appear only at the beginning".into(),
            ));
        }
    }

    Ok(())
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ========================================================
    // ROOT ARRAY
    // ========================================================

    #[test]
    fn extracts_binance_style_rest_array() {
        let body = json!([
            {
                "id": 100,
                "price": "80000",
                "qty": "0.001"
            },
            {
                "id": 101,
                "price": "80001",
                "qty": "0.002"
            }
        ]);

        let plan = TradeExtractionPlan::new("$", TradeRecordRepresentation::Array, 100).unwrap();

        let records = plan.extract(&body).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records.get(0).unwrap()["id"], 100);
        assert_eq!(records.get(1).unwrap()["id"], 101);
    }

    // ========================================================
    // NESTED ARRAY
    // ========================================================

    #[test]
    fn extracts_bybit_style_rest_array() {
        let body = json!({
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "execId": "123",
                        "price": "80000"
                    }
                ]
            }
        });

        let plan =
            TradeExtractionPlan::new("result.list", TradeRecordRepresentation::Array, 100).unwrap();

        let records = plan.extract(&body).unwrap();

        assert_eq!(records.len(), 1);

        assert_eq!(records.get(0).unwrap()["execId"], "123",);
    }

    // ========================================================
    // SINGLE RECORD
    // ========================================================

    #[test]
    fn extracts_single_websocket_trade() {
        let body = json!({
            "e": "trade",
            "data": {
                "t": 123,
                "p": "80000",
                "q": "0.001"
            }
        });

        let plan = TradeExtractionPlan::new("data", TradeRecordRepresentation::Object, 1).unwrap();

        let records = plan.extract(&body).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records.get(0).unwrap()["t"], 123);
    }

    // ========================================================
    // RECORD ORDER
    // ========================================================

    #[test]
    fn preserves_native_record_order() {
        let body = json!({
            "data": [
                {"id": 3},
                {"id": 2},
                {"id": 1}
            ]
        });

        let plan = TradeExtractionPlan::new("data", TradeRecordRepresentation::Array, 10).unwrap();

        let records = plan.extract(&body).unwrap();

        let ids = records
            .iter()
            .map(|record| record["id"].as_u64().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![3, 2, 1]);
    }

    // ========================================================
    // EMPTY ARRAY
    // ========================================================

    #[test]
    fn accepts_empty_trade_array() {
        let body = json!({
            "data": []
        });

        let plan = TradeExtractionPlan::new("data", TradeRecordRepresentation::Array, 10).unwrap();

        assert!(plan.extract(&body).unwrap().is_empty());
    }

    // ========================================================
    // BOUNDARY CONDITIONS
    // ========================================================

    #[test]
    fn rejects_zero_max_records() {
        assert!(TradeExtractionPlan::new("$", TradeRecordRepresentation::Array, 0,).is_err(),);
    }

    #[test]
    fn rejects_oversized_trade_batch() {
        let body = json!([
            {"id": 1},
            {"id": 2}
        ]);

        let plan = TradeExtractionPlan::new("$", TradeRecordRepresentation::Array, 1).unwrap();

        assert!(matches!(
            plan.extract(&body),
            Err(TradeExtractionError::Extraction(
                ExtractionError::TooManyRecords { .. }
            ))
        ));
    }

    #[test]
    fn rejects_wrong_record_representation() {
        let body = json!({
            "data": []
        });

        let plan = TradeExtractionPlan::new("data", TradeRecordRepresentation::Object, 1).unwrap();

        assert!(matches!(
            plan.extract(&body),
            Err(TradeExtractionError::InvalidRecord { .. })
        ));
    }

    #[test]
    fn rejects_missing_record_path() {
        let body = json!({
            "result": {}
        });

        let plan =
            TradeExtractionPlan::new("result.list", TradeRecordRepresentation::Array, 100).unwrap();

        assert!(matches!(
            plan.extract(&body),
            Err(TradeExtractionError::Extraction(
                ExtractionError::MissingField { .. }
            ))
        ));
    }

    #[test]
    fn rejects_malformed_record_path() {
        assert!(
            TradeExtractionPlan::new("result..list", TradeRecordRepresentation::Array, 10,)
                .is_err(),
        );
    }

    // ========================================================
    // ITERATOR
    // ========================================================

    #[test]
    fn record_iterator_preserves_count() {
        let body = json!([
            {"id": 1},
            {"id": 2},
            {"id": 3}
        ]);

        let plan = TradeExtractionPlan::new("$", TradeRecordRepresentation::Array, 10).unwrap();

        let records = plan.extract(&body).unwrap();

        assert_eq!(records.iter().len(), 3);
        assert_eq!(records.iter().count(), 3);
    }

    // ========================================================
    // CONFIGURATION
    // ========================================================

    #[test]
    fn parses_supported_representations() {
        assert_eq!(
            TradeRecordRepresentation::parse("array").unwrap(),
            TradeRecordRepresentation::Array,
        );

        assert_eq!(
            TradeRecordRepresentation::parse("object").unwrap(),
            TradeRecordRepresentation::Object,
        );

        assert!(TradeRecordRepresentation::parse("unknown").is_err(),);
    }
}
