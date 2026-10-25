use serde_json::Value;

use thiserror::Error;

// ============================================================
// EXTRACTION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExtractionError {
    #[error("invalid JSON path '{path}': {reason}")]
    InvalidPath { path: String, reason: String },

    #[error("required JSON field '{path}' is missing")]
    MissingField { path: String },

    #[error("JSON type mismatch at '{path}': expected {expected}, got {actual}")]
    TypeMismatch {
        path: String,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("JSON array index {index} is out of bounds at '{path}' (length {length})")]
    IndexOutOfBounds {
        path: String,
        index: usize,
        length: usize,
    },

    #[error("JSON array at '{path}' exceeds maximum records: {actual} > {maximum}")]
    TooManyRecords {
        path: String,
        actual: usize,
        maximum: usize,
    },

    #[error("invalid record at '{path}', index {index}: {reason}")]
    InvalidRecord {
        path: String,
        index: usize,
        reason: String,
    },
}

// ============================================================
// JSON VALUE TYPE
// ============================================================

pub fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ============================================================
// GENERIC JSON EXTRACTOR
// ============================================================

/// Stateless JSON extraction primitives.
///
/// All returned references borrow from the supplied
/// serde_json::Value.
///
/// Supported paths:
///
/// - "$"                root value
/// - ""                 root value
/// - "result.list"      nested object fields
/// - "$.result.list"    explicit root prefix
/// - "data.0.price"     indexed array elements
///
/// Path segments are separated by dots.
///
/// When traversing an array, a segment must be an
/// unsigned decimal index.
///
/// Dots inside literal JSON object keys are not yet
/// supported. A compiled path representation can be
/// introduced later if needed.
///
/// This extractor does not:
///
/// - Parse numeric values into Decimal.
/// - Convert timestamps.
/// - Interpret exchange-native sides.
/// - Infer missing fields.
/// - Resolve instrument identities.
/// - Maintain stream state.
pub struct JsonExtractor;

impl JsonExtractor {
    // ========================================================
    // OPTIONAL PATH LOOKUP
    // ========================================================

    /// Resolve an optional JSON path.
    ///
    /// Returns None when an object key is absent.
    ///
    /// Missing keys are different from JSON null:
    ///
    /// - Missing key -> None
    /// - Explicit null -> Some(Value::Null)
    ///
    /// Invalid traversal paths and type mismatches
    /// are always reported as errors.
    pub fn optional<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>, ExtractionError> {
        let segments = parse_path(path)?;

        let mut current = root;

        for segment in segments {
            match current {
                // ============================================
                // OBJECT FIELD
                // ============================================
                Value::Object(object) => {
                    let Some(next) = object.get(segment) else {
                        return Ok(None);
                    };

                    current = next;
                }

                // ============================================
                // ARRAY ELEMENT
                // ============================================
                Value::Array(array) => {
                    let index = parse_index(segment, path)?;

                    // Optional lookup treats an absent
                    // array element as missing.
                    let Some(next) = array.get(index) else {
                        return Ok(None);
                    };

                    current = next;
                }

                // ============================================
                // INVALID TRAVERSAL
                // ============================================
                other => {
                    return Err(ExtractionError::TypeMismatch {
                        path: path.to_owned(),
                        expected: "object or array",
                        actual: json_type(other),
                    });
                }
            }
        }

        Ok(Some(current))
    }

    // ========================================================
    // REQUIRED PATH LOOKUP
    // ========================================================

    /// Resolve a required JSON path.
    ///
    /// Missing object fields and absent array elements
    /// are rejected.
    ///
    /// An explicit JSON null is returned as a value.
    /// Callers requiring a non-null value must use a
    /// typed accessor or validate it explicitly.
    pub fn required<'a>(root: &'a Value, path: &str) -> Result<&'a Value, ExtractionError> {
        Self::optional(root, path)?.ok_or_else(|| ExtractionError::MissingField {
            path: path.to_owned(),
        })
    }

    // ========================================================
    // REQUIRED OBJECT
    // ========================================================

    pub fn object<'a>(
        root: &'a Value,
        path: &str,
    ) -> Result<&'a serde_json::Map<String, Value>, ExtractionError> {
        let value = Self::required(root, path)?;

        value
            .as_object()
            .ok_or_else(|| ExtractionError::TypeMismatch {
                path: path.to_owned(),
                expected: "object",
                actual: json_type(value),
            })
    }

    // ========================================================
    // REQUIRED ARRAY
    // ========================================================

    pub fn array<'a>(root: &'a Value, path: &str) -> Result<&'a [Value], ExtractionError> {
        let value = Self::required(root, path)?;

        value
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| ExtractionError::TypeMismatch {
                path: path.to_owned(),
                expected: "array",
                actual: json_type(value),
            })
    }

    // ========================================================
    // REQUIRED STRING
    // ========================================================

    pub fn string<'a>(root: &'a Value, path: &str) -> Result<&'a str, ExtractionError> {
        let value = Self::required(root, path)?;

        value.as_str().ok_or_else(|| ExtractionError::TypeMismatch {
            path: path.to_owned(),
            expected: "string",
            actual: json_type(value),
        })
    }

    // ========================================================
    // REQUIRED BOOLEAN
    // ========================================================

    pub fn boolean(root: &Value, path: &str) -> Result<bool, ExtractionError> {
        let value = Self::required(root, path)?;

        value
            .as_bool()
            .ok_or_else(|| ExtractionError::TypeMismatch {
                path: path.to_owned(),
                expected: "boolean",
                actual: json_type(value),
            })
    }

    // ========================================================
    // REQUIRED INTEGER
    // ========================================================

    /// Extract an unsigned JSON integer.
    ///
    /// Does not accept floating-point JSON numbers,
    /// signed negative values, or numeric strings.
    ///
    /// Numeric strings will be handled explicitly by
    /// the numeric conversion layer using field types.
    pub fn u64(root: &Value, path: &str) -> Result<u64, ExtractionError> {
        let value = Self::required(root, path)?;

        value.as_u64().ok_or_else(|| ExtractionError::TypeMismatch {
            path: path.to_owned(),
            expected: "unsigned integer",
            actual: json_type(value),
        })
    }

    /// Extract a signed JSON integer.
    pub fn i64(root: &Value, path: &str) -> Result<i64, ExtractionError> {
        let value = Self::required(root, path)?;

        value.as_i64().ok_or_else(|| ExtractionError::TypeMismatch {
            path: path.to_owned(),
            expected: "signed integer",
            actual: json_type(value),
        })
    }

    // ========================================================
    // BOUNDED RECORD ARRAY
    // ========================================================

    /// Extract a bounded record array.
    ///
    /// The array itself is returned as a borrowed slice.
    ///
    /// An empty array is valid at this extraction layer.
    ///
    /// The caller decides whether an empty trade batch
    /// or depth snapshot is semantically acceptable.
    pub fn records<'a>(
        root: &'a Value,
        path: &str,
        maximum: usize,
    ) -> Result<&'a [Value], ExtractionError> {
        let records = Self::array(root, path)?;

        if records.len() > maximum {
            return Err(ExtractionError::TooManyRecords {
                path: path.to_owned(),
                actual: records.len(),
                maximum,
            });
        }

        Ok(records)
    }

    // ========================================================
    // OBJECT RECORD ARRAY
    // ========================================================

    /// Extract a bounded array whose entries must all
    /// be JSON objects.
    ///
    /// This validates record representation without
    /// interpreting the fields inside each record.
    pub fn object_records<'a>(
        root: &'a Value,
        path: &str,
        maximum: usize,
    ) -> Result<&'a [Value], ExtractionError> {
        let records = Self::records(root, path, maximum)?;

        for (index, record) in records.iter().enumerate() {
            if !record.is_object() {
                return Err(ExtractionError::InvalidRecord {
                    path: path.to_owned(),
                    index,
                    reason: format!("expected object, got {}", json_type(record),),
                });
            }
        }

        Ok(records)
    }

    // ========================================================
    // ARRAY RECORD ARRAY
    // ========================================================

    /// Extract a bounded array whose entries must all
    /// be JSON arrays.
    ///
    /// Useful for exchange-native depth levels:
    ///
    /// [
    ///     ["80000", "1.5"],
    ///     ["79999", "2.0"]
    /// ]
    pub fn array_records<'a>(
        root: &'a Value,
        path: &str,
        maximum: usize,
    ) -> Result<&'a [Value], ExtractionError> {
        let records = Self::records(root, path, maximum)?;

        for (index, record) in records.iter().enumerate() {
            if !record.is_array() {
                return Err(ExtractionError::InvalidRecord {
                    path: path.to_owned(),
                    index,
                    reason: format!("expected array, got {}", json_type(record),),
                });
            }
        }

        Ok(records)
    }

    // ========================================================
    // POSITIONAL ARRAY ELEMENT
    // ========================================================

    /// Read a required element from an array record.
    ///
    /// This primitive supports the existing
    /// DepthLevelSideConfig fields:
    ///
    /// - price_index
    /// - quantity_index
    /// - order_count_index
    pub fn index(record: &Value, index: usize) -> Result<&Value, ExtractionError> {
        let array = record
            .as_array()
            .ok_or_else(|| ExtractionError::TypeMismatch {
                path: "$".into(),
                expected: "array",
                actual: json_type(record),
            })?;

        array
            .get(index)
            .ok_or_else(|| ExtractionError::IndexOutOfBounds {
                path: "$".into(),
                index,
                length: array.len(),
            })
    }

    // ========================================================
    // OBJECT RECORD FIELD
    // ========================================================

    /// Read a required field from a JSON object record.
    ///
    /// Nested paths are supported.
    pub fn field<'a>(record: &'a Value, field: &str) -> Result<&'a Value, ExtractionError> {
        Self::required(record, field)
    }
}

// ============================================================
// JSON PATH PARSING
// ============================================================

/// Parse a simple dotted JSON path.
///
/// "$" and "" both identify the root.
///
/// "$.result.list" and "result.list" are equivalent.
///
/// Empty path segments are rejected to prevent
/// accidental interpretation of malformed mappings.
///
/// Object keys containing literal dots are not currently
/// representable by this syntax.
fn parse_path(path: &str) -> Result<Vec<&str>, ExtractionError> {
    if path.is_empty() || path == "$" {
        return Ok(Vec::new());
    }

    let normalized = path.strip_prefix("$.").unwrap_or(path);

    if normalized.is_empty() {
        return Err(ExtractionError::InvalidPath {
            path: path.to_owned(),
            reason: "path contains no field after root prefix".into(),
        });
    }

    let segments = normalized.split('.').collect::<Vec<_>>();

    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(ExtractionError::InvalidPath {
            path: path.to_owned(),
            reason: "empty path segment".into(),
        });
    }

    if segments.iter().any(|segment| *segment == "$") {
        return Err(ExtractionError::InvalidPath {
            path: path.to_owned(),
            reason: "root token is only valid at the beginning".into(),
        });
    }

    Ok(segments)
}

// ============================================================
// ARRAY INDEX PARSING
// ============================================================

fn parse_index(segment: &str, path: &str) -> Result<usize, ExtractionError> {
    if !segment.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ExtractionError::InvalidPath {
            path: path.to_owned(),
            reason: format!("array index '{segment}' is not an unsigned integer"),
        });
    }

    segment
        .parse::<usize>()
        .map_err(|error| ExtractionError::InvalidPath {
            path: path.to_owned(),
            reason: format!("invalid array index '{segment}': {error}"),
        })
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ========================================================
    // ROOT EXTRACTION
    // ========================================================

    #[test]
    fn extracts_root_array() {
        let data = json!([
            {"id": 1},
            {"id": 2},
        ]);

        let records = JsonExtractor::object_records(&data, "$", 100).unwrap();

        assert_eq!(records.len(), 2);
    }

    #[test]
    fn empty_path_identifies_root() {
        let data = json!({
            "value": 42
        });

        let root = JsonExtractor::required(&data, "").unwrap();

        assert_eq!(root["value"], 42);
    }

    // ========================================================
    // NESTED EXTRACTION
    // ========================================================

    #[test]
    fn extracts_bybit_trade_records() {
        let data = json!({
            "retCode": 0,
            "result": {
                "list": [
                    {"execId": "1"},
                    {"execId": "2"}
                ]
            }
        });

        let records = JsonExtractor::object_records(&data, "result.list", 10).unwrap();

        assert_eq!(records.len(), 2);

        assert_eq!(JsonExtractor::string(&records[0], "execId",).unwrap(), "1",);
    }

    #[test]
    fn accepts_explicit_root_prefix() {
        let data = json!({
            "result": {
                "list": [
                    {"id": 1}
                ]
            }
        });

        let records = JsonExtractor::records(&data, "$.result.list", 10).unwrap();

        assert_eq!(records.len(), 1);
    }

    // ========================================================
    // ARRAY INDEXING
    // ========================================================

    #[test]
    fn extracts_nested_array_index() {
        let data = json!({
            "data": [
                {"price": "80000"},
                {"price": "80001"}
            ]
        });

        assert_eq!(
            JsonExtractor::string(&data, "data.1.price",).unwrap(),
            "80001",
        );
    }

    #[test]
    fn extracts_depth_level_components() {
        let level = json!(["80000", "1.5", 12]);

        assert_eq!(JsonExtractor::index(&level, 0).unwrap(), "80000",);

        assert_eq!(JsonExtractor::index(&level, 1).unwrap(), "1.5",);

        assert_eq!(JsonExtractor::index(&level, 2).unwrap(), 12,);
    }

    // ========================================================
    // OPTIONAL FIELDS
    // ========================================================

    #[test]
    fn distinguishes_missing_field_from_null() {
        let data = json!({
            "present": null
        });

        assert!(
            JsonExtractor::optional(&data, "missing",)
                .unwrap()
                .is_none(),
        );

        assert!(
            JsonExtractor::optional(&data, "present",)
                .unwrap()
                .unwrap()
                .is_null(),
        );
    }

    #[test]
    fn required_missing_field_fails() {
        let data = json!({
            "price": "80000"
        });

        assert!(matches!(
            JsonExtractor::required(&data, "quantity",),
            Err(ExtractionError::MissingField { .. })
        ));
    }

    // ========================================================
    // TYPE VALIDATION
    // ========================================================

    #[test]
    fn rejects_wrong_array_type() {
        let data = json!({
            "data": {}
        });

        assert!(matches!(
            JsonExtractor::array(&data, "data",),
            Err(ExtractionError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn rejects_numeric_string_as_integer() {
        let data = json!({
            "time": "123456789"
        });

        assert!(matches!(
            JsonExtractor::i64(&data, "time",),
            Err(ExtractionError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn extracts_native_boolean() {
        let data = json!({
            "isBuyerMaker": true
        });

        assert!(JsonExtractor::boolean(&data, "isBuyerMaker",).unwrap(),);
    }

    // ========================================================
    // BOUNDED RECORDS
    // ========================================================

    #[test]
    fn rejects_oversized_record_batch() {
        let data = json!({
            "data": [
                {"id": 1},
                {"id": 2},
                {"id": 3}
            ]
        });

        assert!(matches!(
            JsonExtractor::records(&data, "data", 2,),
            Err(ExtractionError::TooManyRecords {
                actual: 3,
                maximum: 2,
                ..
            })
        ));
    }

    #[test]
    fn accepts_empty_record_batch() {
        let data = json!({
            "data": []
        });

        let records = JsonExtractor::object_records(&data, "data", 100).unwrap();

        assert!(records.is_empty());
    }

    // ========================================================
    // RECORD REPRESENTATION
    // ========================================================

    #[test]
    fn rejects_non_object_trade_record() {
        let data = json!({
            "data": [
                {"id": 1},
                123
            ]
        });

        assert!(matches!(
            JsonExtractor::object_records(&data, "data", 10,),
            Err(ExtractionError::InvalidRecord { index: 1, .. })
        ));
    }

    #[test]
    fn accepts_array_based_depth_levels() {
        let data = json!({
            "bids": [
                ["80000", "1.0"],
                ["79999", "2.0"]
            ]
        });

        let records = JsonExtractor::array_records(&data, "bids", 10).unwrap();

        assert_eq!(records.len(), 2);
    }

    // ========================================================
    // MALFORMED PATHS
    // ========================================================

    #[test]
    fn rejects_empty_path_segments() {
        let data = json!({
            "result": {
                "list": []
            }
        });

        assert!(matches!(
            JsonExtractor::required(&data, "result..list",),
            Err(ExtractionError::InvalidPath { .. })
        ));
    }

    #[test]
    fn rejects_invalid_array_index() {
        let data = json!({
            "data": [1, 2, 3]
        });

        assert!(matches!(
            JsonExtractor::required(&data, "data.first",),
            Err(ExtractionError::InvalidPath { .. })
        ));
    }

    #[test]
    fn rejects_out_of_bounds_index() {
        let level = json!(["80000"]);

        assert!(matches!(
            JsonExtractor::index(&level, 2,),
            Err(ExtractionError::IndexOutOfBounds {
                index: 2,
                length: 1,
                ..
            })
        ));
    }

    // ========================================================
    // NO ECONOMIC INTERPRETATION
    // ========================================================

    #[test]
    fn extraction_preserves_native_decimal_string() {
        let data = json!({
            "price": "80000.123456789123456789"
        });

        let price = JsonExtractor::string(&data, "price").unwrap();

        assert_eq!(price, "80000.123456789123456789",);
    }
}
