use serde::{Deserialize, Serialize};

use super::validation;

// ============================================================
// DERIVED STREAM CREATION REQUEST
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDerivedStreamRequest {
    /// Unique requested name of the derived stream.
    pub name: String,

    /// Merge or currency conversion.
    pub operation: DerivedOperation,

    /// Existing source-stream IDs.
    pub inputs: Vec<String>,

    /// Required for merge operations.
    ///
    /// Not permitted for conversion-only operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordering: Option<MergeOrdering>,

    /// Live event-time lateness allowance in nanoseconds.
    ///
    /// Required for event_time merging.
    /// Not permitted for arrival_order merging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lateness_ns: Option<u64>,

    /// Optional for merge; required for convert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_quote_asset: Option<String>,
}

// ============================================================
// DERIVED OPERATIONS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedOperation {
    Merge,
    Convert,
}

impl DerivedOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Merge => "merge",
            Self::Convert => "convert",
        }
    }
}

// ============================================================
// MERGE ORDERING
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeOrdering {
    ArrivalOrder,
    EventTime,
}

impl MergeOrdering {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArrivalOrder => "arrival_order",
            Self::EventTime => "event_time",
        }
    }
}

// ============================================================
// VALIDATION ENTRY POINT
// ============================================================

impl CreateDerivedStreamRequest {
    /// Validate the request independently of runtime state.
    ///
    /// Resolving source IDs, checking source compatibility,
    /// checking conversion-rate availability, and duplicate
    /// stream detection happen during stream creation.
    pub fn validate(&self) -> Result<(), String> {
        validation::validate_derived(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::live::stream::request::{
        CreateDerivedStreamRequest, DerivedOperation, MergeOrdering,
    };

    use serde_json::{Value, json};

    // ============================================================
    // FIXTURES
    // ============================================================

    fn arrival_merge() -> CreateDerivedStreamRequest {
        serde_json::from_value(json!({
            "name": "btc_arrival_merge",
            "operation": "merge",
            "inputs": ["stream_a", "stream_b"],
            "ordering": "arrival_order"
        }))
        .unwrap()
    }

    fn ordered_merge() -> CreateDerivedStreamRequest {
        serde_json::from_value(json!({
            "name": "btc_ordered_merge",
            "operation": "merge",
            "inputs": ["stream_a", "stream_b"],
            "ordering": "event_time",
            "max_lateness_ns": 100_000_000
        }))
        .unwrap()
    }

    fn conversion() -> CreateDerivedStreamRequest {
        serde_json::from_value(json!({
            "name": "btc_converted",
            "operation": "convert",
            "inputs": ["stream_a"],
            "target_quote_asset": "USDT"
        }))
        .unwrap()
    }

    fn assert_invalid(request: &CreateDerivedStreamRequest, expected: &str) {
        let error = request.validate().expect_err("Expected validation to fail");

        assert!(
            error.contains(expected),
            "Expected {expected:?}, got: {error}"
        );
    }

    // ============================================================
    // VALID REQUESTS
    // ============================================================

    #[test]
    fn accept_arrival_order_merge() {
        let request = arrival_merge();

        request.validate().unwrap();

        assert_eq!(request.operation, DerivedOperation::Merge);
        assert_eq!(request.ordering, Some(MergeOrdering::ArrivalOrder));
        assert_eq!(request.max_lateness_ns, None);
    }

    #[test]
    fn accept_event_time_merge() {
        let request = ordered_merge();

        request.validate().unwrap();

        assert_eq!(request.ordering, Some(MergeOrdering::EventTime));

        assert_eq!(request.max_lateness_ns, Some(100_000_000));
    }

    #[test]
    fn accept_merge_with_conversion() {
        let mut request = ordered_merge();

        request.target_quote_asset = Some("USDT".into());

        request.validate().unwrap();
    }

    #[test]
    fn accept_conversion_only() {
        conversion().validate().unwrap();
    }

    #[test]
    fn accept_many_inputs() {
        let mut request = ordered_merge();

        request.inputs = (0..100).map(|id| format!("stream_{id}")).collect();

        request.validate().unwrap();

        assert_eq!(request.inputs.len(), 100);
    }

    #[test]
    fn accept_minimum_positive_lateness() {
        let mut request = ordered_merge();

        request.max_lateness_ns = Some(1);

        request.validate().unwrap();
    }

    #[test]
    fn accept_large_lateness() {
        let mut request = ordered_merge();

        request.max_lateness_ns = Some(u64::MAX);

        request.validate().unwrap();
    }

    // ============================================================
    // SERIALIZATION
    // ============================================================

    #[test]
    fn requests_round_trip() {
        for request in [arrival_merge(), ordered_merge(), conversion()] {
            let serialized = serde_json::to_string(&request).unwrap();

            let restored: CreateDerivedStreamRequest = serde_json::from_str(&serialized).unwrap();

            assert_eq!(request, restored);
        }
    }

    #[test]
    fn omitted_optional_fields_remain_omitted() {
        let request = arrival_merge();

        let value = serde_json::to_value(&request).unwrap();

        assert!(value.get("max_lateness_ns").is_none());
        assert!(value.get("target_quote_asset").is_none());
    }

    #[test]
    fn reject_unknown_field() {
        let result = serde_json::from_value::<CreateDerivedStreamRequest>(json!({
            "name": "test",
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "arrival_order",
            "buffer_capacity": 1000
        }));

        assert!(result.is_err());
    }

    #[test]
    fn reject_unknown_operation() {
        let result = serde_json::from_value::<CreateDerivedStreamRequest>(json!({
            "name": "test",
            "operation": "aggregate",
            "inputs": ["a", "b"],
            "ordering": "arrival_order"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn reject_unknown_ordering() {
        let result = serde_json::from_value::<CreateDerivedStreamRequest>(json!({
            "name": "test",
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "random"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn reject_negative_lateness() {
        let result = serde_json::from_value::<CreateDerivedStreamRequest>(json!({
            "name": "test",
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "event_time",
            "max_lateness_ns": -1
        }));

        assert!(result.is_err());
    }

    #[test]
    fn reject_fractional_lateness() {
        let result = serde_json::from_value::<CreateDerivedStreamRequest>(json!({
            "name": "test",
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "event_time",
            "max_lateness_ns": 1.5
        }));

        assert!(result.is_err());
    }

    // ============================================================
    // IDENTITY VALIDATION
    // ============================================================

    #[test]
    fn reject_empty_name() {
        let mut request = arrival_merge();

        request.name.clear();

        assert_invalid(&request, "name");
    }

    #[test]
    fn reject_name_with_whitespace() {
        let mut request = arrival_merge();

        request.name = "btc merged".into();

        assert_invalid(&request, "name");
    }

    #[test]
    fn reject_empty_inputs() {
        let mut request = arrival_merge();

        request.inputs.clear();

        assert_invalid(&request, "at least one input");
    }

    #[test]
    fn reject_duplicate_inputs() {
        let mut request = arrival_merge();

        request.inputs = vec!["stream_a".into(), "stream_a".into()];

        assert_invalid(&request, "duplicate");
    }

    #[test]
    fn reject_empty_input_id() {
        let mut request = arrival_merge();

        request.inputs[0].clear();

        assert_invalid(&request, "inputs");
    }

    #[test]
    fn reject_input_with_whitespace() {
        let mut request = arrival_merge();

        request.inputs[0] = "stream a".into();

        assert_invalid(&request, "inputs");
    }

    // ============================================================
    // MERGE VALIDATION
    // ============================================================

    #[test]
    fn reject_merge_with_one_input() {
        let mut request = ordered_merge();

        request.inputs = vec!["stream_a".into()];

        assert_invalid(&request, "at least two inputs");
    }

    #[test]
    fn reject_merge_without_ordering() {
        let mut request = ordered_merge();

        request.ordering = None;

        assert_invalid(&request, "requires ordering");
    }

    #[test]
    fn reject_arrival_order_with_lateness() {
        let mut request = arrival_merge();

        request.max_lateness_ns = Some(100_000_000);

        assert_invalid(&request, "not allowed");
    }

    #[test]
    fn reject_event_time_without_lateness() {
        let mut request = ordered_merge();

        request.max_lateness_ns = None;

        assert_invalid(&request, "requires max_lateness_ns");
    }

    #[test]
    fn reject_zero_lateness() {
        let mut request = ordered_merge();

        request.max_lateness_ns = Some(0);

        assert_invalid(&request, "greater than zero");
    }

    // ============================================================
    // CONVERSION VALIDATION
    // ============================================================

    #[test]
    fn reject_conversion_without_target_asset() {
        let mut request = conversion();

        request.target_quote_asset = None;

        assert_invalid(&request, "requires target_quote_asset");
    }

    #[test]
    fn reject_conversion_with_multiple_inputs() {
        let mut request = conversion();

        request.inputs.push("stream_b".into());

        assert_invalid(&request, "exactly one input");
    }

    #[test]
    fn reject_conversion_with_ordering() {
        let mut request = conversion();

        request.ordering = Some(MergeOrdering::EventTime);

        assert_invalid(&request, "ordering is not allowed");
    }

    #[test]
    fn reject_conversion_with_lateness() {
        let mut request = conversion();

        request.max_lateness_ns = Some(100);

        assert_invalid(&request, "max_lateness_ns is not allowed");
    }

    #[test]
    fn reject_empty_target_asset() {
        let mut request = conversion();

        request.target_quote_asset = Some(String::new());

        assert_invalid(&request, "target_quote_asset");
    }

    #[test]
    fn reject_invalid_target_asset() {
        let mut request = conversion();

        request.target_quote_asset = Some("US DT".into());

        assert_invalid(&request, "target_quote_asset");
    }

    // ============================================================
    // ENUMS
    // ============================================================

    #[test]
    fn operation_as_str() {
        assert_eq!(DerivedOperation::Merge.as_str(), "merge");
        assert_eq!(DerivedOperation::Convert.as_str(), "convert");
    }

    #[test]
    fn ordering_as_str() {
        assert_eq!(MergeOrdering::ArrivalOrder.as_str(), "arrival_order");

        assert_eq!(MergeOrdering::EventTime.as_str(), "event_time");
    }

    // ============================================================
    // VALIDATION PROPERTIES
    // ============================================================

    #[test]
    fn validation_does_not_mutate_request() {
        let request = ordered_merge();
        let original = request.clone();

        request.validate().unwrap();

        assert_eq!(request, original);
    }

    #[test]
    fn validation_is_repeatable() {
        let request = ordered_merge();

        request.validate().unwrap();
        request.validate().unwrap();
    }

    // ============================================================
    // OPTIONAL FIELD DESERIALIZATION
    // ============================================================

    #[test]
    fn accept_explicit_null_optional_fields() {
        let request: CreateDerivedStreamRequest = serde_json::from_value(json!({
            "name": "test",
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "arrival_order",
            "max_lateness_ns": null,
            "target_quote_asset": null
        }))
        .unwrap();

        request.validate().unwrap();
    }

    #[test]
    fn reject_missing_required_name() {
        let value: Value = json!({
            "operation": "merge",
            "inputs": ["a", "b"],
            "ordering": "arrival_order"
        });

        assert!(serde_json::from_value::<CreateDerivedStreamRequest>(value).is_err());
    }

    #[test]
    fn reject_missing_required_inputs() {
        let value: Value = json!({
            "name": "test",
            "operation": "merge",
            "ordering": "arrival_order"
        });

        assert!(serde_json::from_value::<CreateDerivedStreamRequest>(value).is_err());
    }

    #[test]
    fn reject_missing_required_operation() {
        let value: Value = json!({
            "name": "test",
            "inputs": ["a", "b"],
            "ordering": "arrival_order"
        });

        assert!(serde_json::from_value::<CreateDerivedStreamRequest>(value).is_err());
    }
}
