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
