use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityAction {
    Degrade,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityProfile {
    Standard,
    Strict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityRule {
    pub action: IntegrityAction,

    #[serde(default)]
    pub max_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityPolicy {
    pub profile: IntegrityProfile,

    pub parse_failure: IntegrityRule,
    pub invalid_record: IntegrityRule,
    pub sequence_gap: IntegrityRule,
    pub timestamp_regression: IntegrityRule,
    pub missing_snapshot: IntegrityRule,
    pub invalid_book: IntegrityRule,
    pub transformation_failure: IntegrityRule,
}
