use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------
// Integrity violation categories
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityCategory {
    ParseFailure,
    InvalidRecord,
    SequenceGap,
    TimestampRegression,
    MissingSnapshot,
    InvalidBook,
    TransformationFailure,
}
// -----------------------------------------------------------------------------
// Integrity actions
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityAction {
    Degrade,
    Fail,
}

// -----------------------------------------------------------------------------
// Time-window thresholds
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityWindowRule {
    pub max_rate: f64,
    pub minimum_samples: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityWindows {
    #[serde(default)]
    pub daily: Option<IntegrityWindowRule>,

    #[serde(default)]
    pub hourly: Option<IntegrityWindowRule>,
}

// -----------------------------------------------------------------------------
// Integrity rules
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityRule {
    pub action: IntegrityAction,

    #[serde(default)]
    pub max_count: Option<u64>,

    #[serde(default)]
    pub max_rate: Option<f64>,

    #[serde(default)]
    pub minimum_samples: u64,

    #[serde(default)]
    pub windows: IntegrityWindows,
}

// -----------------------------------------------------------------------------
// Global integrity policy
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityPolicy {
    #[serde(
        default = "default_integrity_enabled",
        alias = "profile",
        deserialize_with = "deserialize_integrity_enabled"
    )]
    pub enabled: bool,

    pub parse_failure: IntegrityRule,
    pub invalid_record: IntegrityRule,
    pub sequence_gap: IntegrityRule,
    pub timestamp_regression: IntegrityRule,
    pub missing_snapshot: IntegrityRule,
    pub invalid_book: IntegrityRule,
    pub transformation_failure: IntegrityRule,
}

fn default_integrity_enabled() -> bool {
    true
}

fn deserialize_integrity_enabled<'de, D>(deserializer: D) -> std::result::Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum LegacyEnabled {
        Boolean(bool),
        Profile(String),
    }

    match LegacyEnabled::deserialize(deserializer)? {
        LegacyEnabled::Boolean(enabled) => Ok(enabled),

        LegacyEnabled::Profile(profile) => match profile.as_str() {
            "standard" | "strict" => Ok(true),

            _ => Err(serde::de::Error::custom(format!(
                "unsupported legacy integrity profile: {profile}"
            ))),
        },
    }
}
