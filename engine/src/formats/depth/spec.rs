use serde::Deserialize;
use serde_json::Value;

use crate::{
    error::{MarketForgeError, Result},
    job::{NormalizationConfig, QuantityEncoding, TargetSchema, TimestampEncoding},
};

// -----------------------------------------------------------------------------
// Depth operation
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepthOperation {
    Snapshot(SnapshotSpec),
    AbsoluteUpdate,
    RelativeUpdate(RelativeUpdateSpec),
}

// -----------------------------------------------------------------------------
// Field specifications
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FieldSpec {
    pub source: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct LevelArraySpec {
    pub source: String,
    pub price_index: usize,
    pub quantity_index: usize,

    #[serde(default)]
    pub order_count_index: Option<usize>,

    #[serde(default)]
    pub decode: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ScalarLevelSpec {
    pub price: FieldSpec,
    pub quantity: QuantityFieldSpec,
    pub side: SideSpec,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct QuantityFieldSpec {
    pub source: String,

    #[serde(default)]
    pub transform: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SideSpec {
    pub source: String,

    #[serde(default)]
    pub transform: Option<String>,

    #[serde(default)]
    pub values: Option<Value>,

    #[serde(default)]
    pub positive: Option<String>,

    #[serde(default)]
    pub negative: Option<String>,
}

// -----------------------------------------------------------------------------
// Event filters
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct EventFilter {
    pub source: String,

    #[serde(default)]
    pub equals: Option<String>,

    #[serde(default, rename = "in")]
    pub one_of: Option<Vec<String>>,
}

impl EventFilter {
    fn validate(&self) -> Result<()> {
        if self.source.trim().is_empty() {
            return invalid("depth event filter source must not be empty");
        }

        match (&self.equals, &self.one_of) {
            (Some(_), None) => Ok(()),

            (None, Some(values)) if !values.is_empty() => Ok(()),

            _ => invalid("depth event filter must define exactly one of equals or in"),
        }
    }
}

// -----------------------------------------------------------------------------
// Snapshot specification
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SnapshotSpec {
    #[serde(rename = "type")]
    pub kind: String,

    #[serde(default)]
    pub group_by: Option<String>,
}

// -----------------------------------------------------------------------------
// Relative update specification
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RelativeUpdateSpec {
    pub action_source: String,
    pub add_action: String,
    pub subtract_action: String,

    #[serde(default)]
    pub emit: Option<String>,

    #[serde(default)]
    pub zero_result: Option<String>,

    #[serde(default)]
    pub nonzero_result: Option<String>,
}

// -----------------------------------------------------------------------------
// Generic depth specification
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSpec {
    pub timestamp_encoding: TimestampEncoding,
    pub quantity_encoding: QuantityEncoding,

    pub event_timestamp: FieldSpec,
    pub system_timestamp: Option<FieldSpec>,

    pub instrument: Option<InstrumentFieldSpec>,

    pub event_filter: Option<EventFilter>,
    pub operation: DepthOperation,

    pub bids: Option<LevelArraySpec>,
    pub asks: Option<LevelArraySpec>,

    pub scalar_level: Option<ScalarLevelSpec>,

    pub sequence_start: Option<FieldSpec>,
    pub sequence_count: Option<FieldSpec>,

    pub input_ordering: Option<InputOrderingSpec>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct InstrumentFieldSpec {
    pub source: String,

    #[serde(default)]
    pub lookup: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct InputOrderingSpec {
    pub order: String,
    pub sort_by: String,
}

// -----------------------------------------------------------------------------
// Rule deserialization
// -----------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DepthRules {
    event_timestamp: FieldSpec,

    #[serde(default)]
    system_timestamp: Option<FieldSpec>,

    #[serde(default)]
    instrument: Option<InstrumentFieldSpec>,

    #[serde(default)]
    event_filter: Option<EventFilter>,

    #[serde(default)]
    snapshot: Option<SnapshotSpec>,

    #[serde(default)]
    update_semantics: Option<UpdateSemanticsSpec>,

    #[serde(default)]
    bids: Option<LevelArraySpec>,

    #[serde(default)]
    asks: Option<LevelArraySpec>,

    #[serde(default)]
    price: Option<FieldSpec>,

    #[serde(default)]
    quantity: Option<QuantityFieldSpec>,

    #[serde(default)]
    side: Option<SideSpec>,

    #[serde(default)]
    sequence_start: Option<FieldSpec>,

    #[serde(default)]
    sequence_count: Option<FieldSpec>,

    #[serde(default)]
    input_ordering: Option<InputOrderingSpec>,
}

#[derive(Debug, Deserialize)]
struct UpdateSemanticsSpec {
    #[serde(rename = "type")]
    kind: String,

    #[serde(default)]
    action_source: Option<String>,

    #[serde(default)]
    add_action: Option<String>,

    #[serde(default)]
    subtract_action: Option<String>,

    #[serde(default)]
    emit: Option<String>,

    #[serde(default)]
    zero_result: Option<String>,

    #[serde(default)]
    nonzero_result: Option<String>,
}

// -----------------------------------------------------------------------------
// Compilation
// -----------------------------------------------------------------------------

impl DepthSpec {
    pub fn from_normalization(normalization: &NormalizationConfig) -> Result<Self> {
        if normalization.target_schema != TargetSchema::Depth {
            return invalid("depth specification requires target_schema=depth");
        }

        let rules: DepthRules =
            serde_json::from_value(normalization.rules.clone()).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "invalid depth normalization rules: {error}"
                ))
            })?;

        if let Some(filter) = &rules.event_filter {
            filter.validate()?;
        }

        let operation = match (rules.snapshot, rules.update_semantics) {
            (Some(snapshot), None) => DepthOperation::Snapshot(snapshot),

            (None, Some(update)) => match update.kind.as_str() {
                "absolute_level" => DepthOperation::AbsoluteUpdate,

                "relative_level" => {
                    let relative = RelativeUpdateSpec {
                        action_source: required(update.action_source, "action_source")?,
                        add_action: required(update.add_action, "add_action")?,
                        subtract_action: required(update.subtract_action, "subtract_action")?,
                        emit: update.emit,
                        zero_result: update.zero_result,
                        nonzero_result: update.nonzero_result,
                    };

                    DepthOperation::RelativeUpdate(relative)
                }

                other => {
                    return invalid(format!("unsupported depth update semantics: {other}"));
                }
            },

            (Some(_), Some(_)) => {
                return invalid(
                    "depth normalization cannot define both snapshot and update_semantics",
                );
            }

            (None, None) => {
                return invalid("depth normalization requires snapshot or update_semantics");
            }
        };

        let scalar_level = match (rules.price, rules.quantity, rules.side) {
            (Some(price), Some(quantity), Some(side)) => Some(ScalarLevelSpec {
                price,
                quantity,
                side,
            }),

            (None, None, None) => None,

            _ => {
                return invalid("scalar depth levels require price, quantity, and side rules");
            }
        };

        let has_arrays = rules.bids.is_some() && rules.asks.is_some();
        let has_scalar = scalar_level.is_some();

        if has_arrays == has_scalar {
            return invalid("depth normalization requires either bid/ask arrays or scalar levels");
        }

        if let DepthOperation::Snapshot(snapshot) = &operation {
            match snapshot.kind.as_str() {
                "single_event" | "single_row" | "group_rows" => {}

                other => {
                    return invalid(format!("unsupported depth snapshot type: {other}"));
                }
            }

            if snapshot.kind == "group_rows" && snapshot.group_by.is_none() {
                return invalid("group_rows snapshot requires group_by");
            }
        }

        Ok(Self {
            timestamp_encoding: normalization.timestamp_encoding,
            quantity_encoding: normalization.quantity_encoding,

            event_timestamp: rules.event_timestamp,
            system_timestamp: rules.system_timestamp,

            instrument: rules.instrument,

            event_filter: rules.event_filter,
            operation,

            bids: rules.bids,
            asks: rules.asks,

            scalar_level,

            sequence_start: rules.sequence_start,
            sequence_count: rules.sequence_count,

            input_ordering: rules.input_ordering,
        })
    }
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn required(value: Option<String>, name: &str) -> Result<String> {
    value.ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!("relative depth update requires {name}"))
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
