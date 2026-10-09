use serde_json::Value;

use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

/// Resolve a dotted JSON path such as `data.b` or `data.s`.
pub fn resolve_json_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return None;
    }

    path.split('.')
        .try_fold(record, |current, key| current.get(key))
}

/// Extract a required JSON field.
pub fn required_json_field<'a>(record: &'a Value, path: &str) -> Result<&'a Value> {
    resolve_json_path(record, path)
        .ok_or_else(|| invalid_error(format!("missing depth JSON field: {path}")))
}

/// Extract a string or numeric field as text.
pub fn json_scalar<'a>(record: &'a Value, path: &str) -> Result<&'a str> {
    let value = required_json_field(record, path)?;

    match value {
        Value::String(value) => Ok(value),

        _ => Err(invalid_error(format!(
            "depth JSON field {path} must be a string"
        ))),
    }
}

/// Check whether a JSON record matches an event filter.
pub fn matches_event_filter(record: &Value, filter: &super::EventFilter) -> Result<bool> {
    let value = required_json_field(record, &filter.source)?;

    let actual = match value {
        Value::String(value) => value.as_str(),

        _ => {
            return Err(invalid_error(format!(
                "depth event filter field {} must be a string",
                filter.source
            )));
        }
    };

    if let Some(expected) = &filter.equals {
        return Ok(actual == expected);
    }

    if let Some(values) = &filter.one_of {
        return Ok(values.iter().any(|expected| expected == actual));
    }

    Ok(false)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}
