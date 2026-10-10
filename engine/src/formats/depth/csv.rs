use csv::ByteRecord;
use serde_json::{Map, Value};

use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

#[derive(Debug, Clone)]
pub struct DepthCsvAdapter {
    fields: Vec<(usize, String)>,
    expected_columns: usize,
}

impl DepthCsvAdapter {
    pub fn new(raw_schema: &Value) -> Result<Self> {
        let fields = raw_schema
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_error("CSV raw schema missing fields"))?;

        if fields.is_empty() {
            return invalid("CSV raw schema contains no fields");
        }

        let mut resolved = Vec::with_capacity(fields.len());
        let mut positions = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();

        for field in fields {
            let name = field
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_error("CSV field missing name"))?;

            let position = field
                .get("position")
                .and_then(Value::as_u64)
                .ok_or_else(|| invalid_error("CSV field missing position"))?;

            if position == 0 {
                return invalid("CSV field positions must be 1-based");
            }

            let index = usize::try_from(position - 1)
                .map_err(|_| invalid_error("CSV field position overflow"))?;

            if !positions.insert(index) {
                return invalid("duplicate CSV field position");
            }

            if !names.insert(name.to_owned()) {
                return invalid("duplicate CSV field name");
            }

            resolved.push((index, name.to_owned()));
        }

        let expected_columns = resolved.len();

        // Require contiguous positions: 1..=N.
        if positions.iter().copied().ne(0..expected_columns) {
            return invalid("CSV field positions must be contiguous");
        }

        Ok(Self {
            fields: resolved,
            expected_columns,
        })
    }

    pub fn adapt(&self, record: &ByteRecord) -> Result<Value> {
        if record.len() != self.expected_columns {
            return invalid(format!(
                "CSV column count mismatch: expected {}, received {}",
                self.expected_columns,
                record.len()
            ));
        }

        let mut object = Map::new();

        for (index, name) in &self.fields {
            let bytes = record
                .get(*index)
                .ok_or_else(|| invalid_error("missing CSV field"))?;

            let value = std::str::from_utf8(bytes)
                .map_err(|_| invalid_error("CSV field is not valid UTF-8"))?;

            // Preserve exact decimal and integer representations.
            object.insert(name.clone(), Value::String(value.to_owned()));
        }

        Ok(Value::Object(object))
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
