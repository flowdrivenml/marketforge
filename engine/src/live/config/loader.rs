//! Bounded cold-path TOML loading. Parser diagnostics never echo input values.
use crate::live::model::{configuration::DesiredConfiguration, error::ErrorCode};
use std::{io::Read, path::Path};
use toml_edit::{DocumentMut, Item, Value};

#[derive(Debug, Clone, Copy)]
pub struct LoadLimits {
    pub bytes: usize,
    pub nodes: usize,
    pub depth: usize,
    pub items: usize,
}
/// A parsed, version-checked static specification is NOT a verified protocol or
/// normalization plan. Its owning compiler must interpret every native field.
pub struct StaticSpecification {
    document: DocumentMut,
}
impl StaticSpecification {
    pub fn document(&self) -> &DocumentMut {
        &self.document
    }
}
impl LoadLimits {
    fn check(&self) -> Result<(), ErrorCode> {
        if self.bytes == 0
            || self.bytes == usize::MAX
            || self.nodes == 0
            || self.depth == 0
            || self.depth > 128
            || self.items == 0
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(())
    }
}
pub fn read_static(path: &Path, limits: LoadLimits) -> Result<StaticSpecification, ErrorCode> {
    limits.check()?;
    let file = std::fs::File::open(path).map_err(|_| ErrorCode::Persistence)?;
    let mut bytes = Vec::new();
    file.take(limits.bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ErrorCode::Persistence)?;
    if bytes.len() > limits.bytes {
        return Err(ErrorCode::Limit);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| ErrorCode::Malformed)?;
    parse_static(text, limits)
}
pub fn parse_static(text: &str, limits: LoadLimits) -> Result<StaticSpecification, ErrorCode> {
    limits.check()?;
    if text.len() > limits.bytes {
        return Err(ErrorCode::Limit);
    }
    let document: DocumentMut = text.parse().map_err(|_| ErrorCode::Malformed)?;
    if document.get("schema_version").and_then(Item::as_integer) != Some(1) {
        return Err(ErrorCode::UnsupportedVersion);
    }
    // Check depth/node bounds without recursively walking attacker-controlled input.
    let mut pending = vec![(document.as_item(), 0)];
    let mut nodes = 0usize;
    while let Some((item, depth)) = pending.pop() {
        nodes = nodes.checked_add(1).ok_or(ErrorCode::Limit)?;
        if nodes > limits.nodes || depth > limits.depth {
            return Err(ErrorCode::Limit);
        }
        if let Some(table) = item.as_table() {
            if table.len()
                > limits
                    .nodes
                    .saturating_sub(nodes)
                    .saturating_sub(pending.len())
            {
                return Err(ErrorCode::Limit);
            }
            pending.extend(table.iter().map(|(_, v)| (v, depth + 1)));
        } else if let Some(array) = item.as_array_of_tables() {
            if array.len() > limits.nodes.saturating_sub(nodes) {
                return Err(ErrorCode::Limit);
            }
            // Tables in arrays are visited through a second bounded value conversion below.
        }
    }
    let mut remaining = limits.nodes;
    item_json(document.as_item(), limits.depth, &mut remaining)?;
    Ok(StaticSpecification { document })
}
fn debit(depth: usize, remaining: &mut usize) -> Result<(), ErrorCode> {
    if depth == 0 || *remaining == 0 {
        return Err(ErrorCode::Limit);
    }
    *remaining -= 1;
    Ok(())
}
fn item_json(
    item: &Item,
    depth: usize,
    remaining: &mut usize,
) -> Result<serde_json::Value, ErrorCode> {
    debit(depth, remaining)?;
    match item {
        Item::None => Err(ErrorCode::Malformed),
        Item::Value(value) => value_json(value, depth, remaining),
        Item::Table(table) => table_json(table, depth, remaining),
        Item::ArrayOfTables(array) => array
            .iter()
            .map(|t| table_json(t, depth.saturating_sub(1), remaining))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
    }
}
fn table_json(
    table: &toml_edit::Table,
    depth: usize,
    remaining: &mut usize,
) -> Result<serde_json::Value, ErrorCode> {
    debit(depth, remaining)?;
    table
        .iter()
        .map(|(key, item)| {
            Ok((
                key.to_owned(),
                item_json(item, depth.saturating_sub(1), remaining)?,
            ))
        })
        .collect::<Result<serde_json::Map<_, _>, ErrorCode>>()
        .map(serde_json::Value::Object)
}
fn value_json(
    value: &Value,
    depth: usize,
    remaining: &mut usize,
) -> Result<serde_json::Value, ErrorCode> {
    debit(depth, remaining)?;
    Ok(match value {
        Value::String(v) => v.value().clone().into(),
        Value::Integer(v) => (*v.value()).into(),
        Value::Boolean(v) => (*v.value()).into(),
        Value::Float(v) => serde_json::Number::from_f64(*v.value())
            .ok_or(ErrorCode::InvalidInput)?
            .into(),
        Value::Datetime(_) => return Err(ErrorCode::UnsupportedCapability),
        Value::Array(array) => serde_json::Value::Array(
            array
                .iter()
                .map(|v| value_json(v, depth.saturating_sub(1), remaining))
                .collect::<Result<_, _>>()?,
        ),
        Value::InlineTable(table) => serde_json::Value::Object(
            table
                .iter()
                .map(|(k, v)| {
                    Ok((
                        k.to_owned(),
                        value_json(v, depth.saturating_sub(1), remaining)?,
                    ))
                })
                .collect::<Result<_, ErrorCode>>()?,
        ),
    })
}
/// Load the frozen canonical v1 configuration schema. Absent inheritable fields
/// mean Inherit; all other missing/unknown fields fail deserialization.
pub fn parse_desired(text: &str, limits: LoadLimits) -> Result<DesiredConfiguration, ErrorCode> {
    let spec = parse_static(text, limits)?;
    let mut remaining = limits.nodes;
    let mut value = item_json(spec.document.as_item(), limits.depth, &mut remaining)?;
    for (collection, fields) in [
        ("sources", &["recording", "recovery", "metrics"][..]),
        ("derived", &["recording"][..]),
    ] {
        if let Some(items) = value
            .get_mut(collection)
            .and_then(serde_json::Value::as_array_mut)
        {
            for item in items {
                if let Some(object) = item.as_object_mut() {
                    for field in fields {
                        object
                            .entry((*field).to_owned())
                            .or_insert_with(|| serde_json::json!({"mode":"inherit"}));
                    }
                }
            }
        }
    }
    let desired: DesiredConfiguration =
        serde_json::from_value(value.clone()).map_err(|_| ErrorCode::InvalidInput)?;
    // Shared DTO readers may be permissive. Compare the accepted representation
    // to the input to detect ignored fields without duplicating their schemas.
    let canonical = serde_json::to_value(&desired).map_err(|_| ErrorCode::Internal)?;
    let mut pending = vec![(&value, &canonical)];
    while let Some((input, output)) = pending.pop() {
        match (input, output) {
            (serde_json::Value::Object(input), serde_json::Value::Object(output)) => {
                for (key, value) in input {
                    pending.push((value, output.get(key).ok_or(ErrorCode::InvalidInput)?));
                }
            }
            (serde_json::Value::Array(input), serde_json::Value::Array(output)) => {
                if input.len() != output.len() {
                    return Err(ErrorCode::InvalidInput);
                }
                pending.extend(input.iter().zip(output));
            }
            _ => {}
        }
    }
    desired.validate_structure(limits.items)?;
    Ok(desired)
}

/// Canonical v1 file loading with the same byte/node/structure bounds.
pub fn read_desired(path: &Path, limits: LoadLimits) -> Result<DesiredConfiguration, ErrorCode> {
    limits.check()?;
    let file = std::fs::File::open(path).map_err(|_| ErrorCode::Persistence)?;
    let mut bytes = Vec::new();
    file.take(limits.bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ErrorCode::Persistence)?;
    if bytes.len() > limits.bytes {
        return Err(ErrorCode::Limit);
    }
    parse_desired(
        std::str::from_utf8(&bytes).map_err(|_| ErrorCode::Malformed)?,
        limits,
    )
}
