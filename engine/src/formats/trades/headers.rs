use csv::ByteRecord;
use serde_json::Value;

use crate::error::{MarketForgeError, Result};

pub fn resolve_trade_headers(
    raw_schema: &Value,
    csv_headers: Option<&ByteRecord>,
) -> Result<ByteRecord> {
    let has_headers = raw_schema
        .get("header")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "trade raw schema requires a boolean header field".to_owned(),
            )
        })?;

    if has_headers {
        return csv_headers.cloned().ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "headered trade CSV requires actual CSV headers".to_owned(),
            )
        });
    }

    let fields = raw_schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "headerless trade CSV requires raw_schema.fields".to_owned(),
            )
        })?;

    if fields.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "headerless trade CSV has no field definitions".to_owned(),
        ));
    }

    let mut headers = ByteRecord::new();

    for field in fields {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "raw schema contains an unnamed field".to_owned(),
                )
            })?;

        headers.push_field(name.as_bytes());
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_headerless_fields() {
        let schema = serde_json::json!({
            "header": false,
            "fields": [
                {"name": "timestamp"},
                {"name": "price"},
                {"name": "quantity"}
            ]
        });

        let headers = resolve_trade_headers(&schema, None).unwrap();

        assert_eq!(
            headers,
            ByteRecord::from(vec!["timestamp", "price", "quantity"])
        );
    }

    #[test]
    fn preserves_actual_headers() {
        let schema = serde_json::json!({
            "header": true
        });

        let actual = ByteRecord::from(vec!["timestamp", "price", "size"]);

        let headers = resolve_trade_headers(&schema, Some(&actual)).unwrap();

        assert_eq!(headers, actual);
    }

    #[test]
    fn rejects_missing_headerless_schema() {
        let schema = serde_json::json!({
            "header": false
        });

        assert!(resolve_trade_headers(&schema, None).is_err());
    }

    #[test]
    fn rejects_missing_actual_headers() {
        let schema = serde_json::json!({
            "header": true
        });

        assert!(resolve_trade_headers(&schema, None).is_err());
    }
}
