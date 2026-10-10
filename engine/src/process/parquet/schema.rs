use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

pub const DECIMAL_PRECISION: u8 = 76;
pub const DECIMAL_SCALE: i8 = 28;

pub fn trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("system_timestamp_ns", DataType::Int64, true),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("stream_id", DataType::Utf8, false),
        Field::new("trade_id", DataType::Utf8, true),
        Field::new("sequence", DataType::UInt64, true),
        Field::new("side", DataType::Utf8, false),
        decimal_field("price", false),
        decimal_field("quantity_base", true),
        decimal_field("quantity_quote", true),
        decimal_field("quantity_contracts", true),
        Field::new("is_rpi", DataType::Boolean, true),
        decimal_field("trade_iv", true),
        decimal_field("mark_iv", true),
        decimal_field("index_price", true),
        decimal_field("mark_price", true),
    ]))
}

pub fn depth_event_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("canonical_row_offset", DataType::UInt64, false),
        Field::new("canonical_row_count", DataType::UInt64, false),
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("system_timestamp_ns", DataType::Int64, true),
        Field::new("sequence_start", DataType::UInt64, true),
        Field::new("sequence_end", DataType::UInt64, true),
        Field::new("event_boundary", DataType::UInt8, false),
        Field::new("source_operation", DataType::UInt8, false),
    ]))
}

fn decimal_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Decimal256(DECIMAL_PRECISION, DECIMAL_SCALE),
        nullable,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_schema_contains_all_canonical_fields() {
        let schema = trade_schema();

        assert_eq!(schema.fields().len(), 18);

        let expected = [
            "event_timestamp_ns",
            "system_timestamp_ns",
            "exchange",
            "instrument_id",
            "symbol",
            "stream_id",
            "trade_id",
            "sequence",
            "side",
            "price",
            "quantity_base",
            "quantity_quote",
            "quantity_contracts",
            "is_rpi",
            "trade_iv",
            "mark_iv",
            "index_price",
            "mark_price",
        ];

        for (field, name) in schema.fields().iter().zip(expected) {
            assert_eq!(field.name(), name);
        }
    }

    #[test]
    fn decimal_fields_use_decimal256() {
        let schema = trade_schema();

        for name in [
            "price",
            "quantity_base",
            "quantity_quote",
            "quantity_contracts",
            "trade_iv",
            "mark_iv",
            "index_price",
            "mark_price",
        ] {
            let field = schema.field_with_name(name).unwrap();

            assert_eq!(field.data_type(), &DataType::Decimal256(76, 28));
        }
    }

    #[test]
    fn required_fields_are_not_nullable() {
        let schema = trade_schema();

        for name in [
            "event_timestamp_ns",
            "exchange",
            "instrument_id",
            "symbol",
            "stream_id",
            "side",
            "price",
        ] {
            assert!(!schema.field_with_name(name).unwrap().is_nullable());
        }
    }

    #[test]
    fn optional_fields_are_nullable() {
        let schema = trade_schema();

        for name in [
            "system_timestamp_ns",
            "trade_id",
            "sequence",
            "quantity_base",
            "quantity_quote",
            "quantity_contracts",
            "is_rpi",
            "trade_iv",
            "mark_iv",
            "index_price",
            "mark_price",
        ] {
            assert!(schema.field_with_name(name).unwrap().is_nullable());
        }
    }
}

pub fn depth_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("system_timestamp_ns", DataType::Int64, true),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("stream_id", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        decimal_field("price", false),
        decimal_field("quantity_base", true),
        decimal_field("quantity_quote", true),
        decimal_field("quantity_contracts", true),
        Field::new("order_count", DataType::UInt64, true),
    ]))
}
