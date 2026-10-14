use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema};

pub const CONVERSION_SCHEMA_VERSION: u32 = 1;

pub const CONVERSION_DECIMAL_PRECISION: u8 = 76;
pub const CONVERSION_DECIMAL_SCALE: i8 = 28;

// -----------------------------------------------------------------------------
// Conversion status
// -----------------------------------------------------------------------------

pub const CONVERSION_STATUS_IDENTITY: u8 = 0;
pub const CONVERSION_STATUS_CONVERTED: u8 = 1;
pub const CONVERSION_STATUS_UNAVAILABLE: u8 = 2;

// -----------------------------------------------------------------------------
// Conversion failure codes
// -----------------------------------------------------------------------------

pub const CONVERSION_FAILURE_MISSING: u8 = 1;
pub const CONVERSION_FAILURE_STALE: u8 = 2;
pub const CONVERSION_FAILURE_FUTURE: u8 = 3;
pub const CONVERSION_FAILURE_IDENTITY_MISMATCH: u8 = 4;
pub const CONVERSION_FAILURE_ARITHMETIC: u8 = 5;
pub const CONVERSION_FAILURE_NO_PATH: u8 = 6;

// -----------------------------------------------------------------------------
// Decimal256
// -----------------------------------------------------------------------------

fn decimal_type() -> DataType {
    DataType::Decimal256(CONVERSION_DECIMAL_PRECISION, CONVERSION_DECIMAL_SCALE)
}

// -----------------------------------------------------------------------------
// Reference provenance
// -----------------------------------------------------------------------------

fn reference_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("dataset_id", DataType::UInt64, false),
        Field::new("stream_id", DataType::Utf8, false),
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("stream_rank", DataType::UInt32, false),
        Field::new("task_id", DataType::UInt64, false),
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("direction", DataType::UInt8, false),
        Field::new("reference_price", decimal_type(), false),
    ]))
}

fn conversion_reference_list() -> DataType {
    DataType::List(Arc::new(Field::new("item", reference_struct(), false)))
}

fn conversion_path_list() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
}

// -----------------------------------------------------------------------------
// Conversion metadata
// -----------------------------------------------------------------------------

pub fn conversion_fields() -> Vec<Field> {
    vec![
        Field::new("target_quote", DataType::Utf8, false),
        Field::new("conversion_status", DataType::UInt8, false),
        Field::new("conversion_rate", decimal_type(), true),
        Field::new("conversion_path", conversion_path_list(), true),
        Field::new("conversion_references", conversion_reference_list(), false),
        Field::new("conversion_failure", DataType::UInt8, true),
    ]
}

// -----------------------------------------------------------------------------
// Trade conversion schema
// -----------------------------------------------------------------------------

pub fn converted_trade_schema(source_schema: &Schema) -> Schema {
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    fields.extend([
        Field::new("price_target", decimal_type(), true),
        Field::new("quantity_quote_target", decimal_type(), true),
    ]);

    fields.extend(conversion_fields());

    Schema::new_with_metadata(fields, source_schema.metadata().clone())
}

// -----------------------------------------------------------------------------
// Depth conversion schema
// -----------------------------------------------------------------------------

pub fn converted_depth_schema(source_schema: &Schema) -> Schema {
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    let converted_level = DataType::Struct(Fields::from(vec![
        Field::new("price_target", decimal_type(), true),
        Field::new("quantity_quote_target", decimal_type(), true),
    ]));

    fields.push(Field::new(
        "changes_target",
        DataType::List(Arc::new(Field::new("item", converted_level, false))),
        false,
    ));

    fields.extend(conversion_fields());

    Schema::new_with_metadata(fields, source_schema.metadata().clone())
}

pub fn converted_combined_schema(source_schema: &Schema) -> Schema {
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    let trade_target = DataType::Struct(Fields::from(vec![
        Field::new("price_target", decimal_type(), true),
        Field::new("quantity_quote_target", decimal_type(), true),
    ]));

    let depth_level_target = DataType::Struct(Fields::from(vec![
        Field::new("price_target", decimal_type(), true),
        Field::new("quantity_quote_target", decimal_type(), true),
    ]));

    fields.push(Field::new("trade_target", trade_target, true));

    fields.push(Field::new(
        "depth_target",
        DataType::List(Arc::new(Field::new("item", depth_level_target, false))),
        true,
    ));

    fields.extend(conversion_fields());

    Schema::new_with_metadata(fields, source_schema.metadata().clone())
}
