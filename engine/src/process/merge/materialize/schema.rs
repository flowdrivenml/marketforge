use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::process::parquet::{depth_schema, trade_schema};

fn provenance_fields() -> Vec<Field> {
    vec![
        Field::new("stream_rank", DataType::UInt32, false),
        Field::new("dataset_id", DataType::UInt64, false),
        Field::new("task_id", DataType::UInt64, false),
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("canonical_row_offset", DataType::UInt64, false),
        Field::new("canonical_row_count", DataType::UInt64, false),
    ]
}

fn event_identity_fields() -> Vec<Field> {
    let canonical = trade_schema();

    canonical
        .fields()
        .iter()
        .take(6)
        .map(|field| field.as_ref().clone())
        .collect()
}

fn trade_payload_fields() -> Vec<Field> {
    trade_schema()
        .fields()
        .iter()
        .skip(6)
        .map(|field| field.as_ref().clone())
        .collect()
}

fn depth_level_fields() -> Vec<Field> {
    depth_schema()
        .fields()
        .iter()
        .skip(6)
        .map(|field| field.as_ref().clone())
        .collect()
}

fn depth_changes_field() -> Field {
    let level = DataType::Struct(Fields::from(depth_level_fields()));

    Field::new(
        "changes",
        DataType::List(Arc::new(Field::new("item", level, false))),
        false,
    )
}

fn depth_metadata_fields() -> Vec<Field> {
    vec![
        Field::new("event_boundary", DataType::UInt8, false),
        Field::new("source_operation", DataType::UInt8, false),
        Field::new("sequence_start", DataType::UInt64, true),
        Field::new("sequence_end", DataType::UInt64, true),
        depth_changes_field(),
    ]
}

/// One physical row per trade.
pub fn physical_trade_schema() -> SchemaRef {
    let mut fields = trade_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();

    fields.extend(provenance_fields());

    Arc::new(Schema::new(fields))
}

/// One physical row per depth event.
///
/// All canonical level changes are stored in a nested list.
pub fn physical_depth_schema() -> SchemaRef {
    let mut fields = event_identity_fields();

    fields.extend(provenance_fields());
    fields.extend(depth_metadata_fields());

    Arc::new(Schema::new(fields))
}

/// One physical row per chronological market event.
///
/// Exactly one of trade/depth must be populated.
pub fn physical_combined_schema() -> SchemaRef {
    let mut fields = event_identity_fields();

    fields.push(Field::new("event_kind", DataType::UInt8, false));

    fields.extend(provenance_fields());

    fields.push(Field::new(
        "trade",
        DataType::Struct(Fields::from(trade_payload_fields())),
        true,
    ));

    fields.push(Field::new(
        "depth",
        DataType::Struct(Fields::from(depth_metadata_fields())),
        true,
    ));

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_preserves_canonical_fields() {
        let canonical = trade_schema();
        let physical = physical_trade_schema();

        for field in canonical.fields() {
            assert_eq!(
                physical.field_with_name(field.name()).unwrap(),
                field.as_ref(),
            );
        }

        assert_eq!(physical.fields().len(), 24);
    }

    #[test]
    fn depth_contains_nested_canonical_levels() {
        let schema = physical_depth_schema();

        let changes = schema.field_with_name("changes").unwrap();

        let DataType::List(item) = changes.data_type() else {
            panic!("changes must be List");
        };

        let DataType::Struct(levels) = item.data_type() else {
            panic!("list elements must be Struct");
        };

        let canonical = depth_schema();

        for field in canonical.fields().iter().skip(6) {
            let actual = levels
                .iter()
                .find(|level| level.name() == field.name())
                .expect("missing canonical depth field");

            assert_eq!(actual.as_ref(), field.as_ref());
        }

        assert_eq!(levels.len(), 6);
    }

    #[test]
    fn combined_contains_nullable_payloads() {
        let schema = physical_combined_schema();

        for name in ["trade", "depth"] {
            let field = schema.field_with_name(name).unwrap();

            assert!(field.is_nullable());
            assert!(matches!(field.data_type(), DataType::Struct(_)));
        }

        assert_eq!(
            schema.field_with_name("event_kind").unwrap().data_type(),
            &DataType::UInt8,
        );
    }

    #[test]
    fn depth_changes_are_non_nullable_list() {
        let schema = physical_depth_schema();

        let field = schema.field_with_name("changes").unwrap();

        assert!(!field.is_nullable());
        assert!(matches!(field.data_type(), DataType::List(_)));
    }
}
