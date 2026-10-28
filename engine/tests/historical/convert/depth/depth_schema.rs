use arrow_schema::{DataType, Schema};

use {
    marketforge_engine::historical::convert::output::converted_depth_schema,
    marketforge_engine::historical::merge::physical_depth_schema,
};

fn schema() -> Schema {
    let source = physical_depth_schema();

    converted_depth_schema(source.as_ref())
}

#[test]
fn preserves_original_depth_columns() {
    let source = physical_depth_schema();
    let converted = schema();

    assert_eq!(source.fields().len(), 17);
    assert_eq!(converted.fields().len(), 24);

    for (index, field) in source.fields().iter().enumerate() {
        assert_eq!(converted.field(index), field.as_ref(),);
    }
}

#[test]
fn converted_levels_use_decimal256() {
    let schema = schema();

    let field = schema.field_with_name("changes_target").unwrap();

    assert!(!field.is_nullable());

    let DataType::List(item) = field.data_type() else {
        panic!("changes_target must be a List");
    };

    assert!(!item.is_nullable());

    let DataType::Struct(fields) = item.data_type() else {
        panic!("changes_target items must be Struct");
    };

    assert_eq!(fields.len(), 2);

    for name in ["price_target", "quantity_quote_target"] {
        let field = fields.iter().find(|field| field.name() == name).unwrap();

        assert_eq!(field.data_type(), &DataType::Decimal256(76, 28),);

        assert!(field.is_nullable());
    }
}

#[test]
fn conversion_metadata_matches_trade_schema() {
    let converted = schema();

    let names = [
        "target_quote",
        "conversion_status",
        "conversion_rate",
        "conversion_path",
        "conversion_references",
        "conversion_failure",
    ];

    for name in names {
        assert!(
            converted.field_with_name(name).is_ok(),
            "missing conversion metadata field: {name}",
        );
    }
}

#[test]
fn preserves_source_schema_metadata() {
    let source = physical_depth_schema();
    let converted = converted_depth_schema(source.as_ref());

    assert_eq!(source.metadata(), converted.metadata(),);
}

#[test]
fn contains_no_duplicate_field_names() {
    let converted = schema();

    let mut names = std::collections::HashSet::new();

    for field in converted.fields() {
        assert!(
            names.insert(field.name()),
            "duplicate field: {}",
            field.name(),
        );
    }
}
