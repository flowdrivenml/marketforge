use std::{collections::HashSet, fs::File, path::PathBuf};

use arrow_schema::{DataType, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::process::convert::output::{
    CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE, CONVERSION_FAILURE_IDENTITY_MISMATCH,
    CONVERSION_FAILURE_MISSING, CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE,
    CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
    converted_trade_schema,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn schemas() -> (Schema, Schema) {
    let dataset = root().join("data/merged/conversion-multihop");

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

    let relative_path = manifest["files"][0]["path"].as_str().unwrap();

    let file = File::open(dataset.join(relative_path)).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let source = builder.schema().as_ref().clone();
    let converted = converted_trade_schema(&source);

    (source, converted)
}

// -----------------------------------------------------------------------------
// Original schema preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_all_original_fields() {
    let (source, converted) = schemas();

    assert_eq!(source.fields().len(), 24);
    assert_eq!(converted.fields().len(), 32);

    for (original, output) in source.fields().iter().zip(converted.fields()) {
        assert_eq!(original, output);
    }
}

#[test]
fn preserves_original_field_types_and_nullability() {
    let (source, converted) = schemas();

    for field in source.fields() {
        let output = converted.field_with_name(field.name()).unwrap();

        assert_eq!(field.data_type(), output.data_type());
        assert_eq!(field.is_nullable(), output.is_nullable());
        assert_eq!(field.metadata(), output.metadata());
    }
}

#[test]
fn preserves_original_field_order() {
    let (source, converted) = schemas();

    let original_names: Vec<_> = source.fields().iter().map(|field| field.name()).collect();

    let output_names: Vec<_> = converted
        .fields()
        .iter()
        .take(source.fields().len())
        .map(|field| field.name())
        .collect();

    assert_eq!(original_names, output_names);
}

// -----------------------------------------------------------------------------
// Converted decimal fields
// -----------------------------------------------------------------------------

#[test]
fn converted_values_use_decimal256() {
    let (_, converted) = schemas();

    for name in ["price_target", "quantity_quote_target", "conversion_rate"] {
        let field = converted.field_with_name(name).unwrap();

        assert_eq!(field.data_type(), &DataType::Decimal256(76, 28));
        assert!(field.is_nullable());
    }
}

// -----------------------------------------------------------------------------
// Conversion metadata
// -----------------------------------------------------------------------------

#[test]
fn validates_conversion_metadata_fields() {
    let (_, converted) = schemas();

    let expected = [
        ("target_quote", DataType::Utf8, false),
        ("conversion_status", DataType::UInt8, false),
        ("conversion_failure", DataType::UInt8, true),
    ];

    for (name, data_type, nullable) in expected {
        let field = converted.field_with_name(name).unwrap();

        assert_eq!(field.data_type(), &data_type);
        assert_eq!(field.is_nullable(), nullable);
    }
}

// -----------------------------------------------------------------------------
// Nested provenance
// -----------------------------------------------------------------------------

#[test]
fn validates_reference_provenance_schema() {
    let (_, converted) = schemas();

    let field = converted.field_with_name("conversion_references").unwrap();

    assert!(!field.is_nullable());

    let DataType::List(item) = field.data_type() else {
        panic!("conversion_references must be a list");
    };

    let DataType::Struct(fields) = item.data_type() else {
        panic!("conversion reference must be a struct");
    };

    let expected = [
        "dataset_id",
        "stream_id",
        "event_timestamp_ns",
        "stream_rank",
        "task_id",
        "event_ordinal",
        "direction",
        "reference_price",
    ];

    assert_eq!(fields.len(), expected.len());

    for (field, name) in fields.iter().zip(expected) {
        assert_eq!(field.name(), name);
        assert!(!field.is_nullable());
    }

    assert_eq!(
        fields.last().unwrap().data_type(),
        &DataType::Decimal256(76, 28)
    );
}

#[test]
fn validates_conversion_path_schema() {
    let (_, converted) = schemas();

    let field = converted.field_with_name("conversion_path").unwrap();

    assert!(field.is_nullable());

    let DataType::List(item) = field.data_type() else {
        panic!("conversion_path must be a list");
    };

    assert_eq!(item.data_type(), &DataType::Utf8);
    assert!(!item.is_nullable());
}

// -----------------------------------------------------------------------------
// Schema integrity
// -----------------------------------------------------------------------------

#[test]
fn contains_no_duplicate_field_names() {
    let (_, converted) = schemas();

    let mut names = HashSet::new();

    for field in converted.fields() {
        assert!(
            names.insert(field.name()),
            "duplicate output field: {}",
            field.name()
        );
    }
}

#[test]
fn conversion_fields_are_appended_in_fixed_order() {
    let (source, converted) = schemas();

    let appended: Vec<_> = converted
        .fields()
        .iter()
        .skip(source.fields().len())
        .map(|field| field.name().as_str())
        .collect();

    assert_eq!(
        appended,
        [
            "price_target",
            "quantity_quote_target",
            "target_quote",
            "conversion_status",
            "conversion_rate",
            "conversion_path",
            "conversion_references",
            "conversion_failure",
        ]
    );
}

// -----------------------------------------------------------------------------
// Status and failure codes
// -----------------------------------------------------------------------------

#[test]
fn conversion_status_codes_are_distinct() {
    let statuses = [
        CONVERSION_STATUS_IDENTITY,
        CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_UNAVAILABLE,
    ];

    assert_eq!(statuses.into_iter().collect::<HashSet<_>>().len(), 3);
}

#[test]
fn conversion_failure_codes_are_distinct() {
    let failures = [
        CONVERSION_FAILURE_MISSING,
        CONVERSION_FAILURE_STALE,
        CONVERSION_FAILURE_FUTURE,
        CONVERSION_FAILURE_IDENTITY_MISMATCH,
        CONVERSION_FAILURE_ARITHMETIC,
        CONVERSION_FAILURE_NO_PATH,
    ];

    assert_eq!(failures.into_iter().collect::<HashSet<_>>().len(), 6);
}
