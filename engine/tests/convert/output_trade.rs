use std::{fs::File, path::PathBuf};

use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, UInt8Array};
use arrow_buffer::i256;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::process::convert::{
    ConversionDecimal, ConversionDirection,
    output::{
        CONVERSION_FAILURE_STALE, CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
        CONVERSION_STATUS_UNAVAILABLE, ConvertedTradeBatchBuilder, TradeConversionRow,
    },
    reference::{EvaluatedReference, ReferenceKey},
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn decimal(value: i64) -> ConversionDecimal {
    ConversionDecimal::new(i256::from_i128(i128::from(value) * 10_i128.pow(28))).unwrap()
}

fn source_batch() -> RecordBatch {
    let dataset = root().join("data/merged/conversion-multihop");

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

    let relative_path = manifest["files"][0]["path"].as_str().unwrap();

    let file = File::open(dataset.join(relative_path)).unwrap();

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(3)
        .build()
        .unwrap();

    reader.next().unwrap().unwrap()
}

fn reference() -> EvaluatedReference {
    EvaluatedReference {
        dataset_id: 106,
        stream_id: "binance:spot:spot:BTCUSDT:trade".to_owned(),
        key: ReferenceKey::new(100, 0, 0, 42),
        direction: ConversionDirection::Direct,
        price: decimal(60_000),
    }
}

fn rows() -> Vec<TradeConversionRow> {
    vec![
        TradeConversionRow {
            target_quote: "USDT".to_owned(),
            status: CONVERSION_STATUS_IDENTITY,
            failure: None,
            rate: Some(ConversionDecimal::one()),
            price_target: Some(decimal(60_000)),
            quantity_quote_target: Some(decimal(120_000)),
            path: Some(vec!["USDT".to_owned()]),
            references: vec![],
        },
        TradeConversionRow {
            target_quote: "USDT".to_owned(),
            status: CONVERSION_STATUS_CONVERTED,
            failure: None,
            rate: Some(decimal(60_000)),
            price_target: Some(decimal(3_000)),
            quantity_quote_target: Some(decimal(6_000)),
            path: Some(vec!["BTC".to_owned(), "USDT".to_owned()]),
            references: vec![reference()],
        },
        TradeConversionRow {
            target_quote: "USDT".to_owned(),
            status: CONVERSION_STATUS_UNAVAILABLE,
            failure: Some(CONVERSION_FAILURE_STALE),
            rate: None,
            price_target: None,
            quantity_quote_target: None,
            path: None,
            references: vec![],
        },
    ]
}

fn converted_batch() -> (RecordBatch, RecordBatch) {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    let output = builder.build(&source, &rows()).unwrap();

    (source, output)
}

// -----------------------------------------------------------------------------
// Schema and row preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_source_row_count() {
    let (source, output) = converted_batch();

    assert_eq!(source.num_rows(), 3);
    assert_eq!(output.num_rows(), source.num_rows());
}

#[test]
fn appends_eight_conversion_columns() {
    let (source, output) = converted_batch();

    assert_eq!(output.num_columns(), source.num_columns() + 8);
}

#[test]
fn preserves_original_arrow_arrays() {
    let (source, output) = converted_batch();

    for index in 0..source.num_columns() {
        assert!(
            std::sync::Arc::ptr_eq(source.column(index), output.column(index),),
            "source column {index} was copied"
        );
    }
}

// -----------------------------------------------------------------------------
// Conversion status
// -----------------------------------------------------------------------------

#[test]
fn writes_conversion_statuses() {
    let (_, output) = converted_batch();

    let statuses = output
        .column_by_name("conversion_status")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    assert_eq!(
        statuses.values(),
        &[
            CONVERSION_STATUS_IDENTITY,
            CONVERSION_STATUS_CONVERTED,
            CONVERSION_STATUS_UNAVAILABLE,
        ]
    );
}

// -----------------------------------------------------------------------------
// Decimal256 values
// -----------------------------------------------------------------------------

#[test]
fn writes_converted_decimal_values() {
    let (_, output) = converted_batch();

    let prices = output
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(prices.value(0), decimal(60_000).mantissa());
    assert_eq!(prices.value(1), decimal(3_000).mantissa());
    assert!(prices.is_null(2));
}

#[test]
fn writes_nullable_quote_quantities() {
    let (_, output) = converted_batch();

    let quantities = output
        .column_by_name("quantity_quote_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(quantities.value(0), decimal(120_000).mantissa());
    assert_eq!(quantities.value(1), decimal(6_000).mantissa());
    assert!(quantities.is_null(2));
}

// -----------------------------------------------------------------------------
// Conversion paths
// -----------------------------------------------------------------------------

#[test]
fn writes_conversion_paths() {
    let (_, output) = converted_batch();

    let paths = output
        .column_by_name("conversion_path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(paths.len(), 3);
    assert_eq!(paths.value_length(0), 1);
    assert_eq!(paths.value_length(1), 2);
    assert!(paths.is_null(2));
}

// -----------------------------------------------------------------------------
// Reference provenance
// -----------------------------------------------------------------------------

#[test]
fn writes_reference_provenance() {
    let (_, output) = converted_batch();

    let references = output
        .column_by_name("conversion_references")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(references.len(), 3);

    assert_eq!(references.value_length(0), 0);
    assert_eq!(references.value_length(1), 1);
    assert_eq!(references.value_length(2), 0);

    assert!(!references.is_null(0));
    assert!(!references.is_null(1));
    assert!(!references.is_null(2));
}

// -----------------------------------------------------------------------------
// Failure codes
// -----------------------------------------------------------------------------

#[test]
fn writes_failure_codes() {
    let (_, output) = converted_batch();

    let failures = output
        .column_by_name("conversion_failure")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    assert!(failures.is_null(0));
    assert!(failures.is_null(1));

    assert_eq!(failures.value(2), CONVERSION_FAILURE_STALE);
}

// -----------------------------------------------------------------------------
// Invalid input
// -----------------------------------------------------------------------------

#[test]
fn rejects_row_count_mismatch() {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    assert!(builder.build(&source, &rows()[..2]).is_err());
}

#[test]
fn rejects_invalid_identity_conversion() {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    let mut rows = rows();

    rows[0].rate = None;

    assert!(builder.build(&source, &rows).is_err());
}

#[test]
fn rejects_invalid_converted_path() {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    let mut rows = rows();

    rows[1].path = Some(vec!["USDT".to_owned()]);

    assert!(builder.build(&source, &rows).is_err());
}

#[test]
fn rejects_invalid_unavailable_conversion() {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    let mut rows = rows();

    rows[2].rate = Some(ConversionDecimal::one());

    assert!(builder.build(&source, &rows).is_err());
}
