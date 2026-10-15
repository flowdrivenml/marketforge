use std::{fs::File, path::PathBuf};

use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch};
use arrow_buffer::i256;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    job::MergeParquetConfig,
    process::convert::{
        ConversionDecimal, ConversionDirection,
        output::{
            CONVERSION_FAILURE_STALE, CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
            CONVERSION_STATUS_UNAVAILABLE, ConvertedParquetWriter, ConvertedTradeBatchBuilder,
            TradeConversionRow,
        },
        reference::{EvaluatedReference, ReferenceKey},
    },
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

fn config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 1,
        compression_level: 3,
    }
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

fn conversion_rows() -> Vec<TradeConversionRow> {
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

fn converted_batch() -> RecordBatch {
    let source = source_batch();

    let builder = ConvertedTradeBatchBuilder::new(source.schema()).unwrap();

    builder.build(&source, &conversion_rows()).unwrap()
}

fn read_output(path: &std::path::Path) -> RecordBatch {
    let file = File::open(path).unwrap();

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    reader.next().unwrap().unwrap()
}

// -----------------------------------------------------------------------------
// Physical Parquet round-trip
// -----------------------------------------------------------------------------

#[test]
fn writes_and_reads_converted_trade_batch() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let path = directory.path().join("part-000000.parquet");

    assert!(path.is_file());

    let output = read_output(&path);

    assert_eq!(output.num_rows(), 3);
    assert_eq!(output.schema(), batch.schema());

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

// -----------------------------------------------------------------------------
// Nested provenance
// -----------------------------------------------------------------------------

#[test]
fn preserves_nested_reference_provenance() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let output = read_output(&directory.path().join("part-000000.parquet"));

    let references = output
        .column_by_name("conversion_references")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(references.value_length(0), 0);
    assert_eq!(references.value_length(1), 1);
    assert_eq!(references.value_length(2), 0);

    assert!(!references.is_null(0));
    assert!(!references.is_null(1));
    assert!(!references.is_null(2));
}

// -----------------------------------------------------------------------------
// Writer metrics
// -----------------------------------------------------------------------------

#[test]
fn tracks_written_events() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let metrics = writer.metrics();

    assert_eq!(metrics.events_written, 3);
    assert_eq!(metrics.trade_events, 3);
    assert_eq!(metrics.depth_events, 0);
    assert_eq!(metrics.files_written, 1);
}

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

#[test]
fn finalization_is_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    writer.write_batch(&batch).unwrap();

    writer.finish().unwrap();
    writer.finish().unwrap();

    assert!(writer.is_finished());
    assert!(!writer.is_failed());
}

#[test]
fn rejects_writes_after_finalization() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    assert!(writer.write_batch(&batch).is_err());
}

// -----------------------------------------------------------------------------
// Schema validation
// -----------------------------------------------------------------------------

#[test]
fn rejects_incompatible_arrow_schema() {
    let directory = tempfile::tempdir().unwrap();

    let batch = converted_batch();

    let mut writer =
        ConvertedParquetWriter::new(directory.path(), batch.schema(), &config()).unwrap();

    let source = source_batch();

    assert!(writer.write_batch(&source).is_err());
    assert!(writer.is_failed());
}

// -----------------------------------------------------------------------------
// Input validation
// -----------------------------------------------------------------------------

#[test]
fn rejects_invalid_row_group_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let batch = converted_batch();

    let invalid = MergeParquetConfig {
        row_group_target_mb: 0,
        file_target_mb: 1,
        compression_level: 3,
    };

    assert!(ConvertedParquetWriter::new(directory.path(), batch.schema(), &invalid,).is_err());
}
