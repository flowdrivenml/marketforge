#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Array, Decimal256Array, Int64Array, RecordBatch, StringArray};

use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    formats::depth::DepthProcessingOutcome,
    job::ParquetResourceConfig,
    process::{
        parquet::{ParquetDepthWriter, i256_to_decimal},
        worker::DepthSink,
    },
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path =
            std::env::temp_dir().join(format!("marketforge-{name}-{}-{id}", std::process::id()));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// -----------------------------------------------------------------------------
// Writer configuration
// -----------------------------------------------------------------------------

fn resources() -> ParquetResourceConfig {
    ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    }
}

// -----------------------------------------------------------------------------
// Canonical level helpers
// -----------------------------------------------------------------------------

fn level(timestamp_ns: i64, side: BookSide, price: Decimal, quantity: Decimal) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: timestamp_ns,
            system_timestamp_ns: Some(timestamp_ns + 100),
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:depth".to_owned(),
        },

        side,
        price,

        quantity_base: Some(quantity),
        quantity_quote: Some(price * quantity),
        quantity_contracts: None,

        order_count: None,
    }
}

fn initialization(timestamp_ns: i64, count: usize) -> DepthProcessingOutcome {
    let levels = (0..count)
        .map(|index| {
            level(
                timestamp_ns,
                BookSide::Bid,
                Decimal::from(100_000 - index as i64),
                Decimal::ONE,
            )
        })
        .collect();

    DepthProcessingOutcome::initialization(levels)
}

fn changes(timestamp_ns: i64, count: usize) -> DepthProcessingOutcome {
    let levels = (0..count)
        .map(|index| {
            level(
                timestamp_ns,
                BookSide::Ask,
                Decimal::from(100_001 + index as i64),
                Decimal::new(5, 1),
            )
        })
        .collect();

    DepthProcessingOutcome::changes(levels)
}

// -----------------------------------------------------------------------------
// Parquet inspection
// -----------------------------------------------------------------------------

fn parquet_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();

    files.sort();
    files
}

fn read_batches(directory: &Path) -> Vec<RecordBatch> {
    let mut batches = Vec::new();

    for path in parquet_files(directory) {
        let file = fs::File::open(path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            batches.push(batch.unwrap());
        }
    }

    batches
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

// -----------------------------------------------------------------------------
// Decimal256 round-trip
// -----------------------------------------------------------------------------

#[test]
fn preserves_exact_decimal_values() {
    let directory = TestDirectory::new("depth-decimal");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    let price = Decimal::new(784313, 1);
    let quantity = Decimal::new(162, 5);

    writer
        .write_outcome(DepthProcessingOutcome::initialization(vec![level(
            1000,
            BookSide::Bid,
            price,
            quantity,
        )]))
        .unwrap();

    writer.finish().unwrap();

    let batches = read_batches(&directory.path);

    assert_eq!(total_rows(&batches), 1);

    let batch = &batches[0];

    let prices = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let quantities = batch
        .column_by_name("quantity_base")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(i256_to_decimal(prices.value(0)).unwrap(), price);

    assert_eq!(i256_to_decimal(quantities.value(0)).unwrap(), quantity);
}

// -----------------------------------------------------------------------------
// File rotation
// -----------------------------------------------------------------------------

#[test]
fn rotates_parquet_files() {
    let directory = TestDirectory::new("depth-rotation");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer.write_outcome(initialization(1000, 400)).unwrap();

    for index in 0..100 {
        writer.write_outcome(changes(1001 + index, 20)).unwrap();
    }

    writer.finish().unwrap();

    let files = parquet_files(&directory.path);

    assert!(files.len() > 1);

    for (index, path) in files.iter().enumerate() {
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            format!("part-{index:06}.parquet")
        );
    }

    assert_eq!(writer.metrics().files_written as usize, files.len());

    assert_eq!(total_rows(&read_batches(&directory.path)), 2400);
}

// -----------------------------------------------------------------------------
// Event ordering
// -----------------------------------------------------------------------------

#[test]
fn preserves_canonical_event_order() {
    let directory = TestDirectory::new("depth-order");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(DepthProcessingOutcome::initialization(vec![
            level(1000, BookSide::Bid, Decimal::from(100), Decimal::ONE),
            level(1000, BookSide::Ask, Decimal::from(101), Decimal::ONE),
        ]))
        .unwrap();

    writer
        .write_outcome(DepthProcessingOutcome::changes(vec![
            level(1001, BookSide::Bid, Decimal::from(99), Decimal::ONE),
            level(1001, BookSide::Ask, Decimal::from(102), Decimal::ONE),
        ]))
        .unwrap();

    writer.finish().unwrap();

    let batches = read_batches(&directory.path);

    let mut timestamps = Vec::new();
    let mut sides = Vec::new();

    for batch in &batches {
        let timestamp_column = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let side_column = batch
            .column_by_name("side")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for index in 0..batch.num_rows() {
            timestamps.push(timestamp_column.value(index));
            sides.push(side_column.value(index).to_owned());
        }
    }

    assert_eq!(timestamps, vec![1000, 1000, 1001, 1001]);

    assert_eq!(sides, vec!["bid", "ask", "bid", "ask"]);
}

// -----------------------------------------------------------------------------
// Segment tracking
// -----------------------------------------------------------------------------

#[test]
fn preserves_segment_offsets_across_files() {
    let directory = TestDirectory::new("depth-segments");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer.write_outcome(initialization(1000, 400)).unwrap();
    writer.write_outcome(changes(1001, 100)).unwrap();

    writer.write_outcome(initialization(2000, 400)).unwrap();
    writer.write_outcome(changes(2001, 50)).unwrap();

    writer.finish().unwrap();

    assert_eq!(writer.segments().len(), 2);

    let first = &writer.segments()[0];
    let second = &writer.segments()[1];

    assert_eq!(first.initial_event_offset, 0);
    assert_eq!(first.initial_level_count, 400);

    assert_eq!(second.initial_event_offset, 500);
    assert_eq!(second.initial_level_count, 400);

    assert_eq!(writer.next_event_offset(), 950);

    assert_eq!(total_rows(&read_batches(&directory.path)), 950);
}

// -----------------------------------------------------------------------------
// Invalid outcome
// -----------------------------------------------------------------------------

#[test]
fn rejects_empty_initialization() {
    let directory = TestDirectory::new("depth-invalid");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    assert!(
        writer
            .write_outcome(DepthProcessingOutcome::initialization(vec![]))
            .is_err()
    );

    assert!(writer.is_failed());

    assert!(writer.write_outcome(initialization(1000, 1)).is_err());

    assert!(writer.finish().is_err());
}

// -----------------------------------------------------------------------------
// Finalization
// -----------------------------------------------------------------------------

#[test]
fn rejects_writes_after_finish() {
    let directory = TestDirectory::new("depth-finish");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer.write_outcome(initialization(1000, 10)).unwrap();

    writer.finish().unwrap();

    assert!(writer.is_finished());
    assert!(!writer.is_failed());

    assert!(writer.write_outcome(changes(1001, 1)).is_err());
}

// -----------------------------------------------------------------------------
// Metrics consistency
// -----------------------------------------------------------------------------

#[test]
fn reports_correct_metrics() {
    let directory = TestDirectory::new("depth-metrics");

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer.write_outcome(initialization(1000, 10)).unwrap();
    writer.write_outcome(changes(1001, 5)).unwrap();

    writer.finish().unwrap();

    let metrics = writer.metrics();

    assert_eq!(metrics.levels_written, 15);
    assert_eq!(metrics.outcomes_written, 2);

    assert_eq!(metrics.start_timestamp_ns, Some(1000));
    assert_eq!(metrics.end_timestamp_ns, Some(1001));

    assert_eq!(writer.next_event_offset(), 15);
}
