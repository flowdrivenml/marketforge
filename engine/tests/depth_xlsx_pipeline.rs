#![cfg(feature = "process")]

use std::{
    fs::{self, File},
    io::{Cursor, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Array, Decimal256Array, StringArray, UInt64Array};

use marketforge_engine::{
    book::SequencePolicy,
    job::{SourceContainer, load_processing_job},
    process::{
        boundary::{BoundaryContinuity, DepthBoundaryManifest},
        config::load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use zip::{ZipWriter, write::SimpleFileOptions};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-xlsx-pipeline-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

// -----------------------------------------------------------------------------
// Synthetic XLSX workbook
// -----------------------------------------------------------------------------

fn create_xlsx() -> Vec<u8> {
    let worksheet = r#"
    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheetData>
        <row r="1">
          <c r="A1" t="inlineStr"><is><t>timestamp</t></is></c>
          <c r="B1" t="inlineStr"><is><t>asks</t></is></c>
          <c r="C1" t="inlineStr"><is><t>bids</t></is></c>
        </row>

        <row r="2">
          <c r="A2"><v>200</v></c>
          <c r="B2" t="inlineStr"><is><t>[[101,3],[102,4]]</t></is></c>
          <c r="C2" t="inlineStr"><is><t>[[100,5],[99,6]]</t></is></c>
        </row>

        <row r="3">
          <c r="A3"><v>100</v></c>
          <c r="B3" t="inlineStr"><is><t>[[101,2],[102,4]]</t></is></c>
          <c r="C3" t="inlineStr"><is><t>[[100,5],[99,6]]</t></is></c>
        </row>
      </sheetData>
    </worksheet>
    "#;

    let buffer = Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(buffer);

    writer
        .start_file("xl/worksheets/sheet1.xml", SimpleFileOptions::default())
        .expect("start worksheet");

    writer
        .write_all(worksheet.as_bytes())
        .expect("write worksheet");

    writer.finish().expect("finish XLSX").into_inner()
}

// -----------------------------------------------------------------------------
// Outer Bitget ZIP archive
// -----------------------------------------------------------------------------

fn create_bitget_archive(path: &Path) {
    let file = File::create(path).expect("create archive");

    let mut writer = ZipWriter::new(file);

    writer
        .start_file("20260901_6.xlsx", SimpleFileOptions::default())
        .expect("start XLSX member");

    writer.write_all(&create_xlsx()).expect("write XLSX member");

    writer.finish().expect("finish archive");
}

// -----------------------------------------------------------------------------
// Event index
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EventIndexEntry {
    ordinal: u64,
    offset: u64,
    count: u64,
}

fn read_event_index(directory: &Path) -> Vec<EventIndexEntry> {
    let file = File::open(directory.join("events.parquet")).expect("open event index");

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("create event-index reader")
        .build()
        .expect("build event-index reader");

    let mut entries = Vec::new();

    for batch in reader {
        let batch = batch.expect("read event-index batch");

        let ordinals = batch
            .column_by_name("event_ordinal")
            .expect("missing event_ordinal")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("event_ordinal must be UInt64");

        let offsets = batch
            .column_by_name("canonical_row_offset")
            .expect("missing canonical_row_offset")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("canonical_row_offset must be UInt64");

        let counts = batch
            .column_by_name("canonical_row_count")
            .expect("missing canonical_row_count")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("canonical_row_count must be UInt64");

        for index in 0..batch.num_rows() {
            entries.push(EventIndexEntry {
                ordinal: ordinals.value(index),
                offset: offsets.value(index),
                count: counts.value(index),
            });
        }
    }

    entries
}

#[derive(Debug, Clone)]
struct CanonicalLevel {
    side: String,
    price: Decimal,
    quantity_base: Option<Decimal>,
}

fn decimal_at(array: &Decimal256Array, index: usize) -> Option<Decimal> {
    if array.is_null(index) {
        return None;
    }

    let mantissa = array.value(index).to_string();
    let scale = array.scale();

    let value = if scale == 0 {
        mantissa
    } else if scale > 0 {
        let scale = usize::try_from(scale).unwrap();
        let negative = mantissa.starts_with('-');
        let digits = mantissa.trim_start_matches('-');

        let padded = if digits.len() <= scale {
            format!("{:0>width$}", digits, width = scale + 1)
        } else {
            digits.to_owned()
        };

        let split = padded.len() - scale;

        format!(
            "{}{}.{}",
            if negative { "-" } else { "" },
            &padded[..split],
            &padded[split..],
        )
    } else {
        format!(
            "{}{}",
            mantissa,
            "0".repeat(usize::try_from(-scale).unwrap())
        )
    };

    Some(value.parse().expect("valid canonical decimal"))
}

fn read_canonical_levels(directory: &Path) -> Vec<CanonicalLevel> {
    let mut paths = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "parquet")
                && path
                    .file_name()
                    .is_some_and(|name| name != "events.parquet")
        })
        .collect::<Vec<_>>();

    paths.sort();

    let mut levels = Vec::new();

    for path in paths {
        let file = File::open(&path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let sides = batch
                .column_by_name("side")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

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

            for index in 0..batch.num_rows() {
                levels.push(CanonicalLevel {
                    side: sides.value(index).to_owned(),
                    price: decimal_at(prices, index).expect("missing canonical price"),
                    quantity_base: decimal_at(quantities, index),
                });
            }
        }
    }

    levels
}
fn verify_intermediate_snapshot_replay(directory: &Path, events: &[EventIndexEntry]) {
    let levels = read_canonical_levels(directory);

    assert_eq!(levels.len(), 5);
    assert_eq!(events.len(), 2);

    let mut bids = BTreeMap::<Decimal, Decimal>::new();
    let mut asks = BTreeMap::<Decimal, Decimal>::new();

    for (event_index, event) in events.iter().enumerate() {
        let start = usize::try_from(event.offset).unwrap();

        let end = usize::try_from(event.offset.checked_add(event.count).unwrap()).unwrap();

        assert!(end <= levels.len());

        // ---------------------------------------------------------
        // Apply persisted canonical changes
        // ---------------------------------------------------------

        for level in &levels[start..end] {
            let book = match level.side.as_str() {
                "bid" => &mut bids,
                "ask" => &mut asks,
                other => panic!("unexpected book side: {other}"),
            };

            match level.quantity_base {
                Some(quantity) if quantity > Decimal::ZERO => {
                    book.insert(level.price, quantity);
                }

                Some(quantity) if quantity == Decimal::ZERO => {
                    book.remove(&level.price);
                }

                None => {
                    // Canonical deletions may use nullable quantities.
                    book.remove(&level.price);
                }

                Some(quantity) => {
                    panic!("negative canonical quantity: {quantity}");
                }
            }
        }

        // ---------------------------------------------------------
        // Independently defined expected snapshot
        // ---------------------------------------------------------

        let expected_bids = BTreeMap::from([
            (decimal("100"), decimal("5")),
            (decimal("99"), decimal("6")),
        ]);

        let expected_asks = match event_index {
            0 => BTreeMap::from([
                (decimal("101"), decimal("2")),
                (decimal("102"), decimal("4")),
            ]),

            1 => BTreeMap::from([
                (decimal("101"), decimal("3")),
                (decimal("102"), decimal("4")),
            ]),

            _ => unreachable!(),
        };

        assert_eq!(
            bids, expected_bids,
            "bid book mismatch after event {}",
            event.ordinal
        );

        assert_eq!(
            asks, expected_asks,
            "ask book mismatch after event {}",
            event.ordinal
        );

        println!(
            "  VERIFIED | event={} | bids={} | asks={}",
            event.ordinal,
            bids.len(),
            asks.len()
        );
    }

    println!("\nBITGET INTERMEDIATE PARQUET REPLAY: PASS");
}
// -----------------------------------------------------------------------------
// Integration test
// -----------------------------------------------------------------------------

#[test]
fn processes_bitget_xlsx_pipeline() {
    let root = project_root();

    // -------------------------------------------------------------------------
    // Load existing Bitget processing configuration
    // -------------------------------------------------------------------------

    let job = load_processing_job(
        root.join("data/.jobs/process/bitget-spot-spot-BTCUSDT-l2-20260901-20260904-d110.json"),
    )
    .expect("load Bitget processing job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    // -------------------------------------------------------------------------
    // Create synthetic source archive
    // -------------------------------------------------------------------------

    let directory = TestDirectory::new();

    let input_path = directory.path.join("synthetic.zip");
    let output_path = directory.path.join("output");

    create_bitget_archive(&input_path);

    task.input_path = input_path;
    task.source_compression = SourceContainer::Zip;
    task.archive_member = None;

    // Synthetic snapshots contain two levels per side.
    task.raw_schema["order_book"]["snapshot_depth"]["asks"] = serde_json::json!(2);

    task.raw_schema["order_book"]["snapshot_depth"]["bids"] = serde_json::json!(2);

    // -------------------------------------------------------------------------
    // Execute complete processing pipeline
    // -------------------------------------------------------------------------

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Unsequenced,
    )
    .expect("process Bitget XLSX archive");

    writer.finish().expect("finalize Parquet writer");

    // -------------------------------------------------------------------------
    // Source accounting
    // -------------------------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 2);
    assert_eq!(metrics.counters.records_processed, 2);
    assert_eq!(metrics.counters.records_rejected, 0);

    // Four initialization levels and one subsequent absolute change.
    assert_eq!(metrics.counters.events_written, 5);

    // -------------------------------------------------------------------------
    // Verify canonical event index
    // -------------------------------------------------------------------------

    let events = read_event_index(&output_path);

    // -------------------------------------------------------------------------
    // Verify every snapshot from persisted canonical Parquet
    // -------------------------------------------------------------------------

    verify_intermediate_snapshot_replay(&output_path, &events);

    assert_eq!(events.len(), 2);

    // First chronological snapshot: timestamp 100.
    assert_eq!(
        events[0],
        EventIndexEntry {
            ordinal: 1,
            offset: 0,
            count: 4,
        }
    );

    // Second chronological snapshot: timestamp 200.
    assert_eq!(
        events[1],
        EventIndexEntry {
            ordinal: 2,
            offset: 4,
            count: 1,
        }
    );

    // All canonical rows must be accounted for.
    let total_indexed_rows: u64 = events.iter().map(|event| event.count).sum();

    assert_eq!(total_indexed_rows, 5);

    // -------------------------------------------------------------------------
    // Read boundary manifest
    // -------------------------------------------------------------------------

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(output_path.join("boundary.json")).expect("open boundary manifest"),
    )
    .expect("deserialize boundary manifest");

    // Bitget snapshots have no native sequence identifiers.
    assert_eq!(boundary.continuity, BoundaryContinuity::Unverifiable);

    let initial = boundary.initial.as_ref().expect("missing initial snapshot");

    let final_state = boundary.final_state.as_ref().expect("missing final book");

    // -------------------------------------------------------------------------
    // Verify chronological processing
    // -------------------------------------------------------------------------

    assert_eq!(initial.timestamp_ns, 100_000_000_000);

    assert_eq!(final_state.timestamp_ns, 200_000_000_000);

    // -------------------------------------------------------------------------
    // Verify book depth
    // -------------------------------------------------------------------------

    assert_eq!(initial.state.bids.len(), 2);
    assert_eq!(initial.state.asks.len(), 2);

    assert_eq!(final_state.state.bids.len(), 2);
    assert_eq!(final_state.state.asks.len(), 2);

    // -------------------------------------------------------------------------
    // Verify initial snapshot quantities
    // -------------------------------------------------------------------------

    let initial_bid_100 = initial
        .state
        .bids
        .iter()
        .find(|level| level.price == decimal("100"))
        .expect("missing initial bid 100");

    assert_eq!(initial_bid_100.quantity_base, Some(decimal("5")));

    let initial_bid_99 = initial
        .state
        .bids
        .iter()
        .find(|level| level.price == decimal("99"))
        .expect("missing initial bid 99");

    assert_eq!(initial_bid_99.quantity_base, Some(decimal("6")));

    let initial_ask_101 = initial
        .state
        .asks
        .iter()
        .find(|level| level.price == decimal("101"))
        .expect("missing initial ask 101");

    assert_eq!(initial_ask_101.quantity_base, Some(decimal("2")));

    let initial_ask_102 = initial
        .state
        .asks
        .iter()
        .find(|level| level.price == decimal("102"))
        .expect("missing initial ask 102");

    assert_eq!(initial_ask_102.quantity_base, Some(decimal("4")));

    // -------------------------------------------------------------------------
    // Verify final snapshot quantities
    // -------------------------------------------------------------------------

    let final_bid_100 = final_state
        .state
        .bids
        .iter()
        .find(|level| level.price == decimal("100"))
        .expect("missing final bid 100");

    assert_eq!(final_bid_100.quantity_base, Some(decimal("5")));

    let final_bid_99 = final_state
        .state
        .bids
        .iter()
        .find(|level| level.price == decimal("99"))
        .expect("missing final bid 99");

    assert_eq!(final_bid_99.quantity_base, Some(decimal("6")));

    let final_ask_101 = final_state
        .state
        .asks
        .iter()
        .find(|level| level.price == decimal("101"))
        .expect("missing final ask 101");

    assert_eq!(final_ask_101.quantity_base, Some(decimal("3")));

    let final_ask_102 = final_state
        .state
        .asks
        .iter()
        .find(|level| level.price == decimal("102"))
        .expect("missing final ask 102");

    assert_eq!(final_ask_102.quantity_base, Some(decimal("4")));

    // -------------------------------------------------------------------------
    // Result
    // -------------------------------------------------------------------------

    println!("\nBITGET XLSX PIPELINE: PASS");
    println!("Source snapshots : 2");
    println!("Canonical levels : 5");
    println!("Event index      : VERIFIED");
    println!("Initial book     : VERIFIED");
    println!("Final book       : VERIFIED");
    println!("Continuity       : Unverifiable");
}
