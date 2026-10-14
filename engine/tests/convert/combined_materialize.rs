use std::{fs, path::PathBuf};

use arrow_array::{Array, ListArray, StructArray, UInt8Array};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::materialize_combined_conversion, plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const EVENTS: u64 = 20_000;
const EXPECTED_TRADES: u64 = 3_954;
const EXPECTED_DEPTH: u64 = 16_046;
const EXPECTED_LEVELS: u64 = 741_562;
const EXPECTED_INTERNAL_REFS: u64 = 3_954;
const EXPECTED_EXTERNAL_REFS: u64 = 22_616;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn materializes_real_combined_conversion() {
    let root = root();

    // ---------------------------------------------------------------------
    // Load conversion job
    // ---------------------------------------------------------------------

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // ---------------------------------------------------------------------
    // Temporary physical output
    // ---------------------------------------------------------------------

    let temporary = tempfile::tempdir().unwrap();

    let output = temporary.path().join("combined");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    // ---------------------------------------------------------------------
    // Execute production materializer
    // ---------------------------------------------------------------------

    let start = std::time::Instant::now();

    let metrics = materialize_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &output,
        &parquet_config,
        Some(EVENTS),
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    // ---------------------------------------------------------------------
    // Validate accounting
    // ---------------------------------------------------------------------

    assert_eq!(metrics.events_read, EVENTS);
    assert_eq!(metrics.events_written, EVENTS);

    assert_eq!(metrics.trade_events, EXPECTED_TRADES);
    assert_eq!(metrics.depth_events, EXPECTED_DEPTH);

    assert_eq!(metrics.depth_levels_read, EXPECTED_LEVELS);

    assert_eq!(
        metrics.depth_levels_converted + metrics.depth_levels_unavailable,
        EXPECTED_LEVELS,
    );

    assert_eq!(
        metrics.identity_events + metrics.converted_events + metrics.unavailable_events,
        EVENTS,
    );

    assert_eq!(metrics.internal_reference_updates, EXPECTED_INTERNAL_REFS,);

    assert_eq!(metrics.external_reference_updates, EXPECTED_EXTERNAL_REFS,);

    assert!(metrics.files_written > 0);
    assert!(metrics.row_groups_written > 0);

    // ---------------------------------------------------------------------
    // Discover physical Parquet files
    // ---------------------------------------------------------------------

    let mut files = Vec::new();

    fn collect_parquet(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_dir() {
                collect_parquet(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    collect_parquet(&output, &mut files);
    files.sort();

    assert_eq!(files.len() as u64, metrics.files_written);

    // ---------------------------------------------------------------------
    // Validate physical output
    // ---------------------------------------------------------------------

    let mut physical_events = 0_u64;
    let mut physical_trades = 0_u64;
    let mut physical_depth = 0_u64;
    let mut physical_levels = 0_u64;

    for path in &files {
        let file = fs::File::open(path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(4096)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            physical_events += batch.num_rows() as u64;

            let kinds = batch
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            let trades = batch
                .column_by_name("trade_target")
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let depths = batch
                .column_by_name("depth_target")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let source_depths = batch
                .column_by_name("depth")
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let source_changes = source_depths
                .column_by_name("changes")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            for row in 0..batch.num_rows() {
                match kinds.value(row) {
                    0 => {
                        physical_trades += 1;

                        assert!(!trades.is_null(row));
                        assert!(depths.is_null(row));
                    }

                    1 => {
                        physical_depth += 1;

                        assert!(trades.is_null(row));
                        assert!(!depths.is_null(row));

                        let original_levels = source_changes.value_length(row);

                        let converted_levels = depths.value_length(row);

                        assert_eq!(
                            original_levels, converted_levels,
                            "depth level count mismatch"
                        );

                        physical_levels += original_levels as u64;
                    }

                    kind => panic!("invalid event kind {kind}"),
                }
            }
        }
    }

    assert_eq!(physical_events, EVENTS);
    assert_eq!(physical_trades, EXPECTED_TRADES);
    assert_eq!(physical_depth, EXPECTED_DEPTH);
    assert_eq!(physical_levels, EXPECTED_LEVELS);

    // ---------------------------------------------------------------------
    // Summary
    // ---------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PHYSICAL COMBINED MATERIALIZATION");
    println!("{}", "=".repeat(90));

    println!("Events             : {}", metrics.events_written);
    println!("Trade events       : {}", metrics.trade_events);
    println!("Depth events       : {}", metrics.depth_events);
    println!("Depth levels       : {}", metrics.depth_levels_read);

    println!("Identity           : {}", metrics.identity_events);
    println!("Converted          : {}", metrics.converted_events);
    println!("Unavailable        : {}", metrics.unavailable_events);

    println!(
        "Internal references: {}",
        metrics.internal_reference_updates
    );

    println!(
        "External references: {}",
        metrics.external_reference_updates
    );

    println!("Parquet files      : {}", metrics.files_written);
    println!("Row groups         : {}", metrics.row_groups_written);

    println!("Elapsed            : {:.3} s", elapsed);

    println!(
        "Throughput         : {:.0} events/s",
        EVENTS as f64 / elapsed
    );

    println!("STATUS             : PASS");
}
