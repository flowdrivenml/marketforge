use std::{fs, path::PathBuf, time::Instant};

use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray, UInt8Array};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::materialize_depth_conversion,
            output::{
                CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
                CONVERSION_STATUS_UNAVAILABLE,
            },
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EXPECTED_EVENTS: u64 = 12_194;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn materializes_real_depth_to_parquet() {
    let root = root();
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("depth.staging");

    // -------------------------------------------------------------------------
    // Load real conversion configuration
    // -------------------------------------------------------------------------

    let job = load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_path = job.conversion.reference_merge.as_ref().unwrap();

    let reference_manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_path.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    assert_eq!(plan.source_streams, 15);
    assert_eq!(plan.identity_streams, 8);
    assert_eq!(plan.convertible_streams, 5);
    assert_eq!(plan.unresolved_streams, 2);

    // -------------------------------------------------------------------------
    // Materialize bounded physical output
    // -------------------------------------------------------------------------

    let parquet = MergeParquetConfig {
        row_group_target_mb: 16,
        file_target_mb: 64,
        compression_level: 3,
    };

    let start = Instant::now();

    let metrics = materialize_depth_conversion(
        &job,
        &source,
        &reference_manifest,
        &plan,
        &staging,
        &parquet,
        Some(EXPECTED_EVENTS),
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Verify materialization metrics
    // -------------------------------------------------------------------------

    assert_eq!(metrics.events_read, EXPECTED_EVENTS);
    assert_eq!(metrics.events_written, EXPECTED_EVENTS);

    assert_eq!(metrics.identity_events, 10_695);
    assert_eq!(metrics.converted_events, 779);
    assert_eq!(metrics.unavailable_events, 720);

    assert_eq!(metrics.levels_read, 154_826);
    assert_eq!(metrics.levels_converted, 140_332);
    assert_eq!(metrics.levels_unavailable, 14_494);

    assert!(metrics.files_written > 0);
    assert!(metrics.row_groups_written > 0);

    // -------------------------------------------------------------------------
    // Discover physical Parquet files
    // -------------------------------------------------------------------------

    let mut files = fs::read_dir(&staging)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .collect::<Vec<_>>();

    files.sort();

    assert_eq!(files.len() as u64, metrics.files_written);

    // -------------------------------------------------------------------------
    // Read physical output
    // -------------------------------------------------------------------------

    let mut physical_events = 0_u64;
    let mut physical_levels = 0_u64;

    let mut identity = 0_u64;
    let mut converted = 0_u64;
    let mut unavailable = 0_u64;

    let mut files_checked = 0_u64;
    let mut row_groups_checked = 0_u64;

    for path in &files {
        let file = fs::File::open(path).unwrap();

        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let metadata = builder.metadata();

        row_groups_checked += metadata.num_row_groups() as u64;

        let mut reader = builder.with_batch_size(4_096).build().unwrap();

        while let Some(batch) = reader.next() {
            let batch: RecordBatch = batch.unwrap();

            assert_eq!(batch.num_columns(), 24);

            let changes = batch
                .column_by_name("changes")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let target = batch
                .column_by_name("changes_target")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let statuses = batch
                .column_by_name("conversion_status")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            assert_eq!(changes.value_offsets(), target.value_offsets(),);

            let target_levels = target
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let prices = target_levels
                .column_by_name("price_target")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let offset = target.value_offsets()[row] as usize;
                let length = target.value_length(row) as usize;

                physical_levels += length as u64;

                match statuses.value(row) {
                    CONVERSION_STATUS_IDENTITY => {
                        identity += 1;

                        for index in offset..offset + length {
                            assert!(!prices.is_null(index));
                        }
                    }

                    CONVERSION_STATUS_CONVERTED => {
                        converted += 1;

                        for index in offset..offset + length {
                            assert!(!prices.is_null(index));
                        }
                    }

                    CONVERSION_STATUS_UNAVAILABLE => {
                        unavailable += 1;

                        for index in offset..offset + length {
                            assert!(prices.is_null(index));
                        }
                    }

                    status => {
                        panic!("invalid conversion status: {status}");
                    }
                }
            }

            physical_events += batch.num_rows() as u64;
        }

        files_checked += 1;
    }

    // -------------------------------------------------------------------------
    // Reconcile physical output
    // -------------------------------------------------------------------------

    assert_eq!(physical_events, EXPECTED_EVENTS);
    assert_eq!(physical_levels, metrics.levels_read);

    assert_eq!(identity, metrics.identity_events);
    assert_eq!(converted, metrics.converted_events);
    assert_eq!(unavailable, metrics.unavailable_events);

    assert_eq!(files_checked, metrics.files_written);
    assert_eq!(row_groups_checked, metrics.row_groups_written,);

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PHYSICAL DEPTH MATERIALIZATION");
    println!("{}", "=".repeat(90));

    println!("Events             : {}", physical_events);
    println!("Depth levels       : {}", physical_levels);

    println!("Identity           : {}", identity);
    println!("Converted          : {}", converted);
    println!("Unavailable        : {}", unavailable);

    println!("Files              : {}", files_checked);
    println!("Row groups         : {}", row_groups_checked);

    println!("Reference updates  : {}", metrics.reference_updates);
    println!(
        "Observations read  : {}",
        metrics.reference_observations_read,
    );

    println!("Materialization    : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput         : {:.0} events/s",
            physical_events as f64 / elapsed,
        );

        println!(
            "Level throughput   : {:.0} levels/s",
            physical_levels as f64 / elapsed,
        );
    }

    println!("STATUS             : PASS");
}
