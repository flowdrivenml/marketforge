use std::{fs, path::PathBuf};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::{
                CombinedConversionKernel, CombinedConversionMetrics, CombinedConversionRow,
                CombinedReferenceAlignment, CombinedReferenceSelection, ConversionPartitionReader,
                build_combined_reference_checkpoints, build_conversion_partition_plan,
                execute_combined_partition,
            },
            plan_conversion,
            reference::ExternalReferenceCursor,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const EVENTS: u64 = 20_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn read_parquet(directory: &std::path::Path) -> Vec<RecordBatch> {
    fn collect(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                collect(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    collect(directory, &mut files);
    files.sort();

    let mut batches = Vec::new();

    for path in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
            .unwrap()
            .with_batch_size(4096)
            .build()
            .unwrap();

        for batch in reader {
            batches.push(batch.unwrap());
        }
    }

    batches
}

#[test]
fn production_combined_worker_matches_sequential() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    // Resolve using the original production manifest.
    // Both datasets have identical stream identities.
    let production_source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let resolved = resolve_conversion_catalog(&job, &production_source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let partitions = build_conversion_partition_plan(&fixture_path, &source, 2).unwrap();

    assert_eq!(partitions.total_rows, EVENTS);
    assert_eq!(partitions.partitions.len(), 2);

    // ---------------------------------------------------------------------
    // Build checkpoints using production prepass
    // ---------------------------------------------------------------------

    let prepass = build_combined_reference_checkpoints(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    assert_eq!(prepass.events_scanned, EVENTS);

    // ---------------------------------------------------------------------
    // Execute the second partition using production worker
    // ---------------------------------------------------------------------

    let partition = &partitions.partitions[1];
    let checkpoint = &prepass.checkpoints[1];

    let temporary = tempfile::tempdir().unwrap();

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    // Worker reads from job.input_merge, so point a local job copy
    // at the physical fixture.
    let mut worker_job = job.clone();
    worker_job.input_merge = fixture_path.clone();

    let result = execute_combined_partition(
        &worker_job,
        &source,
        Some(&external),
        &plan,
        &partitions,
        partition,
        checkpoint,
        &selection,
        temporary.path(),
        &parquet_config,
    )
    .unwrap();

    assert_eq!(result.metrics.events_read, partition.rows);
    assert_eq!(result.metrics.events_written, partition.rows);

    // ---------------------------------------------------------------------
    // Sequential baseline over the entire fixture
    // ---------------------------------------------------------------------

    let cursor = ExternalReferenceCursor::new(
        reference_directory,
        &external,
        selection.external_streams.iter().cloned(),
    )
    .unwrap();

    let mut alignment = CombinedReferenceAlignment::new(selection, Some(cursor)).unwrap();

    let kernel = CombinedConversionKernel::new(&worker_job, &plan).unwrap();

    let mut metrics = CombinedConversionMetrics::default();

    let mut expected = Vec::<CombinedConversionRow>::new();

    let mut global_row = 0_u64;

    for source_batch in read_parquet(&fixture_path) {
        let converted = kernel
            .convert_batch(&source_batch, &mut alignment, &mut metrics)
            .unwrap();

        for row in converted {
            if global_row >= partition.start_row && global_row < partition.end_row {
                expected.push(row);
            }

            global_row += 1;
        }
    }

    assert_eq!(global_row, EVENTS);
    assert_eq!(expected.len() as u64, partition.rows);

    // ---------------------------------------------------------------------
    // Independently construct expected physical output
    // ---------------------------------------------------------------------

    use marketforge_engine::historical::convert::output::ConvertedCombinedBatchBuilder;

    let mut expected_batches = Vec::new();
    let mut expected_offset = 0_usize;

    let mut partition_reader =
        ConversionPartitionReader::new(&fixture_path, &source, &partitions, partition, 4096)
            .unwrap();

    while let Some(batch) = partition_reader.next_batch().unwrap() {
        let count = batch.num_rows();

        let builder = ConvertedCombinedBatchBuilder::new(batch.schema()).unwrap();

        let converted = builder
            .build(&batch, &expected[expected_offset..expected_offset + count])
            .unwrap();

        expected_batches.push(converted);
        expected_offset += count;
    }

    assert_eq!(expected_offset as u64, partition.rows);

    // ---------------------------------------------------------------------
    // Compare actual physical Parquet with sequential output
    // ---------------------------------------------------------------------

    let actual_batches = read_parquet(&result.output_directory);

    let schema = expected_batches[0].schema();

    let expected_table = arrow_select::concat::concat_batches(&schema, &expected_batches).unwrap();

    let actual_table = arrow_select::concat::concat_batches(&schema, &actual_batches).unwrap();

    assert_eq!(
        expected_table, actual_table,
        "production combined worker differs from sequential conversion"
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PRODUCTION COMBINED WORKER EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Fixture events    : {}", EVENTS);
    println!("Partitions        : {}", partitions.partitions.len());
    println!("Boundary row      : {}", partition.start_row);
    println!("Events compared   : {}", partition.rows);
    println!("Trade events      : {}", result.metrics.trade_events);
    println!("Depth events      : {}", result.metrics.depth_events);
    println!("Depth levels      : {}", result.metrics.depth_levels_read);
    println!(
        "Internal updates  : {}",
        result.metrics.internal_reference_updates
    );
    println!(
        "External updates  : {}",
        result.metrics.external_reference_updates
    );
    println!("Parquet files     : {}", result.metrics.files_written);
    println!("STATUS            : PASS");
}
