use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use marketforge_engine::{
    job::MergeParquetConfig, process::convert::execution::execute_depth_partition,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::thread;
use std::{collections::HashSet, fs, path::PathBuf, time::Instant};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{
                ConversionPartition, ConversionPartitionPlan, ConversionPartitionReader,
                DepthConversionAlignment, DepthConversionKernel, build_conversion_partition_plan,
                build_depth_reference_checkpoints,
            },
            plan_conversion,
            reference::ExternalReferenceCursor,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

const BATCH_SIZE: usize = 4096;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}
fn read_converted_partition(directory: &std::path::Path) -> Vec<RecordBatch> {
    let mut files = Vec::new();

    fn discover(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_dir() {
                discover(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }

    discover(directory, &mut files);
    files.sort();

    assert!(!files.is_empty());

    let mut batches = Vec::new();

    for path in files {
        let file = fs::File::open(&path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(BATCH_SIZE)
            .build()
            .unwrap();

        for batch in reader {
            batches.push(batch.unwrap());
        }
    }

    batches
}
#[test]
fn checkpointed_depth_matches_sequential() {
    let root = root();

    let job =
        load_convert_job(root.join("data/.jobs/convert/convert-depth-parallel-test.json")).unwrap();

    let source_directory = root.join("data/merged/depth-conversion-test");

    let reference_directory = root.join("data/merged/final-trades");

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(source_directory.join("manifest.json")).unwrap()).unwrap();

    let references: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Build physical row-group index
    // -------------------------------------------------------------------------

    let full = build_conversion_partition_plan(&source_directory, &source, 2).unwrap();

    assert!(full.row_groups.len() >= 2);

    // Use two adjacent row groups near the middle of the dataset.
    // This ensures historical reference state is nonempty.
    let first_group = full.row_groups.len() / 2;
    let second_group = first_group + 1;

    assert!(second_group < full.row_groups.len());

    let first = &full.row_groups[first_group];
    let second = &full.row_groups[second_group];

    let partition_0 = ConversionPartition {
        partition_id: 0,
        first_row_group: first_group,
        end_row_group: second_group,
        start_row: first.global_start_row,
        end_row: second.global_start_row,
        rows: first.rows,
    };

    let partition_1 = ConversionPartition {
        partition_id: 1,
        first_row_group: second_group,
        end_row_group: second_group + 1,
        start_row: second.global_start_row,
        end_row: second.global_start_row + second.rows,
        rows: second.rows,
    };

    let bounded = ConversionPartitionPlan {
        row_groups: full.row_groups.clone(),
        partitions: vec![partition_0.clone(), partition_1.clone()],
        total_rows: full.total_rows,
        total_compressed_bytes: full.total_compressed_bytes,
    };

    // -------------------------------------------------------------------------
    // Build independent historical checkpoints
    // -------------------------------------------------------------------------

    let start = Instant::now();

    let prepass = build_depth_reference_checkpoints(
        &source_directory,
        &source,
        &reference_directory,
        &references,
        &plan,
        &bounded,
    )
    .unwrap();

    assert_eq!(prepass.checkpoints.len(), 2);

    // -------------------------------------------------------------------------
    // Read both source row groups
    // -------------------------------------------------------------------------

    let read_partition = |partition: &ConversionPartition| {
        let mut reader = ConversionPartitionReader::new(
            &source_directory,
            &source,
            &bounded,
            partition,
            BATCH_SIZE,
        )
        .unwrap();

        let mut batches = Vec::new();

        while let Some(batch) = reader.next_batch().unwrap() {
            batches.push(batch);
        }

        batches
    };

    let first_batches = read_partition(&partition_0);
    let second_batches = read_partition(&partition_1);

    // -------------------------------------------------------------------------
    // Selected reference streams
    // -------------------------------------------------------------------------

    let selected = plan
        .streams
        .iter()
        .flat_map(|stream| &stream.paths)
        .flat_map(|path| &path.edges)
        .map(|edge| edge.reference_stream_id.clone())
        .collect::<HashSet<_>>();

    // -------------------------------------------------------------------------
    // Sequential conversion
    // -------------------------------------------------------------------------

    let sequential_cursor = ExternalReferenceCursor::resume(
        &reference_directory,
        &references,
        selected.clone(),
        prepass.checkpoints[0].reference_position,
    )
    .unwrap();

    let sequential_kernel = DepthConversionKernel::new(&job, &plan).unwrap();

    let mut sequential = DepthConversionAlignment::from_checkpoint(
        sequential_kernel,
        sequential_cursor,
        prepass.checkpoints[0].state.snapshot(),
    );

    let mut sequential_batches = Vec::new();

    for batch in first_batches.iter().chain(&second_batches) {
        sequential_batches.push(sequential.convert_batch(batch).unwrap());
    }

    // -------------------------------------------------------------------------
    // Checkpointed conversion
    // -------------------------------------------------------------------------

    let mut checkpointed_batches = Vec::new();

    for (index, batches) in [&first_batches, &second_batches].iter().enumerate() {
        let checkpoint = &prepass.checkpoints[index];

        let cursor = ExternalReferenceCursor::resume(
            &reference_directory,
            &references,
            selected.clone(),
            checkpoint.reference_position,
        )
        .unwrap();

        let kernel = DepthConversionKernel::new(&job, &plan).unwrap();

        let mut alignment =
            DepthConversionAlignment::from_checkpoint(kernel, cursor, checkpoint.state.snapshot());

        for batch in batches.iter() {
            checkpointed_batches.push(alignment.convert_batch(batch).unwrap());
        }
    }

    // -------------------------------------------------------------------------
    // Exact Arrow comparison
    // -------------------------------------------------------------------------

    let schema = sequential_batches[0].schema();

    let sequential = concat_batches(&schema, &sequential_batches).unwrap();

    let checkpointed = concat_batches(&schema, &checkpointed_batches).unwrap();

    assert_eq!(sequential.num_rows(), checkpointed.num_rows());
    assert_eq!(sequential.num_columns(), 24);

    for index in 0..24 {
        assert_eq!(
            sequential.column(index).to_data(),
            checkpointed.column(index).to_data(),
            "depth conversion mismatch in column {}",
            schema.field(index).name(),
        );
    }

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH CHECKPOINT EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("First row group   : {}", first_group);
    println!("Second row group  : {}", second_group);
    println!("Events compared   : {}", sequential.num_rows());
    println!("Columns compared  : {}", sequential.num_columns());
    println!("Reference prepass : {:.3} s", start.elapsed().as_secs_f64());
    println!("STATUS            : PASS");
}

#[test]
fn concurrent_depth_workers_match_sequential() {
    let root = root();
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("parallel");

    let job =
        load_convert_job(root.join("data/.jobs/convert/convert-depth-parallel-test.json")).unwrap();

    let source_directory = root.join("data/merged/depth-conversion-test");

    let reference_directory = root.join("data/merged/final-trades");

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(source_directory.join("manifest.json")).unwrap()).unwrap();

    let references: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------------------
    // Select the same two real row groups
    // -------------------------------------------------------------------------

    let full = build_conversion_partition_plan(&source_directory, &source, 2).unwrap();

    let first_group = 181;
    let second_group = 182;

    let first = &full.row_groups[first_group];
    let second = &full.row_groups[second_group];

    let partition_0 = ConversionPartition {
        partition_id: 0,
        first_row_group: first_group,
        end_row_group: second_group,
        start_row: first.global_start_row,
        end_row: second.global_start_row,
        rows: first.rows,
    };

    let partition_1 = ConversionPartition {
        partition_id: 1,
        first_row_group: second_group,
        end_row_group: second_group + 1,
        start_row: second.global_start_row,
        end_row: second.global_start_row + second.rows,
        rows: second.rows,
    };

    let bounded = ConversionPartitionPlan {
        row_groups: full.row_groups.clone(),
        partitions: vec![partition_0.clone(), partition_1.clone()],
        total_rows: full.total_rows,
        total_compressed_bytes: full.total_compressed_bytes,
    };

    // -------------------------------------------------------------------------
    // Build historical checkpoints
    // -------------------------------------------------------------------------

    let prepass = build_depth_reference_checkpoints(
        &source_directory,
        &source,
        &reference_directory,
        &references,
        &plan,
        &bounded,
    )
    .unwrap();

    assert_eq!(prepass.checkpoints.len(), 2);

    // -------------------------------------------------------------------------
    // Sequential baseline
    // -------------------------------------------------------------------------

    let selected = plan
        .streams
        .iter()
        .flat_map(|stream| &stream.paths)
        .flat_map(|path| &path.edges)
        .map(|edge| edge.reference_stream_id.clone())
        .collect::<HashSet<_>>();

    let cursor = ExternalReferenceCursor::resume(
        &reference_directory,
        &references,
        selected,
        prepass.checkpoints[0].reference_position,
    )
    .unwrap();

    let kernel = DepthConversionKernel::new(&job, &plan).unwrap();

    let mut sequential = DepthConversionAlignment::from_checkpoint(
        kernel,
        cursor,
        prepass.checkpoints[0].state.snapshot(),
    );

    let mut sequential_batches = Vec::new();

    for partition in [&partition_0, &partition_1] {
        let mut reader = ConversionPartitionReader::new(
            &source_directory,
            &source,
            &bounded,
            partition,
            BATCH_SIZE,
        )
        .unwrap();

        while let Some(batch) = reader.next_batch().unwrap() {
            sequential_batches.push(sequential.convert_batch(&batch).unwrap());
        }
    }

    // -------------------------------------------------------------------------
    // Execute both physical workers concurrently
    // -------------------------------------------------------------------------

    let parquet = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 256,
        compression_level: 3,
    };

    let worker_start = Instant::now();

    let mut results = thread::scope(|scope| {
        let handles = bounded
            .partitions
            .iter()
            .zip(&prepass.checkpoints)
            .map(|(partition, checkpoint)| {
                let parquet = parquet.clone();

                let job = &job;
                let source = &source;
                let references = &references;
                let plan = &plan;
                let bounded = &bounded;
                let staging = &staging;

                scope.spawn(move || {
                    execute_depth_partition(
                        job, source, references, plan, bounded, partition, checkpoint, staging,
                        &parquet,
                    )
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let worker_elapsed = worker_start.elapsed().as_secs_f64();

    results.sort_by_key(|result| result.partition_id);

    assert_eq!(results.len(), 2);

    // -------------------------------------------------------------------------
    // Read physical worker output
    // -------------------------------------------------------------------------

    let mut parallel_batches = Vec::new();

    for result in &results {
        parallel_batches.extend(read_converted_partition(&result.output_directory));
    }

    // -------------------------------------------------------------------------
    // Exact Arrow comparison
    // -------------------------------------------------------------------------

    let schema = sequential_batches[0].schema();

    let sequential = concat_batches(&schema, &sequential_batches).unwrap();

    let parallel = concat_batches(&schema, &parallel_batches).unwrap();

    assert_eq!(sequential.num_rows(), parallel.num_rows());
    assert_eq!(sequential.num_columns(), 24);
    assert_eq!(sequential.schema(), parallel.schema());

    for index in 0..24 {
        assert_eq!(
            sequential.column(index).to_data(),
            parallel.column(index).to_data(),
            "concurrent depth mismatch in column {}",
            schema.field(index).name(),
        );
    }

    // -------------------------------------------------------------------------
    // Accounting
    // -------------------------------------------------------------------------

    let events = results
        .iter()
        .map(|result| result.metrics.events_written)
        .sum::<u64>();

    let levels = results
        .iter()
        .map(|result| result.metrics.levels_read)
        .sum::<u64>();

    assert_eq!(events as usize, sequential.num_rows());

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — CONCURRENT DEPTH EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Workers          : 2");
    println!("Row groups       : 181–182");
    println!("Events compared  : {}", events);
    println!("Levels processed : {}", levels);
    println!("Columns compared : 24");
    println!("Worker execution : {:.3} s", worker_elapsed);
    println!("STATUS           : PASS");
}
