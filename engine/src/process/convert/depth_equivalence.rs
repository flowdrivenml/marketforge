use std::{collections::HashSet, fs, path::PathBuf, time::Instant};

use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;

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
