use std::{fs, path::PathBuf, time::Instant};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{build_conversion_partition_plan, build_depth_reference_checkpoints},
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn builds_real_depth_reference_checkpoints() {
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

    let partitions = build_conversion_partition_plan(&source_directory, &source, 2).unwrap();

    assert_eq!(partitions.partitions.len(), 2);
    assert_eq!(partitions.total_rows, 5_661_998);

    let start = Instant::now();

    let prepass = build_depth_reference_checkpoints(
        &source_directory,
        &source,
        &reference_directory,
        &references,
        &plan,
        &partitions,
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    assert_eq!(prepass.checkpoints.len(), 2);

    for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
        assert_eq!(partition.partition_id, checkpoint.partition_id,);

        assert_eq!(partition.start_row, checkpoint.start_row,);

        assert!(checkpoint.reference_position.next_observation_index <= references.events_written);

        for observation in checkpoint.state.observations() {
            assert!(observation.key.timestamp_ns < checkpoint.start_timestamp_ns);
        }
    }

    for pair in prepass.checkpoints.windows(2) {
        assert!(pair[0].start_timestamp_ns <= pair[1].start_timestamp_ns);

        assert!(
            pair[0].reference_position.next_observation_index
                <= pair[1].reference_position.next_observation_index
        );
    }

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — REAL DEPTH REFERENCE CHECKPOINTS");
    println!("{}", "=".repeat(90));

    println!("Depth events       : {}", source.events_written);
    println!("Reference events   : {}", references.events_written);
    println!("Partitions         : {}", partitions.partitions.len());

    for checkpoint in &prepass.checkpoints {
        println!();
        println!("Partition          : {}", checkpoint.partition_id);
        println!("Start row          : {}", checkpoint.start_row);
        println!("Start timestamp    : {}", checkpoint.start_timestamp_ns);
        println!(
            "Reference position : {}",
            checkpoint.reference_position.next_observation_index
        );
        println!(
            "Reference streams  : {}",
            checkpoint.state.observations().count()
        );
    }

    println!();
    println!("Reference updates  : {}", prepass.reference_updates);
    println!("Observations read  : {}", prepass.observations_read);
    println!("Elapsed            : {:.3} s", elapsed);
    println!("STATUS             : PASS");
}
