use marketforge_engine::historical::merge::{
    CheckpointAvailability, InitialMergeCheckpoint, MergeCheckpoint, PhysicalMergeCheckpoint,
    PhysicalReconstructionAvailability, resolve_physical_initial_reconstruction,
};

fn canonical(dataset_id: u64, timestamp_ns: i64) -> InitialMergeCheckpoint {
    InitialMergeCheckpoint {
        checkpoint: MergeCheckpoint {
            dataset_id,
            task_id: 0,
            segment_id: 0,
            timestamp_ns,
            canonical_row_offset: 0,
            canonical_row_count: 2,
        },
        availability: if timestamp_ns <= 200 {
            CheckpointAvailability::Prior
        } else {
            CheckpointAvailability::InsideInterval
        },
    }
}

fn physical(dataset_id: u64, timestamp_ns: i64) -> PhysicalMergeCheckpoint {
    PhysicalMergeCheckpoint {
        dataset_id,
        task_id: 0,
        segment_id: 0,
        timestamp_ns,
        partition_id: 0,
        file_index: 0,
        row_index: 0,
    }
}

#[test]
fn identifies_prior_external_checkpoint() {
    let result = resolve_physical_initial_reconstruction(
        &[canonical(89, 100)],
        &[physical(89, 400)],
        200,
        500,
    )
    .unwrap();

    assert_eq!(result.len(), 1);

    assert_eq!(
        result[0].availability,
        PhysicalReconstructionAvailability::PriorExternal
    );

    assert_eq!(result[0].first_physical_checkpoint_ns, Some(400));
    assert_eq!(result[0].prior_canonical_checkpoint_ns, Some(100));
}

#[test]
fn identifies_unavailable_initialization() {
    let result =
        resolve_physical_initial_reconstruction(&[canonical(89, 300)], &[], 200, 500).unwrap();

    assert_eq!(
        result[0].availability,
        PhysicalReconstructionAvailability::Unavailable
    );

    assert_eq!(result[0].first_physical_checkpoint_ns, None);
}

#[test]
fn identifies_available_boundary_checkpoint() {
    let result = resolve_physical_initial_reconstruction(
        &[canonical(89, 200)],
        &[physical(89, 200)],
        200,
        500,
    )
    .unwrap();

    assert_eq!(
        result[0].availability,
        PhysicalReconstructionAvailability::Available
    );
}

#[test]
fn rejects_duplicate_initial_datasets() {
    let result = resolve_physical_initial_reconstruction(
        &[canonical(89, 100), canonical(89, 100)],
        &[],
        200,
        500,
    );

    assert!(result.is_err());
}

#[test]
fn rejects_out_of_range_physical_checkpoint() {
    let result = resolve_physical_initial_reconstruction(
        &[canonical(89, 100)],
        &[physical(89, 600)],
        200,
        500,
    );

    assert!(result.is_err());
}
