use crate::publication::depth_publication;
use marketforge_engine::historical::merge::{
    CheckpointAvailability, MergeCheckpoint, build_depth_inventory, build_merge_checkpoints,
    resolve_initial_checkpoints,
};

#[test]
fn builds_generated_reconstruction_checkpoints() {
    let workspace = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(workspace.path(), 12)]).unwrap();
    let checkpoints = build_merge_checkpoints(&inventory).unwrap();
    assert_eq!(
        checkpoints,
        [(0, 100), (1, 300)].map(|(task_id, timestamp_ns)| MergeCheckpoint {
            dataset_id: 12,
            task_id,
            segment_id: 0,
            timestamp_ns,
            canonical_row_offset: 0,
            canonical_row_count: 1,
        })
    );
    let initial = resolve_initial_checkpoints(&checkpoints, &[12], 350, 450).unwrap();
    assert_eq!(initial[0].checkpoint, checkpoints[1]);
    assert_eq!(initial[0].availability, CheckpointAvailability::Prior);
}

#[test]
fn resolves_initial_checkpoints() {
    let checkpoint = |dataset_id, timestamp_ns, segment_id| MergeCheckpoint {
        dataset_id,
        task_id: 7,
        segment_id,
        timestamp_ns,
        canonical_row_offset: 2 * segment_id,
        canonical_row_count: 2,
    };
    let checkpoints = [
        checkpoint(1, 10, 0),
        checkpoint(1, 15, 1),
        checkpoint(2, 21, 0),
    ];
    let selected = resolve_initial_checkpoints(&checkpoints, &[2, 1], 15, 22).unwrap();
    assert_eq!(selected[0].checkpoint, checkpoints[2]);
    assert_eq!(
        selected[0].availability,
        CheckpointAvailability::InsideInterval
    );
    assert_eq!(selected[1].checkpoint, checkpoints[1]);
    assert_eq!(selected[1].availability, CheckpointAvailability::Prior);
    assert!(resolve_initial_checkpoints(&checkpoints, &[2], 15, 21).is_err());
}
