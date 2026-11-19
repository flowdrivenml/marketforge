use crate::publication::depth_publication;
use marketforge_engine::historical::merge::{build_depth_inventory, reconcile_depth_boundaries};

#[test]
fn builds_generated_depth_inventory() {
    let workspace = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(workspace.path(), 12)]).unwrap();
    assert_eq!(inventory.len(), 2);
    for (task, expected) in inventory.iter().zip([0, 1]) {
        assert_eq!(
            (task.dataset_id, task.task_id, task.canonical_rows),
            (12, expected, 2)
        );
        assert_eq!(task.segments.total_levels, 2);
        assert_eq!(task.canonical_files.len(), 1);
        assert!(
            task.events_path.is_file()
                && task.segments_path.is_file()
                && task.boundary_path.is_file()
        );
    }
}

#[test]
fn reconciles_generated_depth_boundaries() {
    let workspace = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(workspace.path(), 12)]).unwrap();
    let links = reconcile_depth_boundaries(&inventory).unwrap();
    assert_eq!(links.len(), 1);
    let link = &links[0];
    assert_eq!(
        (
            link.dataset_id,
            link.previous_task_id,
            link.next_task_id,
            link.timestamp_delta_ns
        ),
        (12, 0, 1, Some(100))
    );
}
