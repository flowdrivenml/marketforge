use crate::publication::depth_publication;
use marketforge_engine::historical::merge::{build_depth_inventory, validate_depth_events};

#[test]
fn validates_generated_depth_event_indexes() {
    let workspace = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(workspace.path(), 12)]).unwrap();
    assert_eq!(inventory.len(), 2);
    for task in &inventory {
        let report = validate_depth_events(task).unwrap();
        assert_eq!(
            (
                report.events,
                report.canonical_rows,
                report.initialization_events
            ),
            (2, 2, 1)
        );
    }
}
