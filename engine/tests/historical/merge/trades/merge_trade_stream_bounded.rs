use crate::publication::trade_with_empty_task;
use marketforge_engine::historical::merge::build_trade_inventory;

#[test]
fn accepts_dataset_with_empty_source_task() {
    let workspace = tempfile::tempdir().unwrap();
    let input = trade_with_empty_task(workspace.path(), 17);
    assert_eq!(input.manifest.source_tasks, [0, 1, 2]);
    let inventory = build_trade_inventory(&[input]).unwrap();
    assert_eq!(
        inventory
            .iter()
            .map(|task| (task.task_id, task.canonical_rows))
            .collect::<Vec<_>>(),
        [(0, 2), (1, 2)]
    );
}
