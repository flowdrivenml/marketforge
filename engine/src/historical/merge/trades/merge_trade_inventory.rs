#[test]
fn accepts_dataset_with_empty_source_task() {
    let root = project_root();

    let input = resolve_trade_dataset(&root, 120);

    assert_eq!(input.manifest.source_tasks, vec![0, 1, 2]);

    let inventory =
        build_trade_inventory(&[input]).expect("build inventory with empty source task");

    let task_ids: Vec<u64> = inventory.iter().map(|task| task.task_id).collect();

    assert_eq!(task_ids, vec![0, 2]);

    let total_rows: u64 = inventory.iter().map(|task| task.canonical_rows).sum();

    assert_eq!(total_rows, 162);
}
