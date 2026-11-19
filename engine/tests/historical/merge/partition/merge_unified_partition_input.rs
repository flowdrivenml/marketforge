#![cfg(feature = "historical")]
use crate::publication::depth_publication;
use crate::publication::trade_publication;
use marketforge_engine::historical::merge::{
    MergePartition, build_depth_inventory, build_trade_inventory, depth_task_intervals,
    plan_partition_inputs, plan_unified_partition_inputs, trade_task_intervals,
};

// -----------------------------------------------------------------------------
// Resolve real trade dataset
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Depth planner equivalence
// -----------------------------------------------------------------------------

#[test]
fn unified_planner_matches_depth_planner() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let dataset_ids = [105_u64, 108, 112, 92];

    let inputs = dataset_ids
        .iter()
        .map(|&id| depth_publication(&root, id))
        .collect::<Vec<_>>();

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let intervals = depth_task_intervals(&inventory).expect("depth intervals");

    let base = crate::publication::START;
    let hour = 200_i64;

    let partitions = (0..8)
        .map(|index| MergePartition {
            partition_id: index,
            start_ns: base + i64::from(index) * hour,
            end_ns: base + (i64::from(index) + 1) * hour,
        })
        .collect::<Vec<_>>();

    let old = plan_partition_inputs(&partitions, &inventory).expect("old depth planner");

    let new = plan_unified_partition_inputs(&partitions, &intervals).expect("unified planner");

    assert_eq!(old.len(), new.len());

    for (old_plan, new_plan) in old.iter().zip(&new) {
        let old_tasks = old_plan
            .inputs
            .iter()
            .map(|task| (task.dataset_id, task.task_id))
            .collect::<Vec<_>>();

        let new_tasks = new_plan
            .inputs
            .iter()
            .map(|task| (task.dataset_id, task.task_id))
            .collect::<Vec<_>>();

        assert_eq!(
            old_tasks, new_tasks,
            "partition {}: depth task selection mismatch",
            old_plan.partition.partition_id
        );

        println!(
            "partition={} | depth_tasks={} | MATCH",
            old_plan.partition.partition_id,
            old_tasks.len()
        );
    }
}

// -----------------------------------------------------------------------------
// Trade partition coverage
// -----------------------------------------------------------------------------

#[test]
fn plans_generated_trade_partitions() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let dataset_ids = [111_u64, 109, 115, 100, 97, 99];

    let inputs = dataset_ids
        .iter()
        .map(|&id| trade_publication(&root, id))
        .collect::<Vec<_>>();

    let inventory = build_trade_inventory(&inputs).expect("build trade inventory");

    let intervals = trade_task_intervals(&inventory);

    let base = crate::publication::START;
    let hour = 200_i64;

    let partitions = (0..24)
        .map(|index| MergePartition {
            partition_id: index,
            start_ns: base + i64::from(index) * hour,
            end_ns: base + (i64::from(index) + 1) * hour,
        })
        .collect::<Vec<_>>();

    let plans =
        plan_unified_partition_inputs(&partitions, &intervals).expect("plan trade partitions");

    assert_eq!(plans.len(), 24);

    for plan in &plans {
        for task in &plan.inputs {
            assert!(
                task.end_timestamp_ns >= plan.partition.start_ns
                    && task.start_timestamp_ns < plan.partition.end_ns
            );
        }

        println!(
            "partition={:02} | trade_tasks={}",
            plan.partition.partition_id,
            plan.inputs.len()
        );
    }

    // Every trade task overlapping the requested day must appear
    // in at least one partition.
    for interval in &intervals {
        if interval.end_timestamp_ns < base || interval.start_timestamp_ns >= base + 24 * hour {
            continue;
        }

        assert!(
            plans.iter().any(|plan| {
                plan.inputs.iter().any(|task| {
                    task.dataset_id == interval.dataset_id && task.task_id == interval.task_id
                })
            }),
            "missing trade task {}:{}",
            interval.dataset_id,
            interval.task_id
        );
    }
}

// -----------------------------------------------------------------------------
// Mixed trade/depth planning
// -----------------------------------------------------------------------------

#[test]
fn plans_combined_trade_depth_partitions() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let trade_inputs = [111_u64, 109]
        .iter()
        .map(|&id| trade_publication(&root, id))
        .collect::<Vec<_>>();

    let depth_inputs = [105_u64, 112]
        .iter()
        .map(|&id| depth_publication(&root, id))
        .collect::<Vec<_>>();

    let trades = build_trade_inventory(&trade_inputs).expect("trade inventory");

    let depth = build_depth_inventory(&depth_inputs).expect("depth inventory");

    let mut intervals = trade_task_intervals(&trades);

    intervals.extend(depth_task_intervals(&depth).expect("depth intervals"));

    let base = crate::publication::START;

    let partitions = vec![
        MergePartition {
            partition_id: 0,
            start_ns: base,
            end_ns: base + 200,
        },
        MergePartition {
            partition_id: 1,
            start_ns: base + 200,
            end_ns: base + 400,
        },
    ];

    let plans =
        plan_unified_partition_inputs(&partitions, &intervals).expect("plan combined partitions");

    assert_eq!(plans.len(), 2);

    for plan in &plans {
        let datasets = plan
            .inputs
            .iter()
            .map(|task| task.dataset_id)
            .collect::<std::collections::HashSet<_>>();

        assert!(datasets.contains(&111));
        assert!(datasets.contains(&109));
        assert!(datasets.contains(&105));
        assert!(datasets.contains(&112));

        println!(
            "partition={} | datasets={} | tasks={}",
            plan.partition.partition_id,
            datasets.len(),
            plan.inputs.len()
        );
    }
}
