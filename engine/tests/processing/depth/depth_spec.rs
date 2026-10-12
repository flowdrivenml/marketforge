use std::{
    fs,
    path::{Path, PathBuf},
};

use marketforge_engine::{
    formats::depth::{DepthOperation, DepthSpec},
    job::{TargetSchema, load_processing_job},
};

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn compiles_all_depth_normalization_specs() {
    let directory = project_root().join("data/.jobs/process");

    let mut jobs = 0usize;
    let mut tasks = 0usize;
    let mut specs = 0usize;

    let mut snapshots = 0usize;
    let mut absolute_updates = 0usize;
    let mut relative_updates = 0usize;

    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();

        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        if !filename.contains("-l2-") || !filename.ends_with(".json") {
            continue;
        }

        let job = load_processing_job(&path).unwrap();

        jobs += 1;

        for task in &job.tasks {
            tasks += 1;

            for normalization in &task.normalizations {
                assert_eq!(normalization.target_schema, TargetSchema::Depth);

                let spec = DepthSpec::from_normalization(normalization).unwrap_or_else(|error| {
                    panic!(
                        "failed to compile depth spec: file={}, task={}, error={error}",
                        filename, task.task_id.0
                    )
                });

                specs += 1;

                match spec.operation {
                    DepthOperation::Snapshot(_) => snapshots += 1,
                    DepthOperation::AbsoluteUpdate => absolute_updates += 1,
                    DepthOperation::RelativeUpdate(_) => relative_updates += 1,
                }
            }
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — GENERIC DEPTH SPECIFICATION TEST");
    println!("{}", "=".repeat(80));

    println!("Jobs             : {jobs}");
    println!("Tasks            : {tasks}");
    println!("Specifications   : {specs}");
    println!("Snapshots        : {snapshots}");
    println!("Absolute updates : {absolute_updates}");
    println!("Relative updates : {relative_updates}");

    assert_eq!(jobs, 15);
    assert_eq!(tasks, 252);

    assert_eq!(snapshots, 252);
    assert_eq!(absolute_updates, 27);
    assert_eq!(relative_updates, 216);

    assert_eq!(specs, 495);

    println!("RESULT           : PASS");
}
