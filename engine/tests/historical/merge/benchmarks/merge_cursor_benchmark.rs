#![cfg(feature = "historical")]

use std::{path::PathBuf, time::Instant};

use marketforge_engine::historical::merge::DepthEventCursor;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn benchmark(path: &std::path::Path, start_ns: i64, end_ns: i64, indexed: bool) -> (u64, f64) {
    let start = Instant::now();

    let mut cursor = if indexed {
        DepthEventCursor::open_indexed_range(path, start_ns, end_ns)
    } else {
        DepthEventCursor::open_range(path, start_ns, end_ns)
    }
    .expect("open cursor");

    let mut events = 0_u64;

    while cursor.next_event().expect("read event").is_some() {
        events += 1;
    }

    (events, start.elapsed().as_secs_f64())
}

#[test]
#[ignore = "performance benchmark"]
fn compares_indexed_and_sequential_cursors() {
    let root = project_root();

    let datasets = [105, 108, 112, 92];

    let base = 1788220800000000000_i64;

    // Three one-minute intervals within the first hourly archive.
    let intervals = [
        ("beginning", base),
        ("middle", base + 1_800_000_000_000),
        ("end", base + 3_540_000_000_000),
    ];

    println!("\nMARKETFORGE — PARQUET CURSOR BENCHMARK");
    println!("{}", "=".repeat(90));

    for dataset_id in datasets {
        let path = root.join(format!(
            "data/processed/{dataset_id}/tasks/task-000000/depth/events.parquet"
        ));

        println!("\nDATASET {dataset_id}");

        for (label, start_ns) in intervals {
            let end_ns = start_ns + 60_000_000_000;

            let (sequential_count, sequential_time) = benchmark(&path, start_ns, end_ns, false);

            let (indexed_count, indexed_time) = benchmark(&path, start_ns, end_ns, true);

            assert_eq!(sequential_count, indexed_count);

            let speedup = if indexed_time > 0.0 {
                sequential_time / indexed_time
            } else {
                0.0
            };

            println!(
                "{label:10} | events={sequential_count:8} | \
                 sequential={sequential_time:8.4}s | \
                 indexed={indexed_time:8.4}s | \
                 speedup={speedup:6.2}x"
            );
        }
    }
}
