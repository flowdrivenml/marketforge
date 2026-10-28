#![cfg(feature = "historical")]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use marketforge_engine::historical::merge::TradeCursor;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn find_trade_file(root: &Path, dataset_id: u64) -> PathBuf {
    let datasets = root.join("data/.work/full-trade-test/datasets");

    for entry in std::fs::read_dir(&datasets).expect("read trade datasets") {
        let entry = entry.expect("read dataset entry");

        let manifest_path = entry.path().join("manifest.json");

        if !manifest_path.is_file() {
            continue;
        }

        let manifest: serde_json::Value =
            serde_json::from_reader(File::open(&manifest_path).unwrap()).unwrap();

        if manifest["dataset_id"].as_u64() != Some(dataset_id) {
            continue;
        }

        let relative = manifest["files"][0]["path"]
            .as_str()
            .expect("trade file path");

        return entry.path().join(relative);
    }

    panic!("trade dataset {dataset_id} not found");
}

fn compare_range(path: &Path, start_ns: i64, end_ns: i64) -> u64 {
    let mut sequential = TradeCursor::open(path).expect("open sequential trade cursor");

    let mut indexed =
        TradeCursor::open_indexed_range(path, start_ns, end_ns).expect("open indexed trade cursor");

    let mut expected = Vec::new();

    while let Some(trade) = sequential.next_trade().expect("read sequential trade") {
        if trade.event_timestamp_ns >= start_ns && trade.event_timestamp_ns < end_ns {
            expected.push(trade);
        }
    }

    let mut count = 0_usize;

    while let Some(actual) = indexed.next_trade().expect("read indexed trade") {
        assert_eq!(
            actual, expected[count],
            "indexed trade mismatch at row {count}"
        );

        count += 1;
    }

    assert_eq!(
        count,
        expected.len(),
        "indexed cursor returned incorrect event count"
    );

    count as u64
}

#[test]
fn indexed_trade_cursor_matches_sequential() {
    let root = project_root();

    let datasets = [111_u64, 109, 115, 100, 97, 99];

    let base = 1788220800000000000_i64;

    let intervals = [
        ("beginning", base, base + 60_000_000_000),
        ("middle", base + 1_800_000_000_000, base + 1_860_000_000_000),
        ("end", base + 3_540_000_000_000, base + 3_600_000_000_000),
    ];

    println!("\nMARKETFORGE — INDEXED TRADE CURSOR");
    println!("{}", "=".repeat(90));

    for dataset_id in datasets {
        let path = find_trade_file(&root, dataset_id);

        for (label, start, end) in intervals {
            let count = compare_range(&path, start, end);

            println!(
                "dataset={} | interval={} | events={} | MATCH",
                dataset_id, label, count,
            );
        }
    }

    println!("{}", "=".repeat(90));
    println!("INDEXED TRADE CURSOR: PASS");
}
