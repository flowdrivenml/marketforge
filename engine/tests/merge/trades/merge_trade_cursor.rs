#![cfg(feature = "process")]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use marketforge_engine::process::merge::TradeCursor;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// -----------------------------------------------------------------------------
// Project root
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Find real trade file
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Test sequential cursor
// -----------------------------------------------------------------------------

#[test]
fn reads_real_trade_rows_with_stable_offsets() {
    let root = project_root();

    let datasets = [
        111_u64, // Gate.io
        109,     // Bitget
        115,     // Binance
        100,     // OKX
        97,      // Bybit
        99,      // Binance
    ];

    println!("\nMARKETFORGE — SEQUENTIAL TRADE CURSOR");
    println!("{}", "=".repeat(90));

    for dataset_id in datasets {
        let path = find_trade_file(&root, dataset_id);

        let file = File::open(&path).expect("open Parquet metadata");

        let metadata =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet metadata");

        let expected_rows =
            u64::try_from(metadata.metadata().file_metadata().num_rows()).expect("valid row count");

        let mut cursor = TradeCursor::open(&path).expect("open trade cursor");

        let mut count = 0_u64;
        let mut previous_timestamp = None;

        while let Some(trade) = cursor.next_trade().expect("read trade") {
            assert_eq!(trade.file_row_index, count, "incorrect canonical row index");

            if let Some(previous) = previous_timestamp {
                assert!(
                    trade.event_timestamp_ns >= previous,
                    "trade timestamp regression"
                );
            }

            previous_timestamp = Some(trade.event_timestamp_ns);

            count += 1;
        }

        assert_eq!(count, expected_rows, "trade row count mismatch");

        assert_eq!(cursor.rows_read(), expected_rows);

        println!(
            "dataset={} | rows={} | ordering=PASS | offsets=PASS",
            dataset_id, count,
        );
    }

    println!("{}", "=".repeat(90));
    println!("TRADE CURSOR: PASS");
}
