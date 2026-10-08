#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    job::{ParquetResourceConfig, load_processing_job},
    process::{
        manifest::{DATASET_MANIFEST_FILENAME, DATASET_MANIFEST_VERSION, DatasetManifest},
        parquet::ParquetTradeWriter,
        worker::TradeSink,
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn job_path() -> PathBuf {
    project_root().join("data/.jobs/process").join(JOB_FILE)
}

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-manifest-test-{}-{id}",
        std::process::id()
    ));

    fs::create_dir_all(&directory).unwrap();

    directory
}

fn sample_trade(index: i64) -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_788_220_800_000_000_000 + index,
            system_timestamp_ns: None,
            exchange: Exchange::Okx,
            instrument_id: 120,
            symbol: "BTC-USD_UM-261225".to_owned(),
            stream_id: "okx:BTC-USD_UM-261225:trades".to_owned(),
        },

        trade_id: Some(format!("trade-{index}")),
        sequence: None,

        side: TradeSide::Buy,
        price: "80569.12345678".parse().unwrap(),

        quantity_base: Some("0.0036".parse().unwrap()),
        quantity_quote: Some("290.048845".parse().unwrap()),
        quantity_contracts: Some("0.36".parse().unwrap()),

        is_rpi: Some(false),

        trade_iv: None,
        mark_iv: None,
        index_price: None,
        mark_price: None,
    }
}

#[test]
fn creates_valid_dataset_manifest() {
    let directory = test_directory();

    let trades_dir = directory.join("trades");

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&trades_dir, resources).unwrap();

    for index in 0..100 {
        writer.write_trade(sample_trade(index)).unwrap();
    }

    writer.finish().unwrap();

    let job = load_processing_job(job_path()).unwrap();

    let manifest =
        DatasetManifest::from_processing_job(&job, &directory).expect("build dataset manifest");

    assert_eq!(manifest.protocol_version, DATASET_MANIFEST_VERSION);
    assert_eq!(manifest.job_id, job.job_id.0);
    assert_eq!(manifest.dataset_id, job.dataset_id.0);

    assert_eq!(manifest.events_written, 100);
    assert_eq!(manifest.files_written, manifest.files.len() as u64);

    assert_eq!(manifest.start_timestamp_ns, Some(1_788_220_800_000_000_000));

    assert_eq!(manifest.end_timestamp_ns, Some(1_788_220_800_000_000_099));

    assert_eq!(manifest.source_tasks.len(), job.tasks.len());

    assert!(!manifest.files.is_empty());

    for file in &manifest.files {
        assert!(file.path.starts_with("trades/"));
        assert!(file.path.ends_with(".parquet"));
        assert!(file.rows > 0);
        assert!(file.size_bytes > 0);

        assert!(directory.join(&file.path).is_file());

        assert!(file.start_timestamp_ns.is_some());
        assert!(file.end_timestamp_ns.is_some());
    }

    let manifest_path = manifest.write_to(&directory).unwrap();

    assert_eq!(manifest_path, directory.join(DATASET_MANIFEST_FILENAME));

    let contents = fs::read_to_string(&manifest_path).unwrap();

    let restored: DatasetManifest = serde_json::from_str(&contents).unwrap();

    assert_eq!(restored, manifest);

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — DATASET MANIFEST TEST");
    println!("{}", "=".repeat(80));

    println!("Dataset ID       : {}", manifest.dataset_id);
    println!("Source tasks     : {}", manifest.source_tasks.len());
    println!("Parquet files    : {}", manifest.files_written);
    println!("Canonical trades : {}", manifest.events_written);

    println!(
        "Timestamp range  : {:?} → {:?}",
        manifest.start_timestamp_ns, manifest.end_timestamp_ns
    );

    println!("Manifest         : {}", manifest_path.display());
    println!("RESULT           : PASS");

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_duplicate_manifest() {
    let directory = test_directory();

    let trades_dir = directory.join("trades");

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&trades_dir, resources).unwrap();

    writer.write_trade(sample_trade(0)).unwrap();
    writer.finish().unwrap();

    let job = load_processing_job(job_path()).unwrap();

    let manifest = DatasetManifest::from_processing_job(&job, &directory).unwrap();

    manifest.write_to(&directory).unwrap();

    assert!(manifest.write_to(&directory).is_err());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_missing_parquet_directory() {
    let directory = test_directory();

    let job = load_processing_job(job_path()).unwrap();

    let result = DatasetManifest::from_processing_job(&job, &directory);

    assert!(result.is_err());

    fs::remove_dir_all(directory).unwrap();
}
