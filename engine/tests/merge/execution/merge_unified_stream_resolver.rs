#![cfg(feature = "process")]

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::process::merge::{
    ResolvedMergeInput, UnifiedMergeStreamKind, resolve_unified_merge_streams,
};

#[path = "../../common/mod.rs"]
mod common;

use common::datasets::{project_root, resolve_depth_dataset, resolve_trade_dataset};

// -----------------------------------------------------------------------------
// Temporary project root
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-stream-resolver-{}-{timestamp}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// -----------------------------------------------------------------------------
// Resolve test datasets
// -----------------------------------------------------------------------------

fn resolve_inputs(root: &Path, trade_ids: &[u64], depth_ids: &[u64]) -> Vec<ResolvedMergeInput> {
    trade_ids
        .iter()
        .map(|&id| resolve_trade_dataset(root, id))
        .chain(depth_ids.iter().map(|&id| resolve_depth_dataset(root, id)))
        .collect()
}

// -----------------------------------------------------------------------------
// Real trade metadata
// -----------------------------------------------------------------------------

#[test]
fn resolves_real_trade_streams() {
    let root = project_root();

    let dataset_ids = [111_u64, 109, 115, 100, 97, 99];

    let inputs = resolve_inputs(&root, &dataset_ids, &[]);

    let streams =
        resolve_unified_merge_streams(&inputs, &root).expect("resolve trade stream metadata");

    assert_eq!(streams.len(), dataset_ids.len());

    let mut seen_stream_ids = HashSet::new();

    for (rank, stream) in streams.iter().enumerate() {
        assert_eq!(stream.stream_rank as usize, rank);
        assert_eq!(stream.dataset_id, dataset_ids[rank]);

        assert_eq!(stream.kind, UnifiedMergeStreamKind::Trade);

        assert!(!stream.exchange.is_empty());
        assert!(!stream.symbol.is_empty());
        assert!(!stream.stream_id.is_empty());

        assert!(seen_stream_ids.insert(&stream.stream_id));

        // Currency metadata is not currently available
        // in the canonical processing protocol.
        assert!(stream.base_currency.is_none());
        assert!(stream.quote_currency.is_none());

        println!(
            "dataset={} | exchange={} | symbol={} | stream={} | MATCH",
            stream.dataset_id, stream.exchange, stream.symbol, stream.stream_id
        );
    }

    println!("\nTRADE STREAM METADATA: PASS");
}

// -----------------------------------------------------------------------------
// Real depth metadata
// -----------------------------------------------------------------------------

#[test]
fn resolves_real_depth_streams() {
    let root = project_root();

    let dataset_ids = [105_u64, 108, 112, 92];

    let inputs = resolve_inputs(&root, &[], &dataset_ids);

    let streams =
        resolve_unified_merge_streams(&inputs, &root).expect("resolve depth stream metadata");

    assert_eq!(streams.len(), dataset_ids.len());

    for (rank, stream) in streams.iter().enumerate() {
        assert_eq!(stream.stream_rank as usize, rank);
        assert_eq!(stream.dataset_id, dataset_ids[rank]);

        assert_eq!(stream.kind, UnifiedMergeStreamKind::Depth);

        assert!(!stream.exchange.is_empty());
        assert!(!stream.symbol.is_empty());
        assert!(!stream.stream_id.is_empty());

        println!(
            "dataset={} | exchange={} | symbol={} | MATCH",
            stream.dataset_id, stream.exchange, stream.symbol
        );
    }

    println!("\nDEPTH STREAM METADATA: PASS");
}

// -----------------------------------------------------------------------------
// Mixed stream ordering
// -----------------------------------------------------------------------------

#[test]
fn preserves_mixed_dataset_order() {
    let root = project_root();

    let dataset_order = [111_u64, 105, 109, 112];

    let inputs = vec![
        resolve_trade_dataset(&root, 111),
        resolve_depth_dataset(&root, 105),
        resolve_trade_dataset(&root, 109),
        resolve_depth_dataset(&root, 112),
    ];

    let streams =
        resolve_unified_merge_streams(&inputs, &root).expect("resolve mixed stream metadata");

    for (rank, stream) in streams.iter().enumerate() {
        assert_eq!(stream.stream_rank as usize, rank);
        assert_eq!(stream.dataset_id, dataset_order[rank]);
    }

    assert_eq!(streams[0].kind, UnifiedMergeStreamKind::Trade);
    assert_eq!(streams[1].kind, UnifiedMergeStreamKind::Depth);
    assert_eq!(streams[2].kind, UnifiedMergeStreamKind::Trade);
    assert_eq!(streams[3].kind, UnifiedMergeStreamKind::Depth);

    println!("\nMIXED STREAM ORDERING: PASS");
}

// -----------------------------------------------------------------------------
// Duplicate dataset rejection
// -----------------------------------------------------------------------------

#[test]
fn rejects_duplicate_dataset_ids() {
    let root = project_root();

    let input = resolve_trade_dataset(&root, 111);

    let inputs = vec![input.clone(), input];

    assert!(resolve_unified_merge_streams(&inputs, &root).is_err());
}

// -----------------------------------------------------------------------------
// Missing processing job
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_processing_job() {
    let root = project_root();
    let temporary = TestDirectory::new();

    fs::create_dir_all(temporary.path().join("data/.jobs/process")).unwrap();

    let input = resolve_trade_dataset(&root, 111);

    assert!(resolve_unified_merge_streams(&[input], temporary.path(),).is_err());
}

// -----------------------------------------------------------------------------
// Processing job task mismatch
// -----------------------------------------------------------------------------

#[test]
fn rejects_processing_job_task_mismatch() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let jobs = temporary.path().join("data/.jobs/process");

    fs::create_dir_all(&jobs).unwrap();

    let original =
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-trade-20260901-20261001-d111.json");

    let destination = jobs.join("test-d111.json");

    let mut job: serde_json::Value = serde_json::from_slice(&fs::read(original).unwrap()).unwrap();

    // Replace the real task ID with an incorrect one.
    job["tasks"][0]["task_id"] = serde_json::json!(999_999);

    fs::write(&destination, serde_json::to_vec_pretty(&job).unwrap()).unwrap();

    let input = resolve_trade_dataset(&root, 111);

    assert!(resolve_unified_merge_streams(&[input], temporary.path(),).is_err());
}

// -----------------------------------------------------------------------------
// Inconsistent instrument metadata
// -----------------------------------------------------------------------------

#[test]
fn rejects_inconsistent_instrument_metadata() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let jobs = temporary.path().join("data/.jobs/process");

    fs::create_dir_all(&jobs).unwrap();

    let original =
        root.join("data/.jobs/process/bitget-spot-spot-BTCUSDT-trade-20260901-20260904-d109.json");

    let destination = jobs.join("test-d109.json");

    let mut job: serde_json::Value = serde_json::from_slice(&fs::read(original).unwrap()).unwrap();

    assert!(
        job["tasks"].as_array().unwrap().len() > 1,
        "test requires a multi-task processing job"
    );

    job["tasks"][1]["symbol"] = serde_json::json!("INCORRECT_SYMBOL");

    fs::write(&destination, serde_json::to_vec_pretty(&job).unwrap()).unwrap();

    let input = resolve_trade_dataset(&root, 109);

    assert!(resolve_unified_merge_streams(&[input], temporary.path(),).is_err());
}

// -----------------------------------------------------------------------------
// Dataset identity mismatch
// -----------------------------------------------------------------------------

#[test]
fn rejects_processing_job_dataset_mismatch() {
    let root = project_root();
    let temporary = TestDirectory::new();

    let jobs = temporary.path().join("data/.jobs/process");

    fs::create_dir_all(&jobs).unwrap();

    let original =
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-trade-20260901-20261001-d111.json");

    let destination = jobs.join("test-d111.json");

    let mut job: serde_json::Value = serde_json::from_slice(&fs::read(original).unwrap()).unwrap();

    job["dataset_id"] = serde_json::json!(999_999);

    fs::write(&destination, serde_json::to_vec_pretty(&job).unwrap()).unwrap();

    let input = resolve_trade_dataset(&root, 111);

    assert!(resolve_unified_merge_streams(&[input], temporary.path(),).is_err());
}
