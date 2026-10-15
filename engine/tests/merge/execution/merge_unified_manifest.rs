#![cfg(feature = "historical")]

use marketforge_engine::{
    job::MergeTimeRange,
    process::merge::{
        MergeInputDataset, UNIFIED_MERGE_MANIFEST_VERSION, UNIFIED_TIMELINE_VERSION,
        UnifiedMergeManifest, UnifiedMergeMode, UnifiedMergeStream, UnifiedMergeStreamKind,
    },
};

fn stream(rank: u32, dataset_id: u64, kind: UnifiedMergeStreamKind) -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: rank,
        dataset_id,
        kind,

        exchange: "binance".to_owned(),
        instrument_id: 1,
        symbol: "BTCUSDT".to_owned(),
        stream_id: format!("stream-{dataset_id}"),

        base_currency: Some("BTC".to_owned()),
        quote_currency: Some("USDT".to_owned()),
    }
}

fn input(dataset_id: u64) -> MergeInputDataset {
    MergeInputDataset {
        dataset_id,
        dataset_path: format!("/tmp/dataset-{dataset_id}"),
        manifest_version: 1,
    }
}

fn manifest(mode: UnifiedMergeMode, streams: Vec<UnifiedMergeStream>) -> UnifiedMergeManifest {
    let inputs = streams
        .iter()
        .map(|stream| input(stream.dataset_id))
        .collect();

    UnifiedMergeManifest {
        protocol_version: UNIFIED_MERGE_MANIFEST_VERSION,
        timeline_schema_version: UNIFIED_TIMELINE_VERSION,

        job_id: 900_200,
        mode,

        input_datasets: inputs,

        time_range: MergeTimeRange {
            start_ns: 100,
            end_ns: 200,
        },

        events_written: 0,
        trade_events: 0,
        depth_events: 0,
        files_written: 0,

        start_timestamp_ns: None,
        end_timestamp_ns: None,

        files: Vec::new(),
        streams,

        boundary_links: Vec::new(),
        skipped_initializations: Vec::new(),

        checkpoints: Vec::new(),
        initial_checkpoints: Vec::new(),
    }
}

// -----------------------------------------------------------------------------
// Valid manifests
// -----------------------------------------------------------------------------

#[test]
fn accepts_trade_only_manifest() {
    let manifest = manifest(
        UnifiedMergeMode::Trades,
        vec![stream(0, 111, UnifiedMergeStreamKind::Trade)],
    );

    manifest.validate().unwrap();
}

#[test]
fn accepts_depth_only_manifest() {
    let manifest = manifest(
        UnifiedMergeMode::Depth,
        vec![stream(0, 105, UnifiedMergeStreamKind::Depth)],
    );

    manifest.validate().unwrap();
}

#[test]
fn accepts_combined_manifest() {
    let manifest = manifest(
        UnifiedMergeMode::Combined,
        vec![
            stream(0, 111, UnifiedMergeStreamKind::Trade),
            stream(1, 105, UnifiedMergeStreamKind::Depth),
        ],
    );

    manifest.validate().unwrap();
}

// -----------------------------------------------------------------------------
// Invalid manifests
// -----------------------------------------------------------------------------

#[test]
fn rejects_duplicate_dataset_ids() {
    let manifest = manifest(
        UnifiedMergeMode::Trades,
        vec![
            stream(0, 111, UnifiedMergeStreamKind::Trade),
            stream(1, 111, UnifiedMergeStreamKind::Trade),
        ],
    );

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_incorrect_merge_mode() {
    let manifest = manifest(
        UnifiedMergeMode::Trades,
        vec![
            stream(0, 111, UnifiedMergeStreamKind::Trade),
            stream(1, 105, UnifiedMergeStreamKind::Depth),
        ],
    );

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_combined_mode_without_depth() {
    let manifest = manifest(
        UnifiedMergeMode::Combined,
        vec![stream(0, 111, UnifiedMergeStreamKind::Trade)],
    );

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_invalid_stream_rank() {
    let manifest = manifest(
        UnifiedMergeMode::Trades,
        vec![stream(5, 111, UnifiedMergeStreamKind::Trade)],
    );

    assert!(manifest.validate().is_err());
}

#[test]
fn rejects_event_count_mismatch() {
    let mut manifest = manifest(
        UnifiedMergeMode::Trades,
        vec![stream(0, 111, UnifiedMergeStreamKind::Trade)],
    );

    manifest.events_written = 100;
    manifest.trade_events = 99;

    assert!(manifest.validate().is_err());
}
