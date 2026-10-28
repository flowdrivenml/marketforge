use {
    marketforge_engine::historical::merge::MergeBoundarySummary,
    marketforge_engine::historical::normalize::boundary::DepthBoundaryManifest,
    marketforge_engine::shared::domain::canonical::Exchange,
};

#[test]
fn lightweight_boundary_matches_full_manifest() {
    let full = DepthBoundaryManifest::new(
        Exchange::Bybit,
        100,
        "BTCUSDT".to_owned(),
        "bybit:BTCUSDT:depth".to_owned(),
    );

    let json = serde_json::to_vec(&full).unwrap();

    let decoded_full: DepthBoundaryManifest = serde_json::from_slice(&json).unwrap();

    let decoded_summary: MergeBoundarySummary = serde_json::from_slice(&json).unwrap();

    let expected = MergeBoundarySummary::from(&decoded_full);

    assert_eq!(decoded_summary, expected);
}

#[test]
fn lightweight_boundary_preserves_snapshot_metadata() {
    let json = serde_json::json!({
        "version": 1,
        "exchange": "bybit",
        "instrument_id": 100,
        "symbol": "BTCUSDT",
        "stream_id": "bybit:BTCUSDT:depth",
        "first_sequence": 100,
        "last_sequence": 200,
        "first_timestamp_ns": 1000,
        "last_timestamp_ns": 2000,
        "initial": {
            "sequence": 100,
            "timestamp_ns": 1000,
            "coverage": {
                "depth_per_side": 500,
                "bid_levels": 2,
                "ask_levels": 2
            },
            "state": {
                "bids": [],
                "asks": []
            },
            "fingerprint": "initial-fingerprint"
        },
        "final_state": {
            "sequence": 200,
            "timestamp_ns": 2000,
            "coverage": {
                "depth_per_side": 500,
                "bid_levels": 2,
                "ask_levels": 2
            },
            "state": {
                "bids": [],
                "asks": []
            },
            "fingerprint": "final-fingerprint"
        },
        "continuity": "verified"
    });

    let summary: MergeBoundarySummary = serde_json::from_value(json).unwrap();

    assert_eq!(summary.first_sequence, Some(100));
    assert_eq!(summary.last_sequence, Some(200));

    assert_eq!(
        summary.initial.as_ref().unwrap().fingerprint,
        "initial-fingerprint"
    );

    assert_eq!(
        summary.final_state.as_ref().unwrap().fingerprint,
        "final-fingerprint"
    );

    assert_eq!(summary.initial.as_ref().unwrap().coverage.bid_levels, 2);
}
