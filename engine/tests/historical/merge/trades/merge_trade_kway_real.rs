#![cfg(feature = "historical")]
use crate::publication::trade_publication;

use {
    marketforge_engine::historical::merge::KWayEventMerger,
    marketforge_engine::historical::merge::MergeEventKind,
    marketforge_engine::historical::merge::MergeEventSource,
    marketforge_engine::historical::merge::TradeStream,
    marketforge_engine::historical::merge::build_trade_inventory,
};

// -----------------------------------------------------------------------------
// Resolve real trade dataset
// -----------------------------------------------------------------------------

#[test]
fn merges_generated_trade_datasets() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let dataset_ids = [
        111_u64, // Gate.io
        109,     // Bitget
        115,     // Binance
        100,     // OKX
        97,      // Bybit
        99,      // Binance
    ];

    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::new();
    let mut expected_counts = Vec::new();

    for dataset_id in dataset_ids {
        let input = trade_publication(&root, dataset_id);

        expected_counts.push(input.manifest.events_written);

        let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

        let stream = TradeStream::new(dataset_id, inventory).expect("create trade stream");

        streams.push(Box::new(stream));
    }

    let mut merger = KWayEventMerger::new(streams).expect("construct K-way merger");

    let mut actual_counts = vec![0_u64; dataset_ids.len()];
    let mut previous_key = None;

    let mut total = 0_u64;

    println!("\nMARKETFORGE — REAL MULTI-EXCHANGE TRADE MERGE");
    println!("{}", "=".repeat(90));

    while let Some(merged) = merger.next_event().expect("merge trade event") {
        assert_eq!(merged.event.kind(), MergeEventKind::Trade);

        let rank = merged.stream_rank as usize;

        assert!(rank < dataset_ids.len());

        assert_eq!(
            merged.event.dataset_id(),
            dataset_ids[rank],
            "incorrect dataset identity"
        );

        let key = merged.event.merge_key(merged.stream_rank);

        if let Some(previous) = previous_key {
            assert!(key >= previous, "global merge ordering regression");
        }

        previous_key = Some(key);

        actual_counts[rank] += 1;
        total += 1;
    }

    for (index, dataset_id) in dataset_ids.iter().enumerate() {
        assert_eq!(
            actual_counts[index], expected_counts[index],
            "dataset {dataset_id}: missing or duplicated events"
        );

        println!(
            "dataset={} | events={} | MATCH",
            dataset_id, actual_counts[index],
        );
    }

    assert_eq!(merger.events_emitted(), total);

    assert_eq!(total, 24);

    println!("{}", "=".repeat(90));
    println!("Streams       : {}", dataset_ids.len());
    println!("Merged events : {total}");
    println!("Global order  : VERIFIED");
    println!("Event counts  : VERIFIED");
    println!("Result        : PASS");
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn merges_two_bounded_recorded_trade_streams() {
    crate::recorded_references::check_kway_trade_references();
}
