use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::{ExpectedDepthEvent, run_depth_format_test_with_events};
use crate::common::{ExpectedDepthLevel, verify_snapshot_quantities};
use marketforge_engine::canonical::BookSide;

#[test]
fn validates_gateio_linear_btcusdt() {
    let result = run_depth_format_test_with_events(
        "gateio-perpetual-linear-BTC_USDT-l2-20260901-20260904-d96.json",
        SequencePolicy::Ranged,
        &[
            ExpectedDepthEvent {
                source_ordinal: 44063,
                side: BookSide::Bid,
                price: "78542.1",
                quantity_base: Some("0.2153"),
                quantity_quote: Some("16910.11413"),
                quantity_contracts: Some("2153"),
                base_tolerance: None,
            },
            ExpectedDepthEvent {
                source_ordinal: 44064,
                side: BookSide::Ask,
                price: "79283",
                quantity_base: Some("0.0008"),
                quantity_quote: Some("63.4264"),
                quantity_contracts: Some("8"),
                base_tolerance: None,
            },
        ],
    );

    assert_eq!(result.records_read, 2_705_755);
    assert_eq!(result.records_processed, result.records_read);
    assert_eq!(result.records_rejected, 0);

    assert!(result.levels_written > 0);

    assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified,);

    assert_eq!(result.segment_count, 1);

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    println!("\nGATE.IO LINEAR PERPETUAL RESULT: PASS");
    println!("\nGATE.IO LINEAR QUANTITY NORMALIZATION");

    verify_snapshot_quantities(
        &result.boundary,
        &[
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.0",
                quantity_base: "2.92770",
                quantity_quote: "204939",
                quantity_contracts: Some("29277"),
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.4",
                quantity_base: "0.00040",
                quantity_quote: "28.000160",
                quantity_contracts: Some("4"),
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.9",
                quantity_base: "0.00010",
                quantity_quote: "7.000090",
                quantity_contracts: Some("1"),
                base_tolerance: None,
            },
        ],
    );
}
