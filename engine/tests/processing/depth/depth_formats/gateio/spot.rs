use marketforge_engine::canonical::BookSide;
use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::{ExpectedDepthEvent, run_depth_format_test_with_events};
use crate::common::{ExpectedDepthLevel, verify_snapshot_quantities};

#[test]
fn validates_gateio_spot_btcusdt() {
    let result = run_depth_format_test_with_events(
        "gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json",
        SequencePolicy::Ranged,
        &[
            ExpectedDepthEvent {
                source_ordinal: 53924,
                side: BookSide::Bid,
                price: "78582.8",
                quantity_base: Some("0.003844"),
                quantity_quote: Some("302.0722832"),
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthEvent {
                source_ordinal: 53925,
                side: BookSide::Ask,
                price: "78600.7",
                quantity_base: Some("0.021934"),
                quantity_quote: Some("1724.0277538"),
                quantity_contracts: None,
                base_tolerance: None,
            },
        ],
    );

    assert_eq!(result.records_read, 668_973);
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

    println!("\nGATE.IO SPOT BTC_USDT RESULT: PASS");
    println!("\nGATE.IO SPOT QUANTITY NORMALIZATION");

    verify_snapshot_quantities(
        &result.boundary,
        &[
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.0",
                quantity_base: "3.758773",
                quantity_quote: "263114.11",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.3",
                quantity_base: "0.000076",
                quantity_quote: "5.3200228",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70001.0",
                quantity_base: "0.005471",
                quantity_quote: "382.975471",
                quantity_contracts: None,
                base_tolerance: None,
            },
        ],
    );
}
