use marketforge_engine::canonical::BookSide;
use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::{ExpectedDepthEvent, run_depth_format_test_with_events};
use crate::common::{ExpectedDepthLevel, verify_snapshot_quantities};

#[test]
fn validates_gateio_inverse_btcusd() {
    let result = run_depth_format_test_with_events(
        "gateio-perpetual-inverse-BTC_USD-l2-20260901-20260904-d103.json",
        SequencePolicy::Ranged,
        &[
            ExpectedDepthEvent {
                source_ordinal: 1721,
                side: BookSide::Bid,
                price: "78437.5",
                quantity_base: None,
                quantity_quote: None,
                quantity_contracts: Some("0"),
                base_tolerance: None,
            },
            ExpectedDepthEvent {
                source_ordinal: 1722,
                side: BookSide::Bid,
                price: "78459",
                quantity_base: Some("0.3186377598490931569354694809"),
                quantity_quote: Some("25000"),
                quantity_contracts: Some("25000"),
                base_tolerance: Some("0.000000000000000000000001"),
            },
        ],
    );

    assert_eq!(result.records_read, 310_699);
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

    println!("\nGATE.IO INVERSE PERPETUAL RESULT: PASS");
    println!("\nGATE.IO INVERSE QUANTITY NORMALIZATION");

    verify_snapshot_quantities(
        &result.boundary,
        &[
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70000.0",
                quantity_base: "0.04028571428571428571428571429",
                quantity_quote: "2820",
                quantity_contracts: Some("2820"),
                base_tolerance: Some("0.000000000000000000000001"),
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70017.3",
                quantity_base: "0.001242550055486286960508331512",
                quantity_quote: "87",
                quantity_contracts: Some("87"),
                base_tolerance: Some("0.000000000000000000000001"),
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "70042.2",
                quantity_base: "0.1427710722964155894589261902",
                quantity_quote: "10000",
                quantity_contracts: Some("10000"),
                base_tolerance: Some("0.000000000000000000000001"),
            },
        ],
    );
}
