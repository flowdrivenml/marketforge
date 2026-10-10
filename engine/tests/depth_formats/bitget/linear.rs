use crate::common::{ExpectedDepthLevel, run_depth_format_test, verify_snapshot_quantities};

use marketforge_engine::canonical::BookSide;

use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

#[test]
fn validates_bitget_linear_btcusdt() {
    let result = run_depth_format_test(
        "bitget-perpetual-linear-BTCUSDT-l2-20260901-20260904-d94.json",
        SequencePolicy::Unsequenced,
    );

    assert_eq!(result.records_read, 4294);
    assert_eq!(result.records_processed, 4294);
    assert_eq!(result.records_rejected, 0);

    assert_eq!(result.segment_count, 1);

    assert_eq!(result.boundary.continuity, BoundaryContinuity::Unverifiable,);

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert_eq!(initial.state.bids.len(), 500);
    assert_eq!(initial.state.asks.len(), 500);

    assert_eq!(final_state.state.bids.len(), 500);
    assert_eq!(final_state.state.asks.len(), 500);

    println!("\nBITGET LINEAR BTCUSDT RESULT: PASS");
    println!("\nBITGET BTCUSDT LINEAR QUANTITY NORMALIZATION");

    verify_snapshot_quantities(
        &result.boundary,
        &[
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "78568.9",
                quantity_base: "6.8404",
                quantity_quote: "537442.70356",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "78568.3",
                quantity_base: "0.0771",
                quantity_quote: "6057.61593",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Bid,
                price: "78568.2",
                quantity_base: "0.1746",
                quantity_quote: "13718.00772",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Ask,
                price: "78569.0",
                quantity_base: "0.6702",
                quantity_quote: "52656.94380",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Ask,
                price: "78570.6",
                quantity_base: "0.0001",
                quantity_quote: "7.85706",
                quantity_contracts: None,
                base_tolerance: None,
            },
            ExpectedDepthLevel {
                side: BookSide::Ask,
                price: "78570.8",
                quantity_base: "0.0014",
                quantity_quote: "109.99912",
                quantity_contracts: None,
                base_tolerance: None,
            },
        ],
    );
}
