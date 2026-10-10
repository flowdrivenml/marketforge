use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::run_depth_format_test;

#[test]
fn validates_bybit_inverse_btcusd() {
    let result = run_depth_format_test(
        "bybit-perpetual-inverse-BTCUSD-l2-20260901-20260904-d98.json",
        SequencePolicy::Consecutive,
    );

    // ---------------------------------------------------------
    // Source records
    // ---------------------------------------------------------

    assert_eq!(result.records_read, 851_828);
    assert_eq!(result.records_processed, 851_828);
    assert_eq!(result.records_rejected, 0);

    assert!(result.levels_written > 0);

    // ---------------------------------------------------------
    // Sequence continuity
    // ---------------------------------------------------------

    assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified);

    assert_eq!(result.boundary.first_sequence, Some(32_653_621));

    assert_eq!(result.boundary.last_sequence, Some(33_505_447));

    // ---------------------------------------------------------
    // Boundary states
    // ---------------------------------------------------------

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert_eq!(initial.state.bids.len(), 200);
    assert_eq!(initial.state.asks.len(), 200);

    assert_eq!(final_state.state.bids.len(), 200);
    assert_eq!(final_state.state.asks.len(), 200);

    // ---------------------------------------------------------
    // Quantity representation
    // ---------------------------------------------------------

    let first_bid = &initial.state.bids[0];

    assert!(
        first_bid.quantity_quote.is_some(),
        "inverse depth must preserve quote-denominated quantity"
    );

    println!("\nBYBIT INVERSE QUANTITY");
    println!("  Price          : {}", first_bid.price);
    println!("  Quantity base  : {:?}", first_bid.quantity_base);
    println!("  Quantity quote : {:?}", first_bid.quantity_quote);
    println!("  Contracts      : {:?}", first_bid.quantity_contracts);

    println!("\nBYBIT INVERSE RESULT: PASS");
}
