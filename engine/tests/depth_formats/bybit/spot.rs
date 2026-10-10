use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::run_depth_format_test;

#[test]
fn validates_bybit_spot_ethusdc() {
    let result = run_depth_format_test(
        "bybit-spot-spot-ETHUSDC-l2-20260901-20260904-d114.json",
        SequencePolicy::Consecutive,
    );

    // All source records must be processed.
    assert_eq!(result.records_rejected, 0);
    assert_eq!(result.records_read, result.records_processed);

    // Canonical depth must contain actual updates.
    assert!(result.levels_written > 0);

    // Sequence continuity.
    assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified);

    // Initial and final order books.
    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert_eq!(initial.state.bids.len(), 200);
    assert_eq!(initial.state.asks.len(), 200);

    assert_eq!(final_state.state.bids.len(), 200);
    assert_eq!(final_state.state.asks.len(), 200);

    // Snapshot coverage.
    assert_eq!(initial.coverage.depth_per_side, Some(200));
    assert_eq!(final_state.coverage.depth_per_side, Some(200));

    println!("\nBYBIT SPOT ETHUSDC RESULT: PASS");
}
