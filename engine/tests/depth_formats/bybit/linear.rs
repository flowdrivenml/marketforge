use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

use crate::common::run_depth_format_test;

#[test]
fn validates_bybit_linear_btcusdt() {
    let result = run_depth_format_test(
        "bybit-perpetual-linear-BTCUSDT-l2-20260901-20260904-d89.json",
        SequencePolicy::Consecutive,
    );

    assert_eq!(result.records_read, 863_932);
    assert_eq!(result.records_processed, 863_932);
    assert_eq!(result.records_rejected, 0);

    assert!(result.levels_written > 0);

    assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified);

    assert_eq!(result.boundary.first_sequence, Some(34_096_958));

    assert_eq!(result.boundary.last_sequence, Some(34_960_888));

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert_eq!(initial.state.bids.len(), 200);
    assert_eq!(initial.state.asks.len(), 200);

    assert_eq!(final_state.state.bids.len(), 200);
    assert_eq!(final_state.state.asks.len(), 200);
}
