use marketforge_engine::book::SequencePolicy;

use crate::common::run_depth_format_test;

#[test]
fn validates_okx_spot_btcusdt() {
    let result = run_depth_format_test(
        "okx-spot-spot-BTC-USDT-l2-20260901-20260904-d108.json",
        SequencePolicy::Unsequenced,
    );

    // ---------------------------------------------------------
    // Source records
    // ---------------------------------------------------------

    assert_eq!(result.records_read, 86_398);
    assert_eq!(result.records_processed, 86_398);
    assert_eq!(result.records_rejected, 0);

    assert!(result.levels_written > 0);

    // ---------------------------------------------------------
    // Sequence metadata
    // ---------------------------------------------------------

    assert_eq!(result.boundary.first_sequence, None);
    assert_eq!(result.boundary.last_sequence, None);

    // ---------------------------------------------------------
    // Boundary states
    // ---------------------------------------------------------

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert_eq!(
        result.segment_count, 1,
        "continuous OKX archive should require one reconstruction segment"
    );

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    // ---------------------------------------------------------
    // Order counts
    // ---------------------------------------------------------

    assert!(
        initial
            .state
            .bids
            .iter()
            .any(|level| level.quantity_base.is_some()),
        "OKX must preserve base quantities"
    );

    assert!(
        initial
            .state
            .bids
            .iter()
            .any(|level| level.order_count.is_some()),
        "OKX must preserve order counts"
    );

    println!("\nOKX SPOT BTC-USDT RESULT: PASS");
}
