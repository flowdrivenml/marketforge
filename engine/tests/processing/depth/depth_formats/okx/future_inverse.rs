use marketforge_engine::book::SequencePolicy;
use rust_decimal::Decimal;

use crate::common::run_depth_format_test;

#[test]
fn validates_okx_future_inverse_btcusd() {
    let result = run_depth_format_test(
        "okx-future-inverse-BTC-USD-261225-l2-20260901-20260904-d123.json",
        SequencePolicy::Unsequenced,
    );

    // -------------------------------------------------------------------------
    // Source processing
    // -------------------------------------------------------------------------

    assert!(result.records_read > 0);
    assert_eq!(result.records_read, result.records_processed);
    assert_eq!(result.records_rejected, 0);

    assert!(result.levels_written > 0);

    // -------------------------------------------------------------------------
    // Sequence metadata
    // -------------------------------------------------------------------------

    assert_eq!(result.boundary.first_sequence, None);
    assert_eq!(result.boundary.last_sequence, None);

    // -------------------------------------------------------------------------
    // Reconstruction
    // -------------------------------------------------------------------------

    assert_eq!(result.segment_count, 1);

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    // -------------------------------------------------------------------------
    // Inverse contract normalization
    // -------------------------------------------------------------------------

    let contract_value = Decimal::from(100);

    for level in initial.state.bids.iter().chain(initial.state.asks.iter()) {
        let contracts = level.quantity_contracts.expect("missing contract quantity");

        let quote = level.quantity_quote.expect("missing quote quantity");

        let base = level.quantity_base.expect("missing base quantity");

        assert_eq!(
            quote,
            contracts * contract_value,
            "incorrect inverse contract-to-quote conversion"
        );

        assert_eq!(
            base,
            quote / level.price,
            "incorrect inverse quote-to-base conversion"
        );

        assert!(
            level.order_count.is_some(),
            "OKX futures must preserve order counts"
        );
    }

    println!("\nOKX INVERSE FUTURES NORMALIZATION: VERIFIED");
    println!("OKX INVERSE FUTURES RESULT: PASS");
}
