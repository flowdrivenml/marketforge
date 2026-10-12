use marketforge_engine::book::SequencePolicy;
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::common::run_depth_format_test;

#[test]
fn validates_okx_future_linear_btcusd() {
    let result = run_depth_format_test(
        "okx-future-linear-BTC-USD_UM-261225-l2-20260901-20260904-d121.json",
        SequencePolicy::Unsequenced,
    );

    // Source processing
    assert!(result.records_read > 0);
    assert_eq!(result.records_read, result.records_processed);
    assert_eq!(result.records_rejected, 0);

    // Canonical output
    assert!(result.levels_written > 0);

    // Reconstruction
    assert_eq!(result.segment_count, 1);

    // No source sequence identifiers
    assert_eq!(result.boundary.first_sequence, None);
    assert_eq!(result.boundary.last_sequence, None);

    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    // Contract normalization: 0.01 BTC per contract
    let contract_value = Decimal::from_str("0.01").unwrap();

    for level in initial.state.bids.iter().chain(initial.state.asks.iter()) {
        let contracts = level.quantity_contracts.unwrap();
        let base = level.quantity_base.unwrap();
        let quote = level.quantity_quote.unwrap();

        assert_eq!(
            base,
            contracts * contract_value,
            "incorrect contract-to-base conversion"
        );

        assert_eq!(
            quote,
            base * level.price,
            "incorrect base-to-quote conversion"
        );

        assert!(
            level.order_count.is_some(),
            "OKX futures must preserve order counts"
        );
    }

    println!("\nOKX LINEAR FUTURES NORMALIZATION: VERIFIED");
    println!("OKX LINEAR FUTURES RESULT: PASS");
}
