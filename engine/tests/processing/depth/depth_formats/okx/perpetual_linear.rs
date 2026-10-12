use crate::common::run_depth_format_test;
use marketforge_engine::book::SequencePolicy;
use rust_decimal::Decimal;
use std::str::FromStr;

#[test]
fn validates_okx_perpetual_linear_btcusdt() {
    let result = run_depth_format_test(
        "okx-perpetual-linear-BTC-USDT-SWAP-l2-20260901-20260904-d92.json",
        SequencePolicy::Unsequenced,
    );

    // Source processing
    assert!(result.records_read > 0);
    assert_eq!(result.records_read, result.records_processed);
    assert_eq!(result.records_rejected, 0);

    // Canonical output
    assert!(result.levels_written > 0);

    // Sequence metadata
    assert_eq!(result.boundary.first_sequence, None);
    assert_eq!(result.boundary.last_sequence, None);

    // Reconstruction
    assert_eq!(result.segment_count, 1);

    // Initial and final states
    let initial = result.boundary.initial.as_ref().unwrap();
    let final_state = result.boundary.final_state.as_ref().unwrap();

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    // Contract quantity normalization
    assert!(
        initial.state.bids.iter().any(|level| {
            level.quantity_contracts.is_some()
                && level.quantity_base.is_some()
                && level.quantity_quote.is_some()
        }),
        "linear perpetual must preserve contracts and normalized quantities"
    );

    let contract_value = Decimal::from_str("0.01").unwrap();

    for level in &initial.state.bids {
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
    }

    println!("OKX contract normalization: VERIFIED");

    // Order counts
    assert!(
        initial
            .state
            .bids
            .iter()
            .any(|level| { level.order_count.is_some() }),
        "OKX must preserve order counts"
    );

    println!("\nOKX LINEAR PERPETUAL RESULT: PASS");
}
