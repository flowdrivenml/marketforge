#[cfg(feature = "historical-real-tests")]
mod retained_future_inverse {
    use marketforge_engine::shared::domain::book::SequencePolicy;
    use rust_decimal::Decimal;

    use crate::support::depth_assertions::run_depth_format_test;

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
}

#[cfg(feature = "historical-real-tests")]
mod retained_future_linear {
    use marketforge_engine::shared::domain::book::SequencePolicy;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    use crate::support::depth_assertions::run_depth_format_test;

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
}

#[cfg(feature = "historical-real-tests")]
mod retained_perpetual_inverse {
    use marketforge_engine::shared::domain::book::SequencePolicy;
    use rust_decimal::Decimal;

    use crate::support::depth_assertions::run_depth_format_test;

    #[test]
    fn validates_okx_perpetual_inverse_btcusd() {
        let result = run_depth_format_test(
            "okx-perpetual-inverse-BTC-USD-SWAP-l2-20260901-20260904-d101.json",
            SequencePolicy::Unsequenced,
        );

        // ---------------------------------------------------------
        // Source processing
        // ---------------------------------------------------------

        assert!(result.records_read > 0);
        assert_eq!(result.records_read, result.records_processed);
        assert_eq!(result.records_rejected, 0);

        assert!(result.levels_written > 0);

        // ---------------------------------------------------------
        // Sequence metadata
        // ---------------------------------------------------------

        assert_eq!(result.boundary.first_sequence, None);
        assert_eq!(result.boundary.last_sequence, None);

        // ---------------------------------------------------------
        // Reconstruction
        // ---------------------------------------------------------

        assert_eq!(result.segment_count, 1);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert_eq!(initial.state.bids.len(), 5000);
        assert_eq!(initial.state.asks.len(), 5000);

        assert_eq!(final_state.state.bids.len(), 5000);
        assert_eq!(final_state.state.asks.len(), 5000);

        // ---------------------------------------------------------
        // Inverse contract normalization
        // ---------------------------------------------------------

        let contract_value = Decimal::from(100);

        for level in initial.state.bids.iter().chain(initial.state.asks.iter()) {
            let contracts = level.quantity_contracts.expect("missing contract quantity");

            let quote = level.quantity_quote.expect("missing quote quantity");

            let base = level.quantity_base.expect("missing base quantity");

            // USD notional must be exact.
            assert_eq!(
                quote,
                contracts * contract_value,
                "incorrect inverse contract-to-quote conversion"
            );

            // Inverse division may require decimal rounding.
            let expected_base = quote / level.price;

            assert_eq!(
                base, expected_base,
                "incorrect inverse quote-to-base conversion"
            );

            assert!(
                level.order_count.is_some(),
                "OKX must preserve order counts"
            );
        }

        println!("\nOKX INVERSE CONTRACT NORMALIZATION: VERIFIED");
        println!("OKX INVERSE PERPETUAL RESULT: PASS");
    }
}

#[cfg(feature = "historical-real-tests")]
mod retained_perpetual_linear {
    use crate::support::depth_assertions::run_depth_format_test;
    use marketforge_engine::shared::domain::book::SequencePolicy;
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
}

#[cfg(feature = "historical-real-tests")]
mod retained_spot {
    use marketforge_engine::shared::domain::book::SequencePolicy;

    use crate::support::depth_assertions::run_depth_format_test;

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
}

use crate::support::profile_cases::{Units, synthetic};

#[test]
fn spot_spot_trades_profile() {
    synthetic("okx.spot.spot.trades", Units::Base).unwrap();
}

#[test]
fn spot_spot_depth_profile() {
    synthetic("okx.spot.spot.depth", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_trades_profile() {
    synthetic("okx.perpetual.linear.trades", Units::LinearHundredth).unwrap();
}

#[test]
fn perpetual_linear_depth_profile() {
    synthetic("okx.perpetual.linear.depth", Units::LinearHundredth).unwrap();
}

#[test]
fn perpetual_inverse_trades_profile() {
    synthetic("okx.perpetual.inverse.trades", Units::InverseHundred).unwrap();
}

#[test]
fn perpetual_inverse_depth_profile() {
    synthetic("okx.perpetual.inverse.depth", Units::InverseHundred).unwrap();
}

#[test]
fn future_linear_trades_profile() {
    synthetic("okx.future.linear.trades", Units::LinearHundredth).unwrap();
}

#[test]
fn future_linear_depth_profile() {
    synthetic("okx.future.linear.depth", Units::LinearHundredth).unwrap();
}

#[test]
fn future_inverse_trades_profile() {
    synthetic("okx.future.inverse.trades", Units::InverseHundred).unwrap();
}

#[test]
fn future_inverse_depth_profile() {
    synthetic("okx.future.inverse.depth", Units::InverseHundred).unwrap();
}
