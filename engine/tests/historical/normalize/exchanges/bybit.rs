#[cfg(feature = "historical-real-tests")]
mod retained_inverse {
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::run_depth_format_test;

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
}

#[cfg(feature = "historical-real-tests")]
mod retained_linear {
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::run_depth_format_test;

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
}

#[cfg(feature = "historical-real-tests")]
mod retained_spot {
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::run_depth_format_test;

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
}

use crate::support::profile_cases::{Units, synthetic};

#[test]
fn spot_spot_trades_profile() {
    synthetic("bybit.spot.spot.trades", Units::Base).unwrap();
}

#[test]
fn spot_spot_depth_profile() {
    synthetic("bybit.spot.spot.depth", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_trades_profile() {
    synthetic("bybit.perpetual.linear.trades", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_depth_profile() {
    synthetic("bybit.perpetual.linear.depth", Units::Base).unwrap();
}

#[test]
fn perpetual_inverse_trades_profile() {
    synthetic("bybit.perpetual.inverse.trades", Units::Quote).unwrap();
}

#[test]
fn perpetual_inverse_depth_profile() {
    synthetic("bybit.perpetual.inverse.depth", Units::Quote).unwrap();
}

#[test]
fn future_linear_trades_profile() {
    synthetic("bybit.future.linear.trades", Units::Base).unwrap();
}

#[test]
fn future_linear_depth_profile() {
    synthetic("bybit.future.linear.depth", Units::Base).unwrap();
}
