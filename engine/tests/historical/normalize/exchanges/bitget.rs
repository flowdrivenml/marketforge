#[cfg(feature = "historical-real-tests")]
mod prepared_real_depth {
    use crate::support::depth_assertions::run_prepared_real_depth_test;
    use marketforge_engine::shared::domain::canonical::Exchange;

    #[test]
    fn validates_bitget_linear_depth() {
        let result = run_prepared_real_depth_test(Exchange::Bitget, "perpetual", "linear", 0);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        for level in initial
            .state
            .bids
            .iter()
            .chain(&initial.state.asks)
            .take(30)
        {
            let base = level.quantity_base.expect("missing base quantity");
            let quote = level.quantity_quote.expect("missing quote quantity");
            assert_eq!(quote, base * level.price, "base/quote conversion mismatch");
        }
    }

    #[test]
    fn validates_bitget_spot_depth() {
        let result = run_prepared_real_depth_test(Exchange::Bitget, "spot", "spot", 0);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        for level in initial
            .state
            .bids
            .iter()
            .chain(&initial.state.asks)
            .take(30)
        {
            let base = level.quantity_base.expect("missing base quantity");
            let quote = level.quantity_quote.expect("missing quote quantity");
            assert_eq!(quote, base * level.price, "base/quote conversion mismatch");
        }
    }

    #[test]
    fn validates_bitget_spot_second_archive() {
        let result = run_prepared_real_depth_test(Exchange::Bitget, "spot", "spot", 1);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        for level in initial
            .state
            .bids
            .iter()
            .chain(&initial.state.asks)
            .take(30)
        {
            let base = level.quantity_base.expect("missing base quantity");
            let quote = level.quantity_quote.expect("missing quote quantity");
            assert_eq!(quote, base * level.price, "base/quote conversion mismatch");
        }
    }
}

use crate::support::profile_cases::{Units, synthetic};

#[test]
fn spot_spot_trades_profile() {
    synthetic("bitget.spot.spot.trades", Units::Base).unwrap();
}

#[test]
fn spot_spot_depth_profile() {
    synthetic("bitget.spot.spot.depth", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_trades_profile() {
    synthetic("bitget.perpetual.linear.trades", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_depth_profile() {
    synthetic("bitget.perpetual.linear.depth", Units::Base).unwrap();
}
