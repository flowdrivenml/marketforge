#[cfg(feature = "historical-real-tests")]
mod prepared_real_depth {
    use crate::support::depth_assertions::{
        run_prepared_real_depth_family_test, run_prepared_real_depth_test,
    };
    use marketforge_engine::shared::domain::canonical::Exchange;

    #[test]
    fn validates_okx_spot_depth() {
        let result = run_prepared_real_depth_test(Exchange::Okx, "spot", "spot", 0);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        assert!(
            initial
                .state
                .bids
                .iter()
                .chain(&initial.state.asks)
                .any(|level| level.order_count.is_some()),
            "OKX order counts missing"
        );
        assert!(
            initial
                .state
                .bids
                .iter()
                .chain(&initial.state.asks)
                .any(|level| level.quantity_base.is_some()),
            "OKX base quantities missing"
        );
    }

    #[test]
    #[ignore = "The authoritative prepared batch has no matching OKX inverse-futures source"]
    fn validates_okx_future_inverse_btcusd() {
        panic!(
            "Blocked: The authoritative prepared batch has no matching OKX inverse-futures source"
        );
    }

    #[test]
    fn validates_okx_future_linear_btcusd() {
        let result = run_prepared_real_depth_family_test(Exchange::Okx, "future", "linear", 0, 0);

        assert!(result.records_read > 0);
        assert!(result.levels_written > 0);
        assert!(result.segment_count > 0);
        assert_eq!(result.records_rejected, 0);
    }

    #[test]
    fn validates_okx_perpetual_inverse_btcusd() {
        let result =
            run_prepared_real_depth_family_test(Exchange::Okx, "perpetual", "inverse", 0, 0);

        assert!(result.records_read > 0);
        assert!(result.levels_written > 0);
        assert!(result.segment_count > 0);
        assert_eq!(result.records_rejected, 0);
    }

    #[test]
    fn validates_okx_perpetual_linear_btcusdt() {
        let result =
            run_prepared_real_depth_family_test(Exchange::Okx, "perpetual", "linear", 0, 0);

        assert!(result.records_read > 0);
        assert!(result.levels_written > 0);
        assert!(result.segment_count > 0);
        assert_eq!(result.records_rejected, 0);
    }

    #[test]
    fn validates_okx_future_linear_second_member() {
        let result = run_prepared_real_depth_family_test(Exchange::Okx, "future", "linear", 0, 1);

        assert!(result.records_read > 0);
        assert_eq!(result.records_rejected, 0);
        assert!(result.levels_written > 0);
        assert!(result.segment_count > 0);
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
