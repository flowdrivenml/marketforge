#[cfg(feature = "historical-full-tests")]
mod prepared_real_depth {
    use crate::support::depth_assertions::run_prepared_real_depth_test;
    use marketforge_engine::shared::domain::canonical::Exchange;

    #[test]
    fn validates_gateio_inverse_depth() {
        let result = run_prepared_real_depth_test(Exchange::GateIo, "perpetual", "inverse", 0);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        for level in initial
            .state
            .bids
            .iter()
            .chain(&initial.state.asks)
            .take(30)
        {
            let contracts = level.quantity_contracts.expect("missing contracts");
            let quote = level.quantity_quote.expect("missing quote");
            assert_eq!(quote, contracts, "inverse contract notional mismatch");
            assert!(
                level.quantity_base.is_some(),
                "missing inverse base quantity"
            );
        }
    }

    #[test]
    fn validates_gateio_linear_depth() {
        let result = run_prepared_real_depth_test(Exchange::GateIo, "perpetual", "linear", 0);
        assert!(result.levels_written > 0);
        let initial = result.boundary.initial.as_ref().unwrap();
        let multiplier: rust_decimal::Decimal = "0.0001".parse().unwrap();
        for level in initial
            .state
            .bids
            .iter()
            .chain(&initial.state.asks)
            .take(30)
        {
            let contracts = level.quantity_contracts.expect("missing contracts");
            let base = level.quantity_base.expect("missing base");
            let quote = level.quantity_quote.expect("missing quote");
            assert_eq!(
                base,
                contracts * multiplier,
                "linear contract multiplier mismatch"
            );
            assert_eq!(
                quote,
                base * level.price,
                "linear quote conversion mismatch"
            );
        }
    }

    #[test]
    fn validates_gateio_spot_depth() {
        let result = run_prepared_real_depth_test(Exchange::GateIo, "spot", "spot", 0);
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
    synthetic("gateio.spot.spot.trades", Units::Base).unwrap();
}

#[test]
fn spot_spot_depth_profile() {
    synthetic("gateio.spot.spot.depth", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_trades_profile() {
    synthetic("gateio.perpetual.linear.trades", Units::LinearTenThousandth).unwrap();
}

#[test]
fn perpetual_linear_depth_profile() {
    synthetic("gateio.perpetual.linear.depth", Units::LinearTenThousandth).unwrap();
}

#[test]
fn perpetual_inverse_trades_profile() {
    synthetic("gateio.perpetual.inverse.trades", Units::InverseOne).unwrap();
}

#[test]
fn perpetual_inverse_depth_profile() {
    synthetic("gateio.perpetual.inverse.depth", Units::InverseOne).unwrap();
}
