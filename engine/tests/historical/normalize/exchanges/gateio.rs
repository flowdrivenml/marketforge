#[cfg(feature = "historical-real-tests")]
mod retained_inverse {
    use marketforge_engine::shared::domain::canonical::BookSide;
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::{ExpectedDepthEvent, run_depth_format_test_with_events};
    use crate::support::depth_assertions::{ExpectedDepthLevel, verify_snapshot_quantities};

    #[test]
    fn validates_gateio_inverse_btcusd() {
        let result = run_depth_format_test_with_events(
            "gateio-perpetual-inverse-BTC_USD-l2-20260901-20260904-d103.json",
            SequencePolicy::Ranged,
            &[
                ExpectedDepthEvent {
                    source_ordinal: 1721,
                    side: BookSide::Bid,
                    price: "78437.5",
                    quantity_base: None,
                    quantity_quote: None,
                    quantity_contracts: Some("0"),
                    base_tolerance: None,
                },
                ExpectedDepthEvent {
                    source_ordinal: 1722,
                    side: BookSide::Bid,
                    price: "78459",
                    quantity_base: Some("0.3186377598490931569354694809"),
                    quantity_quote: Some("25000"),
                    quantity_contracts: Some("25000"),
                    base_tolerance: Some("0.000000000000000000000001"),
                },
            ],
        );

        assert_eq!(result.records_read, 310_699);
        assert_eq!(result.records_processed, result.records_read);
        assert_eq!(result.records_rejected, 0);

        assert!(result.levels_written > 0);

        assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified,);

        assert_eq!(result.segment_count, 1);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert!(!initial.state.bids.is_empty());
        assert!(!initial.state.asks.is_empty());

        assert!(!final_state.state.bids.is_empty());
        assert!(!final_state.state.asks.is_empty());

        println!("\nGATE.IO INVERSE PERPETUAL RESULT: PASS");
        println!("\nGATE.IO INVERSE QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.0",
                    quantity_base: "0.04028571428571428571428571429",
                    quantity_quote: "2820",
                    quantity_contracts: Some("2820"),
                    base_tolerance: Some("0.000000000000000000000001"),
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70017.3",
                    quantity_base: "0.001242550055486286960508331512",
                    quantity_quote: "87",
                    quantity_contracts: Some("87"),
                    base_tolerance: Some("0.000000000000000000000001"),
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70042.2",
                    quantity_base: "0.1427710722964155894589261902",
                    quantity_quote: "10000",
                    quantity_contracts: Some("10000"),
                    base_tolerance: Some("0.000000000000000000000001"),
                },
            ],
        );
    }
}

#[cfg(feature = "historical-real-tests")]
mod retained_linear {
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::{ExpectedDepthEvent, run_depth_format_test_with_events};
    use crate::support::depth_assertions::{ExpectedDepthLevel, verify_snapshot_quantities};
    use marketforge_engine::shared::domain::canonical::BookSide;

    #[test]
    fn validates_gateio_linear_btcusdt() {
        let result = run_depth_format_test_with_events(
            "gateio-perpetual-linear-BTC_USDT-l2-20260901-20260904-d96.json",
            SequencePolicy::Ranged,
            &[
                ExpectedDepthEvent {
                    source_ordinal: 44063,
                    side: BookSide::Bid,
                    price: "78542.1",
                    quantity_base: Some("0.2153"),
                    quantity_quote: Some("16910.11413"),
                    quantity_contracts: Some("2153"),
                    base_tolerance: None,
                },
                ExpectedDepthEvent {
                    source_ordinal: 44064,
                    side: BookSide::Ask,
                    price: "79283",
                    quantity_base: Some("0.0008"),
                    quantity_quote: Some("63.4264"),
                    quantity_contracts: Some("8"),
                    base_tolerance: None,
                },
            ],
        );

        assert_eq!(result.records_read, 2_705_755);
        assert_eq!(result.records_processed, result.records_read);
        assert_eq!(result.records_rejected, 0);

        assert!(result.levels_written > 0);

        assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified,);

        assert_eq!(result.segment_count, 1);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert!(!initial.state.bids.is_empty());
        assert!(!initial.state.asks.is_empty());

        assert!(!final_state.state.bids.is_empty());
        assert!(!final_state.state.asks.is_empty());

        println!("\nGATE.IO LINEAR PERPETUAL RESULT: PASS");
        println!("\nGATE.IO LINEAR QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.0",
                    quantity_base: "2.92770",
                    quantity_quote: "204939",
                    quantity_contracts: Some("29277"),
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.4",
                    quantity_base: "0.00040",
                    quantity_quote: "28.000160",
                    quantity_contracts: Some("4"),
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.9",
                    quantity_base: "0.00010",
                    quantity_quote: "7.000090",
                    quantity_contracts: Some("1"),
                    base_tolerance: None,
                },
            ],
        );
    }
}

#[cfg(feature = "historical-real-tests")]
mod retained_spot {
    use marketforge_engine::shared::domain::canonical::BookSide;
    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    use crate::support::depth_assertions::{ExpectedDepthEvent, run_depth_format_test_with_events};
    use crate::support::depth_assertions::{ExpectedDepthLevel, verify_snapshot_quantities};

    #[test]
    fn validates_gateio_spot_btcusdt() {
        let result = run_depth_format_test_with_events(
            "gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json",
            SequencePolicy::Ranged,
            &[
                ExpectedDepthEvent {
                    source_ordinal: 53924,
                    side: BookSide::Bid,
                    price: "78582.8",
                    quantity_base: Some("0.003844"),
                    quantity_quote: Some("302.0722832"),
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthEvent {
                    source_ordinal: 53925,
                    side: BookSide::Ask,
                    price: "78600.7",
                    quantity_base: Some("0.021934"),
                    quantity_quote: Some("1724.0277538"),
                    quantity_contracts: None,
                    base_tolerance: None,
                },
            ],
        );

        assert_eq!(result.records_read, 668_973);
        assert_eq!(result.records_processed, result.records_read);
        assert_eq!(result.records_rejected, 0);

        assert!(result.levels_written > 0);

        assert_eq!(result.boundary.continuity, BoundaryContinuity::Verified,);

        assert_eq!(result.segment_count, 1);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert!(!initial.state.bids.is_empty());
        assert!(!initial.state.asks.is_empty());

        assert!(!final_state.state.bids.is_empty());
        assert!(!final_state.state.asks.is_empty());

        println!("\nGATE.IO SPOT BTC_USDT RESULT: PASS");
        println!("\nGATE.IO SPOT QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.0",
                    quantity_base: "3.758773",
                    quantity_quote: "263114.11",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70000.3",
                    quantity_base: "0.000076",
                    quantity_quote: "5.3200228",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "70001.0",
                    quantity_base: "0.005471",
                    quantity_quote: "382.975471",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
            ],
        );
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
