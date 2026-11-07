#[cfg(feature = "historical-real-tests")]
mod retained_linear {
    use crate::support::depth_assertions::{
        ExpectedDepthLevel, run_depth_format_test, verify_snapshot_quantities,
    };

    use marketforge_engine::shared::domain::canonical::BookSide;

    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    #[test]
    fn validates_bitget_linear_btcusdt() {
        let result = run_depth_format_test(
            "bitget-perpetual-linear-BTCUSDT-l2-20260901-20260904-d94.json",
            SequencePolicy::Unsequenced,
        );

        assert_eq!(result.records_read, 4294);
        assert_eq!(result.records_processed, 4294);
        assert_eq!(result.records_rejected, 0);

        assert_eq!(result.segment_count, 1);

        assert_eq!(result.boundary.continuity, BoundaryContinuity::Unverifiable,);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert_eq!(initial.state.bids.len(), 500);
        assert_eq!(initial.state.asks.len(), 500);

        assert_eq!(final_state.state.bids.len(), 500);
        assert_eq!(final_state.state.asks.len(), 500);

        println!("\nBITGET LINEAR BTCUSDT RESULT: PASS");
        println!("\nBITGET BTCUSDT LINEAR QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78568.9",
                    quantity_base: "6.8404",
                    quantity_quote: "537442.70356",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78568.3",
                    quantity_base: "0.0771",
                    quantity_quote: "6057.61593",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78568.2",
                    quantity_base: "0.1746",
                    quantity_quote: "13718.00772",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78569.0",
                    quantity_base: "0.6702",
                    quantity_quote: "52656.94380",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78570.6",
                    quantity_base: "0.0001",
                    quantity_quote: "7.85706",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78570.8",
                    quantity_base: "0.0014",
                    quantity_quote: "109.99912",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
            ],
        );
    }
}

#[cfg(feature = "historical-real-tests")]
mod retained_spot {
    use crate::support::depth_assertions::{
        ExpectedDepthLevel, run_depth_format_test, verify_snapshot_quantities,
    };

    use marketforge_engine::shared::domain::canonical::BookSide;

    use {
        marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
        marketforge_engine::shared::domain::book::SequencePolicy,
    };

    #[test]
    fn validates_bitget_spot_btcusdt() {
        let result = run_depth_format_test(
            "bitget-spot-spot-BTCUSDT-l2-20260901-20260904-d110.json",
            SequencePolicy::Unsequenced,
        );

        // -------------------------------------------------------------
        // Source accounting
        // -------------------------------------------------------------

        assert_eq!(result.records_read, 4297);
        assert_eq!(result.records_processed, 4297);
        assert_eq!(result.records_rejected, 0);

        // -------------------------------------------------------------
        // Reconstruction
        // -------------------------------------------------------------

        assert_eq!(result.segment_count, 1);

        // Bitget provides no native sequence identifiers.
        assert_eq!(result.boundary.continuity, BoundaryContinuity::Unverifiable,);

        // -------------------------------------------------------------
        // Initial and final book
        // -------------------------------------------------------------

        let initial = result
            .boundary
            .initial
            .as_ref()
            .expect("missing initial Bitget snapshot");

        let final_state = result
            .boundary
            .final_state
            .as_ref()
            .expect("missing final Bitget book");

        assert_eq!(initial.state.bids.len(), 500);
        assert_eq!(initial.state.asks.len(), 500);

        assert_eq!(final_state.state.bids.len(), 500);
        assert_eq!(final_state.state.asks.len(), 500);

        // -------------------------------------------------------------
        // Timestamp ordering
        // -------------------------------------------------------------

        assert!(
            initial.timestamp_ns <= final_state.timestamp_ns,
            "Bitget snapshots were not processed chronologically"
        );

        println!("\nBITGET SPOT BTCUSDT RESULT: PASS");

        println!("\nBITGET BTCUSDT SPOT QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78557.69",
                    quantity_base: "1.037168",
                    quantity_quote: "81477.52222192",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78557.63",
                    quantity_base: "0.06",
                    quantity_quote: "4713.4578",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "78557.62",
                    quantity_base: "0.000019",
                    quantity_quote: "1.49259478",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78557.7",
                    quantity_base: "0.239515",
                    quantity_quote: "18815.7475155",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78559.75",
                    quantity_base: "0.005759",
                    quantity_quote: "452.42560025",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "78561.63",
                    quantity_base: "0.001",
                    quantity_quote: "78.56163",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
            ],
        );
    }

    #[test]
    fn validates_bitget_spot_ethusdc() {
        let result = run_depth_format_test(
            "bitget-spot-spot-ETHUSDC-l2-20260901-20260904-d117.json",
            SequencePolicy::Unsequenced,
        );

        assert_eq!(result.records_read, 4297);
        assert_eq!(result.records_processed, 4297);
        assert_eq!(result.records_rejected, 0);

        assert_eq!(result.segment_count, 1);

        assert_eq!(result.boundary.continuity, BoundaryContinuity::Unverifiable,);

        let initial = result.boundary.initial.as_ref().unwrap();
        let final_state = result.boundary.final_state.as_ref().unwrap();

        assert_eq!(initial.state.bids.len(), 500);
        assert_eq!(initial.state.asks.len(), 500);

        assert_eq!(final_state.state.bids.len(), 500);
        assert_eq!(final_state.state.asks.len(), 500);

        println!("\nBITGET SPOT ETHUSDC RESULT: PASS");

        println!("\nBITGET ETHUSDC SPOT QUANTITY NORMALIZATION");

        verify_snapshot_quantities(
            &result.boundary,
            &[
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "2465.36",
                    quantity_base: "0.8107",
                    quantity_quote: "1998.667352",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "2465.35",
                    quantity_base: "0.8111",
                    quantity_quote: "1999.645385",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Bid,
                    price: "2465.14",
                    quantity_base: "0.116",
                    quantity_quote: "285.95624",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "2465.77",
                    quantity_base: "0.0084",
                    quantity_quote: "20.712468",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "2465.99",
                    quantity_base: "0.1585",
                    quantity_quote: "390.859415",
                    quantity_contracts: None,
                    base_tolerance: None,
                },
                ExpectedDepthLevel {
                    side: BookSide::Ask,
                    price: "2466.0",
                    quantity_base: "0.1585",
                    quantity_quote: "390.86100",
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
