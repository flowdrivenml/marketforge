use crate::common::{ExpectedDepthLevel, run_depth_format_test, verify_snapshot_quantities};

use marketforge_engine::canonical::BookSide;

use marketforge_engine::{book::SequencePolicy, process::boundary::BoundaryContinuity};

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
