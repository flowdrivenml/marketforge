#![cfg(feature = "process")]

use rust_decimal::Decimal;

use marketforge_engine::{
    book::{BookLevel, BookStore},
    canonical::{BookSide, Price},
};

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn level(quantity: &str) -> BookLevel {
    BookLevel {
        quantity_base: Some(decimal(quantity)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn snapshot_one() -> (Vec<(Price, BookLevel)>, Vec<(Price, BookLevel)>) {
    let bids = vec![
        (decimal("100.0"), level("5")),
        (decimal("99.9"), level("3")),
        (decimal("99.8"), level("2")),
    ];

    let asks = vec![
        (decimal("100.1"), level("4")),
        (decimal("100.2"), level("6")),
    ];

    (bids, asks)
}

fn snapshot_two() -> (Vec<(Price, BookLevel)>, Vec<(Price, BookLevel)>) {
    let bids = vec![
        (decimal("100.0"), level("7")),
        (decimal("99.8"), level("2")),
        (decimal("99.7"), level("1")),
    ];

    let asks = vec![
        (decimal("100.1"), level("4")),
        (decimal("100.2"), level("8")),
    ];

    (bids, asks)
}

#[test]
fn snapshot_differencing_emits_only_changed_levels() {
    let mut book = BookStore::new();

    // -------------------------------------------------------------
    // Initialize from the first authoritative snapshot
    // -------------------------------------------------------------

    let (bids, asks) = snapshot_one();

    let initial_changes = book
        .apply_snapshot(bids, asks)
        .expect("apply initial snapshot");

    assert_eq!(initial_changes.len(), 5);

    assert!(book.is_initialized());

    // -------------------------------------------------------------
    // Apply the second authoritative snapshot
    // -------------------------------------------------------------

    let (bids, asks) = snapshot_two();

    let changes = book
        .apply_snapshot(bids, asks)
        .expect("apply second snapshot");

    // Expected:
    // Bid 100.0: 5 → 7
    // Bid  99.9: 3 → 0
    // Bid  99.7: 0 → 1
    // Ask 100.2: 6 → 8

    assert_eq!(changes.len(), 4);

    // -------------------------------------------------------------
    // Verify modified bid
    // -------------------------------------------------------------

    assert!(changes.iter().any(|(side, price, level)| {
        *side == BookSide::Bid
            && *price == decimal("100.0")
            && level.quantity_base == Some(decimal("7"))
    }));

    // -------------------------------------------------------------
    // Verify removed bid
    // -------------------------------------------------------------

    assert!(changes.iter().any(|(side, price, level)| {
        *side == BookSide::Bid && *price == decimal("99.9") && level.is_zero()
    }));

    // -------------------------------------------------------------
    // Verify new bid
    // -------------------------------------------------------------

    assert!(changes.iter().any(|(side, price, level)| {
        *side == BookSide::Bid
            && *price == decimal("99.7")
            && level.quantity_base == Some(decimal("1"))
    }));

    // -------------------------------------------------------------
    // Verify modified ask
    // -------------------------------------------------------------

    assert!(changes.iter().any(|(side, price, level)| {
        *side == BookSide::Ask
            && *price == decimal("100.2")
            && level.quantity_base == Some(decimal("8"))
    }));

    // -------------------------------------------------------------
    // Verify unchanged levels are omitted
    // -------------------------------------------------------------

    assert!(
        !changes
            .iter()
            .any(|(side, price, _)| { *side == BookSide::Bid && *price == decimal("99.8") })
    );

    assert!(
        !changes
            .iter()
            .any(|(side, price, _)| { *side == BookSide::Ask && *price == decimal("100.1") })
    );

    // -------------------------------------------------------------
    // Verify reconstructed book
    // -------------------------------------------------------------

    assert_eq!(book.bids().len(), 3);
    assert_eq!(book.asks().len(), 2);

    assert_eq!(
        book.bids().get(&decimal("100.0")).unwrap().quantity_base,
        Some(decimal("7"))
    );

    assert!(!book.bids().contains_key(&decimal("99.9")));

    assert_eq!(
        book.asks().get(&decimal("100.2")).unwrap().quantity_base,
        Some(decimal("8"))
    );

    println!("\nBITGET SNAPSHOT DIFFERENCING: PASS");
}
