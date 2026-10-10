#![cfg(feature = "process")]

use rust_decimal::Decimal;

use marketforge_engine::{
    book::{BookLevel, BookStore},
    process::boundary::{BoundaryBookLevel, BoundaryBookState, BoundarySnapshot, fingerprint_book},
};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

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

fn boundary_level(price: &str, quantity: Option<&str>) -> BoundaryBookLevel {
    BoundaryBookLevel {
        price: decimal(price),
        quantity_base: quantity.map(decimal),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn state() -> BoundaryBookState {
    BoundaryBookState {
        bids: vec![
            boundary_level("100", Some("5")),
            boundary_level("101", Some("3")),
        ],
        asks: vec![
            boundary_level("102", Some("4")),
            boundary_level("103", Some("6")),
        ],
    }
}

// -----------------------------------------------------------------------------
// Fingerprint determinism
// -----------------------------------------------------------------------------

#[test]
fn identical_books_have_identical_fingerprints() {
    let first = state();
    let second = state();

    assert_eq!(fingerprint_book(&first), fingerprint_book(&second));
}

#[test]
fn equivalent_decimal_scales_have_identical_fingerprints() {
    let first = state();

    let mut second = state();
    second.bids[0].price = decimal("100.0000");
    second.bids[0].quantity_base = Some(decimal("5.000000"));

    assert_eq!(fingerprint_book(&first), fingerprint_book(&second));
}

#[test]
fn different_quantities_change_fingerprint() {
    let first = state();

    let mut second = state();
    second.bids[0].quantity_base = Some(decimal("7"));

    assert_ne!(fingerprint_book(&first), fingerprint_book(&second));
}

#[test]
fn missing_and_zero_quantities_are_distinct() {
    let first = state();

    let mut second = state();
    second.bids[0].quantity_base = None;

    assert_ne!(fingerprint_book(&first), fingerprint_book(&second));

    second.bids[0].quantity_base = Some(Decimal::ZERO);

    assert_ne!(fingerprint_book(&first), fingerprint_book(&second));
}

#[test]
fn order_count_changes_fingerprint() {
    let first = state();

    let mut second = state();
    second.asks[0].order_count = Some(3);

    assert_ne!(fingerprint_book(&first), fingerprint_book(&second));
}

#[test]
fn book_side_changes_fingerprint() {
    let first = state();

    let mut second = state();

    let moved = second.bids.remove(0);
    second.asks.push(moved);

    assert_ne!(fingerprint_book(&first), fingerprint_book(&second));
}

// -----------------------------------------------------------------------------
// BookStore integration
// -----------------------------------------------------------------------------

#[test]
fn book_store_produces_deterministic_boundary_state() {
    let mut first = BookStore::new();
    let mut second = BookStore::new();

    // Insert levels in different orders.
    first
        .apply_snapshot(
            vec![(decimal("101"), level("3")), (decimal("100"), level("5"))],
            vec![(decimal("103"), level("6")), (decimal("102"), level("4"))],
        )
        .unwrap();

    second
        .apply_snapshot(
            vec![(decimal("100"), level("5")), (decimal("101"), level("3"))],
            vec![(decimal("102"), level("4")), (decimal("103"), level("6"))],
        )
        .unwrap();

    let first_state = BoundaryBookState::from_book(&first).unwrap();
    let second_state = BoundaryBookState::from_book(&second).unwrap();

    assert_eq!(first_state, second_state);

    assert_eq!(
        fingerprint_book(&first_state),
        fingerprint_book(&second_state)
    );
}

#[test]
fn uninitialized_book_has_no_boundary_state() {
    let book = BookStore::new();

    assert!(BoundaryBookState::from_book(&book).is_none());
}

// -----------------------------------------------------------------------------
// Serialization
// -----------------------------------------------------------------------------

#[test]
fn boundary_state_json_roundtrip_preserves_values() {
    let original = state();

    let json = serde_json::to_string_pretty(&original).unwrap();

    let restored: BoundaryBookState = serde_json::from_str(&json).unwrap();

    assert_eq!(original, restored);

    assert_eq!(fingerprint_book(&original), fingerprint_book(&restored));
}

#[test]
fn boundary_snapshot_preserves_fingerprint() {
    let state = state();

    let snapshot = BoundarySnapshot::new(
        Some(12345),
        1_788_220_802_715_000_000,
        state.clone(),
        Some(200),
    );

    assert_eq!(snapshot.sequence, Some(12345));

    assert_eq!(snapshot.fingerprint, fingerprint_book(&state));

    assert_eq!(snapshot.state.level_count(), 4);

    assert_eq!(snapshot.coverage.depth_per_side, Some(200));
    assert_eq!(snapshot.coverage.bid_levels, 2);
    assert_eq!(snapshot.coverage.ask_levels, 2);
}

#[test]
fn empty_book_has_deterministic_fingerprint() {
    let empty = BoundaryBookState {
        bids: vec![],
        asks: vec![],
    };

    let first = fingerprint_book(&empty);
    let second = fingerprint_book(&empty);

    assert_eq!(first, second);
    assert!(first.starts_with("sha256:"));
}
