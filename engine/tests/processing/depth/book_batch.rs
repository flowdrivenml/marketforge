use marketforge_engine::{
    book::{BookChange, BookLevel, BookStore},
    canonical::BookSide,
};

use rust_decimal::Decimal;

fn price(value: i64) -> Decimal {
    Decimal::new(value, 0)
}

fn level(quantity: i64) -> BookLevel {
    BookLevel {
        quantity_base: Some(price(quantity)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn initialized_book() -> BookStore {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![(price(100), level(10)), (price(99), level(20))],
        vec![(price(101), level(15))],
    )
    .unwrap();

    book
}

// -----------------------------------------------------------------------------
// Successful batch
// -----------------------------------------------------------------------------

#[test]
fn applies_multiple_levels_atomically() {
    let mut book = initialized_book();

    let updates: Vec<BookChange> = vec![
        (BookSide::Bid, price(100), level(5)),
        (BookSide::Bid, price(99), level(0)),
        (BookSide::Ask, price(102), level(30)),
    ];

    let changes = book.apply_batch(updates).unwrap();

    assert_eq!(changes.len(), 3);

    assert_eq!(
        book.bids().get(&price(100)).unwrap().quantity_base,
        Some(price(5))
    );

    assert!(!book.bids().contains_key(&price(99)));

    assert_eq!(
        book.asks().get(&price(102)).unwrap().quantity_base,
        Some(price(30))
    );
}

// -----------------------------------------------------------------------------
// Atomic failure
// -----------------------------------------------------------------------------

#[test]
fn invalid_batch_does_not_modify_book() {
    let mut book = initialized_book();

    let previous_bids = book.bids().clone();
    let previous_asks = book.asks().clone();

    let updates = vec![
        (BookSide::Bid, price(100), level(5)),
        (BookSide::Ask, price(102), level(-10)),
    ];

    assert!(book.apply_batch(updates).is_err());

    assert_eq!(book.bids(), &previous_bids);
    assert_eq!(book.asks(), &previous_asks);
}

// -----------------------------------------------------------------------------
// Duplicate price validation
// -----------------------------------------------------------------------------

#[test]
fn rejects_duplicate_price_levels() {
    let mut book = initialized_book();

    let updates = vec![
        (BookSide::Bid, price(100), level(5)),
        (BookSide::Bid, price(100), level(7)),
    ];

    assert!(book.apply_batch(updates).is_err());
}

// -----------------------------------------------------------------------------
// Same price on opposite sides
// -----------------------------------------------------------------------------

#[test]
fn permits_same_price_on_different_sides() {
    let mut book = initialized_book();

    let updates = vec![
        (BookSide::Bid, price(100), level(5)),
        (BookSide::Ask, price(100), level(7)),
    ];

    assert!(book.apply_batch(updates).is_ok());
}

// -----------------------------------------------------------------------------
// Unchanged updates
// -----------------------------------------------------------------------------

#[test]
fn suppresses_unchanged_levels_in_batch() {
    let mut book = initialized_book();

    let updates = vec![
        (BookSide::Bid, price(100), level(10)),
        (BookSide::Bid, price(99), level(5)),
    ];

    let changes = book.apply_batch(updates).unwrap();

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].1, price(99));
}

// -----------------------------------------------------------------------------
// Initialization requirement
// -----------------------------------------------------------------------------

#[test]
fn rejects_batch_before_initialization() {
    let mut book = BookStore::new();

    let updates = vec![(BookSide::Bid, price(100), level(5))];

    assert!(book.apply_batch(updates).is_err());
}

// -----------------------------------------------------------------------------
// Empty batch
// -----------------------------------------------------------------------------

#[test]
fn accepts_empty_batch() {
    let mut book = initialized_book();

    let changes = book.apply_batch(vec![]).unwrap();

    assert!(changes.is_empty());
}
