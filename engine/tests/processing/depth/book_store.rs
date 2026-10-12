use marketforge_engine::{
    book::{BookLevel, BookStore},
    canonical::BookSide,
};

use rust_decimal::Decimal;

fn level(quantity: i64) -> BookLevel {
    BookLevel {
        quantity_base: Some(Decimal::new(quantity, 0)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn price(value: i64) -> Decimal {
    Decimal::new(value, 0)
}

#[test]
fn initializes_from_snapshot() {
    let mut book = BookStore::new();

    let changes = book
        .apply_snapshot(
            vec![(price(100), level(10)), (price(99), level(20))],
            vec![(price(101), level(15))],
        )
        .unwrap();

    assert!(book.is_initialized());
    assert_eq!(book.bids().len(), 2);
    assert_eq!(book.asks().len(), 1);
    assert_eq!(changes.len(), 3);
}

#[test]
fn replaces_absolute_quantity() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book.set_level(BookSide::Bid, price(100), level(5)).unwrap();

    assert!(change.is_some());
    assert_eq!(
        book.bids().get(&price(100)).unwrap().quantity_base,
        Some(price(5))
    );
}

#[test]
fn deletes_level_with_zero_quantity() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book.set_level(BookSide::Bid, price(100), level(0)).unwrap();

    assert!(change.is_some());
    assert!(!book.bids().contains_key(&price(100)));
}

#[test]
fn suppresses_unchanged_updates() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book
        .set_level(BookSide::Bid, price(100), level(10))
        .unwrap();

    assert!(change.is_none());
}

#[test]
fn differences_consecutive_snapshots() {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![
            (price(100), level(10)),
            (price(99), level(20)),
            (price(98), level(30)),
        ],
        vec![],
    )
    .unwrap();

    let changes = book
        .apply_snapshot(
            vec![
                (price(100), level(5)),
                (price(98), level(30)),
                (price(97), level(15)),
            ],
            vec![],
        )
        .unwrap();

    assert_eq!(changes.len(), 3);

    assert!(changes.iter().any(|(side, p, l)| {
        *side == BookSide::Bid && *p == price(100) && l.quantity_base == Some(price(5))
    }));

    assert!(changes.iter().any(|(side, p, l)| {
        *side == BookSide::Bid && *p == price(99) && l.quantity_base == Some(Decimal::ZERO)
    }));

    assert!(changes.iter().any(|(side, p, l)| {
        *side == BookSide::Bid && *p == price(97) && l.quantity_base == Some(price(15))
    }));
}

#[test]
fn rejects_updates_before_initialization() {
    let mut book = BookStore::new();

    assert!(
        book.set_level(BookSide::Bid, price(100), level(10))
            .is_err()
    );
}

#[test]
fn rejects_negative_quantities() {
    let mut book = BookStore::new();

    assert!(
        book.apply_snapshot(vec![(price(100), level(-1))], vec![])
            .is_err()
    );
}

#[test]
fn rejects_duplicate_snapshot_prices() {
    let mut book = BookStore::new();

    assert!(
        book.apply_snapshot(
            vec![(price(100), level(10)), (price(100), level(20))],
            vec![],
        )
        .is_err()
    );
}

#[test]
fn reset_requires_new_initialization() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    book.clear();

    assert!(!book.is_initialized());
    assert!(book.bids().is_empty());

    assert!(book.set_level(BookSide::Bid, price(100), level(5)).is_err());
}
