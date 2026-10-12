use marketforge_engine::{
    book::{BookLevel, BookStore, emit_l2_changes},
    canonical::{BookSide, EventEnvelope, Exchange},
};

use rust_decimal::Decimal;

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

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

fn envelope(timestamp_ns: i64) -> EventEnvelope {
    EventEnvelope {
        event_timestamp_ns: timestamp_ns,
        system_timestamp_ns: None,
        exchange: Exchange::Bybit,
        instrument_id: 1,
        symbol: "BTCUSDT".to_owned(),
        stream_id: "bybit:BTCUSDT:depth".to_owned(),
    }
}

// -----------------------------------------------------------------------------
// Initial snapshot
// -----------------------------------------------------------------------------

#[test]
fn emits_initial_snapshot_as_canonical_levels() {
    let mut book = BookStore::new();

    let changes = book
        .apply_snapshot(
            vec![(price(100), level(10)), (price(99), level(20))],
            vec![(price(101), level(15))],
        )
        .unwrap();

    let events = emit_l2_changes(&envelope(1000), changes);

    assert_eq!(events.len(), 3);

    assert!(
        events
            .iter()
            .all(|event| { event.envelope.event_timestamp_ns == 1000 })
    );

    assert!(events.iter().any(|event| {
        event.side == BookSide::Bid
            && event.price == price(100)
            && event.quantity_base == Some(price(10))
    }));

    assert!(events.iter().any(|event| {
        event.side == BookSide::Bid
            && event.price == price(99)
            && event.quantity_base == Some(price(20))
    }));

    assert!(events.iter().any(|event| {
        event.side == BookSide::Ask
            && event.price == price(101)
            && event.quantity_base == Some(price(15))
    }));
}

// -----------------------------------------------------------------------------
// Absolute level replacement
// -----------------------------------------------------------------------------

#[test]
fn emits_absolute_level_replacement() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book
        .set_level(BookSide::Bid, price(100), level(5))
        .unwrap()
        .unwrap();

    let events = emit_l2_changes(&envelope(2000), vec![change]);

    assert_eq!(events.len(), 1);

    assert_eq!(events[0].side, BookSide::Bid);
    assert_eq!(events[0].price, price(100));
    assert_eq!(events[0].quantity_base, Some(price(5)));
    assert_eq!(events[0].envelope.event_timestamp_ns, 2000);
}

// -----------------------------------------------------------------------------
// Zero quantity deletion
// -----------------------------------------------------------------------------

#[test]
fn emits_zero_quantity_for_deletion() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book
        .set_level(BookSide::Bid, price(100), level(0))
        .unwrap()
        .unwrap();

    let events = emit_l2_changes(&envelope(3000), vec![change]);

    assert_eq!(events.len(), 1);

    assert_eq!(events[0].side, BookSide::Bid);
    assert_eq!(events[0].price, price(100));
    assert_eq!(events[0].quantity_base, Some(Decimal::ZERO));
}

// -----------------------------------------------------------------------------
// Unchanged update suppression
// -----------------------------------------------------------------------------

#[test]
fn suppresses_unchanged_levels() {
    let mut book = BookStore::new();

    book.apply_snapshot(vec![(price(100), level(10))], vec![])
        .unwrap();

    let change = book
        .set_level(BookSide::Bid, price(100), level(10))
        .unwrap();

    assert!(change.is_none());
}

// -----------------------------------------------------------------------------
// Snapshot differencing
// -----------------------------------------------------------------------------

#[test]
fn snapshot_differences_emit_only_changes() {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![(price(100), level(10)), (price(99), level(20))],
        vec![],
    )
    .unwrap();

    let changes = book
        .apply_snapshot(vec![(price(100), level(5)), (price(98), level(30))], vec![])
        .unwrap();

    let events = emit_l2_changes(&envelope(4000), changes);

    assert_eq!(events.len(), 3);

    // Quantity replacement.
    assert!(events.iter().any(|event| {
        event.side == BookSide::Bid
            && event.price == price(100)
            && event.quantity_base == Some(price(5))
    }));

    // Removed level.
    assert!(events.iter().any(|event| {
        event.side == BookSide::Bid
            && event.price == price(99)
            && event.quantity_base == Some(Decimal::ZERO)
    }));

    // New level.
    assert!(events.iter().any(|event| {
        event.side == BookSide::Bid
            && event.price == price(98)
            && event.quantity_base == Some(price(30))
    }));
}

// -----------------------------------------------------------------------------
// Quantity normalization preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_quantity_representations() {
    let changes = vec![(
        BookSide::Ask,
        price(100),
        BookLevel {
            quantity_base: Some(price(2)),
            quantity_quote: Some(price(200)),
            quantity_contracts: None,
            order_count: Some(4),
        },
    )];

    let events = emit_l2_changes(&envelope(5000), changes);

    assert_eq!(events.len(), 1);

    assert_eq!(events[0].side, BookSide::Ask);
    assert_eq!(events[0].price, price(100));

    assert_eq!(events[0].quantity_base, Some(price(2)));
    assert_eq!(events[0].quantity_quote, Some(price(200)));
    assert_eq!(events[0].quantity_contracts, None);

    assert_eq!(events[0].order_count, Some(4));
}

// -----------------------------------------------------------------------------
// Envelope preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_event_envelope() {
    let metadata = envelope(6000);

    let changes = vec![
        (BookSide::Bid, price(100), level(10)),
        (BookSide::Ask, price(101), level(15)),
    ];

    let events = emit_l2_changes(&metadata, changes);

    assert_eq!(events.len(), 2);

    for event in &events {
        assert_eq!(event.envelope, metadata);
    }
}

// -----------------------------------------------------------------------------
// Empty changes
// -----------------------------------------------------------------------------

#[test]
fn empty_changes_produce_no_events() {
    let events = emit_l2_changes(&envelope(7000), vec![]);

    assert!(events.is_empty());
}
