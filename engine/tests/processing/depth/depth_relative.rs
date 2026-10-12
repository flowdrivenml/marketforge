use rust_decimal::Decimal;

use marketforge_engine::{
    book::{BookLevel, BookStore},
    canonical::BookSide,
    formats::depth::{ExtractedLevel, RelativeAction, apply_relative_update},
    job::{ContractKind, InstrumentKind, InstrumentSpec, QuantityEncoding},
};

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn level(price: &str, quantity: &str) -> ExtractedLevel {
    ExtractedLevel {
        price: decimal(price),
        quantity: decimal(quantity),
        order_count: None,
    }
}

fn spot_instrument() -> InstrumentSpec {
    InstrumentSpec {
        instrument_kind: InstrumentKind::Spot,
        contract_kind: None,
        tick_size: decimal("0.1"),
        contract_value: None,
        contract_value_asset: None,
    }
}

fn linear_instrument() -> InstrumentSpec {
    InstrumentSpec {
        instrument_kind: InstrumentKind::Perpetual,
        contract_kind: Some(ContractKind::Linear),
        tick_size: decimal("0.1"),
        contract_value: Some(decimal("0.0001")),
        contract_value_asset: Some("BTC".to_owned()),
    }
}

fn inverse_instrument() -> InstrumentSpec {
    InstrumentSpec {
        instrument_kind: InstrumentKind::Perpetual,
        contract_kind: Some(ContractKind::Inverse),
        tick_size: decimal("0.1"),
        contract_value: Some(decimal("1")),
        contract_value_asset: Some("USD".to_owned()),
    }
}

fn initialized_book() -> BookStore {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![(
            decimal("78500"),
            BookLevel {
                quantity_base: Some(decimal("10")),
                quantity_quote: Some(decimal("785000")),
                quantity_contracts: None,
                order_count: None,
            },
        )],
        vec![(
            decimal("78600"),
            BookLevel {
                quantity_base: Some(decimal("5")),
                quantity_quote: Some(decimal("393000")),
                quantity_contracts: None,
                order_count: None,
            },
        )],
    )
    .unwrap();

    book
}

#[test]
fn adds_to_existing_level() {
    let mut book = initialized_book();

    let changes = apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78500", "3"),
        RelativeAction::Add,
        QuantityEncoding::Base,
        &spot_instrument(),
    )
    .unwrap();

    assert_eq!(changes.len(), 1);

    let updated = book.bids().get(&decimal("78500")).unwrap();

    assert_eq!(updated.quantity_base, Some(decimal("13")));
    assert_eq!(updated.quantity_quote, Some(decimal("1020500")));
}

#[test]
fn creates_new_level() {
    let mut book = initialized_book();

    apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78400", "2"),
        RelativeAction::Add,
        QuantityEncoding::Base,
        &spot_instrument(),
    )
    .unwrap();

    assert_eq!(
        book.bids().get(&decimal("78400")).unwrap().quantity_base,
        Some(decimal("2"))
    );
}

#[test]
fn subtracts_from_existing_level() {
    let mut book = initialized_book();

    apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78500", "4"),
        RelativeAction::Subtract,
        QuantityEncoding::Base,
        &spot_instrument(),
    )
    .unwrap();

    assert_eq!(
        book.bids().get(&decimal("78500")).unwrap().quantity_base,
        Some(decimal("6"))
    );
}

#[test]
fn deletes_level_when_quantity_reaches_zero() {
    let mut book = initialized_book();

    let changes = apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78500", "10"),
        RelativeAction::Subtract,
        QuantityEncoding::Base,
        &spot_instrument(),
    )
    .unwrap();

    assert_eq!(changes.len(), 1);
    assert!(!book.bids().contains_key(&decimal("78500")));

    assert_eq!(changes[0].2.quantity_base, Some(Decimal::ZERO));
}

#[test]
fn rejects_excessive_subtraction_without_mutation() {
    let mut book = initialized_book();

    let before = book.bids().clone();

    let result = apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78500", "11"),
        RelativeAction::Subtract,
        QuantityEncoding::Base,
        &spot_instrument(),
    );

    assert!(result.is_err());
    assert_eq!(book.bids(), &before);
}

#[test]
fn rejects_subtraction_from_missing_level() {
    let mut book = initialized_book();

    let result = apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78400", "1"),
        RelativeAction::Subtract,
        QuantityEncoding::Base,
        &spot_instrument(),
    );

    assert!(result.is_err());
    assert!(!book.bids().contains_key(&decimal("78400")));
}

#[test]
fn normalizes_linear_contract_quantities() {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![(
            decimal("78500"),
            BookLevel {
                quantity_base: Some(decimal("0.01")),
                quantity_quote: Some(decimal("785")),
                quantity_contracts: Some(decimal("100")),
                order_count: None,
            },
        )],
        vec![],
    )
    .unwrap();

    apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("78500", "50"),
        RelativeAction::Add,
        QuantityEncoding::Contracts,
        &linear_instrument(),
    )
    .unwrap();

    let updated = book.bids().get(&decimal("78500")).unwrap();

    assert_eq!(updated.quantity_contracts, Some(decimal("150")));
    assert_eq!(updated.quantity_base, Some(decimal("0.015")));
    assert_eq!(updated.quantity_quote, Some(decimal("1177.5")));
}

#[test]
fn normalizes_inverse_contract_quantities() {
    let mut book = BookStore::new();

    book.apply_snapshot(
        vec![(
            decimal("80000"),
            BookLevel {
                quantity_base: Some(decimal("0.00125")),
                quantity_quote: Some(decimal("100")),
                quantity_contracts: Some(decimal("100")),
                order_count: None,
            },
        )],
        vec![],
    )
    .unwrap();

    apply_relative_update(
        &mut book,
        BookSide::Bid,
        level("80000", "100"),
        RelativeAction::Add,
        QuantityEncoding::Contracts,
        &inverse_instrument(),
    )
    .unwrap();

    let updated = book.bids().get(&decimal("80000")).unwrap();

    assert_eq!(updated.quantity_contracts, Some(decimal("200")));
    assert_eq!(updated.quantity_quote, Some(decimal("200")));
    assert_eq!(updated.quantity_base, Some(decimal("0.0025")));
}
