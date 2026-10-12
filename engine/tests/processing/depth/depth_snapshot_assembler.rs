use marketforge_engine::{
    canonical::BookSide,
    formats::depth::{DepthSnapshotAssembler, transforms::ExtractedLevel},
};

use rust_decimal::Decimal;

fn level(price: i64, quantity: i64) -> ExtractedLevel {
    ExtractedLevel {
        price: Decimal::from(price),
        quantity: Decimal::from(quantity),
        order_count: None,
    }
}

#[test]
fn groups_snapshot_rows() {
    let mut assembler = DepthSnapshotAssembler::new();

    assembler
        .push(1000, Some(100), BookSide::Bid, level(99, 5))
        .unwrap();

    assembler
        .push(1000, Some(100), BookSide::Ask, level(101, 7))
        .unwrap();

    assert!(assembler.has_pending());

    let snapshot = assembler.finish().unwrap().unwrap();

    assert_eq!(snapshot.timestamp_ns, 1000);
    assert_eq!(snapshot.sequence, Some(100));

    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.asks.len(), 1);

    assert!(!assembler.has_pending());
}

#[test]
fn rejects_inconsistent_snapshot_sequence() {
    let mut assembler = DepthSnapshotAssembler::new();

    assembler
        .push(1000, Some(100), BookSide::Bid, level(99, 5))
        .unwrap();

    let result = assembler.push(1000, Some(101), BookSide::Ask, level(101, 7));

    assert!(result.is_err());
}

#[test]
fn rejects_incomplete_snapshot() {
    let mut assembler = DepthSnapshotAssembler::new();

    assembler
        .push(1000, Some(100), BookSide::Bid, level(99, 5))
        .unwrap();

    assert!(assembler.finish().is_err());
}
