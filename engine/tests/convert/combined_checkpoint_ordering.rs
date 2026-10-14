use std::collections::HashSet;

use arrow_buffer::i256;

use marketforge_engine::process::convert::{
    ConversionDecimal,
    execution::{
        CombinedCheckpointAccumulator, CombinedReferenceAlignment, CombinedReferenceSelection,
    },
    reference::{ReferenceKey, ReferenceObservation, ReferenceUnavailable},
};

const ETHBTC: &str = "binance:spot:spot:ETHBTC:trade";
const BTCUSDC: &str = "binance:spot:spot:BTCUSDC:trade";

fn selection() -> CombinedReferenceSelection {
    CombinedReferenceSelection {
        internal_streams: HashSet::from([ETHBTC.to_owned(), BTCUSDC.to_owned()]),
        external_streams: HashSet::new(),
    }
}

fn observation(
    dataset_id: u64,
    stream_id: &str,
    timestamp_ns: i64,
    rank: u32,
    ordinal: u64,
    price: i128,
) -> ReferenceObservation {
    ReferenceObservation {
        dataset_id,
        stream_id: stream_id.to_owned(),
        key: ReferenceKey::new(timestamp_ns, rank, 0, ordinal),
        price: ConversionDecimal::new(i256::from_i128(price)).unwrap(),
    }
}

#[test]
fn checkpoint_preserves_equal_timestamp_internal_references() {
    let alignment = CombinedReferenceAlignment::new(selection(), None).unwrap();

    let mut accumulator = CombinedCheckpointAccumulator::new(alignment);

    // First trade at timestamp 100, rank 0.
    accumulator.advance_before(100).unwrap();

    accumulator
        .observe_internal(observation(129, ETHBTC, 100, 0, 1, 5))
        .unwrap();

    accumulator.finish_event().unwrap();

    // Second trade at the same timestamp, rank 1.
    accumulator.advance_before(100).unwrap();

    accumulator
        .observe_internal(observation(130, BTCUSDC, 100, 1, 1, 60_000))
        .unwrap();

    accumulator.finish_event().unwrap();

    // Partition boundary before the next event.
    let checkpoint = accumulator.checkpoint(1, 100);

    assert_eq!(checkpoint.start_row, 2);
    assert_eq!(checkpoint.start_timestamp_ns, 100);
    assert_eq!(checkpoint.state.len(), 2);

    // Restore the reference state.
    let restored =
        CombinedReferenceAlignment::from_checkpoint(selection(), None, checkpoint.state.snapshot())
            .unwrap();

    // Depth event at the same timestamp, rank 2.
    let current = ReferenceKey::new(100, 2, 0, 1);

    let ethbtc = restored.state().evaluate(ETHBTC, current, 100).unwrap();

    let btcusdc = restored.state().evaluate(BTCUSDC, current, 100).unwrap();

    assert_eq!(ethbtc.dataset_id, 129);
    assert_eq!(btcusdc.dataset_id, 130);

    assert_eq!(ethbtc.key.timestamp_ns, 100);
    assert_eq!(btcusdc.key.timestamp_ns, 100);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — EQUAL-TIMESTAMP CHECKPOINT");
    println!("{}", "=".repeat(90));

    println!("Checkpoint row    : {}", checkpoint.start_row);
    println!("Timestamp         : {}", checkpoint.start_timestamp_ns);
    println!("Reference streams : {}", restored.state().len());
    println!("STATUS            : PASS");
}

#[test]
fn checkpoint_rejects_future_equal_timestamp_reference() {
    let alignment = CombinedReferenceAlignment::new(selection(), None).unwrap();

    let mut accumulator = CombinedCheckpointAccumulator::new(alignment);

    accumulator.advance_before(100).unwrap();

    accumulator
        .observe_internal(observation(129, ETHBTC, 100, 2, 1, 5))
        .unwrap();

    accumulator.finish_event().unwrap();

    let checkpoint = accumulator.checkpoint(1, 100);

    let current = ReferenceKey::new(100, 1, 0, 1);

    assert_eq!(
        checkpoint.state.evaluate(ETHBTC, current, 100).unwrap_err(),
        ReferenceUnavailable::Future,
    );
}

#[test]
fn checkpoint_snapshot_is_independent() {
    let alignment = CombinedReferenceAlignment::new(selection(), None).unwrap();

    let mut accumulator = CombinedCheckpointAccumulator::new(alignment);

    accumulator.advance_before(100).unwrap();

    accumulator
        .observe_internal(observation(129, ETHBTC, 100, 0, 1, 5))
        .unwrap();

    accumulator.finish_event().unwrap();

    let checkpoint = accumulator.checkpoint(1, 100);

    accumulator.advance_before(101).unwrap();

    accumulator
        .observe_internal(observation(130, BTCUSDC, 101, 1, 1, 60_000))
        .unwrap();

    accumulator.finish_event().unwrap();

    assert_eq!(checkpoint.state.len(), 1);
    assert_eq!(accumulator.state().len(), 2);
}
