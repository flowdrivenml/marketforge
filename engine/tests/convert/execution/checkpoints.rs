use std::collections::HashSet;

use arrow_buffer::i256;

use marketforge_engine::historical::convert::{
    ConversionDecimal,
    execution::ConversionCheckpointBuilder,
    reference::{ReferenceKey, ReferenceObservation, ReferenceState, ReferenceUnavailable},
};

const BTCUSDT: &str = "binance:spot:spot:BTCUSDT:trade";
const ETHBTC: &str = "binance:spot:spot:ETHBTC:trade";
const BTCUSDC: &str = "binance:spot:spot:BTCUSDC:trade";

fn decimal(value: i64) -> ConversionDecimal {
    ConversionDecimal::new(i256::from_i128(i128::from(value) * 10_i128.pow(28))).unwrap()
}

fn observation(
    dataset_id: u64,
    stream_rank: u32,
    stream_id: &str,
    timestamp_ns: i64,
    ordinal: u64,
    price: i64,
) -> ReferenceObservation {
    ReferenceObservation {
        dataset_id,
        stream_id: stream_id.to_owned(),
        key: ReferenceKey::new(timestamp_ns, stream_rank, 0, ordinal),
        price: decimal(price),
    }
}

fn builder() -> ConversionCheckpointBuilder {
    ConversionCheckpointBuilder::new(HashSet::from([0, 1, 2]))
}

// -----------------------------------------------------------------------------
// Initial state
// -----------------------------------------------------------------------------

#[test]
fn initial_checkpoint_is_empty() {
    let builder = builder();

    let checkpoint = builder.checkpoint(0);

    assert_eq!(checkpoint.partition_id, 0);
    assert_eq!(checkpoint.events_before_partition, 0);
    assert!(checkpoint.state.is_empty());
}

// -----------------------------------------------------------------------------
// Reference updates
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_contains_latest_reference_observations() {
    let mut builder = builder();

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    builder
        .observe(observation(106, 0, BTCUSDT, 110, 1, 60_100))
        .unwrap();

    builder
        .observe(observation(129, 1, ETHBTC, 115, 0, 1))
        .unwrap();

    let checkpoint = builder.checkpoint(1);

    assert_eq!(checkpoint.events_before_partition, 3);
    assert_eq!(checkpoint.state.len(), 2);

    let reference = checkpoint
        .state
        .evaluate(BTCUSDT, ReferenceKey::new(120, 1, 0, 0), 1_000)
        .unwrap();

    assert_eq!(reference.key.timestamp_ns, 110);
    assert_eq!(reference.price, decimal(60_100));
}

// -----------------------------------------------------------------------------
// Snapshot independence
// -----------------------------------------------------------------------------

#[test]
fn checkpoints_are_independent_snapshots() {
    let mut builder = builder();

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    let first = builder.checkpoint(1);

    builder
        .observe(observation(106, 0, BTCUSDT, 200, 1, 61_000))
        .unwrap();

    let second = builder.checkpoint(2);

    let key = ReferenceKey::new(250, 1, 0, 0);

    let first_reference = first.state.evaluate(BTCUSDT, key, 1_000).unwrap();

    let second_reference = second.state.evaluate(BTCUSDT, key, 1_000).unwrap();

    assert_eq!(first_reference.price, decimal(60_000));
    assert_eq!(second_reference.price, decimal(61_000));
}

// -----------------------------------------------------------------------------
// No future-reference leakage
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_excludes_boundary_event() {
    let mut builder = builder();

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    // Partition 1 begins at event 101.
    let checkpoint = builder.checkpoint(1);

    // Event 101 belongs to partition 1, not its checkpoint.
    builder
        .observe(observation(106, 0, BTCUSDT, 101, 1, 61_000))
        .unwrap();

    let reference = checkpoint
        .state
        .evaluate(BTCUSDT, ReferenceKey::new(101, 0, 0, 1), 1_000)
        .unwrap();

    assert_eq!(reference.key.timestamp_ns, 100);
    assert_eq!(reference.price, decimal(60_000));
}

// -----------------------------------------------------------------------------
// Non-reference events
// -----------------------------------------------------------------------------

#[test]
fn ignores_non_reference_streams() {
    let mut builder = ConversionCheckpointBuilder::new(HashSet::from([0]));

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    builder
        .observe(observation(129, 1, ETHBTC, 110, 0, 1))
        .unwrap();

    let checkpoint = builder.checkpoint(1);

    assert_eq!(checkpoint.events_before_partition, 2);
    assert_eq!(checkpoint.state.len(), 1);

    assert!(
        checkpoint
            .state
            .evaluate(ETHBTC, ReferenceKey::new(120, 2, 0, 0), 1_000,)
            .is_err()
    );
}

// -----------------------------------------------------------------------------
// Staleness
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_preserves_staleness_behavior() {
    let mut builder = builder();

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    let checkpoint = builder.checkpoint(1);

    let result = checkpoint
        .state
        .evaluate(BTCUSDT, ReferenceKey::new(200, 1, 0, 0), 50);

    assert_eq!(result, Err(ReferenceUnavailable::Stale));
}

// -----------------------------------------------------------------------------
// Sequential equivalence
// -----------------------------------------------------------------------------

#[test]
fn checkpointed_execution_matches_sequential_reference_state() {
    let events = vec![
        observation(106, 0, BTCUSDT, 100, 0, 60_000),
        observation(129, 1, ETHBTC, 110, 0, 1),
        observation(130, 2, BTCUSDC, 120, 0, 59_900),
        observation(106, 0, BTCUSDT, 130, 1, 60_100),
        observation(129, 1, ETHBTC, 140, 1, 1),
        observation(130, 2, BTCUSDC, 150, 1, 59_950),
    ];

    let mut sequential = ReferenceState::new();

    let mut sequential_results = Vec::new();

    for event in &events {
        let result = sequential
            .evaluate(BTCUSDT, event.key, 1_000)
            .ok()
            .map(|reference| reference.key);

        sequential_results.push(result);

        sequential.update(event.clone()).unwrap();
    }

    // Build checkpoint immediately before event index 3.
    let mut checkpoint_builder = builder();

    for event in &events[..3] {
        checkpoint_builder.observe(event.clone()).unwrap();
    }

    let checkpoint = checkpoint_builder.checkpoint(1);

    assert_eq!(checkpoint.events_before_partition, 3);

    let mut worker_state = checkpoint.state;

    for (index, event) in events.iter().enumerate().skip(3) {
        let result = worker_state
            .evaluate(BTCUSDT, event.key, 1_000)
            .ok()
            .map(|reference| reference.key);

        assert_eq!(
            result, sequential_results[index],
            "checkpointed reference mismatch at event {index}"
        );

        worker_state.update(event.clone()).unwrap();
    }
}

// -----------------------------------------------------------------------------
// Event accounting
// -----------------------------------------------------------------------------

#[test]
fn counts_all_events_including_non_references() {
    let mut builder = ConversionCheckpointBuilder::new(HashSet::from([0]));

    builder
        .observe(observation(106, 0, BTCUSDT, 100, 0, 60_000))
        .unwrap();

    builder
        .observe(observation(129, 1, ETHBTC, 110, 0, 1))
        .unwrap();

    builder
        .observe(observation(130, 2, BTCUSDC, 120, 0, 59_900))
        .unwrap();

    assert_eq!(builder.events_seen(), 3);
    assert_eq!(builder.reference_count(), 1);
}
