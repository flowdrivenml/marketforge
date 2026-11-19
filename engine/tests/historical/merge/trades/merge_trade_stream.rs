use crate::publication::trade_publication;
use marketforge_engine::historical::merge::{TradeEventRef, TradeStream, build_trade_inventory};

fn assert_stream(range: Option<(i64, i64)>, expected: &[(u64, u64, i64)]) {
    let workspace = tempfile::tempdir().unwrap();
    let inventory = build_trade_inventory(&[trade_publication(workspace.path(), 11)]).unwrap();
    let mut stream = match range {
        Some((start, end)) => TradeStream::new_range(11, inventory, start, end).unwrap(),
        None => TradeStream::new(11, inventory).unwrap(),
    };
    for &(task_id, ordinal, timestamp) in expected {
        assert_eq!(
            stream.next_event().unwrap(),
            Some(TradeEventRef {
                dataset_id: 11,
                task_id,
                event_ordinal: ordinal,
                canonical_row_offset: ordinal,
                event_timestamp_ns: timestamp,
            })
        );
    }
    assert!(stream.next_event().unwrap().is_none());
    assert_eq!(stream.events_emitted(), expected.len() as u64);
}

#[test]
fn streams_generated_trade_tasks() {
    assert_stream(None, &[(0, 0, 100), (0, 1, 200), (1, 0, 300), (1, 1, 400)]);
}

#[test]
fn streams_generated_trade_timestamp_ranges() {
    assert_stream(Some((200, 400)), &[(0, 1, 200), (1, 0, 300)]);
}
