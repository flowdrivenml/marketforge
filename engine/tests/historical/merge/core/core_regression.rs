use marketforge_engine::{
    historical::merge::*,
    shared::{
        domain::canonical::{DepthEventBoundary, DepthSourceOperation},
        error::{MarketForgeError, Result},
        persistence::parquet::IndexedDepthEvent,
    },
};
use std::collections::VecDeque;
struct Source {
    events: VecDeque<MergeEventRef>,
    fail: bool,
}
impl MergeEventSource for Source {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        if self.events.is_empty() && self.fail {
            return Err(MarketForgeError::InvalidConfiguration(
                "synthetic source error".into(),
            ));
        }
        Ok(self.events.pop_front())
    }
}
struct DepthSource(Source);
impl DepthEventSource for DepthSource {
    fn next_event(&mut self) -> Result<Option<DepthEventRef>> {
        self.0.next_event().map(|v| {
            v.map(|e| match e {
                MergeEventRef::Depth(d) => d,
                _ => unreachable!(),
            })
        })
    }
    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        &[]
    }
    fn timestamp_regressions_dropped(&self) -> u64 {
        2
    }
}
fn trade(dataset_id: u64, timestamp: i64, task_id: u64, event_ordinal: u64) -> MergeEventRef {
    MergeEventRef::Trade(TradeEventRef {
        dataset_id,
        task_id,
        event_timestamp_ns: timestamp,
        event_ordinal,
        canonical_row_offset: event_ordinal,
    })
}
fn depth(timestamp: i64, ordinal: u64) -> MergeEventRef {
    MergeEventRef::Depth(DepthEventRef {
        dataset_id: 77,
        task_id: 8,
        event: IndexedDepthEvent {
            event_ordinal: ordinal,
            canonical_row_offset: ordinal * 3,
            canonical_row_count: 3,
            event_timestamp_ns: timestamp,
            system_timestamp_ns: Some(timestamp + 1),
            sequence_start: Some(100 + ordinal),
            sequence_end: Some(100 + ordinal),
            event_boundary: if ordinal == 0 {
                DepthEventBoundary::Initialization
            } else {
                DepthEventBoundary::Changes
            },
            source_operation: if ordinal == 0 {
                DepthSourceOperation::Snapshot
            } else {
                DepthSourceOperation::AbsoluteUpdate
            },
        },
    })
}
fn source(events: Vec<MergeEventRef>, fail: bool) -> Source {
    Source {
        events: events.into(),
        fail,
    }
}
#[test]
fn exact_merge_baseline_transcript() {
    let mut transcript = String::new();
    for (label, streams) in [
        (
            "mixed",
            vec![
                source(
                    vec![
                        trade(77, 10, 0, 0),
                        trade(77, 20, 0, 1),
                        trade(77, 20, 1, 0),
                    ],
                    false,
                ),
                source(vec![depth(10, 0), depth(20, 1)], false),
                source(vec![], false),
            ],
        ),
        (
            "regression",
            vec![source(
                vec![trade(1, 10, 0, 0), trade(1, 20, 0, 1), trade(1, 15, 0, 2)],
                false,
            )],
        ),
        ("source_error", vec![source(vec![trade(1, 10, 0, 0)], true)]),
        ("empty", vec![source(vec![], false)]),
    ] {
        let mut merger = KWayEventMerger::new(
            streams
                .into_iter()
                .map(|s| Box::new(s) as Box<dyn MergeEventSource>)
                .collect(),
        )
        .unwrap();
        loop {
            let next = merger.next_event();
            transcript.push_str(&format!(
                "{label}: {next:?}; count={}\n",
                merger.events_emitted()
            ));
            if !matches!(next, Ok(Some(_))) {
                break;
            }
        }
    }
    let mut merger = KWayDepthMerger::new(vec![Box::new(DepthSource(source(
        vec![depth(10, 0), depth(20, 1), depth(15, 2)],
        false,
    )))])
    .unwrap();
    loop {
        let next = merger.next_event();
        transcript.push_str(&format!(
            "depth_regression: {next:?}; count={}; dropped={}\n",
            merger.events_emitted(),
            merger.timestamp_regressions_dropped().unwrap()
        ));
        if !matches!(next, Ok(Some(_))) {
            break;
        }
    }
    println!("{transcript}");
    let expected = include_str!("../fixtures/core-regression.txt");
    assert_eq!(transcript, expected);
}
