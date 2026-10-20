use std::sync::Arc;

use marketforge_engine::{
    formats::depth::{DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation},
    live::model::{
        cursor::{
            BookGeneration, ConnectionGeneration, LogEpoch, StreamCursor, SubscriptionGeneration,
        },
        envelope::{EventProvenance, EventTiming, LiveEvent, LiveL2Group, LivePayload},
        identity::StreamId,
    },
};

pub fn l2_event(stream: &str, epoch: u64, offset: u64) -> Arc<LiveEvent> {
    let stream_id = StreamId(stream.to_owned());

    let cursor = StreamCursor::new(stream_id.clone(), LogEpoch(epoch), offset);

    let timestamp = 1_700_000_000_000_000_000_i64;

    let group = LiveL2Group::new(
        DepthEventBoundary::Changes,
        DepthSourceOperation::AbsoluteUpdate,
        Vec::new(),
        DepthSourceEventMetadata {
            source_event_ordinal: Some(offset),
            event_timestamp_ns: timestamp,
            system_timestamp_ns: None,
            sequence_start: Some(offset),
            sequence_end: Some(offset),
        },
    );

    let provenance = EventProvenance {
        source_stream: stream_id,
        connection_generation: ConnectionGeneration(1),
        subscription_generation: SubscriptionGeneration(1),
        native_sequence: None,
        native_trade_id: None,
        book_generation: Some(BookGeneration(1)),
    };

    let timing = EventTiming {
        exchange_timestamp_ns: Some(timestamp),
        received_timestamp_ns: timestamp,
    };

    Arc::new(
        LiveEvent::try_new(cursor, LivePayload::L2Group(group), provenance, timing)
            .expect("valid LiveEvent fixture"),
    )
}
