use marketforge_engine::shared::{
    domain::canonical::{DepthEventBoundary, DepthSourceOperation},
    persistence::parquet::{DepthEventBatchBuilder, IndexedDepthEvent},
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, path::Path};

pub fn write_index(path: &Path, timestamps: &[i64]) -> Vec<IndexedDepthEvent> {
    let mut builder = DepthEventBatchBuilder::with_capacity(timestamps.len());
    let mut offset = 0;
    let mut expected = Vec::new();
    for (ordinal, &timestamp) in timestamps.iter().enumerate() {
        let count = if ordinal == 3 { 0 } else { 2 };
        let event = IndexedDepthEvent {
            event_ordinal: ordinal as u64,
            event_timestamp_ns: timestamp,
            canonical_row_offset: offset,
            canonical_row_count: count,
            system_timestamp_ns: None,
            sequence_start: Some(ordinal as u64),
            sequence_end: Some(ordinal as u64),
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
        };
        offset += count;
        builder.push(event.clone());
        expected.push(event);
    }
    let batch = builder.finish().unwrap().unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    for start in (0..batch.num_rows()).step_by(2) {
        writer
            .write(&batch.slice(start, (batch.num_rows() - start).min(2)))
            .unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    expected
}
