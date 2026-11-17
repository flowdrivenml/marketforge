//! Fixed-size provenance frames are validated as a stream before publication.
use crate::{
    historical::normalize::{metrics::TaskMetricsReport, streams::StreamOutput},
    shared::error::{MarketForgeError, Result},
};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read},
    path::Path,
};
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    canonical_row: u64,
    task_id: u64,
    source_record: u64,
}
pub(super) fn validate(
    path: &Path,
    stream: &StreamOutput,
    reports: &HashMap<u64, &TaskMetricsReport>,
) -> Result<()> {
    let invalid = |s: &str| {
        MarketForgeError::InvalidConfiguration(format!(
            "trade source-position validation: {}: {s}",
            path.display()
        ))
    };
    let file = std::fs::File::open(path).map_err(|e| invalid(&e.to_string()))?;
    let mut reader = BufReader::with_capacity(4096, file);
    let mut row = 0u64;
    loop {
        // All fields are fixed-width integers: 256 is larger than any valid frame.
        let mut frame = Vec::with_capacity(256);
        let n = (&mut reader)
            .take(257)
            .read_until(b'\n', &mut frame)
            .map_err(|e| invalid(&e.to_string()))?;
        if n == 0 {
            break;
        }
        if n > 256 || frame.last() != Some(&b'\n') {
            return Err(invalid("oversized or incomplete frame"));
        }
        let p: Position = serde_json::from_slice(&frame).map_err(|e| invalid(&e.to_string()))?;
        if p.canonical_row != row || p.source_record == 0 {
            return Err(invalid(
                "noncontiguous canonical row or invalid source ordinal",
            ));
        }
        let owner = stream.sources.iter().find(|s| {
            s.task_id == p.task_id
                && if s.ranges.is_empty() {
                    row >= s.start_row && row < s.end_row
                } else {
                    s.ranges
                        .iter()
                        .any(|r| row >= r.start_row && row < r.end_row)
                }
        });
        let report = reports
            .get(&p.task_id)
            .ok_or_else(|| invalid("unknown task"))?;
        if owner.is_none()
            || p.source_record
                > report
                    .source
                    .as_ref()
                    .map_or(report.counters.records_read, |s| {
                        s.maximum_source_ordinal.max(s.physical_records_read)
                    })
        {
            return Err(invalid("source ownership/ordinal mismatch"));
        }
        row = row
            .checked_add(1)
            .ok_or_else(|| invalid("row counter overflow"))?;
    }
    let expected = stream.sources.iter().try_fold(0u64, |n, s| {
        n.checked_add(s.row_count()?)
            .ok_or_else(|| invalid("row count overflow"))
    })?;
    if row != expected {
        return Err(invalid("source positions do not cover every canonical row"));
    }
    Ok(())
}
