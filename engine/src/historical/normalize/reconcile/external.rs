//! Bounded two-way external merges. Run inventories live on disk: pass/count
//! counters replace an archive-sized list of run paths.
use super::spool::{Leased, Spool, SpoolReader, Workspace};
use super::{SourceObservation, TradeEntry, reliable_source_identity};
use crate::historical::normalize::progress::{ProgressReporter, ProgressTaskId};
use crate::shared::error::{MarketForgeError, Result};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};
fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
fn identity(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    // Enum declaration order is internal only; equality/scope exactly match the
    // reference key. No business interpretation depends on exchange ordering.
    (
        a.trade.envelope.exchange as u8,
        &a.trade.envelope.stream_id,
        a.trade.envelope.instrument_id,
        &a.trade.trade_id,
    )
        .cmp(&(
            b.trade.envelope.exchange as u8,
            &b.trade.envelope.stream_id,
            b.trade.envelope.instrument_id,
            &b.trade.trade_id,
        ))
}
fn precedence(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    (a.priority, a.source, a.ordinal).cmp(&(b.priority, b.source, b.ordinal))
}
fn chronological(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    a.trade
        .envelope
        .event_timestamp_ns
        .cmp(&b.trade.envelope.event_timestamp_ns)
        .then_with(|| precedence(a, b))
}
fn next(reader: &mut SpoolReader<TradeEntry>) -> Result<Option<Leased<TradeEntry>>> {
    let mut row = reader.next()?;
    if let Some(row) = row.as_mut() {
        row.compact_accounting()?;
    }
    Ok(row)
}
struct Cursor {
    row: Leased<TradeEntry>,
    input: usize,
    compare: fn(&TradeEntry, &TradeEntry) -> Ordering,
}
impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Cursor {}
impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.compare)(&other.row.value, &self.row.value).then_with(|| other.input.cmp(&self.input))
    }
}
/// A bounded final merge may feed Parquet directly. Identity resolution always
/// materializes its checked result before chronological publication starts.
pub(crate) enum SortedRows {
    Append(Vec<(usize, Spool)>),
    Spool(Spool),
    Merge {
        workspace: Workspace,
        group: u64,
        pass: u64,
        count: u64,
        compare: fn(&TradeEntry, &TradeEntry) -> Ordering,
    },
}
impl SortedRows {
    fn into_spool(self) -> Result<Spool> {
        match self {
            Self::Spool(spool) => Ok(spool),
            _ => Err(invalid("identity sort must materialize")),
        }
    }
    pub fn rows(self) -> Result<Rows> {
        let mut append_sources = None;
        let (spools, compare) = match self {
            Self::Append(inputs) => {
                let (sources, spools): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
                append_sources = Some(sources);
                (
                    spools,
                    chronological as fn(&TradeEntry, &TradeEntry) -> Ordering,
                )
            }
            Self::Spool(spool) => (
                vec![spool],
                chronological as fn(&TradeEntry, &TradeEntry) -> Ordering,
            ),
            Self::Merge {
                workspace,
                group,
                pass,
                count,
                compare,
            } => (
                (0..count)
                    .map(|index| workspace.run(group, pass, index))
                    .collect(),
                compare,
            ),
        };
        let mut rows = Rows {
            readers: Vec::with_capacity(if append_sources.is_some() {
                1
            } else {
                spools.len()
            }),
            heap: BinaryHeap::with_capacity(if append_sources.is_some() {
                0
            } else {
                spools.len()
            }),
            spools,
            done: false,
            failed: false,
            append_sources,
            append_index: 0,
            append_first: true,
            previous_timestamp: None,
        };
        if rows.append_sources.is_some() {
            return Ok(rows);
        }
        for input in 0..rows.spools.len() {
            let mut reader = rows.spools[input].trade_reader()?;
            if let Some(row) = next(&mut reader)? {
                rows.heap.push(Cursor {
                    row,
                    input,
                    compare,
                });
            }
            rows.readers.push(reader);
        }
        Ok(rows)
    }
}
pub(crate) struct Rows {
    readers: Vec<SpoolReader<TradeEntry>>,
    heap: BinaryHeap<Cursor>,
    spools: Vec<Spool>,
    done: bool,
    failed: bool,
    append_sources: Option<Vec<usize>>,
    append_index: usize,
    append_first: bool,
    previous_timestamp: Option<i64>,
}
impl Rows {
    pub fn finish(mut self) -> Result<()> {
        if !self.done || self.failed {
            return Err(invalid("final merge was not completely consumed"));
        }
        self.heap.clear();
        self.readers.clear();
        while let Some(spool) = self.spools.pop() {
            spool.delete()?;
        }
        Ok(())
    }
    fn next_append(&mut self) -> Result<Option<Leased<TradeEntry>>> {
        loop {
            if self.append_index == self.spools.len() {
                self.done = true;
                return Ok(None);
            }
            if self.readers.is_empty() {
                self.readers
                    .push(self.spools[self.append_index].trade_reader()?);
            }
            match next(&mut self.readers[0])? {
                Some(row) => {
                    let timestamp = row.value.trade.envelope.event_timestamp_ns;
                    if row.value.source != self.append_sources.as_ref().unwrap()[self.append_index]
                        || self.previous_timestamp.is_some_and(|last| {
                            if self.append_first {
                                timestamp <= last
                            } else {
                                timestamp < last
                            }
                        })
                    {
                        return Err(invalid(
                            "chronological append spool source/ordering mismatch",
                        ));
                    }
                    self.previous_timestamp = Some(timestamp);
                    self.append_first = false;
                    return Ok(Some(row));
                }
                None => {
                    self.readers.clear();
                    self.append_index += 1;
                    self.append_first = true;
                }
            }
        }
    }
    pub fn next(&mut self) -> Result<Option<Leased<TradeEntry>>> {
        if self.done {
            return Ok(None);
        }
        if self.append_sources.is_some() {
            let result = self.next_append();
            if result.is_err() {
                self.done = true;
                self.failed = true;
            }
            return result;
        }
        let Some(cursor) = self.heap.pop() else {
            self.done = true;
            return Ok(None);
        };
        match next(&mut self.readers[cursor.input]) {
            Ok(Some(row)) => self.heap.push(Cursor {
                row,
                input: cursor.input,
                compare: cursor.compare,
            }),
            Ok(None) => {}
            Err(error) => {
                self.done = true;
                self.failed = true;
                return Err(error);
            }
        }
        Ok(Some(cursor.row))
    }
}
impl Drop for Rows {
    fn drop(&mut self) {
        self.heap.clear();
        self.readers.clear();
        for spool in self.spools.drain(..) {
            if let Err(error) = spool.delete() {
                crate::historical::normalize::progress::log::diagnostic(format_args!(
                    "final merge scratch cleanup failed: {error}"
                ));
            }
        }
    }
}
/// Runs and merge cursors are bounded. The only in-memory inventory is the
/// already-admitted set of sources, never the number of records/runs/identities.
fn sort(
    workspace: &Workspace,
    inputs: Vec<Spool>,
    compare: fn(&TradeEntry, &TradeEntry) -> Ordering,
    reporter: &ProgressReporter,
    id: ProgressTaskId,
    stream: &str,
    chronological_sort: bool,
) -> Result<SortedRows> {
    reporter.stream_phase(
        id.job_id,
        stream,
        if chronological_sort {
            "chronological run generation"
        } else {
            "identity run generation"
        },
    );
    let group = workspace.group()?;
    let mut runs = 0u64;
    let mut record_memory = 0u64;
    let mut chunk: Vec<Leased<TradeEntry>> = Vec::new();
    let flush = |chunk: &mut Vec<Leased<TradeEntry>>, runs: &mut u64| -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let spare = (chunk.len() >= 8192)
            .then(crate::historical::normalize::resources::compute::spare)
            .flatten();
        if let Some(permit) = spare {
            workspace.parallel_sort_run();
            // Only Send/Sync canonical references cross the scope. Quota leases
            // and scratch handles stay on their owning parent thread.
            let _index_memory = workspace.quota().reserve(
                (chunk.len() * std::mem::size_of::<&TradeEntry>()) as u64,
                "parallel run sort references",
            )?;
            let mut rows: Vec<&TradeEntry> = chunk.iter().map(|entry| &entry.value).collect();
            let limits = crate::shared::historical::limits::current().unwrap_or_default();
            let context = crate::shared::historical::limits::current_scope().unwrap_or_default();
            let middle = rows.len() / 2;
            let (left, right) = rows.split_at_mut(middle);
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    let _permit = permit;
                    let _limits =
                        crate::shared::historical::limits::LimitScope::enter(limits, context);
                    right.sort_unstable_by(|a, b| compare(a, b));
                });
                left.sort_unstable_by(|a, b| compare(a, b));
            });
            let output = workspace.run(group, 0, *runs);
            let mut writer = output.writer()?;
            let (mut a, mut b) = (0, middle);
            while a < middle || b < rows.len() {
                let take_left = b == rows.len()
                    || (a < middle && compare(rows[a], rows[b]) != Ordering::Greater);
                let entry = if take_left {
                    let entry = rows[a];
                    a += 1;
                    entry
                } else {
                    let entry = rows[b];
                    b += 1;
                    entry
                };
                writer.push_trade(entry)?;
            }
            writer.finish()?;
            workspace.generated_run();
            *runs = runs
                .checked_add(1)
                .ok_or_else(|| invalid("external run count overflow"))?;
            drop(rows);
            *chunk = Vec::new();
        } else {
            chunk.sort_unstable_by(|a, b| compare(&a.value, &b.value));
            let output = workspace.run(group, 0, *runs);
            let mut writer = output.writer()?;
            for entry in chunk.drain(..) {
                writer.push_trade(&entry.value)?;
            }
            *chunk = Vec::new();
            writer.finish()?;
            workspace.generated_run();
            *runs = runs
                .checked_add(1)
                .ok_or_else(|| invalid("external run count overflow"))?;
        }
        reporter.stage_count(id.job_id, stream, *runs, "sorted runs");
        Ok(())
    };
    for input in inputs {
        {
            let mut reader = input.trade_reader()?;
            while let Some(mut entry) = reader.next()? {
                record_memory = record_memory.max(entry.accounted_bytes());
                entry.compact_accounting()?;
                chunk.push(entry);
                if workspace.quota().used() > workspace.quota().maximum() / 3 {
                    flush(&mut chunk, &mut runs)?;
                }
            }
        }
        input.delete()?;
    }
    flush(&mut chunk, &mut runs)?;
    drop(chunk);
    if runs == 0 {
        let empty = workspace.run(group, 0, 0);
        empty.writer()?.finish()?;
        return Ok(SortedRows::Spool(empty));
    }
    reporter.stream_phase(
        id.job_id,
        stream,
        if chronological_sort {
            "chronological merging"
        } else {
            "identity merging"
        },
    );
    let mut pass = 0u64;
    while runs > 1 {
        reporter.stage_count(id.job_id, stream, pass, "merge passes");
        if chronological_sort && runs <= workspace.fan_in(record_memory) as u64 {
            workspace.merge_pass(runs as usize);
            return Ok(SortedRows::Merge {
                workspace: workspace.clone(),
                group,
                pass,
                count: runs,
                compare,
            });
        }
        workspace.merge_pass(workspace.fan_in(record_memory).min(runs as usize));
        let next_pass = pass
            .checked_add(1)
            .ok_or_else(|| invalid("external merge pass overflow"))?;
        let mut next_count = 0u64;
        let mut index = 0u64;
        while index < runs {
            let left = workspace.run(group, pass, index);
            let output = workspace.run(group, next_pass, next_count);
            if index + 1 == runs {
                left.transfer_to(&output)?;
                index += 1;
            } else {
                let fan = workspace.fan_in(record_memory).min((runs - index) as usize);
                let mut readers = Vec::with_capacity(fan);
                let mut heap = BinaryHeap::with_capacity(fan);
                for offset in 0..fan {
                    let mut reader = workspace
                        .run(group, pass, index + offset as u64)
                        .trade_reader()?;
                    if let Some(row) = next(&mut reader)? {
                        heap.push(Cursor {
                            row,
                            input: offset,
                            compare,
                        });
                    }
                    readers.push(reader);
                }
                let mut writer = output.writer()?;
                while let Some(cursor) = heap.pop() {
                    let input = cursor.input;
                    writer.push_trade(&cursor.row.value)?;
                    drop(cursor);
                    if let Some(row) = next(&mut readers[input])? {
                        heap.push(Cursor {
                            row,
                            input,
                            compare,
                        });
                    }
                }
                writer.finish()?;
                drop(readers);
                for offset in 0..fan {
                    workspace.run(group, pass, index + offset as u64).delete()?;
                }
                index += fan as u64;
            }
            next_count = next_count
                .checked_add(1)
                .ok_or_else(|| invalid("external merge count overflow"))?;
        }
        runs = next_count;
        pass = pass
            .checked_add(1)
            .ok_or_else(|| invalid("external merge pass overflow"))?;
    }
    Ok(SortedRows::Spool(workspace.run(group, pass, 0)))
}
#[cfg(test)]
pub(crate) fn trades(
    workspace: &Workspace,
    inputs: Vec<Spool>,
    observations: &mut BTreeMap<usize, SourceObservation>,
) -> Result<Spool> {
    let sorted = trades_with_progress(
        workspace,
        inputs,
        observations,
        &ProgressReporter::disabled(),
        ProgressTaskId::new(0, 0),
        "",
    )?;
    match sorted {
        SortedRows::Spool(spool) => Ok(spool),
        sorted => {
            let spool = workspace.create()?;
            let mut writer = spool.writer()?;
            let mut rows = sorted.rows()?;
            while let Some(row) = rows.next()? {
                writer.push_trade(&row.value)?;
            }
            rows.finish()?;
            writer.finish()?;
            Ok(spool)
        }
    }
}
pub(crate) fn trades_with_progress(
    workspace: &Workspace,
    inputs: Vec<Spool>,
    observations: &mut BTreeMap<usize, SourceObservation>,
    reporter: &ProgressReporter,
    id: ProgressTaskId,
    stream: &str,
) -> Result<SortedRows> {
    for o in observations.values() {
        if let Some(contract) = &o.trade_identity {
            contract.validate().map_err(invalid)?;
        }
        if o.ordering_validated == Some(false) {
            return Err(invalid(
                "source reconciliation requires validated chronological sources; timestamp regressions cannot be repaired by reordering source-ordered records",
            ));
        }
    }
    let sorted = sort(
        workspace,
        inputs,
        |a, b| identity(a, b).then_with(|| precedence(a, b)),
        reporter,
        id,
        stream,
        false,
    )?
    .into_spool()?;
    reporter.stream_phase(id.job_id, stream, "identity resolution");
    let winners = workspace.create()?;
    {
        let mut reader = sorted.trade_reader()?;
        let mut writer = winners.writer()?;
        let mut first: Option<Leased<TradeEntry>> = None;
        while let Some(entry) = next(&mut reader)? {
            let e = &entry.value;
            if !reliable_source_identity(
                &e.trade,
                observations
                    .get(&e.source)
                    .ok_or_else(|| invalid("missing source observation"))?,
            ) {
                let own = observations
                    .get(&e.source)
                    .ok_or_else(|| invalid("missing source observation"))?;
                if let (Some(start), Some(end)) = (
                    own.minimum_accepted_timestamp_ns,
                    own.maximum_accepted_timestamp_ns,
                ) && observations.iter().any(|(index, other)| {
                    *index != e.source
                        && other
                            .minimum_accepted_timestamp_ns
                            .zip(other.maximum_accepted_timestamp_ns)
                            .is_some_and(|(a, b)| start <= b && a <= end)
                }) {
                    return Err(invalid(
                        "ambiguous trade overlap: an overlapping source lacks an established native trade identity/reset domain (including missing IDs)",
                    ));
                }
            }
            if observations[&e.source]
                .trade_identity
                .as_ref()
                .is_some_and(|c| {
                    c.scope == crate::shared::formats::trades::TradeIdScope::SourceLocal
                })
            {
                writer.push_trade(e)?;
                continue;
            }
            let same = first.as_ref().is_some_and(|f| {
                e.trade.trade_id.is_some() && identity(&f.value, e) == Ordering::Equal
            });
            if !same {
                writer.push_trade(e)?;
                first = Some(entry);
                continue;
            }
            let authoritative = &first.as_ref().unwrap().value;
            if authoritative.source == e.source {
                writer.push_trade(e)?;
                continue;
            }
            if !reliable_source_identity(
                &e.trade,
                observations
                    .get(&e.source)
                    .ok_or_else(|| invalid("missing source observation"))?,
            ) {
                let id = e
                    .trade
                    .trade_id
                    .as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(64)
                    .collect::<String>();
                let preferred = observations
                    .get(&authoritative.source)
                    .map_or(0, |o| o.original_task_id);
                let later = observations
                    .get(&e.source)
                    .map_or(0, |o| o.original_task_id);
                return Err(invalid(format!(
                    "native trade identity/reset domain is not established; cannot choose an authoritative cross-source event safely: exchange={:?}, instrument={}, native_id={id:?}, preferred_task={preferred} source={} ordinal={} timestamp={}, other_task={later} source={} ordinal={} timestamp={}; supply authoritative event identities or established reset-domain support; archive dates do not establish identity domains",
                    e.trade.envelope.exchange,
                    e.trade.envelope.instrument_id,
                    authoritative.source,
                    authoritative.ordinal,
                    authoritative.trade.envelope.event_timestamp_ns,
                    e.source,
                    e.ordinal,
                    e.trade.envelope.event_timestamp_ns
                )));
            }
            let o = observations
                .get_mut(&e.source)
                .ok_or_else(|| invalid("missing source observation"))?;
            let count = if e.trade == authoritative.trade {
                &mut o.duplicates
            } else {
                &mut o.conflicts
            };
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("reconciliation counter overflow"))?;
        }
        writer.finish()?;
    }
    sorted.delete()?;
    // This second external sort produces chronological runs; fixed two-way
    // multipass merging handles arbitrarily many runs without opening all sources.
    sort(
        workspace,
        vec![winners],
        chronological,
        reporter,
        id,
        stream,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::super::{
        Quota, reconcile_trades,
        tests::{event, observations},
    };
    use super::*;
    fn run(
        entries: Vec<TradeEntry>,
        memory: u64,
    ) -> (
        Result<Vec<TradeEntry>>,
        BTreeMap<usize, SourceObservation>,
        (u64, u64, u64),
    ) {
        let root = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(root.path(), 8 << 20, Quota::new(memory)).unwrap();
        let mut observed = observations(&entries);
        let input = workspace.create().unwrap();
        let mut writer = input.writer().unwrap();
        for e in entries {
            writer.push(&e).unwrap();
        }
        writer.finish().unwrap();
        let result = trades(&workspace, vec![input], &mut observed).and_then(|spool| {
            let mut reader = spool.trade_reader()?;
            let mut out = Vec::new();
            while let Some(e) = next(&mut reader)? {
                out.push(e.value);
            }
            Ok(out)
        });
        (result, observed, workspace.peaks())
    }
    #[test]
    fn a_large_stream_borrows_spare_session_compute_without_moving_leases() {
        use crate::historical::normalize::resources::compute;
        let root = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(root.path(), 64 << 20, Quota::new(64 << 20)).unwrap();
        let file = workspace.create().unwrap();
        let mut writer = file.writer().unwrap();
        let mut input = Vec::new();
        for i in 0..9000 {
            let id = format!("id-{}", 9000 - i);
            let mut entry = event(0, 0, i, Some(&id), 1);
            entry.ordinal = i as u64 + 1;
            writer.push_trade(&entry).unwrap();
            input.push(entry);
        }
        writer.finish().unwrap();
        let mut observed = observations(&input);
        let compute = compute::ComputeBudget::new(2);
        let output =
            compute::with_budget(&compute, || trades(&workspace, vec![file], &mut observed))
                .unwrap();
        assert!(workspace.parallel_sort_runs() >= 1);
        let mut reader = output.trade_reader().unwrap();
        for expected in input {
            let actual = reader.next().unwrap().unwrap().value;
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        assert!(reader.next().unwrap().is_none());
    }
    #[test]
    fn many_runs_fan_ins_typed_and_codecs_match_complete_reference() {
        use crate::historical::normalize::resources::compute;
        let mut input = Vec::new();
        for source in 0..2 {
            for i in 0..5000 {
                let id = format!("id-{i}");
                let mut entry = event(
                    source,
                    source as u64,
                    i * 2 + if source == 0 { 1 } else { 0 },
                    Some(&id),
                    source as i64 + 1,
                );
                entry.ordinal = i as u64 + 1;
                input.push(entry);
            }
        }
        let mut expected_observed = observations(&input);
        let expected = reconcile_trades(input.clone(), &mut expected_observed).unwrap();
        for fan in [2, 3, 4, 8] {
            for typed in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let workspace = Workspace::new(root.path(), 64 << 20, Quota::new(4 << 20)).unwrap();
                workspace.configure_merge(fan);
                workspace.configure_typed_trades(typed);
                workspace.configure_raw(typed);
                let file = workspace.create().unwrap();
                let mut writer = file.writer().unwrap();
                for entry in &input {
                    writer.push_trade(entry).unwrap();
                }
                writer.finish().unwrap();
                let mut observed = observations(&input);
                let compute = compute::ComputeBudget::new(2);
                let output = compute::with_budget(&compute, || {
                    trades(&workspace, vec![file], &mut observed)
                })
                .unwrap();
                assert_eq!(workspace.maximum_fan_in(), fan);
                let mut reader = output.trade_reader().unwrap();
                let mut actual = Vec::new();
                while let Some(entry) = reader.next().unwrap() {
                    actual.push(entry.value);
                }
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(&expected).unwrap()
                );
                assert_eq!(observed, expected_observed);
                drop(reader);
                output.delete().unwrap();
                assert_eq!(workspace.quota().used(), 0);
            }
        }
    }
    #[test]
    fn precedence_conflicts_equal_times_and_same_source_duplicates_match_oracle() {
        let mut repeat = event(0, 0, 300, Some("ABC"), 3);
        repeat.ordinal = 2;
        let mut tail = event(1, 1, 400, Some("DEF"), 4);
        tail.ordinal = 2;
        let mut equal = event(1, 1, 400, Some("GHI"), 4);
        equal.ordinal = 3;
        let input = vec![
            event(0, 0, 300, Some("ABC"), 3),
            repeat,
            event(1, 1, 100, Some("ABC"), 1),
            tail,
            equal,
        ];
        let mut reference = observations(&input);
        let expected = reconcile_trades(input.clone(), &mut reference).unwrap();
        let (out, observed, peaks) = run(input.clone(), 4 << 20);
        let out = out.unwrap();
        assert_eq!(
            out.iter()
                .map(|e| (e.source, e.ordinal, e.trade.envelope.event_timestamp_ns))
                .collect::<Vec<_>>(),
            [(0, 1, 300), (0, 2, 300), (1, 2, 400), (1, 3, 400)]
        );
        assert_eq!(
            serde_json::to_value(out).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(observed, reference);
        assert_eq!(observed[&1].conflicts, 1);
        assert!(peaks.2 <= 3);
        let reversed = input.into_iter().rev().collect::<Vec<_>>();
        // Observation comes from source decode, not external sort arrival order.
        let root = tempfile::tempdir().unwrap();
        let w = Workspace::new(root.path(), 1 << 20, Quota::new(4 << 20)).unwrap();
        let file = w.create().unwrap();
        let mut writer = file.writer().unwrap();
        for e in reversed {
            writer.push(&e).unwrap();
        }
        writer.finish().unwrap();
        let mut observed = reference;
        observed.get_mut(&1).unwrap().conflicts = 0;
        let out = trades(&w, vec![file], &mut observed).unwrap();
        let mut r = out.trade_reader().unwrap();
        assert_eq!(r.next().unwrap().unwrap().value.source, 0);
    }
    #[test]
    fn identity_domain_and_missing_id_failures_match_reference() {
        use crate::shared::domain::canonical::Exchange;
        for case in 0..6 {
            let mut a = event(0, 0, 10, Some("ID"), 1);
            let mut b = event(1, 1, 10, Some("ID"), 1);
            match case {
                0 => {
                    a.trade.trade_id = None;
                    b.trade.trade_id = None;
                }
                1 => {
                    a.trade.envelope.exchange = Exchange::Bybit;
                    b.trade.envelope.exchange = Exchange::Bybit;
                }
                2 => {
                    a.trade.envelope.exchange = Exchange::GateIo;
                    b.trade.envelope.exchange = Exchange::GateIo;
                    b.trade.envelope.event_timestamp_ns = 100;
                }
                3 => {
                    b.trade.envelope.instrument_id = 2;
                }
                4 => {
                    b.trade.envelope.event_timestamp_ns = 100;
                }
                _ => {
                    a.trade.trade_id = None;
                    b.trade.trade_id = None;
                    b.trade.envelope.event_timestamp_ns = 100;
                }
            }
            let input = vec![a, b];
            let mut o = observations(&input);
            let expected = reconcile_trades(input.clone(), &mut o);
            let (actual, actual_observed, _) = run(input, 4 << 20);
            assert_eq!(actual.is_ok(), expected.is_ok(), "case {case}");
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
                assert_eq!(actual_observed, o);
            }
        }
    }
    #[test]
    fn unique_events_inside_overlap_and_later_tail_survive_exactly() {
        let mut input = vec![
            event(0, 0, 100, Some("a"), 1),
            event(0, 0, 300, Some("c"), 3),
            event(1, 1, 150, Some("b"), 2),
            event(1, 1, 300, Some("c"), 3),
            event(1, 1, 400, Some("d"), 4),
        ];
        input[1].ordinal = 2;
        input[3].ordinal = 2;
        input[4].ordinal = 3;
        let mut reference = observations(&input);
        let expected = reconcile_trades(input.clone(), &mut reference).unwrap();
        let (output, observed, _) = run(input, 2 << 20);
        let output = output.unwrap();
        assert_eq!(
            output
                .iter()
                .map(|e| (e.source, e.trade.envelope.event_timestamp_ns))
                .collect::<Vec<_>>(),
            [(0, 100), (1, 150), (0, 300), (1, 400)]
        );
        assert_eq!(observed[&1].duplicates, 1);
        assert_eq!(observed[&1].conflicts, 0);
        assert_eq!(
            serde_json::to_value(output).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(observed, reference);
    }
    #[test]
    fn bybit_numeric_ids_without_an_explicit_contract_still_fail_the_reference_too() {
        let mut input = vec![
            event(0, 0, 100, Some("1"), 1),
            event(1, 1, 200, Some("1"), 2),
            event(2, 2, 300, Some("1"), 3),
        ];
        for entry in &mut input {
            entry.trade.envelope.exchange = crate::shared::domain::canonical::Exchange::Bybit;
        }
        let mut observed = observations(&input);
        let reference = reconcile_trades(input.clone(), &mut observed).unwrap_err();
        assert!(
            reference
                .to_string()
                .contains("native trade identity/reset domain is not established")
        );
        let (external, _, _) = run(input, 2 << 20);
        assert!(
            external
                .err()
                .unwrap()
                .to_string()
                .contains("native trade identity/reset domain is not established")
        );
    }
    #[test]
    fn frequent_identity_multipass_and_small_resident_budget() {
        let mut last_peak = 0;
        for count in [100, 6000] {
            let input = (0..count)
                .map(|i| {
                    let mut e = event(
                        usize::from(i >= count / 2),
                        u64::from(i >= count / 2),
                        10,
                        Some("same"),
                        1,
                    );
                    e.ordinal = (i % (count / 2) + 1) as u64;
                    e
                })
                .collect::<Vec<_>>();
            let (out, observed, peaks) = run(input, 2 << 20);
            let out = out.unwrap();
            assert_eq!(out.len(), count / 2);
            assert!(out.iter().all(|e| e.source == 0));
            assert_eq!(observed[&1].duplicates, (count / 2) as u64);
            assert!(peaks.0 <= 2 << 20);
            assert!(peaks.2 <= 3);
            if count == 6000 {
                assert_eq!(peaks.2, 3);
            }
            last_peak = peaks.0;
        }
        crate::historical::normalize::progress::log::diagnostic(format_args!(
            "synthetic external-sort accounted resident peak={last_peak} budget={}",
            2 << 20
        ));
    }
}
