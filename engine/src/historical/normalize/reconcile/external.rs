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
fn ordered_run(observation: &SourceObservation) -> bool {
    observation
        .trade_identity
        .as_ref()
        .is_some_and(|c| c.scope == crate::shared::formats::trades::TradeIdScope::OrderedRun)
}
fn validate_run_order(previous: &TradeEntry, current: &TradeEntry, stage: &str) -> Result<()> {
    let increasing = previous
        .trade
        .trade_id
        .as_deref()
        .zip(current.trade.trade_id.as_deref())
        .and_then(|(a, b)| crate::historical::normalize::policies::numeric_id_cmp(a, b))
        == Some(Ordering::Less);
    if !increasing {
        return Err(MarketForgeError::TradeIdentityViolation {
            source_index: current.source,
            previous_source_index: Some(previous.source),
            reason: format!(
                "ordered_run native trade ID reset/order conflict at {stage}: previous_source={} previous_ordinal={} previous_id={:?} previous_timestamp={} source={} ordinal={} native_id={:?} timestamp={}; no cross-reset identity inference or preference fallback is permitted",
                previous.source,
                previous.ordinal,
                previous
                    .trade
                    .trade_id
                    .as_deref()
                    .map(|s| s.chars().take(64).collect::<String>()),
                previous.trade.envelope.event_timestamp_ns,
                current.source,
                current.ordinal,
                current
                    .trade
                    .trade_id
                    .as_deref()
                    .map(|s| s.chars().take(64).collect::<String>()),
                current.trade.envelope.event_timestamp_ns
            ),
        });
    }
    Ok(())
}
fn exchange_order(exchange: &crate::shared::domain::canonical::Exchange) -> (u8, &str) {
    use crate::shared::domain::canonical::Exchange::*;
    match exchange {
        Bybit => (0, ""),
        Binance => (1, ""),
        Okx => (2, ""),
        Bitget => (3, ""),
        GateIo => (4, ""),
        Manual(_) => (5, exchange.as_str()),
    }
}

fn identity(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    // Enum declaration order is internal only; equality/scope exactly match the
    // reference key. No business interpretation depends on exchange ordering.
    (
        exchange_order(&a.trade.envelope.exchange),
        &a.trade.envelope.stream_id,
        a.trade.envelope.instrument_id,
        &a.trade.trade_id,
    )
        .cmp(&(
            exchange_order(&b.trade.envelope.exchange),
            &b.trade.envelope.stream_id,
            b.trade.envelope.instrument_id,
            &b.trade.trade_id,
        ))
}
fn precedence(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    (a.priority, a.source, a.ordinal).cmp(&(b.priority, b.source, b.ordinal))
}
// Scratch-only markers carry suppressed observations through the native-order
// loss-span pass. They can never reach canonical publication.
const VERIFIED_DUPLICATE: u64 = u64::MAX;
const OVERLAP_EXCLUDED: u64 = u64::MAX - 1;
fn marker(
    writer: &mut super::spool::SpoolWriter,
    entry: &Leased<TradeEntry>,
    priority: u64,
    workspace: &Workspace,
) -> Result<()> {
    let _memory = workspace
        .quota()
        .reserve(entry.accounted_bytes(), "trade decision marker")?;
    let mut value = entry.value.clone();
    value.priority = priority;
    writer.push_trade(&value)
}
fn chronological(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    a.trade
        .envelope
        .event_timestamp_ns
        .cmp(&b.trade.envelope.event_timestamp_ns)
        .then_with(|| {
            crate::historical::normalize::policies::native_id_order(
                a.trade.trade_id.as_deref(),
                b.trade.trade_id.as_deref(),
            )
        })
        .then_with(|| precedence(a, b))
}
fn native(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    (a.source, a.ordinal).cmp(&(b.source, b.ordinal))
}
fn preserved(a: &TradeEntry, b: &TradeEntry) -> Ordering {
    (a.priority, a.source, a.ordinal).cmp(&(b.priority, b.source, b.ordinal))
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
    OrderedAppend(Vec<(usize, Spool)>),
    Inputs {
        inputs: Vec<Spool>,
        ranks: BTreeMap<usize, u64>,
    },
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
        let mut ordered = false;
        let mut ranks = None;
        let (spools, compare) = match self {
            Self::OrderedAppend(inputs) => {
                ordered = true;
                let (sources, spools): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
                append_sources = Some(sources);
                (
                    spools,
                    chronological as fn(&TradeEntry, &TradeEntry) -> Ordering,
                )
            }
            Self::Inputs {
                inputs,
                ranks: table,
            } => {
                ranks = Some(table);
                (
                    inputs,
                    chronological as fn(&TradeEntry, &TradeEntry) -> Ordering,
                )
            }
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
            ordered,
            previous_native: String::new(),
            ranks,
        };
        if rows.append_sources.is_some() {
            return Ok(rows);
        }
        for input in 0..rows.spools.len() {
            let mut reader = rows.spools[input].trade_reader()?;
            if let Some(mut row) = next(&mut reader)? {
                if let Some(ranks) = &rows.ranks {
                    row.value.priority = ranks[&row.value.source];
                }
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
    ordered: bool,
    previous_native: String,
    ranks: Option<BTreeMap<usize, u64>>,
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
                            if self.append_first && !self.ordered {
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
                    if self.ordered {
                        let id = row
                            .value
                            .trade
                            .trade_id
                            .as_deref()
                            .filter(|s| s.len() <= 128)
                            .ok_or_else(|| {
                                invalid("ordered append native identity evidence mismatch")
                            })?;
                        if !self.previous_native.is_empty()
                            && crate::historical::normalize::policies::numeric_id_cmp(
                                &self.previous_native,
                                id,
                            ) != Some(Ordering::Less)
                        {
                            return Err(invalid("ordered append native ID reset/conflict"));
                        }
                        self.previous_native.clear();
                        self.previous_native.push_str(id);
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
            Ok(Some(mut row)) => {
                if let Some(ranks) = &self.ranks {
                    row.value.priority = ranks[&row.value.source];
                }
                self.heap.push(Cursor {
                    row,
                    input: cursor.input,
                    compare: cursor.compare,
                });
            }
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
    sort_ranked(
        workspace,
        inputs,
        compare,
        reporter,
        id,
        stream,
        SortOptions {
            chronological_sort,
            ranks: None,
        },
    )
}
struct SortOptions<'a> {
    chronological_sort: bool,
    ranks: Option<&'a BTreeMap<usize, u64>>,
}
fn sort_ranked(
    workspace: &Workspace,
    inputs: Vec<Spool>,
    compare: fn(&TradeEntry, &TradeEntry) -> Ordering,
    reporter: &ProgressReporter,
    id: ProgressTaskId,
    stream: &str,
    options: SortOptions<'_>,
) -> Result<SortedRows> {
    let SortOptions {
        chronological_sort,
        ranks,
    } = options;
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
                if let Some(ranks) = ranks {
                    entry.value.priority = ranks[&entry.value.source];
                }
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
    }
    use crate::shared::historical::policies::{
        Selection, TradeOverlap, TradeTimestamp, milliseconds,
    };
    let policy = crate::historical::normalize::policies::active();
    let prefer = policy.reconciliation.trades.overlap_policy == TradeOverlap::PreferSource;
    let mut sources = observations.keys().copied().collect::<Vec<_>>();
    sources.sort_by(|a, b| {
        let starts = observations[a]
            .minimum_accepted_timestamp_ns
            .cmp(&observations[b].minimum_accepted_timestamp_ns);
        let starts =
            if prefer && policy.reconciliation.trades.prefer_source.selection == Selection::Later {
                starts.reverse()
            } else {
                starts
            };
        starts
            .then_with(|| {
                observations[a]
                    .stable_source_identity
                    .cmp(&observations[b].stable_source_identity)
            })
            .then(a.cmp(b))
    });
    let ranks = sources
        .into_iter()
        .enumerate()
        .map(|(rank, source)| (source, rank as u64))
        .collect::<BTreeMap<_, _>>();
    let _ownership_memory = workspace.quota().reserve(
        (observations.len() as u64)
            .checked_mul(2048)
            .ok_or_else(|| invalid("trade coverage index overflow"))?,
        "trade coverage interval index",
    )?;
    let ranges = observations
        .iter()
        .filter_map(|(i, o)| {
            Some((
                *i,
                i128::from(o.minimum_accepted_timestamp_ns?),
                i128::from(o.maximum_accepted_timestamp_ns?) + 1,
            ))
        })
        .collect::<Vec<_>>();
    let overlaps = super::super::coverage::overlapping(&ranges);
    let owners = super::super::coverage::ownership(&ranges, &ranks);
    let compare: fn(&TradeEntry, &TradeEntry) -> Ordering =
        |a, b| identity(a, b).then_with(|| precedence(a, b));
    let sorted = sort_ranked(
        workspace,
        inputs,
        compare,
        reporter,
        id,
        stream,
        SortOptions {
            chronological_sort: false,
            ranks: Some(&ranks),
        },
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
            if ordered_run(&observations[&e.source])
                && e.trade
                    .trade_id
                    .as_deref()
                    .and_then(|id| crate::historical::normalize::policies::numeric_id_cmp(id, id))
                    .is_none()
            {
                return Err(MarketForgeError::TradeIdentityViolation {
                    source_index: e.source,
                    previous_source_index: None,
                    reason: format!(
                        "ordered_run requires a nonempty decimal native trade ID: source={}, ordinal={}; missing IDs cannot enter source-preference loss",
                        e.source, e.ordinal
                    ),
                });
            }
            if !reliable_source_identity(
                &e.trade,
                observations
                    .get(&e.source)
                    .ok_or_else(|| invalid("missing source observation"))?,
            ) && overlaps.contains(&e.source)
            {
                if !prefer {
                    return Err(invalid(
                        "ambiguous trade overlap: an overlapping source lacks an established native trade identity/reset domain (including missing IDs)",
                    ));
                }
                let timestamp = e.trade.envelope.event_timestamp_ns;
                let slot = owners.partition_point(|s| s.end <= i128::from(timestamp));
                let owner = owners
                    .get(slot)
                    .filter(|s| s.start <= i128::from(timestamp))
                    .map(|s| s.source)
                    .ok_or_else(|| invalid("no source owns conflict interval"))?;
                if owner != e.source {
                    let chosen_task = observations[&owner].original_task_id;
                    let o = observations.get_mut(&e.source).unwrap();
                    o.processing.exclude(
                        timestamp,
                        1,
                        false,
                        false,
                        e.ordinal,
                        Some(chosen_task),
                    )?;
                    // Duration is evaluated below in native source order,
                    // never in identity-sort order.
                    o.processing.reset_spans();
                    crate::historical::normalize::policies::enforce_source_overlap(
                        &policy.reconciliation.trades.prefer_source,
                        &o.processing,
                    )?;
                    marker(&mut writer, &entry, OVERLAP_EXCLUDED, workspace)?;
                    continue;
                }
                writer.push_trade(e)?;
                continue;
            }
            if prefer && !reliable_source_identity(&e.trade, &observations[&e.source]) {
                writer.push_trade(e)?;
                continue;
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
            if authoritative.source == e.source
                && !reliable_source_identity(&e.trade, &observations[&e.source])
            {
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
            let equivalent = super::trade_equivalence(
                &e.trade,
                &observations[&e.source],
                &authoritative.trade,
                &observations[&authoritative.source],
            )
            .ok_or_else(|| invalid("incomparable native trade identity/reset domains"))?;
            let preferred_task = observations[&authoritative.source].original_task_id;
            let o = observations
                .get_mut(&e.source)
                .ok_or_else(|| invalid("missing source observation"))?;
            let count = if equivalent {
                &mut o.duplicates
            } else {
                &mut o.conflicts
            };
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("reconciliation counter overflow"))?;
            if !equivalent {
                if !prefer || ordered_run(o) {
                    return Err(invalid(
                        "conflicting contents under reliable trade identity; strict/ordered-run reconciliation cannot exclude a conflicting observation",
                    ));
                }
                o.processing.exclude(
                    e.trade.envelope.event_timestamp_ns,
                    1,
                    false,
                    false,
                    e.ordinal,
                    Some(preferred_task),
                )?;
                o.processing.reset_spans();
                crate::historical::normalize::policies::enforce_source_overlap(
                    &policy.reconciliation.trades.prefer_source,
                    &o.processing,
                )?;
            }
            marker(
                &mut writer,
                &entry,
                if equivalent {
                    VERIFIED_DUPLICATE
                } else {
                    OVERLAP_EXCLUDED
                },
                workspace,
            )?;
        }
        writer.finish()?;
    }
    sorted.delete()?;
    // Prove the identity domain on the complete unique union BEFORE timestamp
    // discard could exclude evidence. Verified-duplicate markers do not count
    // as new executions; keep them for the native-order accounting pass.
    let winners = if observations.values().any(ordered_run) {
        let chronological_rows = sort(
            workspace,
            vec![winners],
            chronological,
            reporter,
            id,
            stream,
            false,
        )?
        .into_spool()?;
        {
            let mut reader = chronological_rows.trade_reader()?;
            let mut previous: Option<Leased<TradeEntry>> = None;
            while let Some(entry) = next(&mut reader)? {
                if matches!(entry.value.priority, VERIFIED_DUPLICATE | OVERLAP_EXCLUDED) {
                    continue;
                }
                if let Some(previous) = &previous {
                    validate_run_order(
                        &previous.value,
                        &entry.value,
                        "chronological source union",
                    )?;
                }
                previous = Some(entry);
            }
        }
        chronological_rows
    } else {
        winners
    };
    // Validate/repair surviving logical events in original concrete source order.
    // Verified duplicates have already been removed; raw anomaly evidence is kept.
    reporter.stream_phase(
        id.job_id,
        stream,
        "timestamp processing in native event order",
    );
    let native_rows = sort(
        workspace,
        vec![winners],
        native,
        reporter,
        id,
        stream,
        false,
    )?
    .into_spool()?;
    let survivors = workspace.create()?;
    {
        let mut reader = native_rows.trade_reader()?;
        let mut writer = survivors.writer()?;
        let mut source = None;
        let mut high = None;
        let mut previous_native: Option<Leased<TradeEntry>> = None;
        while let Some(entry) = next(&mut reader)? {
            let e = &entry.value;
            if source != Some(e.source) {
                source = Some(e.source);
                high = None;
            }
            let timestamp = e.trade.envelope.event_timestamp_ns;
            if e.priority == VERIFIED_DUPLICATE {
                continue;
            }
            if e.priority == OVERLAP_EXCLUDED {
                let o = observations.get_mut(&e.source).unwrap();
                o.processing.track_exclusion_span(timestamp, false)?;
                crate::historical::normalize::policies::enforce_source_overlap(
                    &policy.reconciliation.trades.prefer_source,
                    &o.processing,
                )?;
                continue;
            }
            if ordered_run(&observations[&e.source])
                && let Some(previous) = previous_native
                    .as_ref()
                    .filter(|p| p.value.source == e.source)
            {
                validate_run_order(&previous.value, e, "native source order")?;
            }
            let regression = high
                .filter(|p| timestamp < *p)
                .map(|p: i64| {
                    p.checked_sub(timestamp)
                        .ok_or_else(|| invalid("trade timestamp regression arithmetic overflow"))
                })
                .transpose()?
                .unwrap_or(0);
            let o = observations.get_mut(&e.source).unwrap();
            if regression > 0 {
                o.processing.unresolved_timestamp_regressions += 1;
                let bound = milliseconds(policy.timestamp.trades.max_regression_ms)?;
                if policy.timestamp.trades.policy == TradeTimestamp::Strict || regression > bound {
                    return Err(invalid(format!(
                        "unresolved trade timestamp regression: source={}, ordinal={}, high_water={high:?}, timestamp={timestamp}, regression_ns={regression}, permitted_ns={bound}",
                        e.source, e.ordinal
                    )));
                }
                if policy.timestamp.trades.policy == TradeTimestamp::Discard {
                    let known = super::reliable_source_identity(&e.trade, o);
                    o.processing
                        .exclude(timestamp, 1, true, known, e.ordinal, None)?;
                    if o.processing.timestamp_exclusions
                        > policy.timestamp.trades.max_discarded_events
                    {
                        return Err(invalid("trade timestamp discard event limit exceeded"));
                    }
                    continue;
                }
                if policy.timestamp.trades.policy == TradeTimestamp::Reorder
                    && regression > milliseconds(policy.timestamp.trades.max_reorder_window_ms)?
                {
                    return Err(invalid("trade reorder high-water window exceeded"));
                }
            }
            o.processing.retain();
            o.processing.published_native_order =
                policy.timestamp.trades.policy == TradeTimestamp::Preserve;
            o.processing.reordered = policy.timestamp.trades.policy == TradeTimestamp::Reorder
                && o.processing.unresolved_timestamp_regressions > 0;
            if o.processing.logical_source_events > 0 {
                o.processing.published_logical_events += 1;
            }
            high = Some(high.map_or(timestamp, |p: i64| p.max(timestamp)));
            writer.push_trade(e)?;
            if ordered_run(o) {
                previous_native = Some(entry);
            }
        }
        writer.finish()?;
    }
    native_rows.delete()?;
    if observations.values().any(ordered_run) {
        for o in observations.values_mut() {
            o.native_identity_order_validated = true;
        }
    }
    // Bounded external merge handles arbitrarily many runs/sources.
    sort(
        workspace,
        vec![survivors],
        if policy.timestamp.trades.policy == TradeTimestamp::Preserve {
            preserved
        } else {
            chronological
        },
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
                let mut entry = event(source, source as u64, i * 2, Some(&id), 1);
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
    fn verified_duplicates_equal_times_and_same_source_observations_match_oracle() {
        let mut repeat = event(0, 0, 300, Some("ABC"), 3);
        repeat.ordinal = 2;
        let mut tail = event(1, 1, 400, Some("DEF"), 4);
        tail.ordinal = 2;
        let mut equal = event(1, 1, 400, Some("GHI"), 4);
        equal.ordinal = 3;
        let input = vec![
            event(0, 0, 300, Some("ABC"), 3),
            repeat,
            event(1, 1, 300, Some("ABC"), 3),
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
            [(0, 1, 300), (1, 2, 400), (1, 3, 400)]
        );
        assert_eq!(
            serde_json::to_value(out).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(observed, reference);
        assert_eq!(observed[&1].duplicates, 1);
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
        observed.get_mut(&1).unwrap().duplicates = 0;
        let out = trades(&w, vec![file], &mut observed).unwrap();
        let mut r = out.trade_reader().unwrap();
        assert_eq!(r.next().unwrap().unwrap().value.source, 0);
    }
    #[test]
    fn strict_conflicting_identity_contents_fail_before_publication() {
        let input = vec![
            event(0, 0, 100, Some("100"), 1),
            event(1, 1, 100, Some("100"), 2),
        ];
        let mut reference = observations(&input);
        assert!(
            reconcile_trades(input.clone(), &mut reference)
                .unwrap_err()
                .to_string()
                .contains("conflicting contents")
        );
        let (result, observed, _) = run(input, 4 << 20);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("conflicting contents")
        );
        assert_eq!(observed[&1].conflicts, 1);
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
            assert_eq!(out.len(), 1);
            assert_eq!(observed[&0].duplicates, (count / 2 - 1) as u64);
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

/// Source-local evidence is captured during normal ingress; no raw re-read.
/// A sealed ordered merge proves the whole unique domain before loss processing.
pub(crate) fn checked_ordered(
    workspace: &Workspace,
    inputs: Vec<(usize, Spool)>,
    observations: &mut BTreeMap<usize, SourceObservation>,
    append: bool,
    selection: crate::shared::historical::policies::Selection,
) -> Result<SortedRows> {
    let mut order = inputs.iter().map(|(i, _)| *i).collect::<Vec<_>>();
    order.sort_by(|a, b| {
        observations[a]
            .minimum_accepted_timestamp_ns
            .cmp(&observations[b].minimum_accepted_timestamp_ns)
            .then_with(|| {
                crate::historical::normalize::policies::native_id_order(
                    observations[a].first_native_id.as_deref(),
                    observations[b].first_native_id.as_deref(),
                )
            })
            .then_with(|| {
                observations[a]
                    .stable_source_identity
                    .cmp(&observations[b].stable_source_identity)
            })
    });
    if append {
        let mut map = inputs.into_iter().collect::<BTreeMap<_, _>>();
        for o in observations.values_mut() {
            o.native_identity_order_validated = true;
        }
        return Ok(SortedRows::OrderedAppend(
            order
                .into_iter()
                .map(|i| (i, map.remove(&i).unwrap()))
                .collect(),
        ));
    }
    // Duplicate ownership uses the same coverage/stable-identity rank as the
    // general path. Native-ID ordering is only an append proof, never quality.
    order.sort_by(|a, b| {
        let starts = observations[a]
            .minimum_accepted_timestamp_ns
            .cmp(&observations[b].minimum_accepted_timestamp_ns);
        (if selection == crate::shared::historical::policies::Selection::Later {
            starts.reverse()
        } else {
            starts
        })
        .then_with(|| {
            observations[a]
                .stable_source_identity
                .cmp(&observations[b].stable_source_identity)
        })
        .then(a.cmp(b))
    });
    let ranks = order
        .into_iter()
        .enumerate()
        .map(|(rank, i)| (i, rank as u64))
        .collect();
    let mut rows = SortedRows::Inputs {
        inputs: inputs.into_iter().map(|(_, s)| s).collect(),
        ranks,
    }
    .rows()?;
    let output = workspace.create()?;
    let mut writer = output.writer()?;
    let mut previous: Option<Leased<TradeEntry>> = None;
    while let Some(entry) = rows.next()? {
        let e = &entry.value;
        if let Some(p) = &previous {
            let cmp = crate::historical::normalize::policies::numeric_id_cmp(
                p.value.trade.trade_id.as_deref().unwrap(),
                e.trade.trade_id.as_deref().unwrap(),
            );
            if cmp == Some(Ordering::Equal) {
                if super::trade_equivalence(
                    &p.value.trade,
                    &observations[&p.value.source],
                    &e.trade,
                    &observations[&e.source],
                ) != Some(true)
                {
                    observations.get_mut(&e.source).unwrap().conflicts += 1;
                    return Err(invalid(
                        "conflicting contents under reliable trade identity in ordered merge",
                    ));
                }
                observations.get_mut(&e.source).unwrap().duplicates += 1;
                continue;
            }
            validate_run_order(&p.value, e, "streaming chronological source union")?;
        }
        writer.push_trade(e)?;
        previous = Some(entry);
    }
    rows.finish()?;
    writer.finish()?;
    for o in observations.values_mut() {
        o.native_identity_order_validated = true;
    }
    Ok(SortedRows::Spool(output))
}
