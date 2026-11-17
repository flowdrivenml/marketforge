//! Selected histories are replayed from their own independent source books.
//! At most a source book and the selected book are live. Returning to a source
//! restores a bounded exact checkpoint and consumes a sealed forward partition.
use super::*;
use crate::historical::normalize::boundary::{BoundaryBookState, DepthBoundaryTracker};
use crate::shared::{
    domain::{
        book::{BookLevel, BookStore},
        canonical::{
            BookSide, DepthEventBoundary, DepthSourceOperation, EventEnvelope, L2LevelUpdate,
        },
    },
    historical::policies::{DepthOverlap, DepthTimestamp, Selection, SwitchBoundary, milliseconds},
};
#[derive(Clone, Copy)]
pub(super) struct Interval {
    source: usize,
    start: i128,
    end: i128,
}
#[derive(Default)]
struct Ordinals {
    ranges: Vec<(u64, u64)>,
}
impl Ordinals {
    fn add(&mut self, ordinal: u64, quota: &Quota, maximum: u64) -> Result<()> {
        if let Some(last) = self.ranges.last_mut() {
            if ordinal < last.1 {
                return Err(invalid(
                    "depth ownership would reorder native source events",
                ));
            }
            if ordinal == last.1 {
                last.1 = ordinal
                    .checked_add(1)
                    .ok_or_else(|| invalid("source ordinal overflow"))?;
                return Ok(());
            }
        }
        crate::shared::historical::limits::check(
            "depth ownership ranges",
            maximum,
            self.ranges.len() as u64 + 1,
            "depth ownership",
        )?;
        quota.charge(64, "depth selected source ordinal ranges")?;
        self.ranges.push((
            ordinal,
            ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("source ordinal overflow"))?,
        ));
        Ok(())
    }
    fn contains(&self, ordinal: u64) -> bool {
        let i = self.ranges.partition_point(|(_, end)| *end <= ordinal);
        self.ranges
            .get(i)
            .is_some_and(|(start, end)| *start <= ordinal && ordinal < *end)
    }
}
fn level(event: &L2LevelUpdate) -> BookLevel {
    BookLevel {
        quantity_base: event.quantity_base,
        quantity_quote: event.quantity_quote,
        quantity_contracts: event.quantity_contracts,
        order_count: event.order_count,
    }
}
fn apply(book: &mut BookStore, outcome: &DepthProcessingOutcome) -> Result<()> {
    if outcome.boundary == DepthEventBoundary::Initialization {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for e in &outcome.events {
            match e.side {
                BookSide::Bid => bids.push((e.price, level(e))),
                BookSide::Ask => asks.push((e.price, level(e))),
            }
        }
        book.replace_snapshot(bids, asks)?;
    } else {
        for e in &outcome.events {
            book.set_level(e.side, e.price, level(e))?;
        }
    }
    Ok(())
}
fn full(
    book: &BookStore,
    task: &WorkTask,
    source: &crate::shared::domain::canonical::DepthSourceEventMetadata,
) -> Vec<L2LevelUpdate> {
    let envelope = EventEnvelope {
        event_timestamp_ns: source.event_timestamp_ns,
        system_timestamp_ns: source.system_timestamp_ns,
        exchange: task.exchange,
        instrument_id: task.instrument_id,
        symbol: task.symbol.clone(),
        stream_id: task.stream_id.0.clone(),
    };
    book.bids()
        .iter()
        .map(|(p, l)| (BookSide::Bid, p, l))
        .chain(book.asks().iter().map(|(p, l)| (BookSide::Ask, p, l)))
        .map(|(side, p, l)| L2LevelUpdate {
            envelope: envelope.clone(),
            side,
            price: *p,
            quantity_base: l.quantity_base,
            quantity_quote: l.quantity_quote,
            quantity_contracts: l.quantity_contracts,
            order_count: l.order_count,
        })
        .collect()
}
fn same(a: &BookStore, b: &BookStore) -> bool {
    a.is_initialized() && b.is_initialized() && a.bids() == b.bids() && a.asks() == b.asks()
}
fn exact(a: &BookStore, b: &BookStore, metrics: &mut super::super::policies::Outcomes) -> bool {
    metrics.depth_full_comparisons += 1;
    metrics.depth_levels_compared +=
        (a.bids().len() + a.asks().len() + b.bids().len() + b.asks().len()) as u64;
    same(a, b)
}
fn proof(a: &Depth, b: &Depth) -> bool {
    match (&a.last_proof, &b.first_proof) {
        (Some(a), Some(b)) => {
            a.operation == DepthSourceOperation::Snapshot
                && b.operation == DepthSourceOperation::Snapshot
                && b.boundary == DepthEventBoundary::Initialization
                && reconcile::event_equivalence(
                    a.source.sequence_end.is_some()
                        && a.source.sequence_end == b.source.sequence_end,
                    &a.fingerprint,
                    &b.fingerprint,
                ) == Some(true)
                && a.source.event_timestamp_ns == b.source.event_timestamp_ns
                && a.source.system_timestamp_ns == b.source.system_timestamp_ns
        }
        _ => false,
    }
}
fn intervals(
    observations: &BTreeMap<usize, SourceObservation>,
    config: &ProcessingConfig,
    quota: &Quota,
) -> Result<Vec<Interval>> {
    let ranges = observations
        .iter()
        .filter_map(|(source, o)| {
            Some(Interval {
                source: *source,
                start: i128::from(o.minimum_accepted_timestamp_ns?),
                end: i128::from(o.maximum_accepted_timestamp_ns?) + 1,
            })
        })
        .collect::<Vec<_>>();
    quota.charge(
        (ranges.len() as u64)
            .checked_mul(2048)
            .ok_or_else(|| invalid("depth interval inventory overflow"))?,
        "depth ownership interval inventory",
    )?;
    if config.reconciliation.depth.overlap_policy != DepthOverlap::PreferSource {
        let mut ranges = ranges;
        ranges.sort_by(|a, b| {
            a.start
                .cmp(&b.start)
                .then_with(|| {
                    observations[&a.source]
                        .stable_source_identity
                        .cmp(&observations[&b.source].stable_source_identity)
                })
                .then(a.source.cmp(&b.source))
        });
        return Ok(ranges);
    }
    let mut sources = ranges.iter().collect::<Vec<_>>();
    sources.sort_by(|a, b| {
        let order = a.start.cmp(&b.start);
        (if config.reconciliation.depth.prefer_source.selection == Selection::Later {
            order.reverse()
        } else {
            order
        })
        .then_with(|| {
            observations[&a.source]
                .stable_source_identity
                .cmp(&observations[&b.source].stable_source_identity)
        })
        .then(a.source.cmp(&b.source))
    });
    let ranks = sources
        .into_iter()
        .enumerate()
        .map(|(rank, r)| (r.source, rank as u64))
        .collect();
    let result = super::super::coverage::ownership(
        &ranges
            .iter()
            .map(|r| (r.source, r.start, r.end))
            .collect::<Vec<_>>(),
        &ranks,
    )
    .into_iter()
    .map(|r| Interval {
        source: r.source,
        start: r.start,
        end: r.end,
    })
    .collect::<Vec<_>>();
    crate::shared::historical::limits::check(
        "depth ownership intervals",
        config.resources.normalization_limits.metric_entries,
        result.len() as u64,
        "depth ownership sweep",
    )?;
    Ok(result)
}
fn reset(sequence: Option<u64>, previous: Option<u64>, snapshot: bool) -> bool {
    snapshot && sequence.zip(previous).is_some_and(|(a, b)| a < b)
}
fn adjusted(metrics: &mut TaskMetrics, old: u64, new: u64) -> Result<()> {
    if new >= old {
        let delta = new - old;
        metrics.counters.events_normalized = metrics
            .counters
            .events_normalized
            .checked_add(delta)
            .ok_or_else(|| invalid("depth canonical reprojection overflow"))?;
        metrics.counters.rows_unpublished = metrics
            .counters
            .rows_unpublished
            .checked_add(delta)
            .ok_or_else(|| invalid("depth reprojection pending overflow"))?;
    } else {
        let delta = old - new;
        metrics.counters.events_normalized = metrics
            .counters
            .events_normalized
            .checked_sub(delta)
            .ok_or_else(|| invalid("depth canonical reprojection underflow"))?;
        metrics.counters.rows_unpublished = metrics
            .counters
            .rows_unpublished
            .checked_sub(delta)
            .ok_or_else(|| invalid("depth reprojection pending underflow"))?;
    }
    Ok(())
}
#[path = "depth_partitions.rs"]
mod partitions;
pub(super) fn publish(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    depths: BTreeMap<usize, Depth>,
    observations: &mut BTreeMap<usize, SourceObservation>,
    executions: &mut [(usize, TaskExecution)],
    reporter: &ProgressReporter,
) -> Result<()> {
    let quota = depths
        .values()
        .next()
        .map(|d| d.workspace.quota().clone())
        .ok_or_else(|| invalid("depth replay has no source"))?;
    for o in observations.values_mut() {
        o.processing.published_native_order = false;
    }
    let plan = intervals(observations, config, &quota)?;
    let mut duplicate = BTreeMap::<usize, u64>::new();
    // Prefer verified seam reconciliation under every policy. A changing terminal
    // snapshot is represented by the following complete initialization instead.
    let mut chronological = observations
        .keys()
        .copied()
        .filter(|i| observations[i].minimum_accepted_timestamp_ns.is_some())
        .collect::<Vec<_>>();
    chronological.sort_by(|a, b| {
        observations[a]
            .minimum_accepted_timestamp_ns
            .cmp(&observations[b].minimum_accepted_timestamp_ns)
            .then_with(|| {
                observations[a]
                    .stable_source_identity
                    .cmp(&observations[b].stable_source_identity)
            })
            .then(a.cmp(b))
    });
    for pair in chronological.windows(2) {
        let equal = observations[&pair[0]].maximum_accepted_timestamp_ns
            == observations[&pair[1]].minimum_accepted_timestamp_ns;
        if equal
            && depths
                .get(&pair[0])
                .zip(depths.get(&pair[1]))
                .is_some_and(|(a, b)| proof(a, b))
        {
            let p = depths[&pair[0]].last_proof.as_ref().unwrap();
            let ordinal = p
                .source
                .source_event_ordinal
                .ok_or_else(|| invalid("duplicate snapshot lacks source ordinal"))?;
            duplicate.insert(pair[0], ordinal);
            let of = unit
                .tasks
                .iter()
                .find(|(i, _)| *i == pair[1])
                .unwrap()
                .1
                .task_id
                .0;
            let o = observations.get_mut(&pair[0]).unwrap();
            o.duplicates += 1;
            o.boundary_duplicate_of_task = Some(of);
            o.suppressed_depth_event_ordinal = Some(ordinal);
            o.boundary_duplicate_source_sha256 = Some(super::super::hex_digest(p.fingerprint));
            o.boundary_duplicate_sequence = p.source.sequence_end;
        } else if config.reconciliation.depth.overlap_policy == DepthOverlap::Strict
            && observations[&pair[0]]
                .maximum_accepted_timestamp_ns
                .zip(observations[&pair[1]].minimum_accepted_timestamp_ns)
                .is_some_and(|(a, b)| a >= b)
        {
            return Err(invalid(
                "unsupported depth overlap: independent histories require proven event equivalence, snapshot_recovery without loss, or explicitly bounded prefer_source",
            ));
        }
    }
    for depth in depths.values() {
        if let Some(boundary) = &depth.boundary {
            let mut reader = boundary.reader::<DepthBoundaryManifest>()?;
            let b = reader
                .next()?
                .ok_or_else(|| invalid("missing depth boundary"))?;
            reconcile::validate_depth_recovery(&b.value)?;
            if reader.next()?.is_some() {
                return Err(invalid("multiple source boundary records"));
            }
        }
    }
    let prepared = partitions::prepare(&plan, &depths, config, observations, &quota)?;
    let mut segment_counts = BTreeMap::<usize, usize>::new();
    for interval in &plan {
        *segment_counts.entry(interval.source).or_default() += 1;
    }
    let continuation = segment_counts.values().all(|count| *count == 1);
    for o in observations.values_mut() {
        o.processing.execution_path = if continuation {
            "independent_depth_continuation"
        } else {
            "checkpointed_depth_selection"
        }
        .into();
    }
    crate::historical::normalize::progress::log::diagnostic(format_args!(
        "depth_execution_path={} source_segments={} prefix_replay=none",
        if continuation {
            "independent_depth_continuation"
        } else {
            "checkpointed_depth_selection"
        },
        plan.len()
    ));
    let directory = format!("streams/{}/depth", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetDepthWriter::new(&path, config.resources.parquet.clone())?;
    let segmented = config.reconciliation.depth.overlap_policy == DepthOverlap::PreferSource;
    if segmented {
        writer.enable_segmented_sources()?;
    } else {
        writer.enable_sources()?;
    }
    let mut global = BookStore::new();
    let mut global_sequence = None;
    let mut last_timestamp = None;
    let mut previous_source = None;
    let mut selected = BTreeMap::<usize, Ordinals>::new();
    let mut spans = Vec::new();
    let mut output_sources = std::collections::BTreeSet::new();
    let positions = executions
        .iter()
        .enumerate()
        .map(|(p, (i, _))| (*i, p))
        .collect::<BTreeMap<_, _>>();
    let mut range_count = 0u64;
    for (segment, interval) in plan.iter().enumerate() {
        if segment_counts[&interval.source] > 1 && !prepared.contains_key(&segment) {
            continue;
        }
        reporter.stage_count(
            unit.job.job_id.0,
            &unit.key,
            segment as u64,
            "validated depth ownership segments",
        );
        let task = unit
            .tasks
            .iter()
            .find(|(i, _)| *i == interval.source)
            .unwrap()
            .1;
        let depth = depths
            .get(&interval.source)
            .ok_or_else(|| invalid("missing selected depth source"))?;
        let mut source_book = BookStore::new();
        let mut source_sequence = None;
        let mut high = None;
        let mut previous_time = None;
        let mut recovering = false;
        let part = prepared.get(&segment);
        let mut expected_first = None;
        if let Some(part) = part {
            let (seq, h, t, r, ordinal) =
                partitions::restore(part, interval.source, segment, &mut source_book)?;
            source_sequence = seq;
            high = h;
            previous_time = t;
            recovering = r;
            expected_first = Some(ordinal);
            observations
                .get_mut(&interval.source)
                .unwrap()
                .processing
                .depth_checkpoint_restores += 1;
        }
        let mut reader = part.map_or(&depth.entries, |p| &p.entries).depth_reader()?;
        let mut tracker = None;
        let start = writer.metrics().levels_written;
        let mut events = 0u64;
        let mut equivalent = false;
        let mut meta_reader = part
            .map_or(&depth.metadata, |p| &p.metadata)
            .reader::<DepthMeta>()?;
        let execution = &mut executions[positions[&interval.source]].1;
        while let Some(row) = reader.next()? {
            let meta = meta_reader
                .next()?
                .ok_or_else(|| invalid("missing depth metadata frame"))?;
            if meta.value.payload_hash != row.checksum()
                || meta.value.source != row.value.source
                || meta.value.boundary != row.value.boundary
                || meta.value.operation != row.value.operation
                || meta.value.rows != row.value.events.len() as u64
            {
                return Err(invalid("depth metadata/payload mismatch"));
            }
            observations
                .get_mut(&interval.source)
                .unwrap()
                .processing
                .depth_payload_events += 1;
            let mut outcome = row.value;
            let ordinal = outcome
                .source
                .source_event_ordinal
                .ok_or_else(|| invalid("depth event missing ordinal"))?;
            if let Some(expected) = expected_first.take()
                && ordinal != expected
            {
                return Err(invalid("depth checkpoint source position mismatch"));
            }
            let timestamp = outcome.source.event_timestamp_ns;
            let snapshot = outcome.operation == DepthSourceOperation::Snapshot;
            if reset(outcome.source.sequence_end, source_sequence, snapshot) {
                high = None;
                previous_time = None;
                recovering = false;
            }
            let previous_sequence = source_sequence;
            let regression = high
                .filter(|v| timestamp < *v)
                .map(|v: i64| {
                    v.checked_sub(timestamp)
                        .ok_or_else(|| invalid("depth timestamp arithmetic overflow"))
                })
                .transpose()?
                .unwrap_or(0);
            if previous_time.is_some_and(|p| timestamp < p)
                && config.timestamp_regression.depth.policy == DepthTimestamp::SnapshotRecovery
            {
                recovering = !snapshot;
            }
            let recovery_checkpoint = snapshot && recovering;
            if snapshot {
                recovering = false;
            }
            high = Some(high.map_or(timestamp, |v: i64| v.max(timestamp)));
            previous_time = Some(timestamp);
            let need_proof = tracker.is_none()
                && global.is_initialized()
                && (config.reconciliation.depth.overlap_policy == DepthOverlap::SnapshotRecovery
                    || config.reconciliation.depth.overlap_policy == DepthOverlap::PreferSource
                        && config.reconciliation.depth.prefer_source.switch_boundary
                            == SwitchBoundary::SourceBoundary);
            let proven_before = need_proof
                && previous_sequence
                    .zip(global_sequence)
                    .is_some_and(|(a, b)| a == b)
                && exact(
                    &global,
                    &source_book,
                    &mut observations.get_mut(&interval.source).unwrap().processing,
                );
            apply(&mut source_book, &outcome)?;
            source_sequence = outcome.source.sequence_end;
            if i128::from(timestamp) < interval.start
                || i128::from(timestamp) >= interval.end
                || duplicate.get(&interval.source) == Some(&ordinal)
                || recovering
            {
                if !outcome.events.is_empty()
                    || outcome.boundary == DepthEventBoundary::Initialization
                {
                    equivalent = false;
                }
                continue;
            }
            if config.timestamp_regression.depth.policy == DepthTimestamp::SnapshotRecovery
                && regression > milliseconds(config.timestamp_regression.depth.max_regression_ms)?
            {
                return Err(invalid("depth recovery tolerance exceeded"));
            }
            let new_source = tracker.is_none();
            if new_source {
                if global.is_initialized()
                    && config.reconciliation.depth.overlap_policy == DepthOverlap::PreferSource
                {
                    match config.reconciliation.depth.prefer_source.switch_boundary {
                        SwitchBoundary::Snapshot if !snapshot => continue,
                        SwitchBoundary::SourceBoundary => {
                            let checkpoint = proven_before
                                || (snapshot
                                    && source_sequence
                                        .zip(global_sequence)
                                        .is_some_and(|(a, b)| a == b)
                                    && exact(
                                        &global,
                                        &source_book,
                                        &mut observations
                                            .get_mut(&interval.source)
                                            .unwrap()
                                            .processing,
                                    ));
                            if !checkpoint {
                                return Err(invalid(
                                    "depth source_boundary switch lacks matching reconstruction state and native sequence evidence",
                                ));
                            }
                        }
                        _ => {}
                    }
                } else if !snapshot {
                    return Err(invalid(
                        "depth history must start from an authoritative snapshot",
                    ));
                }
                writer.begin_source(task.task_id.0)?;
                tracker = Some(DepthBoundaryTracker::new(
                    task.exchange,
                    task.instrument_id,
                    task.symbol.clone(),
                    task.stream_id.0.clone(),
                    super::super::resolve_sequence_policy(task)?,
                    None,
                ));
            }
            let force_initial = new_source
                || (config.timestamp_regression.depth.policy == DepthTimestamp::SnapshotRecovery
                    && snapshot
                    && (regression > 0 || recovery_checkpoint));
            let old = outcome.events.len() as u64;
            if force_initial {
                outcome.boundary = DepthEventBoundary::Initialization;
                outcome.events = full(&source_book, task, &outcome.source);
            }
            adjusted(&mut execution.metrics, old, outcome.events.len() as u64)?;
            let o = observations.get_mut(&interval.source).unwrap();
            if last_timestamp.is_some_and(|last| timestamp < last) {
                o.processing.published_native_order = true;
            }
            if force_initial && global.is_initialized() {
                o.processing.recovery_segments += 1;
                if config.reconciliation.depth.overlap_policy == DepthOverlap::SnapshotRecovery
                    && !proven_before
                {
                    o.processing.unproven_continuity = true;
                }
            }
            if new_source && previous_source.is_some_and(|p| p != interval.source) {
                o.processing.source_switches += 1;
            }
            let selection = selected.entry(interval.source).or_default();
            let before = selection.ranges.len();
            selection.add(
                ordinal,
                &quota,
                config.resources.normalization_limits.metric_entries,
            )?;
            range_count += (selection.ranges.len() - before) as u64;
            crate::shared::historical::limits::check(
                "depth ownership ranges",
                config.resources.normalization_limits.metric_entries,
                range_count,
                "depth ownership aggregate inventory",
            )?;
            let metadata = outcome.source.clone();
            let boundary = outcome.boundary;
            let count = outcome.events.len() as u64;
            apply(&mut global, &outcome)?;
            // Induction: equal states + identical controlled updates change only
            // declared keys. Exclusions/switches invalidate it; exact transitions
            // re-establish it. No probabilistic state fingerprints.
            if !equivalent || boundary == DepthEventBoundary::Initialization {
                if !exact(&global, &source_book, &mut o.processing) {
                    return Err(invalid(
                        "selected depth diffs do not replay to the source reconstruction state",
                    ));
                }
                equivalent = true;
            } else {
                for event in &outcome.events {
                    o.processing.depth_changed_level_checks += 1;
                    let (a, b) = match event.side {
                        BookSide::Bid => (
                            global.bids().get(&event.price),
                            source_book.bids().get(&event.price),
                        ),
                        BookSide::Ask => (
                            global.asks().get(&event.price),
                            source_book.asks().get(&event.price),
                        ),
                    };
                    if a != b {
                        return Err(invalid("selected depth changed-level mismatch"));
                    }
                }
            }
            let initial = if boundary == DepthEventBoundary::Initialization {
                BoundaryBookState::from_book(&global)
            } else {
                None
            };
            writer.write_outcome(outcome)?;
            write_row(&mut execution.metrics, count)?;
            if boundary == DepthEventBoundary::Initialization && events > 0 {
                tracker.as_mut().unwrap().invalidate();
                if !force_initial {
                    o.processing.recovery_segments += 1;
                }
            }
            tracker
                .as_mut()
                .unwrap()
                .record_accepted(&metadata, boundary, initial);
            execution.metrics.record_timestamp(timestamp);
            events += 1;
            o.processing.published_logical_events += 1;
            last_timestamp = Some(timestamp);
            global_sequence = metadata.sequence_end;
            previous_source = Some(interval.source);
        }
        if meta_reader.next()?.is_some() {
            return Err(invalid("extra depth metadata frames"));
        }
        if let Some(mut tracker) = tracker {
            tracker.set_final_state(BoundaryBookState::from_book(&global));
            writer.write_boundary(tracker.finish())?;
            spans.push(SourceSpan {
                task_id: task.task_id.0,
                start_row: start,
                end_row: writer.metrics().levels_written,
                ranges: vec![],
            });
            output_sources.insert(interval.source);
        }
        let _ = events;
    }
    // Compact metadata traversal accounts each logical observation once in native
    // order. Never decode full snapshot levels just to count/classify them.
    for (index, depth) in &depths {
        let task = unit.tasks.iter().find(|(i, _)| i == index).unwrap().1;
        let (_, execution) = executions.iter_mut().find(|(i, _)| i == index).unwrap();
        let o = observations.get_mut(index).unwrap();
        let mut unselected_frames = if !segment_counts.contains_key(index) {
            Some(depth.entries.frame_reader()?)
        } else {
            None
        };
        let mut reader = depth.metadata.reader::<DepthMeta>()?;
        let mut high = None;
        let mut previous_time = None;
        let mut sequence = None;
        let mut recovering = false;
        let mut metadata_hash = 0;
        let mut metadata_count = 0;
        while let Some(row) = reader.next()? {
            if let Some(frames) = unselected_frames.as_mut() {
                let payload = frames
                    .next()?
                    .ok_or_else(|| invalid("unselected depth payload missing frame"))?;
                if payload.checksum() != row.value.payload_hash {
                    return Err(invalid("unselected depth payload/metadata mismatch"));
                }
            }
            metadata_count += 1;
            metadata_hash = chain(metadata_hash, row.checksum());
            if row.value.index != metadata_count {
                return Err(invalid("depth metadata index/order mismatch"));
            }
            o.processing.depth_metadata_events += 1;
            let outcome = row.value;
            let ordinal = outcome
                .source
                .source_event_ordinal
                .ok_or_else(|| invalid("depth accounting metadata missing source ordinal"))?;
            let timestamp = outcome.source.event_timestamp_ns;
            let snapshot = outcome.operation == DepthSourceOperation::Snapshot;
            if reset(outcome.source.sequence_end, sequence, snapshot) {
                high = None;
                previous_time = None;
                recovering = false;
            }
            let regression = previous_time.is_some_and(|v| timestamp < v);
            if regression
                && config.timestamp_regression.depth.policy == DepthTimestamp::SnapshotRecovery
            {
                recovering = !snapshot;
            }
            if snapshot {
                recovering = false;
            }
            previous_time = Some(timestamp);
            high = Some(high.map_or(timestamp, |v: i64| v.max(timestamp)));
            sequence = outcome.source.sequence_end;
            if selected.get(index).is_some_and(|s| s.contains(ordinal)) {
                o.processing.retain();
                continue;
            }
            let rows = outcome.rows;
            if duplicate.get(index) != Some(&ordinal) {
                if config.reconciliation.depth.overlap_policy != DepthOverlap::PreferSource
                    && !recovering
                {
                    return Err(invalid(
                        "snapshot_recovery overlap cannot intentionally exclude unique supplied observations",
                    ));
                }
                let chosen = if recovering {
                    None
                } else {
                    plan.get(plan.partition_point(|p| p.end <= i128::from(timestamp)))
                        .filter(|p| {
                            p.start <= i128::from(timestamp) && i128::from(timestamp) < p.end
                        })
                        .and_then(|p| unit.tasks.iter().find(|(id, _)| *id == p.source))
                        .map(|(_, task)| task.task_id.0)
                };
                o.processing
                    .exclude(timestamp, rows, recovering, false, ordinal, chosen)?;
                if recovering
                    && o.processing.timestamp_exclusions
                        > config.timestamp_regression.depth.max_discarded_events
                {
                    return Err(invalid(
                        "depth timestamp recovery requires an explicit sufficient discard allowance",
                    ));
                }
                super::super::policies::enforce_source_overlap(
                    &config.reconciliation.depth.prefer_source.loss(),
                    &o.processing,
                )?;
            }
            execution.metrics.counters.rows_suppressed = execution
                .metrics
                .counters
                .rows_suppressed
                .checked_add(rows)
                .ok_or_else(|| invalid("depth suppression overflow"))?;
            execution.metrics.counters.rows_unpublished = execution
                .metrics
                .counters
                .rows_unpublished
                .checked_sub(rows)
                .ok_or_else(|| invalid("depth suppression pending underflow"))?;
        }
        if let Some(frames) = unselected_frames.as_mut()
            && frames.next()?.is_some()
        {
            return Err(invalid("unselected depth payload has extra frames"));
        }
        if metadata_count != depth.entries.count() || metadata_hash != depth.metadata_chain {
            return Err(invalid("depth metadata sealed digest/count mismatch"));
        }
        if recovering {
            return Err(invalid(
                "depth timestamp recovery ended without a validated recovery snapshot",
            ));
        }
        if !output_sources.contains(index) {
            writer.begin_source(task.task_id.0)?;
            writer.write_boundary(DepthBoundaryManifest::new(
                task.exchange,
                task.instrument_id,
                task.symbol.clone(),
                task.stream_id.0.clone(),
            ))?;
            spans.push(SourceSpan {
                task_id: task.task_id.0,
                start_row: writer.metrics().levels_written,
                end_row: writer.metrics().levels_written,
                ranges: vec![],
            });
        }
        execution.output_paths.push(path.clone());
        execution.metrics.counters.validate()?;
    }
    writer.finish()?;
    super::super::streams::write_output(
        &path,
        &StreamOutput {
            stream_id: unit.tasks[0].1.stream_id.0.clone(),
            directory,
            sources: spans,
        },
    )
}
