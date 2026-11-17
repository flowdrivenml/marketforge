//! Fragmented ownership: one native source traversal, sealed segment payloads
//! and exact checkpoints. No archive re-read or source-prefix reconstruction.
use super::*;
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct Checkpoint {
    source: usize,
    segment: usize,
    next_ordinal: u64,
    state: Option<BoundaryBookState>,
    sequence: Option<u64>,
    high: Option<i64>,
    previous_time: Option<i64>,
    recovering: bool,
}
pub(super) struct Part {
    pub(super) entries: Spool,
    pub(super) metadata: Spool,
    checkpoint: Spool,
}
type Restored = (Option<u64>, Option<i64>, Option<i64>, bool, u64);
pub(super) fn restore(
    part: &Part,
    source: usize,
    segment: usize,
    book: &mut BookStore,
) -> Result<Restored> {
    let mut reader = part.checkpoint.reader::<Checkpoint>()?;
    let cp = reader
        .next()?
        .ok_or_else(|| invalid("missing depth checkpoint"))?
        .value;
    if reader.next()?.is_some() || cp.source != source || cp.segment != segment {
        return Err(invalid("depth checkpoint identity mismatch"));
    }
    if let Some(state) = cp.state {
        let convert = |levels: Vec<crate::shared::domain::book::boundary::BoundaryBookLevel>| {
            levels
                .into_iter()
                .map(|l| {
                    (
                        l.price,
                        BookLevel {
                            quantity_base: l.quantity_base,
                            quantity_quote: l.quantity_quote,
                            quantity_contracts: l.quantity_contracts,
                            order_count: l.order_count,
                        },
                    )
                })
                .collect()
        };
        book.replace_snapshot(convert(state.bids), convert(state.asks))?;
    }
    Ok((
        cp.sequence,
        cp.high,
        cp.previous_time,
        cp.recovering,
        cp.next_ordinal,
    ))
}
pub(super) fn prepare(
    plan: &[Interval],
    depths: &BTreeMap<usize, Depth>,
    config: &ProcessingConfig,
    observations: &mut BTreeMap<usize, SourceObservation>,
    quota: &Quota,
) -> Result<BTreeMap<usize, Part>> {
    let mut result = BTreeMap::new();
    for (index, depth) in depths {
        let owned_intervals: Vec<_> = plan
            .iter()
            .enumerate()
            .filter(|(_, p)| p.source == *index)
            .collect();
        if owned_intervals.len() < 2 {
            continue;
        }
        let mut book = BookStore::new();
        let mut sequence = None;
        let mut high = None;
        let mut previous_time = None;
        let mut recovering = false;
        let mut current = None;
        let mut writers: Option<(SpoolWriter, SpoolWriter)> = None;
        let mut reader = depth.entries.depth_reader()?;
        let mut metas = depth.metadata.reader::<DepthMeta>()?;
        let mut digest = 0;
        let mut count = 0;
        while let Some(row) = reader.next()? {
            let meta = metas
                .next()?
                .ok_or_else(|| invalid("missing partition metadata"))?;
            digest = chain(digest, meta.checksum());
            count += 1;
            if meta.value.payload_hash != row.checksum()
                || meta.value.source != row.value.source
                || meta.value.rows != row.value.events.len() as u64
                || meta.value.boundary != row.value.boundary
                || meta.value.operation != row.value.operation
                || meta.value.index != count
            {
                return Err(invalid("partition metadata/payload mismatch"));
            }
            let outcome = row.value;
            let timestamp = outcome.source.event_timestamp_ns;
            let snapshot = outcome.operation == DepthSourceOperation::Snapshot;
            let pos = owned_intervals.partition_point(|(_, p)| p.end <= i128::from(timestamp));
            let owned = owned_intervals
                .get(pos)
                .filter(|(_, p)| p.start <= i128::from(timestamp))
                .map(|(i, _)| *i);
            if let Some(segment) = owned
                && current != Some(segment)
            {
                if current.is_some_and(|old| segment < old) {
                    return Err(invalid(
                        "depth ownership would reorder native source events",
                    ));
                }
                if let Some((a, b)) = writers.take() {
                    a.finish()?;
                    b.finish()?;
                }
                quota.charge(512, "depth partition inventory")?;
                let checkpoint = depth.workspace.create()?;
                let mut cp = checkpoint.writer()?;
                cp.push(&Checkpoint {
                    source: *index,
                    segment,
                    next_ordinal: outcome
                        .source
                        .source_event_ordinal
                        .ok_or_else(|| invalid("checkpoint ordinal missing"))?,
                    state: BoundaryBookState::from_book(&book),
                    sequence,
                    high,
                    previous_time,
                    recovering,
                })?;
                cp.finish()?;
                let entries = depth.workspace.create()?;
                let metadata = depth.workspace.create()?;
                writers = Some((entries.writer()?, metadata.writer()?));
                result.insert(
                    segment,
                    Part {
                        entries,
                        metadata,
                        checkpoint,
                    },
                );
                current = Some(segment);
            }
            if let Some((a, b)) = writers.as_mut() {
                a.push_depth(&outcome)?;
                let mut meta = meta.value;
                meta.payload_hash = a.last_checksum();
                b.push(&meta)?;
            }
            if reset(outcome.source.sequence_end, sequence, snapshot) {
                high = None;
                previous_time = None;
                recovering = false;
            }
            if previous_time.is_some_and(|p| timestamp < p)
                && config.timestamp_regression.depth.policy == DepthTimestamp::SnapshotRecovery
            {
                recovering = !snapshot;
            }
            if snapshot {
                recovering = false;
            }
            high = Some(high.map_or(timestamp, |v: i64| v.max(timestamp)));
            previous_time = Some(timestamp);
            sequence = outcome.source.sequence_end;
            apply(&mut book, &outcome)?;
            observations
                .get_mut(index)
                .unwrap()
                .processing
                .depth_payload_events += 1;
        }
        if metas.next()?.is_some()
            || count != depth.entries.count()
            || digest != depth.metadata_chain
        {
            return Err(invalid("depth partition sealed-source mismatch"));
        }
        if let Some((a, b)) = writers.take() {
            a.finish()?;
            b.finish()?;
        }
        // Full original payload was consumed and checked. Only metadata is used
        // for accounting from here; segment spools own all required payloads.
        depth.entries.clone().delete()?;
    }
    Ok(result)
}
