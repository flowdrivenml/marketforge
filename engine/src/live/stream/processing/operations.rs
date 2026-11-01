//! Production derived processors. State is owned by the existing CPU registration.
use super::*;
use crate::live::{
    infrastructure::processing::{MailboxLimits, Processor},
    stream::router::*,
};
use crate::{
    live::{
        config::general::DerivedProcessingConfig,
        infrastructure::processing::mailbox::WorkBatch,
        stream::{persistence::StreamDefinition, registry::StreamGraph, request::*},
    },
    shared::merge::MergeKey,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default)]
pub struct DerivedStatus {
    pub ordering: Option<MergeOrdering>,
    pub watermarks: Vec<Option<i64>>,
    pub retained_publications: usize,
    pub retained_events: usize,
    pub retained_bytes: usize,
    pub last_reference_times: Vec<Option<i64>>,
    pub reference_ages_ns: Vec<Option<u64>>,
    pub watermark_lag_ns: Vec<Option<u64>>,
    pub worker_thread: Option<String>,
}
pub(crate) struct PendingInput {
    batch: WorkBatch<ProcessingInput>,
    cursor: usize,
    admitted_at: Instant,
}
#[derive(Default)]
pub(crate) struct RetainedProcessingState {
    pending: BTreeMap<u64, PendingInput>,
    // Exactly one cached head for each pending ordered publication. None is an
    // explicit untimed baseline barrier and sorts before timed data. Payloads
    // stay charged in pending until their final cursor is consumed.
    heads: BinaryHeap<Reverse<(Option<MergeKey>, u64)>>,
    reference_holds: Vec<Option<WorkBatch<ProcessingInput>>>,
    pub(crate) deadline: Option<Instant>,
    pub(crate) status: DerivedStatus,
}
pub struct RetainedProcessorInput {
    pub batch: WorkBatch<ProcessingInput>,
    /// Prefix already consumed; terminal originals must never be blindly replayed.
    pub next_record_ordinal: usize,
    pub reference_hold: bool,
}
impl RetainedProcessingState {
    fn refresh(&mut self) -> Result<(), String> {
        self.status.retained_publications =
            self.pending.len() + self.reference_holds.iter().filter(|h| h.is_some()).count();
        let mut batches = self
            .pending
            .values()
            .map(|p| &p.batch)
            .chain(self.reference_holds.iter().filter_map(Option::as_ref));
        let (events, bytes) = batches
            .try_fold((0usize, 0usize), |(events, bytes), batch| {
                Some((
                    events.checked_add(count(publication(&batch[0])))?,
                    bytes.checked_add(batch.accounted_bytes())?,
                ))
            })
            .ok_or("retained accounting overflow")?;
        self.status.retained_events = events;
        self.status.retained_bytes = bytes;
        Ok(())
    }
    pub(crate) fn terminal(&mut self) -> Vec<RetainedProcessorInput> {
        self.deadline = None;
        self.heads.clear();
        std::mem::take(&mut self.pending)
            .into_values()
            .map(|p| RetainedProcessorInput {
                batch: p.batch.canceled(),
                next_record_ordinal: p.cursor,
                reference_hold: false,
            })
            .chain(
                self.reference_holds
                    .iter_mut()
                    .filter_map(Option::take)
                    .map(|b| {
                        let next_record_ordinal = count(publication(&b[0]));
                        RetainedProcessorInput {
                            batch: b.canceled(),
                            next_record_ordinal,
                            reference_hold: true,
                        }
                    }),
            )
            .collect()
    }
}
fn count(p: &RouterPublication) -> usize {
    match p {
        RouterPublication::ReplayComplete => 0,
        RouterPublication::Trades { records, .. } => records.len(),
        RouterPublication::Depth(events) => events.len(),
        RouterPublication::Derived(events) => events.len(),
    }
}
fn publication(input: &ProcessingInput) -> &RouterPublication {
    match &input.delivery {
        RouterDelivery::Publication { publication, .. } => publication,
        _ => unreachable!("adapter rejects discontinuity"),
    }
}
fn event(input: &ProcessingInput, ordinal: usize) -> DerivedEvent {
    DerivedEvent::from_delivery(
        &input.delivery,
        input.input_position,
        input.admission_ordinal,
        ordinal,
    )
    .expect("managed Router input has valid record evidence")
}

fn key(input: &ProcessingInput, ordinal: usize) -> Result<Option<MergeKey>, String> {
    let (timestamp, baseline) = match publication(input) {
        RouterPublication::Trades { records, .. } => {
            (Some(records[ordinal].envelope.event_timestamp_ns), false)
        }
        RouterPublication::Depth(events) => (
            events[ordinal].timestamp_ns(),
            matches!(events[ordinal], RouterDepthEvent::Snapshot { .. }),
        ),
        RouterPublication::Derived(events) => (
            events[ordinal].native.timestamp_ns(),
            matches!(
                events[ordinal].native,
                NativeMarketEvent::Depth(RouterDepthEvent::Snapshot { .. })
            ),
        ),
        RouterPublication::ReplayComplete => unreachable!(),
    };
    match timestamp {
        Some(t) => Ok(Some(MergeKey::new(
            t,
            u32::try_from(input.input_position).map_err(|_| "rank overflow")?,
            input.admission_ordinal,
            ordinal as u64,
        ))),
        None if baseline => Ok(None),
        None => Err("missing exchange timestamp on ordered data event".into()),
    }
}

struct DerivedProcessor {
    context: ProcessorContext,
    limits: DerivedProcessingConfig,
    ordered: bool,
    lateness: i64,
    observed: Vec<Option<i64>>,
    last_local: BTreeMap<(usize, String), MergeKey>,
    depth_sequences: BTreeMap<(usize, String), (u64, u64, u64)>,
    last_emitted: Option<MergeKey>,
    conversion: Option<super::conversion::LiveConversion>,
    finished: Vec<bool>,
    completion_emitted: bool,
}
impl DerivedProcessor {
    fn admit(&mut self, state: &mut RetainedProcessingState) -> Result<(), String> {
        let batch = self.context.input.take_work(1);
        if batch.is_empty() {
            batch.complete();
            return Ok(());
        }
        let input = &batch[0];
        let rank = input.input_position;
        if rank >= self.observed.len() {
            return Err("invalid input position".into());
        }
        // Retain first so every later validation failure is terminal-reportable.
        let id = input.admission_ordinal;
        state.pending.insert(
            id,
            PendingInput {
                batch,
                cursor: 0,
                admitted_at: Instant::now(),
            },
        );
        let input = &state.pending[&id].batch[0];
        if self.finished[rank] {
            return Err("input after finite replay completion".into());
        }
        if matches!(publication(input), RouterPublication::ReplayComplete) {
            self.finished[rank] = true;
            state.pending.remove(&id).unwrap().batch.complete();
            return Ok(());
        }
        for ordinal in 0..count(publication(input)) {
            let e = event(input, ordinal);
            if let NativeMarketEvent::Depth(depth) = &e.native {
                let identity = (rank, e.origin.stream_id.clone());
                let (g, b) = depth.generations();
                match depth {
                    RouterDepthEvent::Snapshot { baseline, .. } => {
                        if self.depth_sequences.get(&identity).is_some_and(
                            |(old_g, old_b, old_seq)| {
                                g < *old_g
                                    || (g == *old_g && b < *old_b)
                                    || (g == *old_g
                                        && b == *old_b
                                        && baseline.last_sequence < *old_seq)
                            },
                        ) {
                            return Err("depth snapshot sequence regression".into());
                        }
                        self.depth_sequences
                            .insert(identity, (g, b, baseline.last_sequence));
                    }
                    RouterDepthEvent::Delta { baseline, .. } => {
                        if self.depth_sequences.get(&identity).is_none_or(
                            |(old_g, old_b, old_seq)| {
                                g != *old_g || b != *old_b || baseline.last_sequence <= *old_seq
                            },
                        ) {
                            return Err("depth sequence conflict or missing baseline".into());
                        }
                        self.depth_sequences
                            .insert(identity, (g, b, baseline.last_sequence));
                    }
                    RouterDepthEvent::Invalidated { .. } => {
                        return Err("upstream depth continuity invalidated".into());
                    }
                }
            }
        }
        if self.ordered {
            for ordinal in 0..count(publication(input)) {
                if let Some(k) = key(input, ordinal)? {
                    let origin = event(input, ordinal).origin.stream_id;
                    if self
                        .last_local
                        .get(&(rank, origin.clone()))
                        .is_some_and(|previous| k < *previous)
                    {
                        return Err("source-local timestamp/sequence conflict".into());
                    }
                    if self.observed[rank]
                        .is_some_and(|max| k.timestamp_ns < max.saturating_sub(self.lateness))
                        || self.last_emitted.is_some_and(|old| k <= old)
                    {
                        return Err(
                            "late event violates emitted frontier or lateness contract".into()
                        );
                    }
                    self.observed[rank] = Some(
                        self.observed[rank].map_or(k.timestamp_ns, |old| old.max(k.timestamp_ns)),
                    );
                    self.last_local.insert((rank, origin), k);
                } else if self
                    .last_local
                    .contains_key(&(rank, event(input, ordinal).origin.stream_id))
                {
                    return Err("untimed baseline after ordered data".into());
                }
            }
            state.status.watermarks = self
                .observed
                .iter()
                .map(|t| t.map(|t| t.saturating_sub(self.lateness)))
                .collect();
        }
        let frontier = state
            .status
            .watermarks
            .iter()
            .copied()
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().min());
        state.status.watermark_lag_ns = self
            .observed
            .iter()
            .map(|t| {
                t.zip(frontier)
                    .map(|(t, w)| u64::try_from(i128::from(t) - i128::from(w)).unwrap_or(u64::MAX))
            })
            .collect();
        if self.ordered {
            state.heads.push(Reverse((key(input, 0)?, id)));
        }
        state.refresh()?;
        if state.status.retained_events > self.limits.state_events
            || state.status.retained_bytes > self.limits.state_bytes
        {
            return Err("derived state capacity exhausted".into());
        }
        Ok(())
    }
    fn next(
        &self,
        state: &RetainedProcessingState,
    ) -> Result<Option<(u64, Option<MergeKey>)>, String> {
        if !self.ordered {
            return Ok(state.pending.first_key_value().map(|(&id, _)| (id, None)));
        }
        let Some(&Reverse((key, id))) = state.heads.peek() else {
            return Ok(None);
        };
        Ok((key.is_none()
            || state.status.watermarks.iter().enumerate().all(|(r, w)| {
                self.finished[r] || w.is_some_and(|w| w > key.unwrap().timestamp_ns)
            }))
        .then_some((id, key)))
    }
}
impl Processor for DerivedProcessor {
    fn run_turn(&mut self, _: usize) -> bool {
        unreachable!("fallible production processor")
    }
    fn try_run_turn(&mut self, max: usize) -> Result<bool, String> {
        // The state mutex has no pool, registry or Router lock beneath it.
        // Service reads it only for status/deadlines and after worker retirement.
        let shared = self.context.retained.clone();
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        state.status.worker_thread = std::thread::current().name().map(str::to_owned);
        for _ in 0..max {
            if self.context.input.is_empty() {
                break;
            }
            self.admit(&mut state)?;
        }
        for _ in 0..max {
            let Some((i, k)) = self.next(&state)? else {
                break;
            };
            // A publication can reveal another head after a prefix is consumed.
            // Validate the actual emission frontier as well as admission progress.
            if let (Some(current), Some(previous)) = (k, self.last_emitted) {
                if current < previous {
                    return Err(format!(
                        "event-time emission frontier regressed: {previous:?} -> {current:?}"
                    ));
                }
            }
            let p = &state.pending[&i];
            let mut e = event(&p.batch[0], p.cursor);
            let rank = p.batch[0].input_position;
            let reference = p.batch[0].role == InputRole::Reference;
            if let Some(conversion) = &mut self.conversion {
                if reference {
                    conversion.reference(
                        &p.batch[0].stream_id,
                        k.ok_or("missing reference timestamp")?,
                        &e,
                    )?;
                    state.status.last_reference_times[rank] = k.map(|k| k.timestamp_ns);
                } else if let Some(key) = k.or_else(|| {
                    e.native.timestamp_ns().map(|t| {
                        MergeKey::new(
                            t,
                            rank as u32,
                            p.batch[0].admission_ordinal,
                            p.cursor as u64,
                        )
                    })
                }) {
                    conversion.value(key, &mut e)?;
                } else if !e.baseline_barrier {
                    return Err("missing exchange timestamp on conversion data event".into());
                }
                // Untimed snapshots are explicit native baseline barriers only.
            }
            if !reference {
                if let Some(v) = e.valuation.as_ref().filter(|_| self.conversion.is_some()) {
                    for r in &v.evaluation.references {
                        state.status.reference_ages_ns[r.key.stream_rank as usize] = Some(
                            u64::try_from(
                                i128::from(e.native.timestamp_ns().unwrap())
                                    - i128::from(r.key.timestamp_ns),
                            )
                            .map_err(|_| "reference age overflow")?,
                        );
                    }
                }
                self.context
                    .output
                    .try_emit(RouterPublication::Derived(vec![e].into()))
                    .map_err(|e| e.reason.to_owned())?;
            }
            self.last_emitted = k.or(self.last_emitted);
            if self.ordered {
                state.heads.pop();
            }
            state.pending.get_mut(&i).unwrap().cursor += 1;
            if state.pending[&i].cursor == count(publication(&state.pending[&i].batch[0])) {
                let batch = state.pending.remove(&i).unwrap().batch;
                if reference {
                    if let Some(old) = state.reference_holds[rank].replace(batch) {
                        old.complete();
                    }
                } else {
                    batch.complete();
                }
            } else if self.ordered {
                let p = &state.pending[&i];
                let head = key(&p.batch[0], p.cursor)?;
                state.heads.push(Reverse((head, i)));
            }
        }
        state.refresh()?;
        // Startup and source continuity have their own lifecycle checks. Only
        // retained work with no eligible head arms a computation-progress timer.
        state.deadline =
            if self.ordered && !state.pending.is_empty() && self.next(&state)?.is_none() {
                state
                    .pending
                    .values()
                    .map(|p| p.admitted_at + Duration::from_millis(self.limits.stall_timeout_ms))
                    .min()
            } else {
                None
            };
        if self.finished.iter().all(|done| *done)
            && state.pending.is_empty()
            && !self.completion_emitted
        {
            self.context
                .output
                .try_emit(RouterPublication::ReplayComplete)
                .map_err(|e| e.reason.to_owned())?;
            self.completion_emitted = true;
            state.deadline = None;
        }
        if state.deadline.is_some_and(|d| Instant::now() >= d) {
            let mut reason =
                "event-time frontier stalled waiting for required input progress".to_owned();
            if let Some(conversion) = &self.conversion {
                if let Some(p) = state
                    .pending
                    .values()
                    .find(|p| p.batch[0].role == InputRole::Data)
                {
                    if let Some(key) = key(&p.batch[0], p.cursor)? {
                        let mut event = event(&p.batch[0], p.cursor);
                        if let Err(detail) = conversion.value(key, &mut event) {
                            reason.push_str(&format!(": {detail}"));
                        }
                    }
                }
            }
            return Err(reason);
        }
        Ok(!self.context.input.is_empty() || self.next(&state)?.is_some())
    }
}
pub(crate) fn resolve_start(
    graph: &StreamGraph,
    id: &str,
    limits: DerivedProcessingConfig,
) -> Result<StartProcessor, String> {
    let StreamDefinition::Derived(request) =
        &graph.get(id).ok_or("unknown derived stream")?.definition
    else {
        return Err("not a derived stream".into());
    };
    if request.input_specs().len() > limits.max_inputs {
        return Err("processing dependency fan-in exceeds limit".into());
    }
    let conversion = if request.operation == DerivedOperation::Convert {
        Some(super::conversion::LiveConversion::resolve(
            graph,
            request,
            limits.max_inputs,
            limits.planning_steps,
        )?)
    } else {
        None
    };
    if request.operation == DerivedOperation::Merge && request.target_quote_asset.is_some() {
        return Err("Merge does not convert values; declare a Convert dependency".into());
    }
    if graph.data_sources(id).map_err(|e| e.to_string())?.len() > limits.max_inputs {
        return Err("original source fan-in exceeds limit".into());
    }
    let ordered =
        request.ordering == Some(MergeOrdering::EventTime) || !request.reference_inputs.is_empty();
    let lateness = i64::try_from(if request.operation == DerivedOperation::Convert {
        request.conversion.unwrap_or_default().max_lateness_ns
    } else {
        request.max_lateness_ns.unwrap_or(0)
    })
    .map_err(|_| "lateness exceeds timestamp range")?;
    let inputs = request.input_specs();
    let n = inputs.len();
    let ordering = request.ordering;
    Ok(StartProcessor {
        stream_id: id.into(),
        inputs,
        output_semantics: PublicationSemantics::Derived,
        input_limits: MailboxLimits {
            max_events: limits.state_events,
            max_bytes: limits.state_bytes,
        },
        output_limits: MailboxLimits {
            max_events: limits.state_events,
            max_bytes: limits.state_bytes,
        },
        factory: Box::new(move |context| {
            context.retained.lock().unwrap().reference_holds = (0..n).map(|_| None).collect();
            context.retained.lock().unwrap().status = DerivedStatus {
                ordering: Some(if ordered {
                    MergeOrdering::EventTime
                } else {
                    ordering.unwrap_or(MergeOrdering::ArrivalOrder)
                }),
                watermarks: vec![None; n],
                last_reference_times: vec![None; n],
                reference_ages_ns: vec![None; n],
                watermark_lag_ns: vec![None; n],
                ..Default::default()
            };
            Ok(PreparedProcessor::ready(DerivedProcessor {
                context,
                limits,
                ordered,
                lateness,
                observed: vec![None; n],
                last_local: BTreeMap::new(),
                depth_sequences: BTreeMap::new(),
                last_emitted: None,
                conversion,
                finished: vec![false; n],
                completion_emitted: false,
            }))
        }),
    })
}

#[cfg(all(test, target_os = "linux"))]
mod selection_measurement {
    use super::*;
    use crate::live::infrastructure::processing::{ProcessorMailbox, mailbox::BufferBudget};
    use crate::live::stream::{persistence::*, registry::*};
    use std::sync::{Arc, Mutex};
    #[repr(C)]
    struct Timespec {
        seconds: std::os::raw::c_long,
        nanoseconds: std::os::raw::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: i32, time: *mut Timespec) -> i32;
    }
    fn cpu_ns() -> u64 {
        let mut time = Timespec {
            seconds: 0,
            nanoseconds: 0,
        };
        // Linux CLOCK_THREAD_CPUTIME_ID: benchmark-only timing, no new dependency.
        assert_eq!(unsafe { clock_gettime(3, &mut time) }, 0);
        time.seconds as u64 * 1_000_000_000 + time.nanoseconds as u64
    }
    #[test]
    #[ignore = "small explicit local selection benchmark; not a correctness test"]
    fn selector_cpu_benchmark() {
        let source=PersistedStreamDefinition{stream_id:"seed".into(),recording_stream_key:Some(1),definition:StreamDefinition::Source(serde_json::from_value(serde_json::json!({"instrument_id":15,"exchange":"binance","symbol":"BTCUSDT","market_category":"spot","instrument_type":"spot","base_asset":"BTC","quote_asset":"USDT","quantity_type":"base","channel":"trades"})).unwrap()),desired:Default::default(),revision:1};
        let graph = StreamGraph::restore(vec![source], vec![]).unwrap();
        let mut runtime = StreamRuntimeRegistry::new(&graph);
        let generation = runtime.begin_start("seed").unwrap();
        let template:crate::shared::domain::canonical::Trade=serde_json::from_value(serde_json::json!({"envelope":{"event_timestamp_ns":1,"exchange":"binance","instrument_id":15,"symbol":"BTCUSDT","stream_id":"seed"},"sequence":1,"side":"Buy","price":"2","quantity_base":"3"})).unwrap();
        println!("mode,inputs,backlog,emitted,cpu_ns_per_event,wall_ns_per_event");
        for ordered in [false, true] {
            for inputs in [1, 4, 16] {
                for backlog in [32, 512, 2048] {
                    let mut cpu = 0;
                    let mut wall = 0;
                    let repeats = (4096 / backlog).max(1);
                    for _ in 0..repeats {
                        let limits = MailboxLimits {
                            max_events: backlog + 1,
                            max_bytes: 64 << 20,
                        };
                        let budget = Arc::new(BufferBudget::new(limits).unwrap());
                        let input = Arc::new(ProcessorMailbox::new(limits).unwrap());
                        input.manage(budget.clone());
                        let output = ProcessingOutput::new(
                            limits,
                            budget,
                            &input,
                            Arc::new(tokio::sync::Notify::new()),
                        )
                        .unwrap();
                        let mut state = RetainedProcessingState::default();
                        state.status.watermarks = vec![Some(i64::MAX); inputs];
                        for i in 0..backlog {
                            let mut t = template.clone();
                            t.envelope.event_timestamp_ns = i as i64;
                            let publication = RouterPublication::trades(vec![t]);
                            let admission = RouterAdmission::fixture(
                                &format!("s{}", i % inputs),
                                generation,
                                (i / inputs + 1) as u64,
                                &publication,
                            );
                            let value = ProcessingInput {
                                stream_id: format!("s{}", i % inputs),
                                input_position: i % inputs,
                                role: InputRole::Data,
                                admission_ordinal: i as u64 + 1,
                                delivery: RouterDelivery::Publication {
                                    sequence: (i / inputs + 1) as u64,
                                    runtime_generation: generation,
                                    publication: Arc::new(publication),
                                    synchronized: true,
                                    event_valid: true,
                                    admission,
                                },
                            };
                            let bytes = value.retained_bytes().unwrap();
                            input.push_managed(value, bytes).unwrap();
                            let batch = input.take_work(1);
                            if ordered {
                                state
                                    .heads
                                    .push(Reverse((key(&batch[0], 0).unwrap(), i as u64 + 1)));
                            }
                            state.pending.insert(
                                i as u64 + 1,
                                PendingInput {
                                    batch,
                                    cursor: 0,
                                    admitted_at: Instant::now(),
                                },
                            );
                        }
                        let processor = DerivedProcessor {
                            context: ProcessorContext {
                                input,
                                output,
                                retained: Arc::new(Mutex::new(RetainedProcessingState::default())),
                            },
                            limits: DerivedProcessingConfig::default(),
                            ordered,
                            lateness: 10,
                            observed: vec![Some(i64::MAX); inputs],
                            last_local: BTreeMap::new(),
                            depth_sequences: BTreeMap::new(),
                            last_emitted: None,
                            conversion: None,
                            finished: vec![false; inputs],
                            completion_emitted: false,
                        };
                        let start_cpu = cpu_ns();
                        let start = Instant::now();
                        let mut emitted = 0;
                        while let Some((i, _)) = processor.next(&state).unwrap() {
                            if ordered {
                                state.heads.pop();
                            }
                            state.pending.remove(&i).unwrap().batch.complete();
                            emitted += 1;
                        }
                        wall += start.elapsed().as_nanos() as u64;
                        cpu += cpu_ns() - start_cpu;
                        assert_eq!(emitted, backlog);
                    }
                    let count = (backlog * repeats) as u64;
                    println!(
                        "{},{inputs},{backlog},{count},{},{}",
                        if ordered { "event_time" } else { "arrival" },
                        cpu / count,
                        wall / count
                    );
                }
            }
        }
    }
}
