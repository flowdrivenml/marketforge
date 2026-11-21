use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use crate::historical::convert::progress::{
    ConversionOutcome, ConversionPhase, ConversionProgress, ConversionProgressEvent,
    ConversionProgressState,
};

use super::{ConversionDashboardContext, rate::Rate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum View {
    Overview,
    Details,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PartitionState {
    Queued,
    Running,
    Complete,
    Stopped,
    Failed(String),
}

pub(super) struct Partition {
    pub(super) state: PartitionState,
    pub(super) expected: Option<u64>,
    pub(super) metrics: ConversionProgress,
    pub(super) started: Option<Instant>,
    pub(super) ended: Option<Instant>,
    pub(super) rate: Rate,
}

impl Default for Partition {
    fn default() -> Self {
        Self {
            state: PartitionState::Queued,
            expected: None,
            metrics: ConversionProgress::default(),
            started: None,
            ended: None,
            rate: Rate::default(),
        }
    }
}

impl Partition {
    pub(super) fn elapsed(&self, now: Instant) -> Option<Duration> {
        self.started
            .map(|start| self.ended.unwrap_or(now).saturating_duration_since(start))
    }
}

pub(super) struct Stage {
    pub(super) phase: ConversionPhase,
    pub(super) started: Instant,
    pub(super) done: u64,
    pub(super) expected: Option<u64>,
    pub(super) work: String,
    pub(super) rate: Rate,
}

impl Stage {
    pub(super) fn unit(&self) -> &'static str {
        match self.phase {
            ConversionPhase::ReferencePrepass | ConversionPhase::TimestampScan => {
                "source rows scanned"
            }
            ConversionPhase::PhysicalConversion => "physical rows written",
            ConversionPhase::PhysicalValidation => "physical rows validated",
            ConversionPhase::SourcePreservation => "physical rows compared",
            _ => "",
        }
    }
}

pub(super) struct Dashboard {
    pub(super) context: ConversionDashboardContext,
    pub(super) started: Instant,
    pub(super) ended: Option<Instant>,
    pub(super) active: Option<Stage>,
    pub(super) completed: Vec<(ConversionPhase, Duration)>,
    pub(super) partitions: BTreeMap<u32, Partition>,
    pub(super) selected: Option<u32>,
    pub(super) workers: Option<usize>,
    pub(super) total: ConversionProgress,
    pub(super) authoritative: bool,
    aggregate_overflow: bool,
    pub(super) restored_updates: Option<u64>,
    pub(super) checkpoints: (usize, usize),
    pub(super) outcome: Option<ConversionOutcome>,
    pub(super) error: Option<String>,
    pub(super) interrupting: bool,
    pub(super) activity: VecDeque<String>,
    pub(super) view: View,
    pub(super) details_scroll: usize,
    pub(super) details_max_scroll: usize,
    pub(super) table_offset: usize,
    pub(super) page_height: usize,
}

impl Dashboard {
    pub(super) fn new(context: ConversionDashboardContext, now: Instant) -> Self {
        Self {
            context,
            started: now,
            ended: None,
            active: None,
            completed: Vec::new(),
            partitions: BTreeMap::new(),
            selected: None,
            workers: None,
            total: ConversionProgress::default(),
            authoritative: false,
            aggregate_overflow: false,
            restored_updates: None,
            checkpoints: (0, 0),
            outcome: None,
            error: None,
            interrupting: false,
            activity: VecDeque::new(),
            view: View::Overview,
            details_scroll: 0,
            details_max_scroll: 0,
            table_offset: 0,
            page_height: 1,
        }
    }

    fn log(&mut self, message: impl Into<String>) {
        if self.activity.len() == 24 {
            self.activity.pop_front();
        }
        self.activity.push_back(message.into());
    }

    pub(super) fn interrupt(&mut self) {
        if !self.interrupting && self.outcome.is_none() {
            self.interrupting = true;
            self.log("Interruption requested");
        }
    }

    pub(super) fn receive(&mut self, event: ConversionProgressEvent, now: Instant) {
        if self.outcome.is_some() {
            return;
        }
        match event {
            ConversionProgressEvent::PhaseStarted { phase }
                if !self
                    .completed
                    .iter()
                    .any(|&(complete, _)| complete == phase)
                    && self
                        .active
                        .as_ref()
                        .is_none_or(|stage| stage.phase != phase) =>
            {
                let expected = (phase == ConversionPhase::PhysicalConversion)
                    .then_some(self.context.expected_rows);
                self.active = Some(Stage {
                    phase,
                    started: now,
                    done: 0,
                    expected,
                    work: String::new(),
                    rate: Rate::default(),
                });
                self.log(format!("Started {}", phase.label()));
            }
            ConversionProgressEvent::PhaseCompleted { phase }
                if self
                    .active
                    .as_ref()
                    .is_some_and(|stage| stage.phase == phase) =>
            {
                let stage = self.active.take().unwrap();
                self.completed
                    .push((phase, now.saturating_duration_since(stage.started)));
                self.log(format!("Completed {}", phase.label()));
            }
            ConversionProgressEvent::PartitionsPlanned {
                partitions,
                workers,
            } => {
                self.workers = Some(workers);
                for (key, expected) in partitions {
                    self.partitions
                        .entry(key.partition_id)
                        .or_default()
                        .expected = Some(expected);
                }
                self.select_first();
            }
            ConversionProgressEvent::PartitionStarted { key } => {
                let partition = self.partitions.entry(key.partition_id).or_default();
                if partition.state == PartitionState::Queued {
                    partition.state = PartitionState::Running;
                    partition.started = Some(now);
                }
                self.select_first();
            }
            ConversionProgressEvent::PartitionCompleted { key, metrics } => {
                let partition = self.partitions.entry(key.partition_id).or_default();
                if matches!(
                    partition.state,
                    PartitionState::Queued | PartitionState::Running
                ) {
                    partition.state = PartitionState::Complete;
                    partition.metrics = metrics;
                    partition.ended = Some(now);
                    self.log(format!("Partition {} complete", key.partition_id));
                }
                self.select_first();
            }
            ConversionProgressEvent::PartitionFailed { key, message } => {
                let partition = self.partitions.entry(key.partition_id).or_default();
                if matches!(
                    partition.state,
                    PartitionState::Queued | PartitionState::Running
                ) {
                    partition.state = PartitionState::Failed(message.clone());
                    partition.ended = Some(now);
                    self.error = Some(message);
                    self.log(format!("Partition {} failed", key.partition_id));
                }
                self.select_first();
            }
            ConversionProgressEvent::TimestampScanProgress { scanned, expected } => {
                if let Some(stage) = self.stage_mut(ConversionPhase::TimestampScan) {
                    stage.done = stage.done.max(scanned);
                    stage.expected = Some(expected);
                }
            }
            ConversionProgressEvent::MaterializationTotals(metrics) => {
                self.total = metrics;
                self.authoritative = true;
            }
            ConversionProgressEvent::PublishedReceipt(receipt) if !self.authoritative => {
                self.total = ConversionProgress {
                    events_read: receipt.events_read,
                    events_written: receipt.events_written,
                    identity_events: receipt.identity_events,
                    converted_events: receipt.converted_events,
                    unavailable_events: receipt.unavailable_events,
                    skipped_events: receipt.skipped_events,
                    depth_levels_read: receipt.depth.as_ref().map_or(0, |depth| depth.levels_read),
                    depth_levels_converted: receipt
                        .depth
                        .as_ref()
                        .map_or(0, |depth| depth.levels_converted),
                    depth_levels_unavailable: receipt
                        .depth
                        .as_ref()
                        .map_or(0, |depth| depth.levels_unavailable),
                    depth_levels_skipped: receipt
                        .depth
                        .as_ref()
                        .map_or(0, |depth| depth.levels_skipped),
                    trade_events: self.context.trade_events,
                    depth_events: self.context.depth_events,
                    files_written: receipt.files_written,
                    row_groups_written: receipt.row_groups_written,
                    ..ConversionProgress::default()
                };
                self.restored_updates = Some(receipt.reference_updates);
                self.authoritative = true;
            }
            ConversionProgressEvent::ReferencePrepassProgress {
                events_scanned,
                total_events,
                checkpoints_created,
                total_checkpoints,
            } => {
                if let Some(stage) = self.stage_mut(ConversionPhase::ReferencePrepass) {
                    stage.done = stage.done.max(events_scanned);
                    stage.expected = Some(total_events);
                    stage.work = format!("Checkpoints {checkpoints_created}/{total_checkpoints}");
                    self.checkpoints = (checkpoints_created, total_checkpoints);
                }
            }
            ConversionProgressEvent::ValidationStarted {
                total_files,
                expected_events,
            } => {
                let expected = if expected_events == 0 {
                    self.context.expected_rows
                } else {
                    expected_events
                };
                if let Some(stage) = self.stage_mut(ConversionPhase::PhysicalValidation) {
                    stage.expected = Some(expected);
                    stage.work = format!("Validating {total_files} files");
                }
            }
            ConversionProgressEvent::ValidationProgress {
                files_completed,
                events_validated,
                row_groups_validated,
            }
            | ConversionProgressEvent::ValidationCompleted {
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                if let Some(stage) = self.stage_mut(ConversionPhase::PhysicalValidation) {
                    stage.done = stage.done.max(events_validated);
                    stage.work = format!(
                        "{files_completed} files · {row_groups_validated} row groups checked"
                    );
                }
            }
            ConversionProgressEvent::SourcePreservationProgress {
                events_compared,
                total_events,
            } => {
                if let Some(stage) = self.stage_mut(ConversionPhase::SourcePreservation) {
                    stage.done = stage.done.max(events_compared);
                    stage.expected = Some(total_events);
                }
            }
            ConversionProgressEvent::Finished(outcome) => {
                if let ConversionOutcome::Failed(message) = &outcome {
                    self.error = Some(message.clone());
                }
                if !outcome.succeeded() {
                    for partition in self.partitions.values_mut() {
                        if partition.state == PartitionState::Running {
                            partition.state = PartitionState::Stopped;
                            partition.ended = Some(now);
                        }
                    }
                }
                self.ended = Some(now);
                self.log(outcome.label());
                self.outcome = Some(outcome);
            }
            _ => {}
        }
    }

    fn stage_mut(&mut self, phase: ConversionPhase) -> Option<&mut Stage> {
        self.active.as_mut().filter(|stage| stage.phase == phase)
    }

    fn select_first(&mut self) {
        if self.selected.is_none() {
            self.selected = self.partitions.keys().next().copied();
        }
    }

    pub(super) fn sample(&mut self, progress: &ConversionProgressState, now: Instant) {
        let snapshots = progress.snapshots();
        for (key, metrics) in snapshots {
            if let Some(partition) = self.partitions.get_mut(&key.partition_id) {
                if partition.state != PartitionState::Complete {
                    partition.metrics = metrics;
                }
                if partition.state == PartitionState::Running && self.outcome.is_none() {
                    partition.rate.sample(now, metrics.events_written);
                }
            }
        }
        if !self.authoritative {
            let total =
                ConversionProgress::checked_sum(self.partitions.values().map(|p| &p.metrics));
            self.aggregate_overflow = total.is_none();
            if let Some(total) = total {
                self.total = total;
            }
        }
        if let Some(stage) = &mut self.active {
            if stage.phase == ConversionPhase::PhysicalConversion {
                stage.done = self.total.events_written;
            }
            if self.outcome.is_none() && stage.expected.is_some_and(|total| total > 0) {
                stage.rate.sample(now, stage.done);
            }
        }
    }

    pub(super) fn status(&self) -> &str {
        if let Some(outcome) = &self.outcome {
            outcome.label()
        } else if self.interrupting {
            "INTERRUPTING"
        } else {
            "RUNNING"
        }
    }

    pub(super) fn counter_warning(&self) -> Option<&'static str> {
        if self.aggregate_overflow && !self.authoritative {
            return Some("Partition counters exceed aggregate capacity; showing last valid totals");
        }
        if self.authoritative {
            let total = &self.total;
            let accounted = u128::from(total.identity_events)
                + u128::from(total.converted_events)
                + u128::from(total.unavailable_events)
                + u128::from(total.skipped_events);
            if accounted != u128::from(total.events_written) {
                return Some("Final valuation totals disagree with written rows");
            }
        }
        if self
            .active
            .as_ref()
            .is_some_and(|stage| stage.expected.is_some_and(|expected| stage.done > expected))
        {
            return Some("Progress exceeds its declared row count");
        }
        None
    }

    pub(super) fn navigate(&mut self, down: bool, page: bool) {
        let step = if page { self.page_height.max(1) } else { 1 };
        let move_index = |index: usize, max| {
            if down {
                index.saturating_add(step).min(max)
            } else {
                index.saturating_sub(step)
            }
        };
        match self.view {
            View::Overview => {
                let index = self
                    .partitions
                    .keys()
                    .position(|id| Some(*id) == self.selected)
                    .unwrap_or(0);
                self.selected = self
                    .partitions
                    .keys()
                    .nth(move_index(index, self.partitions.len().saturating_sub(1)))
                    .copied();
            }
            View::Details => {
                self.details_scroll = move_index(self.details_scroll, self.details_max_scroll)
            }
            View::Help => {}
        }
    }
}
