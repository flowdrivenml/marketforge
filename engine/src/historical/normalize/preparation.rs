//! Bounded source-local normalization using spare session compute permits.
//! Only owned canonical events and unsealed observations cross threads. Books,
//! Rc scratch workspaces and retained report handles never cross that boundary.
use super::boundary::DepthBoundaryManifest;
use super::{
    config::ProcessingConfig,
    metrics::*,
    progress::{ProgressReporter, ProgressTaskId},
    resources::compute,
    worker::{DepthSink, TradeSink},
};
use crate::{
    historical::job::WorkTask,
    shared::{
        domain::canonical::{DepthProcessingOutcome, Trade},
        error::{MarketForgeError, Result},
        historical::limits::LimitScope,
        normalization::TargetSchema,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::JoinHandle,
};

pub(crate) enum Event {
    Trade(Trade, u64),
    Depth(DepthProcessingOutcome, Option<[u8; 32]>),
    Boundary(DepthBoundaryManifest),
}
struct Sink {
    tx: SyncSender<Event>,
    fingerprint: Option<[u8; 32]>,
}
fn stopped() -> MarketForgeError {
    MarketForgeError::NotAttempted {
        reason: "source preparation consumer stopped".into(),
    }
}
impl Sink {
    fn send(&self, event: Event) -> Result<()> {
        self.tx.send(event).map_err(|_| stopped())
    }
}
impl TradeSink for Sink {
    fn write_trade(&mut self, _: Trade) -> Result<()> {
        Err(MarketForgeError::InvalidConfiguration(
            "parallel trade preparation requires source ordinal".into(),
        ))
    }
    fn write_trade_at(&mut self, trade: Trade, ordinal: u64) -> Result<()> {
        self.send(Event::Trade(trade, ordinal))
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
impl DepthSink for Sink {
    fn observe_source_record(&mut self, record: &serde_json::Value) -> Result<()> {
        self.fingerprint = Some(super::reconciled::fingerprint(record)?);
        Ok(())
    }
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        let fingerprint = self.fingerprint.take();
        self.send(Event::Depth(outcome, fingerprint))
    }
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        self.send(Event::Boundary(boundary))
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
// ScopedIntegrityMetrics contains Rc retained spills. Source-local workers have
// not sealed/spilled yet: move their bounded maps explicitly, without serializing
// or rematerializing a retained report, and reconstruct on the consuming worker.
struct Portable {
    accepted: u64,
    source: Option<super::reconcile::SourceObservation>,
    counters: ProcessingCounters,
    integrity: IntegrityMetrics,
    global: CategoryStatistics,
    unattributed: CategoryStatistics,
    files: BTreeMap<IntegrityScope, CategoryStatistics>,
    hourly: BTreeMap<(IntegrityScope, i64), CategoryStatistics>,
    daily: BTreeMap<(IntegrityScope, i64), CategoryStatistics>,
    start: Option<i64>,
    end: Option<i64>,
    result: Result<()>,
}
impl Portable {
    fn from(metrics: TaskMetrics, result: Result<()>) -> Self {
        assert!(
            metrics.scoped_integrity.retained.is_empty(),
            "source-local worker cannot seal retained observations"
        );
        let scoped = metrics.scoped_integrity;
        Self {
            accepted: metrics.accepted_depth_events,
            source: metrics.source,
            counters: metrics.counters,
            integrity: metrics.integrity,
            global: scoped.global,
            unattributed: scoped.unattributed,
            files: scoped.files,
            hourly: scoped.hourly,
            daily: scoped.daily,
            start: metrics.start_timestamp_ns,
            end: metrics.end_timestamp_ns,
            result,
        }
    }
    fn unpack(self, metrics: &mut TaskMetrics) -> Result<()> {
        *metrics = TaskMetrics {
            accepted_depth_events: self.accepted,
            source: self.source,
            counters: self.counters,
            integrity: self.integrity,
            scoped_integrity: ScopedIntegrityMetrics {
                global: self.global,
                unattributed: self.unattributed,
                files: self.files,
                hourly: self.hourly,
                daily: self.daily,
                ..Default::default()
            },
            start_timestamp_ns: self.start,
            end_timestamp_ns: self.end,
        };
        self.result
    }
}
pub(crate) struct Prepared {
    rx: Option<Receiver<Event>>,
    worker: Option<JoinHandle<Portable>>,
}
impl Prepared {
    pub fn consume(
        mut self,
        metrics: &mut TaskMetrics,
        mut emit: impl FnMut(Event) -> Result<()>,
    ) -> Result<()> {
        let mut primary = Ok(());
        while let Ok(event) = self.rx.as_ref().unwrap().recv() {
            if let Err(error) = emit(event) {
                primary = Err(error);
                break;
            }
        }
        drop(self.rx.take());
        let result = self.join().and_then(|result| result.unpack(metrics));
        primary.and(result)
    }
    pub fn cancel(mut self, metrics: &mut TaskMetrics) -> Result<()> {
        drop(self.rx.take());
        self.join()?.unpack(metrics)
    }
    fn join(&mut self) -> Result<Portable> {
        self.worker.take().unwrap().join().map_err(|_| {
            MarketForgeError::InvalidConfiguration(
                "source preparation worker panicked before validated completion".into(),
            )
        })
    }
}
impl Drop for Prepared {
    fn drop(&mut self) {
        drop(self.rx.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn spawn(
    task: WorkTask,
    config: ProcessingConfig,
    reporter: ProgressReporter,
    id: ProgressTaskId,
    permit: compute::Permit,
) -> Prepared {
    reporter.source_spooled(id, 0);
    let (tx, rx) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let _permit = permit;
        let _limits = LimitScope::enter(
            config.resources.normalization_limits,
            format!(
                "job={}, task={}, parallel source preparation",
                id.job_id, id.task_id
            ),
        );
        let _policy = super::policies::Scope::enter(&config, true);
        let key = format!("{}:task={}:preparation", task.stream_id.0, id.task_id);
        reporter.phase(id, &task.stream_id.0, "parallel source preparation");
        reporter.resource_snapshot(&key,serde_json::json!({"active_source_preparations":1,"task_id":id.task_id,"stage":"source decode/normalize"}));
        let mut sink = Sink {
            tx,
            fingerprint: None,
        };
        let mut metrics = TaskMetrics::default();
        let policy = super::task::worker_integrity_policy(&config.integrity_policy);
        let mut progress = super::progress::WorkerProgress::new(&reporter, id);
        let result = if task.normalizations[0].target_schema == TargetSchema::Trade {
            super::worker::process_trade_task_with_metrics_and_progress(
                &task,
                &mut sink,
                &mut metrics,
                &policy,
                &ScopedIntegrityMetrics::default(),
                &mut progress,
            )
        } else {
            super::resolve_sequence_policy(&task).and_then(|sequence| {
                super::worker::process_depth_task_with_metrics_and_progress(
                    &task,
                    &mut sink,
                    &mut metrics,
                    &policy,
                    &ScopedIntegrityMetrics::default(),
                    sequence,
                    &mut progress,
                )
            })
        };
        progress.finish(&metrics.counters);
        reporter.resource_snapshot(&key,serde_json::json!({"active_source_preparations":0,"records_read":metrics.counters.records_read,"container_validated":result.is_ok()}));
        Portable::from(metrics, result)
    });
    Prepared {
        rx: Some(rx),
        worker: Some(worker),
    }
}
pub(crate) struct Preparations {
    ready: BTreeMap<usize, Prepared>,
    launched: BTreeSet<usize>,
    capacity: usize,
}
impl Preparations {
    pub fn new(capacity: usize) -> Self {
        Self {
            ready: BTreeMap::new(),
            launched: BTreeSet::new(),
            capacity,
        }
    }
    pub fn cancel(&mut self, index: usize, metrics: &mut TaskMetrics) -> Result<()> {
        self.ready
            .remove(&index)
            .map_or(Ok(()), |p| p.cancel(metrics))
    }
    pub fn take(
        &mut self,
        index: usize,
        tasks: &[(usize, &WorkTask)],
        config: &ProcessingConfig,
        reporter: &ProgressReporter,
        job: u64,
    ) -> Option<Prepared> {
        let position = tasks.iter().position(|(i, _)| *i == index)?;
        for (i, task) in &tasks[position..] {
            if self.ready.len() >= self.capacity {
                break;
            }
            if self.launched.contains(i) {
                continue;
            }
            let Some(permit) = compute::spare() else {
                break;
            };
            self.launched.insert(*i);
            self.ready.insert(
                *i,
                spawn(
                    (*task).clone(),
                    config.clone(),
                    reporter.clone(),
                    ProgressTaskId::new(job, task.task_id.0),
                    permit,
                ),
            );
        }
        self.ready.remove(&index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::historical::config::{
        FormatCatalog, normalization_fixture::bitget_trade_sources,
        selection_test_support as support,
    };
    use std::{io::Write, time::Duration};
    #[test]
    fn independent_sources_make_progress_before_consumption_and_cancel_without_permit_deadlock() {
        let root = tempfile::tempdir().unwrap();
        let batch = bitget_trade_sources(false, 2).unwrap();
        let config = support::runtime();
        let mut job = batch
            .resolve_job(
                &batch.jobs[0],
                &FormatCatalog::load(support::root().join("data/.config/historical")).unwrap(),
                &config,
                &support::root(),
            )
            .unwrap();
        for (i, task) in job.tasks.iter_mut().enumerate() {
            task.input_path = root.path().join(format!("{i}.zip"));
            let mut zip = zip::ZipWriter::new(std::fs::File::create(&task.input_path).unwrap());
            zip.start_file(
                "BTCUSDT_UMCBL_test.csv",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
            zip.write_all(b"trade_id,timestamp,price,side,volume(quote),size(base)\n1,1000,100,buy,100,1\n2,1001,100,buy,100,1\n3,1002,100,buy,100,1\n").unwrap();
            zip.finish().unwrap();
        }
        let budget = compute::ComputeBudget::new(3);
        compute::with_budget(&budget, || {
            let reporter = ProgressReporter::coalesced();
            let mut preparations = Preparations::new(2);
            // Non-monotonic original indices exercise task-order scheduling.
            let tasks = vec![(9, &job.tasks[0]), (2, &job.tasks[1])];
            let mut first = preparations
                .take(9, &tasks, &config, &reporter, job.job_id.0)
                .unwrap();
            let mut second = preparations
                .take(2, &tasks, &config, &reporter, job.job_id.0)
                .unwrap();
            assert_eq!(preparations.launched.len(), 2);
            assert!(compute::spare().is_none());
            // Both canonical queues become ready before either source is drained.
            // These are progress handshakes, not wall-time throughput assertions.
            assert!(matches!(
                first
                    .rx
                    .as_mut()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap(),
                Event::Trade(_, 1)
            ));
            assert!(matches!(
                second
                    .rx
                    .as_mut()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap(),
                Event::Trade(_, 1)
            ));
            let events = reporter.state().take_events(100);
            assert!(events.len() < 100);
            let mut a = TaskMetrics::default();
            let mut b = TaskMetrics::default();
            let _ = first.cancel(&mut a);
            let _ = second.cancel(&mut b);
            assert!(a.counters.records_read > 0 && b.counters.records_read > 0);
            assert!(compute::spare().is_some());
            assert!(
                preparations
                    .take(2, &tasks, &config, &reporter, job.job_id.0)
                    .is_none()
            );
        });
    }
}
