mod depth;
pub(crate) mod integrity;
mod sink;
mod trades;

pub use depth::{process_depth_task_with_metrics, process_depth_task_with_metrics_and_progress};
pub use integrity::{enforce_integrity, record_integrity_error};
pub use sink::{DepthSink, TradeSink};
pub use trades::{
    TradeWorkerMetrics, process_trade_task, process_trade_task_with_metrics,
    process_trade_task_with_metrics_and_progress,
};

mod ordering;

pub(super) fn resource_scope(
    task: &crate::historical::job::WorkTask,
) -> crate::shared::historical::limits::LimitScope {
    use crate::shared::historical::limits::{LimitScope, current};
    LimitScope::enter(
        current().unwrap_or_default(),
        format!(
            "{}, stream={}, task={}, source={}",
            crate::shared::historical::limits::current_scope()
                .unwrap_or_else(|| "normalization".into()),
            task.stream_id.0,
            task.task_id.0,
            task.input_path.display()
        ),
    )
}

pub(crate) mod decoded;

pub(super) fn begin_observation(
    task: &crate::historical::job::WorkTask,
    metrics: &mut crate::historical::normalize::metrics::TaskMetrics,
) {
    metrics.source = Some(crate::historical::normalize::reconcile::SourceObservation {
        original_task_id: task.raw_schema["marketforge_source"]["task_id"]
            .as_u64()
            .unwrap_or(task.task_id.0),
        source_priority: task.raw_schema["marketforge_source"]["source_priority"]
            .as_u64()
            .unwrap_or(0),
        family_candidate: crate::historical::normalize::streams::is_family(task),
        archive_member: task.archive_member.clone(),
        ..Default::default()
    });
}
pub(super) fn finish_observation(metrics: &mut crate::historical::normalize::metrics::TaskMetrics) {
    if let Some(source) = metrics.source.as_mut() {
        source.physical_records_read = metrics.counters.records_read;
        source.observed_selected_records = metrics.counters.records_matched;
        source.filtered_records = metrics.counters.records_skipped_instrument;
    }
}
