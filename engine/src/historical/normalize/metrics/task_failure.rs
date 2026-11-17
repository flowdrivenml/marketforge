//! One bounded structured error per task, carried by the existing streamed report.
use crate::{
    historical::job::{ProcessingJob, WorkTask},
    shared::error::MarketForgeError,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFailure {
    pub job_id: u64,
    pub task_id: u64,
    pub instrument_id: i64,
    pub stream_id: String,
    pub physical_source: String,
    pub archive_member: Option<String>,
    pub stage: String,
    pub typed_cause: String,
    pub message: String,
    pub stopped_by_another_failure: bool,
    pub details: Option<serde_json::Value>,
}
impl TaskFailure {
    pub fn new(job: &ProcessingJob, task: &WorkTask, error: &MarketForgeError) -> Self {
        let mut stage = "source processing";
        let mut cause = error;
        let mut reconciliation = None;
        loop {
            match cause {
                MarketForgeError::StreamFailure {
                    stage: s, source, ..
                } => {
                    stage = s;
                    cause = source;
                }
                MarketForgeError::ReportPersistence { processing, .. } => cause = processing,
                MarketForgeError::ReconciliationFailure { source, details } => {
                    reconciliation = Some(details.as_ref().clone());
                    cause = source;
                }
                _ => break,
            }
        }
        let (typed, details) = match cause {
            MarketForgeError::TradeIdentityViolation {
                source_index,
                previous_source_index,
                ..
            } => (
                "trade_identity",
                Some(
                    serde_json::json!({"source_index":source_index,"previous_source_index":previous_source_index}),
                ),
            ),
            MarketForgeError::NotAttempted { .. } => ("not_attempted", None),
            MarketForgeError::ResourceLimit(e) => (
                "resource_limit",
                Some(
                    serde_json::json!({"resource":e.resource,"permitted":e.permitted,"requested":e.requested,"stage":e.stage,"scope":e.scope}),
                ),
            ),
            MarketForgeError::RecordIntegrity { category, .. } => (
                "record_integrity",
                Some(serde_json::json!({"category":category})),
            ),
            MarketForgeError::SourceOpen { .. } => ("source_open", None),
            MarketForgeError::SourceRead { .. } => ("source_read", None),
            MarketForgeError::ArchiveOpen { .. } => ("archive_container", None),
            MarketForgeError::ArchiveMemberNotFound { .. } => ("archive_member_not_found", None),
            MarketForgeError::AmbiguousArchiveMembers { .. } => ("ambiguous_archive_members", None),
            MarketForgeError::InvalidConfiguration(_) => ("configuration_or_reconciliation", None),
            MarketForgeError::InvalidCanonical(_) => ("canonical_validation", None),
            _ => ("processing", None),
        };
        let typed = if reconciliation.is_some() && typed == "configuration_or_reconciliation" {
            "trade_reconciliation"
        } else {
            typed
        };
        let details = match (details, reconciliation) {
            (Some(mut cause), Some(context)) => {
                if let Some(object) = cause.as_object_mut() {
                    object.insert("reconciliation".into(), context);
                }
                Some(cause)
            }
            (None, Some(context)) => Some(serde_json::json!({"reconciliation":context})),
            (details, None) => details,
        };
        let stage = match cause {
            MarketForgeError::ResourceLimit(e) => e.stage,
            MarketForgeError::NotAttempted { .. } => "not_attempted",
            _ => stage,
        };
        Self {
            job_id: job.job_id.0,
            task_id: task.task_id.0,
            instrument_id: task.instrument_id,
            stream_id: task.stream_id.0.clone(),
            physical_source: task.input_path.to_string_lossy().into_owned(),
            archive_member: task.archive_member.clone(),
            stage: stage.into(),
            typed_cause: typed.into(),
            message: error.to_string().chars().take(1024).collect(),
            stopped_by_another_failure: matches!(cause, MarketForgeError::NotAttempted { .. }),
            details,
        }
    }
}
