//! Stable errors contain bounded public context, never raw credentials or payloads.
use super::operation::ResourceId;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    #[error("unsupported protocol version")]
    UnsupportedVersion,
    #[error("malformed message")]
    Malformed,
    #[error("message or resource bound exceeded")]
    Limit,
    #[error("invalid input")]
    InvalidInput,
    #[error("permission denied")]
    PermissionDenied,
    #[error("resource not found")]
    NotFound,
    #[error("configuration revision conflict")]
    RevisionConflict,
    #[error("generation is stale")]
    StaleGeneration,
    #[error("request id conflicts with earlier content")]
    IdempotencyConflict,
    #[error("owner capacity unavailable")]
    Capacity,
    #[error("operation cancelled")]
    Cancelled,
    #[error("deadline exceeded")]
    Deadline,
    #[error("unsupported capability")]
    UnsupportedCapability,
    #[error("dependency prevents operation")]
    Dependency,
    #[error("invalid operation state transition")]
    InvalidTransition,
    #[error("completion does not meet the declared objective")]
    InvalidCompletion,
    #[error("service unavailable")]
    Unavailable,
    #[error("persistence failed")]
    Persistence,
    #[error("commit outcome is unknown")]
    CommitUnknown,
    #[error("internal error")]
    Internal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Validation,
    Authorization,
    Conflict,
    Resource,
    Transport,
    Persistence,
    Internal,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ControlError {
    pub category: ErrorCategory,
    pub code: ErrorCode,
    pub message: String,
    pub resource: Option<ResourceId>,
    pub retryable: bool,
}
impl ControlError {
    pub fn new(code: ErrorCode, resource: Option<ResourceId>) -> Self {
        use ErrorCode::*;
        let category = match code {
            PermissionDenied => ErrorCategory::Authorization,
            RevisionConflict | StaleGeneration | IdempotencyConflict | Dependency => {
                ErrorCategory::Conflict
            }
            Capacity | Limit => ErrorCategory::Resource,
            Persistence | CommitUnknown => ErrorCategory::Persistence,
            Unavailable | Deadline => ErrorCategory::Transport,
            Internal => ErrorCategory::Internal,
            _ => ErrorCategory::Validation,
        };
        Self {
            category,
            code,
            message: code.to_string(),
            resource,
            retryable: matches!(code, Capacity | Unavailable | Deadline | CommitUnknown),
        }
    }
}
