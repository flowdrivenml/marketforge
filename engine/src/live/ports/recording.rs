//! Backend contract. No backend receives permission to allocate event identities.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::{
    model::error::ControlError,
    recording::{
        commit::{RecordingWritePlan, TransactionOutcome},
        schema::SchemaSnapshot,
    },
};
pub trait RecordingBackendPort: Send + Sync {
    fn inspect_schema<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<SchemaSnapshot, ControlError>>;
    /// Atomically persist identities, parents, ordered children and coverage.
    /// Lost commit acknowledgement returns Unknown, never a guessed failure.
    fn commit<'a>(
        &'a self,
        context: &'a CallContext,
        plan: &'a RecordingWritePlan,
    ) -> RoutingFuture<'a, TransactionOutcome>;
}
