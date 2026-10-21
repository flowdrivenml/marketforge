//! Processing owners serialize mutation, acceptance and captured-view boundaries.
use super::{
    context::CallContext,
    protocol::ProtocolMessage,
    routing::{EventBatch, RoutingFuture},
};
use crate::live::model::{
    codec::ExactDecimal,
    cursor::{BookGeneration, StreamCursor},
    error::ControlError,
    operation::ResourceVersion,
    replay::BookViewBoundary,
};
pub trait SourceProcessorPort: Send + Sync {
    fn ingest<'a>(
        &'a self,
        context: &'a CallContext,
        message: ProtocolMessage,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
    fn resynchronize<'a>(
        &'a self,
        context: &'a CallContext,
        generation: BookGeneration,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait ProcessorFactoryPort: Send + Sync {
    fn create<'a>(
        &'a self,
        context: &'a CallContext,
        stream: ResourceVersion,
        plan: super::protocol::AcquisitionPlan,
    ) -> RoutingFuture<'a, Result<std::sync::Arc<dyn SourceProcessorPort>, ControlError>>;
}
pub trait EventInputPort: Send + Sync {
    fn next<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<EventBatch, ControlError>>;
}
#[derive(Debug, Clone)]
pub struct BookViewCapture {
    pub boundary: BookViewBoundary,
    pub levels: Vec<crate::canonical::L2LevelUpdate>,
}
pub trait BookViewPort: Send + Sync {
    /// Capturing the book and installing follow at boundary must be serialized
    /// by the source owner. A separately sampled cursor is not a valid capture.
    fn capture<'a>(
        &'a self,
        context: &'a CallContext,
        stream: ResourceVersion,
        max_levels: usize,
        max_bytes: usize,
    ) -> RoutingFuture<'a, Result<BookViewCapture, ControlError>>;
}
#[derive(Debug, Clone)]
pub struct ReferenceQuote {
    pub cursor: StreamCursor,
    pub rate: ExactDecimal,
    pub exchange_at_ns: i64,
    pub received_at_ns: i64,
}
pub trait ReferenceReaderPort: Send + Sync {
    fn lookup<'a>(
        &'a self,
        context: &'a CallContext,
        source: ResourceVersion,
        at_ns: i64,
        max_age_ns: u64,
    ) -> RoutingFuture<'a, Result<ReferenceQuote, ControlError>>;
}

pub trait DerivedFactoryPort: Send + Sync {
    fn create<'a>(
        &'a self,
        context: &'a CallContext,
        definition: crate::live::model::derived::DerivedDefinition,
    ) -> RoutingFuture<
        'a,
        Result<std::sync::Arc<dyn super::lifecycle::OwnerLifecyclePort>, ControlError>,
    >;
}
