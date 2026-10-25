mod book_adapter;
mod bound_depth_ingress;
mod bound_trade_ingress;
mod depth_book_budget;
mod depth_buffer;
mod depth_continuity;
mod depth_continuity_compiler;
mod depth_controller;
mod depth_publication;
mod depth_resources;
mod depth_source_binding;
mod depth_timestamp;
mod incident;
mod incident_lifecycle;
mod model;
mod rest_depth_endpoint;
mod rest_depth_request;
mod rest_depth_sync;
mod sequence_adapter;
mod source_plan_compatibility;
mod tracker;
mod trade_controller;
mod trade_identity;
mod trade_integrity_compiler;
mod trade_integrity_evaluator;
mod trade_source_binding;
mod validation;

pub use book_adapter::{to_book_change, to_book_changes};
pub use bound_depth_ingress::BoundDepthIngress;
pub use bound_trade_ingress::{BoundTradeIngress, BoundTradeIngressOutcome};
pub use depth_book_budget::{DepthBookBudget, DepthBookBudgetError, DepthBookLimits};
pub use depth_buffer::{
    DepthBufferError, DepthBufferLimits, DepthSyncToken, PreSnapshotDepthBuffer,
};
pub use depth_continuity::{
    DepthContinuityDecision, DepthContinuityValidator, DepthIgnoreReason, DepthSequenceBaseline,
    DepthViolationReason,
};
pub use depth_continuity_compiler::{
    CompiledDepthContinuityPlan, DepthBridgeRule, DepthContinuityCompileError,
    DepthContinuityCompiler, DepthContinuityFamily, DepthSnapshotSource,
};
pub use depth_controller::{
    AcceptedDepthOutput, DepthControllerIgnoreReason, DepthControllerOutcome,
    DepthControllerRecoveryReason, DepthCoverage, LiveDepthController,
};
pub use depth_publication::{
    DepthPublicationError, DepthPublicationEvent, DepthPublicationFence, DepthPublicationPhase,
    DepthPublicationState, PreparedDepthPublication,
};
pub use depth_resources::{DepthResourceAccounting, DepthResourceError, DepthResourceLimits};
pub use depth_source_binding::{DepthSourceBinding, DepthSourceBindingError, DepthSourceEpoch};
pub use depth_timestamp::{
    DepthAcceptedTimestamp, DepthSourceTimestamp, DepthTimestampComposition,
};
pub use incident::{
    ConfirmedIntegrityIncident, IncidentOrigin, IncidentReportingError, IntegrityIncidentReporter,
};
pub use incident_lifecycle::{
    ClassifiedEpisodeOutcome, ClosedIntegrityEpisode, EpisodeReportOutcome, IntegrityEpisodeError,
    IntegrityEpisodeResolution, IntegrityEpisodeToken, IntegrityFailureIdentity,
    IntegrityFailureKind, IntegrityIncidentLifecycle,
};
pub use model::{
    GapHistoryCoverage, GapHistorySnapshot, GapReason, GapRecord, GapWindowStats, GapWindows,
    IntegrityWindow, StreamIntegritySnapshot,
};
pub use rest_depth_endpoint::{CompiledRestDepthEndpoint, RestDepthEndpointError};
pub use rest_depth_request::{RestDepthRequestContext, RestDepthRequestError};
pub use rest_depth_sync::{RestDepthSyncCoordinator, RestDepthSyncPhase};
pub use sequence_adapter::{
    BookSequenceAdapter, LegacySequenceMode, PreparedSequenceUpdate, SequenceAdapterError,
};
pub use source_plan_compatibility::{SourcePlanCompatibility, SourcePlanCompatibilityError};
pub use tracker::GapTracker;
pub use trade_controller::{
    AcceptedTradeBatch, LiveTradeController, TradeControllerError, TradeControllerOutcome,
    TradeCoverageHold,
};
pub use trade_identity::{
    TradeIdentityDecision, TradeIdentityError, TradeIdentityLimits, TradeIdentityTracker,
};
pub use trade_integrity_compiler::{
    CompiledTradeIntegrityPlan, TradeCoverageAction, TradeIdentityKind, TradeIntegrityCompileError,
    TradeIntegrityCompiler, TradeSequenceKind,
};
pub use trade_integrity_evaluator::{
    TradeIntegrityBaseline, TradeIntegrityEvaluation, TradeIntegrityEvaluationError,
    TradeIntegrityEvaluator, TradeIntegrityObservation, TradeIntegrityObservationKind,
};
pub use trade_source_binding::{TradeSourceBinding, TradeSourceBindingError, TradeSourceEpoch};
