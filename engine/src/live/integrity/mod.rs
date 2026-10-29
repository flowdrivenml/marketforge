mod book_adapter;
mod depth;
mod facade;
mod incidents;
mod model;
mod sequence_adapter;
mod source_plan_compatibility;
mod tracker;
mod trades;
mod validation;

pub use book_adapter::{to_book_change, to_book_changes};
pub use depth::binding::{DepthSourceBinding, DepthSourceBindingError, DepthSourceEpoch};
pub use depth::book_budget::{DepthBookBudget, DepthBookBudgetError, DepthBookLimits};
pub use depth::buffer::{
    DepthBufferError, DepthBufferLimits, DepthSyncToken, PreSnapshotDepthBuffer,
};
pub use depth::continuity::{
    DepthContinuityDecision, DepthContinuityValidator, DepthIgnoreReason, DepthSequenceBaseline,
    DepthViolationReason,
};
pub use depth::continuity_compiler::{
    CompiledDepthContinuityPlan, DepthBridgeRule, DepthContinuityCompileError,
    DepthContinuityCompiler, DepthContinuityFamily, DepthSnapshotSource,
};
pub use depth::controller::{
    AcceptedDepthOutput, DepthControllerIgnoreReason, DepthControllerOutcome,
    DepthControllerRecoveryReason, DepthCoverage, LiveDepthController,
};
pub use depth::ingress::BoundDepthIngress;
pub use depth::publication::{
    DepthPublicationError, DepthPublicationEvent, DepthPublicationFence, DepthPublicationPhase,
    DepthPublicationState, PreparedDepthPublication,
};
pub use depth::resources::{DepthResourceAccounting, DepthResourceError, DepthResourceLimits};
pub use depth::rest::endpoint::{CompiledRestDepthEndpoint, RestDepthEndpointError};
pub use depth::rest::request::{RestDepthRequestContext, RestDepthRequestError};
pub use depth::rest::sync::{RestDepthSyncCoordinator, RestDepthSyncPhase};
pub use depth::timestamps::{
    DepthAcceptedTimestamp, DepthSourceTimestamp, DepthTimestampComposition,
};
pub use facade::{
    CreateDepthIntegrity, CreateTradeIntegrity, IntegrityAdmissionError, IntegrityAdmissionState,
    IntegrityBatchResult, IntegrityChannel, IntegrityDecision, IntegrityEffect,
    IntegrityFacadeFactory, IntegrityFactoryError, IntegrityInput, IntegrityLifecycleError,
    IntegrityLifecycleEvent, IntegrityLifecycleOutcome, IntegrityOutcome,
    IntegrityPublicationAdapter, IntegrityPublicationError, IntegrityRecoveryError,
    IntegrityRecoveryResult, IntegrityTradeRecoveryCommitError, IntegrityTradeRecoveryError,
    RestDepthEndpointConfig, StreamIntegrityHandle, StreamIntegrityStatus, TradeRecoveryPreflight,
};
pub use incidents::lifecycle::{
    ClassifiedEpisodeOutcome, ClosedIntegrityEpisode, EpisodeReportOutcome, IntegrityEpisodeError,
    IntegrityEpisodeResolution, IntegrityEpisodeToken, IntegrityFailureIdentity,
    IntegrityFailureKind, IntegrityIncidentLifecycle,
};
pub use incidents::reporting::{
    ConfirmedIntegrityIncident, IncidentOrigin, IncidentReportingError, IntegrityIncidentReporter,
};
pub use model::{
    GapHistoryCoverage, GapHistorySnapshot, GapReason, GapRecord, GapWindowStats, GapWindows,
    IntegrityWindow, StreamIntegritySnapshot,
};
pub use sequence_adapter::{
    BookSequenceAdapter, LegacySequenceMode, PreparedSequenceUpdate, SequenceAdapterError,
};
pub use source_plan_compatibility::{SourcePlanCompatibility, SourcePlanCompatibilityError};
pub use tracker::GapTracker;
pub use trades::binding::{TradeSourceBinding, TradeSourceBindingError, TradeSourceEpoch};
pub use trades::compiler::{
    CompiledTradeIntegrityPlan, TradeCoverageAction, TradeIdentityKind, TradeIntegrityCompileError,
    TradeIntegrityCompiler, TradeSequenceKind,
};
pub use trades::controller::{
    AcceptedTradeBatch, LiveTradeController, TradeControllerError, TradeControllerOutcome,
    TradeCoverageHold,
};
pub use trades::evaluator::{
    TradeIntegrityBaseline, TradeIntegrityEvaluation, TradeIntegrityEvaluationError,
    TradeIntegrityEvaluator, TradeIntegrityObservation, TradeIntegrityObservationKind,
};
pub use trades::identity::{
    TradeIdentityDecision, TradeIdentityError, TradeIdentityLimits, TradeIdentityTracker,
};
pub use trades::ingress::{BoundTradeIngress, BoundTradeIngressOutcome};
