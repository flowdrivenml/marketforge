//! Compilation is pure; compare-and-swap persists desired state before effects.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{configuration::*, error::ControlError};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledConfiguration {
    pub base_revision: ConfigRevisionId,
    pub impacts: Vec<ConfigurationImpact>,
    pub desired: DesiredConfiguration,
    pub resolved: ResolvedConfiguration,
    pub boundaries: Vec<ReconfigurationBoundary>,
}
pub trait ConfigurationCompilerPort: Send + Sync {
    fn compile(
        &self,
        base: &ConfigurationRevision,
        proposal: &ConfigurationProposal,
    ) -> Result<CompiledConfiguration, ValidationReport>;
}
pub trait ConfigurationStorePort: Send + Sync {
    fn load<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>>;
    fn revision<'a>(
        &'a self,
        context: &'a CallContext,
        id: ConfigRevisionId,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>>;
    fn history<'a>(
        &'a self,
        context: &'a CallContext,
        page: crate::live::model::snapshot::PageRequest,
    ) -> RoutingFuture<
        'a,
        Result<crate::live::model::snapshot::Page<ConfigurationRevision>, ControlError>,
    >;
    /// Implementations MUST atomically check expected, durably persist, then
    /// return the new revision. A no-op must not invent a semantic revision.
    fn compare_and_swap<'a>(
        &'a self,
        context: &'a CallContext,
        expected: ConfigRevisionId,
        desired: DesiredConfiguration,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>>;
}
pub trait ConfigurationApplyPort: Send + Sync {
    /// Only a successfully persisted revision may be reconciled. Desired,
    /// runtime-applied and individually adopted revisions remain distinct.
    fn apply<'a>(
        &'a self,
        context: &'a CallContext,
        revision: ConfigurationRevision,
        plan: CompiledConfiguration,
    ) -> RoutingFuture<'a, Result<EffectiveConfiguration, ControlError>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationImpact {
    pub resource: crate::live::model::operation::ResourceId,
    pub boundary: ReconfigurationBoundary,
}
