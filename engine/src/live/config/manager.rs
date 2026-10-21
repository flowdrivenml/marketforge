//! Desired-state management only. There is deliberately no ConfigurationApplyPort
//! here: WP-24 owns runtime application and its separately observed status.
use crate::live::{
    model::{
        configuration::*,
        error::{ControlError, ErrorCode},
        snapshot::{Page, PageRequest},
    },
    ports::{
        configuration::{CompiledConfiguration, ConfigurationCompilerPort, ConfigurationStorePort},
        context::CallContext,
    },
};
use std::sync::Arc;
#[derive(Debug)]
pub enum ManagementError {
    Control(ControlError),
    Validation(ValidationReport),
}
impl From<ControlError> for ManagementError {
    fn from(e: ControlError) -> Self {
        Self::Control(e)
    }
}
/// Hand-off to WP-24, available only after the durable store returns success.
#[derive(Debug, Clone)]
pub struct AcceptedConfiguration {
    pub revision: ConfigurationRevision,
    pub plan: CompiledConfiguration,
}
pub struct ConfigurationManager {
    compiler: Arc<dyn ConfigurationCompilerPort>,
    store: Arc<dyn ConfigurationStorePort>,
}
impl ConfigurationManager {
    pub fn new(
        compiler: Arc<dyn ConfigurationCompilerPort>,
        store: Arc<dyn ConfigurationStorePort>,
    ) -> Self {
        Self { compiler, store }
    }
    pub async fn desired(
        &self,
        context: &CallContext,
    ) -> Result<ConfigurationRevision, ControlError> {
        self.store.load(context).await
    }
    pub async fn revision(
        &self,
        context: &CallContext,
        id: ConfigRevisionId,
    ) -> Result<ConfigurationRevision, ControlError> {
        self.store.revision(context, id).await
    }
    pub async fn history(
        &self,
        context: &CallContext,
        page: PageRequest,
    ) -> Result<Page<ConfigurationRevision>, ControlError> {
        self.store.history(context, page).await
    }
    pub async fn diff(
        &self,
        context: &CallContext,
        before: ConfigRevisionId,
        after: ConfigRevisionId,
    ) -> Result<ConfigurationDiff, ControlError> {
        let before = self.store.revision(context, before).await?;
        let after = self.store.revision(context, after).await?;
        super::diff::between(&before, &after).map_err(|e| ControlError::new(e, None))
    }
    pub async fn validate(
        &self,
        context: &CallContext,
        proposal: &ConfigurationProposal,
    ) -> Result<CompiledConfiguration, ManagementError> {
        let base = self.store.load(context).await?;
        let plan = self
            .compiler
            .compile(&base, proposal)
            .map_err(ManagementError::Validation)?;
        if plan
            .boundaries
            .contains(&ReconfigurationBoundary::ServiceRestart)
        {
            return Err(ManagementError::Control(ControlError::new(
                ErrorCode::UnsupportedCapability,
                None,
            )));
        }
        Ok(plan)
    }
    pub async fn accept(
        &self,
        context: &CallContext,
        proposal: &ConfigurationProposal,
    ) -> Result<AcceptedConfiguration, ManagementError> {
        let plan = self.validate(context, proposal).await?;
        let revision = self
            .store
            .compare_and_swap(context, plan.base_revision, plan.desired.clone())
            .await?;
        if revision.desired != plan.desired {
            return Err(ManagementError::Control(ControlError::new(
                ErrorCode::InvalidCompletion,
                None,
            )));
        }
        Ok(AcceptedConfiguration { revision, plan })
    }
    pub async fn restore(
        &self,
        context: &CallContext,
        expected: ConfigRevisionId,
        revision: ConfigRevisionId,
    ) -> Result<AcceptedConfiguration, ManagementError> {
        let contents = self.store.revision(context, revision).await?;
        self.accept(
            context,
            &ConfigurationProposal::Restore {
                expected_revision: expected,
                contents,
            },
        )
        .await
    }
    /// Compose desired status with a snapshot supplied by the runtime owner.
    /// Absence is represented honestly; WP-09 never declares adoption itself.
    pub async fn status(
        &self,
        context: &CallContext,
        observed: Option<&EffectiveConfiguration>,
    ) -> Result<EffectiveConfiguration, ControlError> {
        let desired = self.store.load(context).await?;
        if let Some(observed) = observed {
            if observed.desired.0 > desired.id.0
                || observed.applied.is_some_and(|id| id.0 > desired.id.0)
                || observed.adopted.iter().any(|(_, id)| id.0 > desired.id.0)
            {
                return Err(ControlError::new(ErrorCode::StaleGeneration, None));
            }
            let mut status = observed.clone();
            status.desired = desired.id;
            return Ok(status);
        }
        // Resolved here denotes the desired plan, not runtime effectiveness.
        let resolved = desired
            .desired
            .resolve(usize::MAX)
            .map_err(|e| ControlError::new(e, None))?;
        Ok(EffectiveConfiguration {
            desired: desired.id,
            applied: None,
            adopted: vec![],
            resolved,
        })
    }
}
