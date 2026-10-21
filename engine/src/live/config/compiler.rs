//! Pure candidates and fail-closed configuration compilation. Catalog and
//! verified protocol evidence are required; derived economic compilation remains
//! an explicit missing dependency rather than structural approval.
use crate::live::model::{configuration::*, error::ErrorCode};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationCandidate {
    pub desired: DesiredConfiguration,
    pub resolved: ResolvedConfiguration,
}
fn report(path: String, code: ErrorCode) -> ValidationReport {
    ValidationReport {
        valid: false,
        issues: vec![ConfigIssue {
            path,
            code,
            message: code.to_string(),
        }],
        boundaries: vec![],
    }
}
pub fn build_candidate(
    base: &ConfigurationRevision,
    proposal: &ConfigurationProposal,
    max_items: usize,
    max_edges: usize,
) -> Result<ConfigurationCandidate, ValidationReport> {
    let expected = match proposal {
        ConfigurationProposal::Changes(update) => update.expected_revision,
        ConfigurationProposal::Restore {
            expected_revision, ..
        } => *expected_revision,
    };
    if expected != base.id {
        return Err(report(
            "expected_revision".into(),
            ErrorCode::RevisionConflict,
        ));
    }
    let mut desired = base.desired.clone();
    match proposal {
        ConfigurationProposal::Restore { contents, .. } => desired = contents.desired.clone(),
        ConfigurationProposal::Changes(update) => {
            if update.changes.len() > max_items {
                return Err(report("changes".into(), ErrorCode::Limit));
            }
            for (i, change) in update.changes.iter().enumerate() {
                change
                    .validate_structure(max_items)
                    .map_err(|e| report(format!("changes[{i}]"), e))?;
                apply_change(&mut desired, change)
                    .map_err(|e| report(format!("changes[{i}]"), e))?;
            }
        }
    }
    let resolved = desired
        .resolve(max_items)
        .map_err(|e| report("configuration".into(), e))?;
    super::validation::dependency_order(&desired, max_items, max_edges).map_err(|issue| {
        ValidationReport {
            valid: false,
            issues: vec![issue],
            boundaries: vec![],
        }
    })?;
    Ok(ConfigurationCandidate { desired, resolved })
}
fn apply_change(
    desired: &mut DesiredConfiguration,
    change: &ConfigChange,
) -> Result<(), ErrorCode> {
    match change {
        ConfigChange::SetLogging { level } => desired.global.logging = *level,
        ConfigChange::SetMetrics { settings } => desired.global.metrics = settings.clone(),
        ConfigChange::SetRecordingDefaults { settings } => {
            desired.global.recording = settings.clone()
        }
        ConfigChange::SetRecordingBatch {
            batch_events,
            flush_interval_ns,
        } => {
            desired.global.recording.batch_events = *batch_events;
            desired.global.recording.flush_interval_ns = *flush_interval_ns;
        }
        ConfigChange::SetMaxSubscribers { value } => desired.global.max_subscribers = *value,
        ConfigChange::SetBudgets { budgets } => desired.global.budgets = budgets.clone(),
        ConfigChange::RegisterSource { definition } => {
            if desired.sources.iter().any(|s| s.id == definition.id) {
                return Err(ErrorCode::InvalidInput);
            }
            desired.sources.push(definition.clone());
        }
        ConfigChange::RemoveSource { stream } => {
            let i = desired
                .sources
                .iter()
                .position(|s| &s.id == stream)
                .ok_or(ErrorCode::NotFound)?;
            desired.sources.remove(i);
        }
        ConfigChange::SetSourceEnabled { stream, enabled } => {
            desired
                .sources
                .iter_mut()
                .find(|s| &s.id == stream)
                .ok_or(ErrorCode::NotFound)?
                .enabled = *enabled
        }
        ConfigChange::SetSourceRecording { stream, value } => {
            desired
                .sources
                .iter_mut()
                .find(|s| &s.id == stream)
                .ok_or(ErrorCode::NotFound)?
                .recording = value.clone()
        }
        ConfigChange::SetSourceRecovery { stream, value } => {
            desired
                .sources
                .iter_mut()
                .find(|s| &s.id == stream)
                .ok_or(ErrorCode::NotFound)?
                .recovery = value.clone()
        }
        ConfigChange::SetSourceMetrics { stream, value } => {
            desired
                .sources
                .iter_mut()
                .find(|s| &s.id == stream)
                .ok_or(ErrorCode::NotFound)?
                .metrics = value.clone()
        }
        ConfigChange::PutDerived { definition } => {
            if let Some(current) = desired.derived.iter_mut().find(|d| d.id == definition.id) {
                *current = definition.clone();
            } else {
                desired.derived.push(definition.clone());
            }
        }
        ConfigChange::RemoveDerived { stream } => {
            let i = desired
                .derived
                .iter()
                .position(|d| &d.id == stream)
                .ok_or(ErrorCode::NotFound)?;
            desired.derived.remove(i);
        }
        ConfigChange::ReplaceProfile { profile } => {
            if profile.api_family.trim().is_empty() {
                return Err(ErrorCode::InvalidInput);
            }
            if let Some(current) = desired
                .profiles
                .iter_mut()
                .find(|p| p.exchange == profile.exchange && p.api_family == profile.api_family)
            {
                *current = profile.clone();
            } else {
                desired.profiles.push(profile.clone());
            }
        }
    }
    Ok(())
}

/// Pure compiler: shared catalog and protocol capabilities, no runtime effects.
pub struct ConfigurationCompiler {
    pub catalog: std::sync::Arc<crate::instrument::InstrumentCatalog>,
    pub protocol: std::sync::Arc<dyn crate::live::ports::protocol::ExchangeProtocolPort>,
    pub max_items: usize,
    pub max_edges: usize,
}
impl crate::live::ports::configuration::ConfigurationCompilerPort for ConfigurationCompiler {
    fn compile(
        &self,
        base: &ConfigurationRevision,
        proposal: &ConfigurationProposal,
    ) -> Result<crate::live::ports::configuration::CompiledConfiguration, ValidationReport> {
        use crate::live::{
            model::identity::ChannelKind, ports::configuration::CompiledConfiguration,
        };
        if self.max_items == 0 {
            return Err(report("limits".into(), ErrorCode::InvalidInput));
        }
        let candidate = build_candidate(base, proposal, self.max_items, self.max_edges)?;
        let fail = |path: &str, code| report(path.into(), code);
        let mut profiles = std::collections::HashSet::new();
        for p in &candidate.desired.profiles {
            if p.api_family.trim().is_empty()
                || p.profile_revision == 0
                || !profiles.insert((p.exchange, &p.api_family))
            {
                return Err(fail("profiles", ErrorCode::InvalidInput));
            }
        }
        for source in &candidate.desired.sources {
            if source
                .key
                .validate()
                .unresolved_classifications()
                .next()
                .is_some()
            {
                return Err(fail(
                    "sources.instrument.classification",
                    ErrorCode::Dependency,
                ));
            }
            if self.catalog.resolve(&source.key.instrument).is_none() {
                return Err(fail("sources.instrument.catalog", ErrorCode::Dependency));
            }
            let plan = self
                .protocol
                .compile(&source.key)
                .map_err(|e| fail("sources.protocol", e.code))?;
            if plan.source != source.key
                || plan.profile.exchange != source.key.exchange
                || !candidate.desired.profiles.contains(&plan.profile)
            {
                return Err(fail("sources.profile", ErrorCode::InvalidInput));
            }
            plan.evidence
                .require(plan.profile.profile_revision)
                .map_err(|e| fail("sources.capability", e))?;
            if plan.max_message_bytes == 0
                || plan.subscription_payload.len() > plan.max_message_bytes
                || plan.unsubscribe_payload.len() > plan.max_message_bytes
            {
                return Err(fail("sources.message_limit", ErrorCode::Limit));
            }
            plan.heartbeat
                .validate(plan.max_message_bytes)
                .map_err(|e| fail("sources.heartbeat", e))?;
            if source.key.channel.kind == ChannelKind::L2 {
                plan.depth
                    .as_ref()
                    .ok_or_else(|| fail("sources.depth", ErrorCode::UnsupportedCapability))?
                    .validate_for_activation(plan.profile.profile_revision)
                    .map_err(|e| fail("sources.depth", e))?;
            }
        }
        // DAG validation is not proof of economic compatibility. The approved
        // pure derived-economic compiler/metadata contract is still absent.
        if !candidate.desired.derived.is_empty() {
            return Err(fail("derived.economic_compiler", ErrorCode::Dependency));
        }
        let after = ConfigurationRevision {
            id: base.id,
            parent: base.parent,
            created_at_ns: base.created_at_ns,
            desired: candidate.desired.clone(),
        };
        let diff =
            super::diff::between(base, &after).map_err(|e| fail("diff.frozen_contract", e))?;
        if diff.changes.is_empty() && candidate.desired != base.desired {
            return Err(fail("diff.order_only", ErrorCode::UnsupportedCapability));
        }
        let mut changes = diff.changes;
        // Preserve the frozen explicit default-change boundary even when its
        // final values happen to differ only in batch sizing. Remove net no-ops.
        if let ConfigurationProposal::Changes(update) = proposal {
            if update
                .changes
                .iter()
                .any(|c| matches!(c, ConfigChange::SetRecordingDefaults { .. }))
            {
                for change in &mut changes {
                    if matches!(change, ConfigChange::SetRecordingBatch { .. }) {
                        *change = ConfigChange::SetRecordingDefaults {
                            settings: candidate.desired.global.recording.clone(),
                        };
                    }
                }
            }
        }
        let impacts = impacts(base, &candidate.resolved, &changes, self.max_items)?;
        let mut boundaries = vec![];
        for change in &changes {
            let boundary = change.boundary();
            if !boundaries.contains(&boundary) {
                boundaries.push(boundary);
            }
        }
        Ok(CompiledConfiguration {
            base_revision: base.id,
            desired: candidate.desired,
            resolved: candidate.resolved,
            impacts,
            boundaries,
        })
    }
}
fn impacts(
    base: &ConfigurationRevision,
    next: &ResolvedConfiguration,
    changes: &[ConfigChange],
    max_items: usize,
) -> Result<Vec<crate::live::ports::configuration::ConfigurationImpact>, ValidationReport> {
    use crate::live::{model::operation::ResourceId, ports::configuration::ConfigurationImpact};
    use ReconfigurationBoundary::*;
    let old = base
        .desired
        .resolve(max_items)
        .map_err(|e| report("base".into(), e))?;
    let mut result = vec![];
    let mut add = |resource, boundary| {
        let item = ConfigurationImpact { resource, boundary };
        if !result.contains(&item) {
            result.push(item);
        }
    };
    for change in changes {
        match change {
            ConfigChange::SetLogging { .. }
            | ConfigChange::SetMetrics { .. }
            | ConfigChange::SetMaxSubscribers { .. }
            | ConfigChange::SetBudgets { .. }
            | ConfigChange::ReplaceProfile { .. } => add(ResourceId::Runtime, change.boundary()),
            _ => {}
        }
    }
    for s in &next.sources {
        if let Some(a) = old.sources.iter().find(|a| a.id == s.id) {
            if a.key != s.key || a.enabled != s.enabled || a.recovery != s.recovery {
                add(ResourceId::Source(s.id.clone()), StreamTransition);
            }
            if a.metrics != s.metrics {
                add(ResourceId::Source(s.id.clone()), NextMeasurement);
            }
            if a.recording != s.recording {
                let explicit_change = changes.iter().any(|c| matches!(c, ConfigChange::SetSourceRecording {stream,..} if *stream == s.id));
                let batch_only = !explicit_change
                    && changes
                        .iter()
                        .any(|c| matches!(c, ConfigChange::SetRecordingBatch { .. }));
                add(
                    ResourceId::Recording(s.id.clone()),
                    if batch_only {
                        NextBatch
                    } else {
                        StreamTransition
                    },
                );
            }
        } else {
            add(ResourceId::Source(s.id.clone()), StreamTransition);
        }
    }
    for a in &old.sources {
        if !next.sources.iter().any(|s| s.id == a.id) {
            add(ResourceId::Source(a.id.clone()), StreamTransition);
        }
    }
    Ok(result)
}
