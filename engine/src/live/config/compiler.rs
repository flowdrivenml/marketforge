//! Candidate construction only. No candidate is an accepted/economically
//! compiled plan: catalog, normalization and verified capability checks remain
//! required before the ConfigurationCompilerPort may approve persistence.
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
