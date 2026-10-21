//! Deterministic semantic diffs. Unrepresentable frozen-contract changes fail
//! explicitly rather than disappearing from restore/diff responses.
use crate::live::model::{configuration::*, error::ErrorCode};
pub fn between(
    before: &ConfigurationRevision,
    after: &ConfigurationRevision,
) -> Result<ConfigurationDiff, ErrorCode> {
    let a = &before.desired;
    let b = &after.desired;
    if a.schema_version != b.schema_version
        || a.global.recovery != b.global.recovery
        || a.global.shutdown_deadline_ns != b.global.shutdown_deadline_ns
    {
        return Err(ErrorCode::UnsupportedCapability);
    }
    let mut changes = vec![];
    if a.global.logging != b.global.logging {
        changes.push(ConfigChange::SetLogging {
            level: b.global.logging,
        });
    }
    if a.global.metrics != b.global.metrics {
        changes.push(ConfigChange::SetMetrics {
            settings: b.global.metrics.clone(),
        });
    }
    if a.global.recording != b.global.recording {
        if a.global.recording.enabled == b.global.recording.enabled
            && a.global.recording.policy == b.global.recording.policy
        {
            changes.push(ConfigChange::SetRecordingBatch {
                batch_events: b.global.recording.batch_events,
                flush_interval_ns: b.global.recording.flush_interval_ns,
            });
        } else {
            changes.push(ConfigChange::SetRecordingDefaults {
                settings: b.global.recording.clone(),
            });
        }
    }
    if a.global.max_subscribers != b.global.max_subscribers {
        changes.push(ConfigChange::SetMaxSubscribers {
            value: b.global.max_subscribers,
        });
    }
    if a.global.budgets != b.global.budgets {
        changes.push(ConfigChange::SetBudgets {
            budgets: b.global.budgets.clone(),
        });
    }
    for old in &a.sources {
        match b.sources.iter().find(|s| s.id == old.id) {
            None => changes.push(ConfigChange::RemoveSource {
                stream: old.id.clone(),
            }),
            Some(new) if old.key != new.key => {
                changes.push(ConfigChange::RemoveSource {
                    stream: old.id.clone(),
                });
                changes.push(ConfigChange::RegisterSource {
                    definition: new.clone(),
                });
            }
            Some(new) => {
                if old.enabled != new.enabled {
                    changes.push(ConfigChange::SetSourceEnabled {
                        stream: new.id.clone(),
                        enabled: new.enabled,
                    });
                }
                if old.recording != new.recording {
                    changes.push(ConfigChange::SetSourceRecording {
                        stream: new.id.clone(),
                        value: new.recording.clone(),
                    });
                }
                if old.recovery != new.recovery {
                    changes.push(ConfigChange::SetSourceRecovery {
                        stream: new.id.clone(),
                        value: new.recovery.clone(),
                    });
                }
                if old.metrics != new.metrics {
                    changes.push(ConfigChange::SetSourceMetrics {
                        stream: new.id.clone(),
                        value: new.metrics.clone(),
                    });
                }
            }
        }
    }
    for new in &b.sources {
        if !a.sources.iter().any(|s| s.id == new.id) {
            changes.push(ConfigChange::RegisterSource {
                definition: new.clone(),
            });
        }
    }
    for old in &a.derived {
        if !b.derived.iter().any(|d| d.id == old.id) {
            changes.push(ConfigChange::RemoveDerived {
                stream: old.id.clone(),
            });
        }
    }
    for new in &b.derived {
        if a.derived.iter().find(|d| d.id == new.id) != Some(new) {
            changes.push(ConfigChange::PutDerived {
                definition: new.clone(),
            });
        }
    }
    for old in &a.profiles {
        if !b
            .profiles
            .iter()
            .any(|p| p.exchange == old.exchange && p.api_family == old.api_family)
        {
            return Err(ErrorCode::UnsupportedCapability);
        }
    }
    for new in &b.profiles {
        if !a.profiles.contains(new) {
            changes.push(ConfigChange::ReplaceProfile {
                profile: new.clone(),
            });
        }
    }
    Ok(ConfigurationDiff {
        before: before.id,
        after: after.id,
        changes,
    })
}
