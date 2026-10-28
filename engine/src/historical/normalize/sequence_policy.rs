use crate::{
    historical::job::WorkTask,
    shared::domain::book::SequencePolicy,
    shared::error::{MarketForgeError, Result},
    shared::normalization::TargetSchema,
};

/// Resolve depth sequence validation from normalization metadata.
///
/// Consecutive:
///     sequence_start is present.
///
/// Ranged:
///     sequence_start and sequence_count are present.
///
/// Unsequenced:
///     neither field is present.
///
/// All depth normalizations must declare compatible sequence semantics.
pub fn resolve_sequence_policy(task: &WorkTask) -> Result<SequencePolicy> {
    let mut resolved = None;

    for normalization in &task.normalizations {
        if normalization.target_schema != TargetSchema::Depth {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "task {} contains non-depth normalization",
                task.task_id.0,
            )));
        }

        let rules = normalization.rules.as_object().ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!(
                "task {} depth normalization rules must be an object",
                task.task_id.0,
            ))
        })?;

        let has_start = rules.contains_key("sequence_start");
        let has_count = rules.contains_key("sequence_count");

        let policy = match (has_start, has_count) {
            (false, false) => SequencePolicy::Unsequenced,

            (true, false) => SequencePolicy::Consecutive,

            (true, true) => SequencePolicy::Ranged,

            (false, true) => {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} declares sequence_count without sequence_start",
                    task.task_id.0,
                )));
            }
        };

        if let Some(previous) = resolved {
            if previous != policy {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "task {} contains conflicting depth sequence policies",
                    task.task_id.0,
                )));
            }
        }

        resolved = Some(policy);
    }

    resolved.ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!(
            "task {} has no depth normalization",
            task.task_id.0,
        ))
    })
}
