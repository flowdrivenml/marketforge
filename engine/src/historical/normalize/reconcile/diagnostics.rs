//! Bounded context attached to the existing task/stream failure report.
use super::SourceObservation;
use crate::{
    historical::normalize::{config::ProcessingConfig, streams::StreamUnit},
    shared::error::MarketForgeError,
};
use std::collections::BTreeMap;
fn text(value: &str) -> String {
    value.chars().take(512).collect()
}
pub(crate) fn failure(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    observations: &BTreeMap<usize, SourceObservation>,
    source: MarketForgeError,
) -> MarketForgeError {
    let (hint, previous_hint) = if let MarketForgeError::TradeIdentityViolation {
        source_index,
        previous_source_index,
        ..
    } = source.processing_cause()
    {
        (Some(*source_index), *previous_source_index)
    } else {
        (None, None)
    };
    let source_tier = |i: usize| {
        if hint == Some(i) {
            0
        } else if previous_hint == Some(i) {
            1
        } else {
            2
        }
    };
    let mut indices = observations.keys().copied().collect::<Vec<_>>();
    indices.sort_by_key(|i| {
        (
            source_tier(*i),
            std::cmp::Reverse(observations[i].processing.overlap_exclusions),
            std::cmp::Reverse(observations[i].conflicts),
            *i,
        )
    });
    let focus = indices.first().and_then(|i| observations.get(i));
    indices.sort_by_key(|i| {
        let o = &observations[i];
        let intersects = focus.is_some_and(|f| {
            f.minimum_accepted_timestamp_ns
                .zip(f.maximum_accepted_timestamp_ns)
                .zip(
                    o.minimum_accepted_timestamp_ns
                        .zip(o.maximum_accepted_timestamp_ns),
                )
                .is_some_and(|((a, b), (c, d))| a <= d && c <= b)
        });
        (
            source_tier(*i),
            std::cmp::Reverse(o.processing.overlap_exclusions),
            std::cmp::Reverse(o.conflicts),
            !intersects,
            *i,
        )
    });
    let sources=indices.iter().take(4).filter_map(|index| {
        let o=&observations[index];let task=unit.tasks.iter().find(|(i,_)|i==index)?.1;
        let overlap=focus.and_then(|f|Some((f.minimum_accepted_timestamp_ns?.max(o.minimum_accepted_timestamp_ns?),f.maximum_accepted_timestamp_ns?.min(o.maximum_accepted_timestamp_ns?)))).filter(|(a,b)|a<=b);
        Some(serde_json::json!({"task_id":task.task_id.0,"physical_source":text(&task.input_path.to_string_lossy()),"archive_member":task.archive_member.as_deref().map(text),"coverage_start_ns":o.minimum_accepted_timestamp_ns,"coverage_end_ns":o.maximum_accepted_timestamp_ns,"overlap_with_focus_ns":overlap,"identity_contract":o.trade_identity.as_ref().map(|c|serde_json::json!({"scope":c.scope,"reset_domain":c.reset_domain.as_deref().map(text)})),"native_identity_order_validated":o.native_identity_order_validated,"verified_duplicates":o.duplicates,"known_unique_exclusions":o.processing.known_unique_event_exclusions,"unverified_exclusions":o.processing.unverified_observation_exclusions,"decision_samples":o.processing.decision_samples.iter().take(2).collect::<Vec<_>>()}))
    }).collect::<Vec<_>>();
    let task = unit.tasks[0].1;
    let totals = observations
        .values()
        .fold((0u128, 0u128, 0u128), |(d, k, u), o| {
            (
                d + u128::from(o.duplicates),
                k + u128::from(o.processing.known_unique_event_exclusions),
                u + u128::from(o.processing.unverified_observation_exclusions),
            )
        });
    let settings = &config.reconciliation.trades;
    let current_task = hint
        .and_then(|i| unit.tasks.iter().find(|(index, _)| *index == i))
        .map(|(_, t)| t.task_id.0);
    let previous_task = previous_hint
        .and_then(|i| unit.tasks.iter().find(|(index, _)| *index == i))
        .map(|(_, t)| t.task_id.0);
    let details = serde_json::json!({"failure_scope":"stream","current_source_task_id":current_task,"previous_source_task_id":previous_task,"stream_hash":unit.key,"canonical_stream_id":text(&task.stream_id.0),"exchange":task.exchange,"instrument_id":task.instrument_id,"symbol":text(&task.symbol),"policy":settings,"reason":text(&source.to_string()),"source_count":observations.len(),"source_samples_truncated":observations.len()>4,"diagnostic_string_max_chars":512,"sampled_sources":sources,"verified_duplicate_count":totals.0.to_string(),"known_unique_excluded_count":totals.1.to_string(),"unverified_excluded_count":totals.2.to_string(),"applied_exclusion_limit":settings.prefer_source.max_excluded_events});
    MarketForgeError::ReconciliationFailure {
        source: Box::new(source),
        details: Box::new(details),
    }
}
