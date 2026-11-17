//! Bounded, deferred source reconciliation. Nothing is emitted before every
//! participating source has been observed and precedence can no longer change.
use crate::shared::{
    domain::canonical::Trade,
    error::{MarketForgeError, Result},
    historical::limits,
};
use serde::{Deserialize, Serialize};
use std::{cell::Cell, collections::BTreeMap, rc::Rc};
fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
#[derive(Clone)]
pub(crate) struct Quota {
    used: Rc<Cell<u64>>,
    maximum: u64,
    peak: Rc<Cell<u64>>,
}
impl Quota {
    pub fn new(maximum: u64) -> Self {
        Self {
            used: Rc::new(Cell::new(0)),
            maximum,
            peak: Rc::new(Cell::new(0)),
        }
    }
    pub fn charge(&self, bytes: u64, stage: &'static str) -> Result<()> {
        let requested = self
            .used
            .get()
            .checked_add(bytes)
            .ok_or_else(|| invalid("reconciliation accounting overflow"))?;
        limits::check("reconciliation bytes", self.maximum, requested, stage)?;
        self.used.set(requested);
        self.peak.set(self.peak.get().max(requested));
        Ok(())
    }
    pub fn reserve(&self, bytes: u64, stage: &'static str) -> Result<MemoryLease> {
        self.charge(bytes, stage)?;
        Ok(MemoryLease {
            quota: self.clone(),
            bytes,
        })
    }
    pub fn used(&self) -> u64 {
        self.used.get()
    }
    pub fn maximum(&self) -> u64 {
        self.maximum
    }
    pub fn peak(&self) -> u64 {
        self.peak.get()
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceObservation {
    #[serde(default)]
    pub processing: super::policies::Outcomes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_identity: Option<crate::shared::formats::trades::TradeIdentityContract>,
    #[serde(default)]
    pub stable_source_identity: String,
    pub original_task_id: u64,
    pub family_candidate: bool,
    pub source_priority: u64,
    pub archive_member: Option<String>,
    pub member_ordinal: Option<u64>,
    pub first_accepted_timestamp_ns: Option<i64>,
    pub last_accepted_timestamp_ns: Option<i64>,
    pub minimum_accepted_timestamp_ns: Option<i64>,
    pub maximum_accepted_timestamp_ns: Option<i64>,
    pub ordering_validated: Option<bool>,
    pub observed_selected_records: u64,
    pub physical_records_read: u64,
    #[serde(default)]
    pub maximum_source_ordinal: u64,
    #[serde(default)]
    pub native_identity_order_validated: bool,
    #[serde(default)]
    pub native_source_order_validated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_native_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_native_id: Option<String>,
    pub filtered_records: u64,
    pub outside_scope_records: u64,
    pub selection_filtered_records: u64,
    pub missing_metadata: u64,
    pub unsupported_instruments: u64,
    pub duplicates: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary_duplicate_of_task: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppressed_depth_event_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary_duplicate_source_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary_duplicate_sequence: Option<u64>,
    pub conflicts: u64,
    pub redundant_of_task: Option<u64>,
    pub skipped_stage: Option<String>,
}
impl SourceObservation {
    pub fn observe_native(&mut self, id: Option<&str>) {
        if self.native_source_order_validated == Some(false) {
            return;
        }
        let Some(id) =
            id.filter(|s| s.len() <= 128 && super::policies::numeric_id_cmp(s, s).is_some())
        else {
            self.native_source_order_validated = Some(false);
            return;
        };
        if self.last_native_id.as_deref().is_some_and(|last| {
            super::policies::numeric_id_cmp(last, id) != Some(std::cmp::Ordering::Less)
        }) {
            self.native_source_order_validated = Some(false);
            return;
        }
        self.first_native_id.get_or_insert_with(|| id.to_owned());
        let last = self.last_native_id.get_or_insert_with(String::new);
        last.clear();
        last.push_str(id);
        self.native_source_order_validated = Some(true);
    }
    pub fn observe(&mut self, timestamp: i64) {
        let ordered = self
            .last_accepted_timestamp_ns
            .is_none_or(|last| timestamp >= last);
        self.ordering_validated = Some(self.ordering_validated.unwrap_or(true) && ordered);
        self.first_accepted_timestamp_ns.get_or_insert(timestamp);
        self.last_accepted_timestamp_ns = Some(timestamp);
        self.minimum_accepted_timestamp_ns = Some(
            self.minimum_accepted_timestamp_ns
                .map_or(timestamp, |v| v.min(timestamp)),
        );
        self.maximum_accepted_timestamp_ns = Some(
            self.maximum_accepted_timestamp_ns
                .map_or(timestamp, |v| v.max(timestamp)),
        );
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TradeEntry {
    pub source: usize,
    pub priority: u64,
    pub ordinal: u64,
    pub trade: Trade,
}
pub(super) fn reliable_identity(trade: &Trade) -> bool {
    use crate::shared::domain::canonical::Exchange;
    let Some(id) = trade.trade_id.as_ref() else {
        return false;
    };
    match trade.envelope.exchange {
        // Native trade IDs mapped by the validated profiles, scoped to the
        // authoritative concrete instrument lifetime, compatible profile/feed
        // and namespace. OKX explicitly specifies uniqueness per instId:
        // https://my.okx.com/docs-v5/trick_en/#identifiers
        Exchange::Binance | Exchange::Okx => true,
        Exchange::Bybit => {
            id.len() == 36
                && id.bytes().enumerate().all(|(i, b)| {
                    if matches!(i, 8 | 13 | 18 | 23) {
                        b == b'-'
                    } else {
                        b.is_ascii_hexdigit()
                    }
                })
        }
        _ => false,
    }
}
/// Explicit profile contracts override legacy exchange/ID-shape recognition.
/// Missing identities remain unreliable even under a reliable profile contract.
pub(super) fn reliable_source_identity(trade: &Trade, observation: &SourceObservation) -> bool {
    use crate::shared::formats::trades::TradeIdScope;
    if trade.trade_id.is_none() {
        return false;
    }
    match observation.trade_identity.as_ref().map(|c| c.scope) {
        Some(TradeIdScope::Global | TradeIdScope::InstrumentScoped | TradeIdScope::OrderedRun) => {
            true
        }
        Some(TradeIdScope::SourceLocal | TradeIdScope::Unreliable) => false,
        None => reliable_identity(trade),
    }
}
/// Shared proof gate. Representation or state equality without an established
/// native event domain cannot establish duplicate-event identity.
pub(crate) fn event_equivalence<T: PartialEq>(domain_proven: bool, a: &T, b: &T) -> Option<bool> {
    domain_proven.then(|| a == b)
}
/// Event equivalence requires both a comparable identity domain and complete
/// canonical event contents. Equal timestamps or prices are never identities.
pub(crate) fn trade_equivalence(
    a: &Trade,
    a_source: &SourceObservation,
    b: &Trade,
    b_source: &SourceObservation,
) -> Option<bool> {
    if !reliable_source_identity(a, a_source)
        || !reliable_source_identity(b, b_source)
        || a_source.trade_identity != b_source.trade_identity
        || a.envelope.exchange != b.envelope.exchange
        || a.envelope.instrument_id != b.envelope.instrument_id
        || a.envelope.stream_id != b.envelope.stream_id
        || a.trade_id != b.trade_id
    {
        return None;
    }
    event_equivalence(true, a, b)
}
/// Only explicit source-local contracts permit skipping identity validation.
/// Reliable cross-source IDs can conflict even when their timestamps are disjoint.
pub(crate) fn chronological_trade_order(
    observations: &BTreeMap<usize, SourceObservation>,
) -> Result<Option<Vec<usize>>> {
    use crate::shared::formats::trades::TradeIdScope;
    let mut intervals = Vec::with_capacity(observations.len());
    for (&source, o) in observations {
        if let Some(contract) = &o.trade_identity {
            contract.validate().map_err(invalid)?;
        }
        if o.redundant_of_task.is_some() {
            continue;
        }
        if o.ordering_validated == Some(false) {
            return Ok(None);
        }
        if o.trade_identity.as_ref().map(|c| c.scope) != Some(TradeIdScope::SourceLocal) {
            return Ok(None);
        }
        match (
            o.minimum_accepted_timestamp_ns,
            o.maximum_accepted_timestamp_ns,
        ) {
            (Some(min), Some(max))
                if min <= max
                    && o.ordering_validated == Some(true)
                    && o.first_accepted_timestamp_ns == Some(min)
                    && o.last_accepted_timestamp_ns == Some(max) =>
            {
                intervals.push((min, max, source))
            }
            (None, None)
                if o.first_accepted_timestamp_ns.is_none()
                    && o.last_accepted_timestamp_ns.is_none()
                    && o.ordering_validated.is_none() => {}
            _ => return Err(invalid("incomplete accepted source timestamp evidence")),
        }
    }
    intervals.sort_unstable();
    if intervals.windows(2).any(|pair| pair[0].1 >= pair[1].0) {
        return Ok(None);
    }
    let mut order: Vec<_> = intervals.into_iter().map(|(_, _, s)| s).collect();
    // Empty sources must still be consumed, validating spool completion markers.
    order.extend(
        observations
            .iter()
            .filter(|(_, o)| {
                o.redundant_of_task.is_none() && o.minimum_accepted_timestamp_ns.is_none()
            })
            .map(|(&s, _)| s),
    );
    Ok(Some(order))
}
/// Authoritative native IDs are interpreted only inside one concrete stream.
/// Unknown reset domains are never inferred from a timestamp/day window.
#[cfg(test)]
pub(crate) fn reconcile_trades(
    mut entries: Vec<TradeEntry>,
    observations: &mut BTreeMap<usize, SourceObservation>,
) -> Result<Vec<TradeEntry>> {
    for observation in observations.values() {
        if let Some(contract) = &observation.trade_identity {
            contract.validate().map_err(invalid)?;
        }
        if observation.ordering_validated == Some(false) {
            return Err(invalid(
                "source reconciliation requires validated chronological sources; timestamp regressions cannot be repaired by reordering source-ordered records",
            ));
        }
    }
    let ranges: Vec<_> = observations
        .iter()
        .filter_map(|(source, o)| {
            Some((
                *source,
                o.minimum_accepted_timestamp_ns?,
                o.maximum_accepted_timestamp_ns?,
            ))
        })
        .collect();
    for (i, &(a, start, end)) in ranges.iter().enumerate() {
        for &(b, other_start, other_end) in &ranges[i + 1..] {
            if start <= other_end
                && other_start <= end
                && entries.iter().any(|e| {
                    (e.source == a || e.source == b)
                        && !reliable_source_identity(&e.trade, &observations[&e.source])
                })
            {
                return Err(invalid(
                    "ambiguous trade overlap: an overlapping source lacks an established native trade identity/reset domain (including missing IDs); timestamps/price/quantity/side are not event identities",
                ));
            }
        }
    }
    entries.sort_unstable_by_key(|e| (e.priority, e.source, e.ordinal));
    let mut identities = BTreeMap::<(String, String, i64, String), usize>::new();
    let mut kept: Vec<TradeEntry> = Vec::new();
    for entry in entries {
        if observations[&entry.source]
            .trade_identity
            .as_ref()
            .is_some_and(|c| c.scope == crate::shared::formats::trades::TradeIdScope::SourceLocal)
        {
            kept.push(entry);
            continue;
        }
        if let Some(id) = &entry.trade.trade_id {
            let key = (
                format!("{:?}", entry.trade.envelope.exchange),
                entry.trade.envelope.stream_id.clone(),
                entry.trade.envelope.instrument_id,
                id.clone(),
            );
            if let Some(&index) = identities.get(&key) {
                let winner = &kept[index];
                if winner.source != entry.source
                    || reliable_source_identity(&entry.trade, &observations[&entry.source])
                {
                    if !reliable_source_identity(&entry.trade, &observations[&entry.source]) {
                        return Err(invalid(
                            "native trade identity/reset domain is not established for this profile; cannot choose an authoritative cross-source event safely",
                        ));
                    }
                    let equivalent = trade_equivalence(
                        &entry.trade,
                        &observations[&entry.source],
                        &winner.trade,
                        &observations[&winner.source],
                    )
                    .ok_or_else(|| invalid("incomparable native trade identity/reset domains"))?;
                    let observation = observations
                        .get_mut(&entry.source)
                        .ok_or_else(|| invalid("missing reconciliation source"))?;
                    if equivalent {
                        observation.duplicates += 1;
                    } else {
                        observation.conflicts += 1;
                        return Err(invalid(
                            "conflicting contents under reliable trade identity; strict reconciliation cannot exclude a unique observation",
                        ));
                    }
                    continue;
                }
            } else {
                identities.insert(key, kept.len());
            }
        }
        kept.push(entry);
    }
    kept.sort_unstable_by_key(|e| {
        (
            e.trade.envelope.event_timestamp_ns,
            e.priority,
            e.source,
            e.ordinal,
        )
    });
    Ok(kept)
}
/// Depth is reconciled as independently reconstructable source event sequences,
/// never as individual level rows. Partial overlap needs stronger recovery / event
/// equivalence evidence than the currently supported profiles provide.
#[cfg(test)]
pub(crate) fn depth_source_order(
    observations: &BTreeMap<usize, SourceObservation>,
) -> Result<Vec<usize>> {
    depth_source_order_with_equivalence(observations, |_, _| false)
}
#[cfg(test)]
pub(crate) fn depth_source_order_with_equivalence(
    observations: &BTreeMap<usize, SourceObservation>,
    equivalent: impl Fn(usize, usize) -> bool,
) -> Result<Vec<usize>> {
    let mut sources: Vec<_> = observations
        .iter()
        .filter_map(|(index, o)| {
            o.minimum_accepted_timestamp_ns
                .map(|min| (*index, min, o.maximum_accepted_timestamp_ns.unwrap()))
        })
        .collect();
    for observation in observations.values() {
        if observation.ordering_validated == Some(false) {
            return Err(invalid(
                "depth source is not chronological; stateful events cannot be timestamp-sorted",
            ));
        }
    }
    sources.sort_by_key(|(index, first, _)| (*first, observations[index].source_priority, *index));
    for pair in sources.windows(2) {
        if pair[0].2 > pair[1].1 || (pair[0].2 == pair[1].1 && !equivalent(pair[0].0, pair[1].0)) {
            return Err(invalid(
                "unsupported depth overlap: independent source books overlap without proven event equivalence and snapshot/sequence recovery; refusing to splice level diffs or discard recovery data",
            ));
        }
    }
    let mut order: Vec<_> = sources.into_iter().map(|v| v.0).collect();
    order.extend(
        observations
            .keys()
            .filter(|i| !order_contains(observations, **i))
            .copied(),
    );
    Ok(order)
}
#[cfg(test)]
fn order_contains(observations: &BTreeMap<usize, SourceObservation>, i: usize) -> bool {
    observations[&i].minimum_accepted_timestamp_ns.is_some()
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;

pub(crate) fn validate_depth_recovery(
    boundary: &crate::shared::domain::book::boundary::DepthBoundaryManifest,
) -> Result<()> {
    use crate::shared::domain::book::boundary::BoundaryContinuity;
    if matches!(
        boundary.continuity,
        BoundaryContinuity::GapDetected
            | BoundaryContinuity::Invalid
            | BoundaryContinuity::BoundaryMismatch
    ) {
        return Err(invalid(
            "depth source ends with an unrecovered gap or invalid book; a proven snapshot recovery is required before successful publication",
        ));
    }
    Ok(())
}

pub(crate) struct MemoryLease {
    quota: Quota,
    bytes: u64,
}
impl MemoryLease {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn shrink_to(&mut self, bytes: u64) {
        if bytes < self.bytes {
            self.quota
                .used
                .set(self.quota.used.get() - (self.bytes - bytes));
            self.bytes = bytes;
        }
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.quota.used.set(self.quota.used.get() - self.bytes);
    }
}
pub(crate) mod external;
pub(crate) mod spool;
mod typed;

#[cfg(test)]
mod disk_tests;

#[cfg(test)]
mod append_tests;

#[cfg(test)]
mod policy_tests;

pub(crate) mod diagnostics;

#[cfg(test)]
mod bitget_tests;

mod typed_depth;
