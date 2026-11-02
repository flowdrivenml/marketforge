use crate::stream_protocol::{
    DataIntegrityEvidence, DataIntegritySnapshot, DataIntegrityUpdate, MAX_OBSERVATIONS,
    MAX_SOURCES, SequenceObservation,
};

#[derive(Debug, Clone)]
pub enum RouterStreamMessage {
    Market(RouterDelivery),
    IntegritySnapshot(DataIntegritySnapshot),
    IntegrityUpdate(DataIntegrityUpdate),
}
fn trade_evidence(
    stream: &str,
    generation: u64,
    revision: Option<u64>,
    sequence: Option<u64>,
    metadata: Option<&RouterTradeIntegrity>,
    recovered: bool,
) -> DataIntegrityEvidence {
    let mut e = DataIntegrityEvidence::unavailable(stream.into(), Some(generation));
    e.id.revision = revision;
    e.publication_sequence = sequence;
    e.recovered_records = recovered;
    if let Some(m) = metadata {
        e.source_generation = m.source_generation;
        e.coverage = if m.coverage_history_unverified {
            "unverified"
        } else {
            "observed_window"
        }
        .into();
        e.received_records = Some(m.received_records as u64);
        e.duplicate_records = Some(m.duplicate_records as u64);
        e.identity_history_truncated = m.identity_history_truncated;
        e.coverage_history_unverified = m.coverage_history_unverified;
        e.numeric_trade_id = m.baseline.numeric_trade_id;
        e.cross_sequence = m.baseline.cross_sequence;
        e.details_truncated = m.observations.len() > MAX_OBSERVATIONS;
        e.observations = m.observations.iter().take(MAX_OBSERVATIONS).map(|o| SequenceObservation {
            ordinal: o.index as u64,
            kind: match o.kind {
                crate::live::integrity::TradeIntegrityObservationKind::NumericIdRegression => "numeric_id_regression",
                crate::live::integrity::TradeIntegrityObservationKind::NumericIdDiscontinuity => "numeric_id_discontinuity",
                crate::live::integrity::TradeIntegrityObservationKind::CrossSequenceRegression => "cross_sequence_regression",
                crate::live::integrity::TradeIntegrityObservationKind::UnexpectedEqualCrossSequence => "unexpected_equal_cross_sequence",
            }.into(), previous: o.previous, observed: o.current, missing_records: None,
        }).collect();
    }
    e
}
// Exact JSON byte count without constructing a serialized buffer. JSON is the
// conservative publication-view bound; it is distinct from decoded retention.
fn json_charge(value: &impl serde::Serialize) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    if serde_json::to_writer(&mut count, value).is_err() {
        usize::MAX
    } else {
        count.0
    }
}
impl Shared {
    fn integrity_sources<'a>(&'a self, stream: &'a str) -> Result<BTreeSet<String>, ()> {
        let mut result = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut pending = vec![stream];
        while let Some(id) = pending.pop() {
            if visited.len() >= 4096 {
                return Err(());
            }
            if !visited.insert(id) {
                continue;
            }
            if self.source(id).is_ok() {
                if id.len() > 4096 || result.len() >= MAX_SOURCES {
                    return Err(());
                }
                result.insert(id.to_owned());
            } else if let Some(inputs) = self.context.graph.inputs(id) {
                if inputs.len() > 4096 || pending.len().saturating_add(inputs.len()) > 8192 {
                    return Err(());
                }
                pending.extend(inputs.iter().map(String::as_str));
            }
        }
        Ok(result)
    }
    fn integrity_snapshot(&self, state: &State, stream: &str) -> DataIntegritySnapshot {
        self.integrity_snapshot_with_discontinuity(state, stream, None)
    }
    fn integrity_snapshot_with_discontinuity(
        &self,
        state: &State,
        stream: &str,
        discontinuity: Option<crate::stream_protocol::StreamDiscontinuity>,
    ) -> DataIntegritySnapshot {
        let resolved = self.integrity_sources(stream);
        let scope_truncated = resolved.is_err();
        let sources = resolved.unwrap_or_default();
        let mut snapshot = DataIntegritySnapshot {
            stream_id: stream.into(),
            runtime_generation: state
                .bindings
                .get(stream)
                .map(|b| b.authority.generation().get()),
            revision: state.integrity_revision,
            scope_truncated,
            history_truncated: state.integrity_history_truncated,
            discontinuity,
            evidence_history: state
                .integrity_history
                .iter()
                .filter(|e| {
                    sources.contains(&e.id.source)
                        && state.bindings.get(&e.id.source).is_some_and(|b| {
                            Some(b.authority.generation().get()) == e.id.runtime_generation
                        })
                })
                .map(|e| (**e).clone())
                .collect(),
            sources: sources
                .iter()
                .take(MAX_SOURCES)
                .map(|id| {
                    state
                        .bindings
                        .get(id)
                        .and_then(|b| b.integrity.clone())
                        .map(|e| (*e).clone())
                        .unwrap_or_else(|| {
                            DataIntegrityEvidence::unavailable(
                                id.clone(),
                                state
                                    .bindings
                                    .get(id)
                                    .map(|b| b.authority.generation().get()),
                            )
                        })
                })
                .collect(),
        };
        // Count once, then select removals in one linear pass. Subtract only
        // element bytes (retain comma allowance), so the bound stays conservative.
        let mut charge = json_charge(&snapshot);
        let mut omitted = 0;
        for e in &snapshot.evidence_history {
            if charge <= 256 * 1024 {
                break;
            }
            charge = charge.saturating_sub(json_charge(e));
            omitted += 1;
        }
        if omitted > 0 {
            snapshot.evidence_history.drain(..omitted);
            snapshot.history_truncated = true;
        }
        for e in &mut snapshot.sources {
            if charge <= 256 * 1024 {
                break;
            }
            if !e.observations.is_empty() {
                let before = json_charge(e);
                e.observations.clear();
                e.details_truncated = true;
                charge = charge.saturating_sub(before.saturating_sub(json_charge(e)));
            }
        }
        while charge > 256 * 1024 {
            let Some(e) = snapshot.sources.pop() else {
                break;
            };
            charge = charge.saturating_sub(json_charge(&e));
            snapshot.scope_truncated = true;
        }
        snapshot
    }
    fn record_integrity(
        &self,
        state: &mut State,
        stream: &str,
        evidence: DataIntegrityEvidence,
    ) -> Arc<DataIntegrityEvidence> {
        let evidence = Arc::new(evidence);
        if !evidence.observations.is_empty()
            || evidence.identity_history_truncated
            || evidence.recovered_records
            || evidence.diagnostic.is_some()
        {
            if state.integrity_history.len() >= 32 {
                state.integrity_history.pop_front();
                state.integrity_history_truncated = true;
            }
            state.integrity_history.push_back(evidence.clone());
        }
        if let Some(b) = state.bindings.get_mut(stream) {
            b.integrity = Some(evidence.clone());
        }
        let update = DataIntegrityUpdate {
            revision: state.integrity_revision,
            evidence: (*evidence).clone(),
        };
        let charge = json_charge(&update).saturating_add(128);
        let consumers: Vec<_> = state
            .consumers
            .values()
            .filter(|c| {
                c.integrity_scope
                    .as_ref()
                    .is_some_and(|ids| ids.contains(stream))
            })
            .cloned()
            .collect();
        for consumer in consumers {
            let mut q = lock(&consumer.queue);
            if q.integrity.is_none() {
                continue;
            }
            if !self.config.consumer.admits(
                q.integrity.as_ref().unwrap().len(),
                q.integrity_bytes,
                charge,
            ) {
                // Keep one bounded final snapshot explaining the latest known state.
                q.integrity = Some(VecDeque::from([(
                    RouterStreamMessage::IntegritySnapshot(
                        self.integrity_snapshot(state, &consumer.stream),
                    ),
                    0,
                )]));
                q.integrity_bytes = 0;
                drop(q);
                self.remove(
                    state,
                    consumer.id,
                    Some(ContinuityFailure::ConsumerCapacity),
                );
            } else {
                q.integrity
                    .as_mut()
                    .unwrap()
                    .push_back((RouterStreamMessage::IntegrityUpdate(update.clone()), charge));
                q.integrity_bytes += charge;
                consumer.notify.notify_one();
            }
        }
        evidence
    }
}
impl ProducerHandle {
    /// Metadata-only Source observation. Never creates a market publication or readiness.
    pub fn publish_trade_integrity(
        &self,
        metadata: &RouterTradeIntegrity,
        coverage_hold: bool,
    ) -> Result<(), PublishError> {
        let mut pending = trade_evidence(
            &self.stream,
            self.runtime_generation,
            None,
            None,
            Some(metadata),
            false,
        );
        if coverage_hold {
            pending.coverage = "coverage_review_required".into();
            pending.received_records = None;
            pending.duplicate_records = None;
        }
        if self.semantics == PublicationSemantics::Source {
            *lock(&self.pending_integrity) = Some(pending);
        }
        let deadline = Instant::now() + self.shared.config.source_lock_timeout;
        let mut state = match self.shared.state.try_lock() {
            Some(state) => state,
            None => {
                #[cfg(test)]
                self.shared.note_source_wait("state");
                self.shared.state.try_lock_until(deadline).ok_or_else(|| {
                    #[cfg(test)]
                    self.shared.note_source_wait("timeout");
                    PublishError::ContentionTimeout
                })?
            }
        };
        let b = state
            .bindings
            .get(&self.stream)
            .filter(|b| b.id == self.binding && b.semantics == PublicationSemantics::Source)
            .ok_or(PublishError::Revoked)?;
        let after_publication_sequence = b.sequence;
        let authority = b.authority.clone();
        let guard = match authority.try_guard() {
            Some(guard) => guard,
            None => {
                #[cfg(test)]
                self.shared.note_source_wait("authority");
                authority.guard_until(deadline).ok_or_else(|| {
                    #[cfg(test)]
                    self.shared.note_source_wait("timeout");
                    PublishError::ContentionTimeout
                })?
            }
        };
        if Instant::now() >= deadline {
            #[cfg(test)]
            self.shared.note_source_wait("timeout");
            return Err(PublishError::ContentionTimeout);
        }
        if !*guard {
            return Err(PublishError::Revoked);
        }
        state.integrity_revision = state
            .integrity_revision
            .checked_add(1)
            .ok_or(PublishError::Exhausted)?;
        let mut evidence = trade_evidence(
            &self.stream,
            authority.generation().get(),
            Some(state.integrity_revision),
            None,
            Some(metadata),
            false,
        );
        evidence.after_publication_sequence =
            (after_publication_sequence > 0).then_some(after_publication_sequence);
        if coverage_hold {
            evidence.coverage = "coverage_review_required".into();
            evidence.received_records = None;
            evidence.duplicate_records = None;
        }
        self.shared
            .record_integrity(&mut state, &self.stream, evidence);
        lock(&self.pending_integrity).take();
        Ok(())
    }
}
impl SubscriptionHandle {
    /// Unified observation receive; registration and initial snapshot share one Router cut.
    /// Cancellation leaves queued messages intact.
    pub async fn recv_live(&mut self) -> Option<RouterStreamMessage> {
        loop {
            let notified = self.consumer.notify.notified();
            {
                let mut state = self.shared.state.lock();
                let dependencies = state.bindings.get(&self.consumer.stream).map(|binding| {
                    self.shared.dependency_authorities(
                        &state,
                        &self.consumer.stream,
                        binding.id,
                        true,
                    )
                });
                let authorities = dependencies.and_then(Result::ok);
                let guards = authorities
                    .as_ref()
                    .map(|deps| deps.iter().map(|a| a.guard()).collect::<Vec<_>>());
                if state.bindings.contains_key(&self.consumer.stream)
                    && guards
                        .as_ref()
                        .is_none_or(|guards| guards.iter().any(|g| !**g))
                {
                    drop(guards);
                    self.shared.retire(&mut state, &self.consumer.stream);
                    continue;
                }
                let mut q = lock(&self.consumer.queue);
                // Initial/final snapshots remain prerequisites. With ordinary
                // updates, service one queued market publication after eight
                // notifications, even if fresh observations keep arriving.
                let snapshot_first = q
                    .integrity
                    .as_ref()
                    .and_then(|q| q.front())
                    .is_some_and(|(m, _)| matches!(m, RouterStreamMessage::IntegritySnapshot(_)));
                if q.integrity_streak >= 8 && !snapshot_first && q.terminal_integrity.is_none() {
                    if let Some((delivery, bytes)) = q.queue.pop_front() {
                        q.bytes -= bytes;
                        q.integrity_streak = 0;
                        return Some(RouterStreamMessage::Market(delivery));
                    }
                }
                if let Some((m, n)) = q.integrity.as_mut().and_then(VecDeque::pop_front) {
                    q.integrity_bytes -= n;
                    q.integrity_streak = if matches!(m, RouterStreamMessage::IntegrityUpdate(_)) {
                        q.integrity_streak.saturating_add(1)
                    } else {
                        0
                    };
                    return Some(m);
                }
                if let Some(snapshot) = q.terminal_integrity.take() {
                    return Some(RouterStreamMessage::IntegritySnapshot(snapshot));
                }
                q.integrity_streak = 0;
                drop(q);
                if let Some(result) = take_delivery(&self.consumer) {
                    return result.map(RouterStreamMessage::Market);
                }
            }
            notified.await;
        }
    }
}
impl StreamRouter {
    fn describe_stream(&self, id: &str) -> Option<crate::stream_protocol::StreamDescription> {
        let d = self.shared.context.graph.get(id)?;
        if !d.desired.enabled {
            return None;
        }
        Some(crate::stream_protocol::StreamDescription {
            stream_id: id.into(),
            semantics: match &d.definition {
                StreamDefinition::Source(s) => match s.channel {
                    SourceChannel::Trades => "canonical_trades",
                    SourceChannel::Depth => "canonical_depth_changes",
                },
                StreamDefinition::Derived(_) => "derived_market_events",
            }
            .into(),
            dependencies: self
                .shared
                .context
                .graph
                .inputs(id)
                .unwrap_or_default()
                .to_vec(),
            schemas: vec![1],
            source: match &d.definition {
                StreamDefinition::Source(s) => Some(crate::stream_protocol::SourceDescription {
                    exchange: s.exchange.as_str().into(),
                    instrument_id: s.instrument_id,
                    symbol: s.symbol.clone(),
                    market_category: s.market_category.as_str().into(),
                    instrument_type: s.instrument_type.clone(),
                    base_asset: s.base_asset.clone(),
                    quote_asset: s.quote_asset.clone(),
                }),
                _ => None,
            },
            operation: match &d.definition {
                StreamDefinition::Derived(r) => Some(r.operation.as_str().into()),
                _ => None,
            },
            reference_inputs: match &d.definition {
                StreamDefinition::Derived(r) => r.reference_inputs.clone(),
                _ => vec![],
            },
        })
    }
    pub(crate) fn describe_stream_bounded(
        &self,
        id: &str,
        budget: usize,
    ) -> Result<Option<crate::stream_protocol::StreamDescription>, ()> {
        self.description_charge(id, budget)?
            .map(|_| self.describe_stream(id).ok_or(()))
            .transpose()
    }
    fn description_charge(&self, id: &str, budget: usize) -> Result<Option<usize>, ()> {
        if id.len() > 4096 {
            return Err(());
        }
        let Some(d) = self
            .shared
            .context
            .graph
            .get(id)
            .filter(|d| d.desired.enabled)
        else {
            return Ok(None);
        };
        let mut used = 1024usize;
        let mut charge = |value: &str| -> Result<(), ()> {
            if value.len() > 4096 {
                return Err(());
            }
            used = used.checked_add(value.len() + 64).ok_or(())?;
            if used > budget {
                return Err(());
            }
            Ok(())
        };
        charge(id)?;
        let dependencies = self.shared.context.graph.inputs(id).unwrap_or_default();
        if dependencies.len() > 16384 {
            return Err(());
        }
        for dep in dependencies {
            charge(dep)?;
        }
        match &d.definition {
            StreamDefinition::Source(s) => {
                charge(&s.symbol)?;
                charge(&s.instrument_type)?;
                if let Some(v) = &s.base_asset {
                    charge(v)?;
                }
                if let Some(v) = &s.quote_asset {
                    charge(v)?;
                }
            }
            StreamDefinition::Derived(r) => {
                if r.reference_inputs.len() > 16384 {
                    return Err(());
                }
                for v in &r.reference_inputs {
                    charge(v)?;
                }
            }
        }
        Ok(Some(used))
    }
    pub(crate) fn list_streams_bounded(
        &self,
        page: &crate::stream_protocol::PageRequest,
        frame_limit: usize,
    ) -> Result<crate::stream_protocol::StreamPage, ()> {
        if page.after.as_ref().is_some_and(|a| a.len() > 4096) {
            return Err(());
        }
        let budget = frame_limit.saturating_sub(512).min(1024 * 1024);
        let limit = usize::from(page.limit.clamp(1, 128));
        let mut ids = self
            .shared
            .context
            .graph
            .stream_ids_after(page.after.as_deref())
            .peekable();
        let mut streams = Vec::new();
        let mut cursor = page.after.clone();
        let mut used = 0usize;
        // Bound inspected entries as well as returned entries, including disabled definitions.
        for _ in 0..256 {
            let Some(id) = ids.next() else {
                break;
            };
            match self.description_charge(id, budget.saturating_sub(used)) {
                Ok(Some(charge)) => {
                    streams.push(self.describe_stream(id).ok_or(())?);
                    used += charge;
                }
                Ok(None) => (),
                Err(()) if !streams.is_empty() => {
                    return Ok(crate::stream_protocol::StreamPage {
                        streams,
                        next: cursor,
                    });
                }
                Err(()) => return Err(()),
            }
            if id.len() > 4096 {
                return Err(());
            }
            cursor = Some(id.to_owned());
            if streams.len() >= limit {
                break;
            }
        }
        let next = ids.peek().is_some().then_some(cursor).flatten();
        Ok(crate::stream_protocol::StreamPage { streams, next })
    }
}
impl ProducerHandle {
    /// Copies the existing depth owner's conclusions; does not check sequences.
    pub fn publish_depth_integrity(
        &self,
        outcome: &crate::live::integrity::DepthControllerOutcome,
        status: crate::live::integrity::StreamIntegrityStatus,
    ) -> Result<(), PublishError> {
        use crate::live::integrity::DepthControllerOutcome as O;
        if matches!(outcome, O::Ignored { .. }) {
            return Ok(());
        }
        if let O::RecoveryRequired { reason } = outcome {
            let mut pending = DataIntegrityEvidence::unavailable(
                self.stream.clone(),
                Some(self.runtime_generation),
            );
            pending.source_generation = Some(status.stream_generation);
            pending.coverage = "depth_recovery_required".into();
            let (detail, truncated) = depth_recovery_diagnostic(reason);
            pending.diagnostic = Some(detail.into());
            pending.details_truncated = truncated;
            if self.semantics == PublicationSemantics::Source {
                *lock(&self.pending_integrity) = Some(pending);
            }
        }
        let deadline = Instant::now() + self.shared.config.source_lock_timeout;
        let mut state = match self.shared.state.try_lock() {
            Some(state) => state,
            None => {
                #[cfg(test)]
                self.shared.note_source_wait("state");
                self.shared.state.try_lock_until(deadline).ok_or_else(|| {
                    #[cfg(test)]
                    self.shared.note_source_wait("timeout");
                    PublishError::ContentionTimeout
                })?
            }
        };
        let b = state
            .bindings
            .get(&self.stream)
            .filter(|b| b.id == self.binding && b.semantics == PublicationSemantics::Source)
            .ok_or(PublishError::Revoked)?;
        let authority = b.authority.clone();
        let guard = match authority.try_guard() {
            Some(guard) => guard,
            None => {
                #[cfg(test)]
                self.shared.note_source_wait("authority");
                authority.guard_until(deadline).ok_or_else(|| {
                    #[cfg(test)]
                    self.shared.note_source_wait("timeout");
                    PublishError::ContentionTimeout
                })?
            }
        };
        if Instant::now() >= deadline {
            #[cfg(test)]
            self.shared.note_source_wait("timeout");
            return Err(PublishError::ContentionTimeout);
        }
        if !*guard {
            return Err(PublishError::Revoked);
        }
        let mut e = b
            .integrity
            .as_ref()
            .map(|e| (**e).clone())
            .unwrap_or_else(|| {
                DataIntegrityEvidence::unavailable(
                    self.stream.clone(),
                    Some(authority.generation().get()),
                )
            });
        e.after_publication_sequence = (b.sequence > 0).then_some(b.sequence);
        state.integrity_revision = state
            .integrity_revision
            .checked_add(1)
            .ok_or(PublishError::Exhausted)?;
        e.id.revision = Some(state.integrity_revision);
        e.publication_sequence = None;
        e.source_generation = Some(status.stream_generation);
        e.depth_coverage =
            status
                .depth_coverage
                .map(|c| crate::stream_protocol::integrity::DepthCoverage {
                    subscribed_depth: c.subscribed_depth,
                    retained_bid_levels: c.retained_bid_levels as u64,
                    retained_ask_levels: c.retained_ask_levels as u64,
                    internal_synchronized: c.synchronized,
                    snapshot_source: c.snapshot_source.map(|s| s.as_str().into()),
                    snapshot_declared_depth: c.snapshot_declared_depth,
                });
        e.diagnostic = None;
        match outcome {
            O::RecoveryRequired { reason } => {
                e.coverage = "depth_recovery_required".into();
                let (detail, truncated) = depth_recovery_diagnostic(reason);
                e.diagnostic = Some(detail.into());
                e.details_truncated |= truncated;
            }
            O::SnapshotStaged { snapshot_sequence } => {
                e.coverage = "snapshot_staged_unverified".into();
                e.depth_sequence = Some(*snapshot_sequence);
            }
            O::Accepted {
                book_generation,
                baseline,
                ..
            } => {
                e.coverage = "internal_book_baseline".into();
                e.book_generation = Some(*book_generation);
                e.depth_sequence = Some(baseline.last_sequence);
                e.cross_sequence = baseline.cross_sequence;
            }
            _ => (),
        }
        self.shared.record_integrity(&mut state, &self.stream, e);
        lock(&self.pending_integrity).take();
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct DataDiscontinuitySink(
    Arc<Mutex<Option<crate::stream_protocol::StreamDiscontinuity>>>,
);
impl DataDiscontinuitySink {
    pub(crate) fn record(&self, detail: &str) {
        if ![
            "late event",
            "source-local timestamp",
            "depth sequence",
            "depth snapshot sequence",
            "upstream depth continuity",
            "upstream continuity",
            "missing exchange timestamp",
            "event-time emission frontier",
            "MissingReference",
            "FutureReference",
            "StaleReference",
            "ReferenceIdentityMismatch",
            "ArithmeticFailure",
        ]
        .iter()
        .any(|prefix| detail.starts_with(prefix))
        {
            return;
        }
        let mut slot = lock(&self.0);
        if slot.is_none() {
            *slot = Some(crate::stream_protocol::StreamDiscontinuity {
                revision: None,
                detail: detail.chars().take(512).collect(),
                detail_truncated: detail.chars().count() > 512,
                after_publication_sequence: None,
            });
        }
    }
}
impl ProducerHandle {
    pub(crate) fn data_discontinuity_sink(&self) -> DataDiscontinuitySink {
        self.discontinuity.clone()
    }
    pub(crate) fn record_data_discontinuity(&self, detail: &str) {
        self.discontinuity.record(detail);
    }
}

impl ProducerHandle {
    pub(super) fn retain_rejected_trade_evidence(&self, metadata: &RouterTradeIntegrity) {
        if self.semantics != PublicationSemantics::Source {
            return;
        }
        let mut e = trade_evidence(
            &self.stream,
            self.runtime_generation,
            None,
            None,
            Some(metadata),
            false,
        );
        // The candidate baseline/eviction was not committed. Preserve only observations.
        e.coverage = "publication_rejected_unverified".into();
        e.numeric_trade_id = None;
        e.cross_sequence = None;
        e.identity_history_truncated = false;
        e.coverage_history_unverified = true;
        e.details_truncated = true;
        *lock(&self.pending_integrity) = Some(e);
    }
}

fn depth_recovery_diagnostic(
    reason: &crate::live::integrity::DepthControllerRecoveryReason,
) -> (&'static str, bool) {
    use crate::live::integrity::{DepthControllerRecoveryReason as R, DepthViolationReason::*};
    (
        match reason {
            R::InvalidSequence(r) => match r {
                MissingSequence => "missing_sequence",
                InvalidRange => "invalid_range",
                SequenceGap => "sequence_gap",
                PreviousSequenceMismatch => "previous_sequence_mismatch",
                SequenceRegression => "sequence_regression",
                UnexpectedEqualSequence => "unexpected_equal_sequence",
                ResetRequiresSnapshot => "reset_requires_snapshot",
                UnsupportedMetadata => "unsupported_metadata",
                SequenceOverflow => "sequence_overflow",
            },
            R::SnapshotRequired => "snapshot_required",
            R::InvalidBook(_) => "invalid_book",
            R::InvalidSnapshot(_) => "invalid_snapshot",
            R::WrongStream => "wrong_stream",
            R::InvalidSource => "invalid_source",
            R::BookGenerationExhausted => "book_generation_exhausted",
            R::SnapshotBridgeFailed => "snapshot_bridge_failed",
        },
        matches!(reason, R::InvalidBook(_) | R::InvalidSnapshot(_)),
    )
}

#[cfg(test)]
mod snapshot_charge_tests {
    use super::*;
    #[test]
    fn nonallocating_count_matches_serialized_bytes_with_escaping() {
        let mut evidence = DataIntegrityEvidence::unavailable("\\\"\nλ".repeat(40), Some(u64::MAX));
        evidence.observations = (0..64)
            .map(|ordinal| SequenceObservation {
                ordinal,
                kind: "gap\n".repeat(40),
                previous: u64::MAX,
                observed: 1,
                missing_records: None,
            })
            .collect();
        assert_eq!(
            json_charge(&evidence),
            serde_json::to_vec(&evidence).unwrap().len()
        );
        let snapshot = DataIntegritySnapshot {
            stream_id: "s".into(),
            runtime_generation: Some(1),
            revision: 1,
            sources: vec![evidence.clone(); 128],
            scope_truncated: false,
            evidence_history: vec![evidence; 32],
            history_truncated: false,
            discontinuity: None,
        };
        assert_eq!(
            json_charge(&snapshot),
            serde_json::to_vec(&snapshot).unwrap().len()
        );
    }
}
