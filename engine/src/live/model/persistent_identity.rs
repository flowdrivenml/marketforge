//! Persistent identity evidence reconstructed ONLY from committed configuration
//! revisions (Option A). No identity journal, allocator or runtime demand owner.
use super::{
    configuration::{
        ConfigRevisionId, ConfigurationRevision, DesiredConfiguration, ReconfigurationBoundary,
        SourceDefinition,
    },
    cursor::DerivedGeneration,
    derived::{DerivedDefinition, StreamRef},
    error::ErrorCode,
    identity::{DerivedStreamId, SourceStreamKey, StreamId},
};
use std::collections::HashMap;
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("identity reconstruction resource limit")]
    Limit,
    #[error("invalid committed revision chain")]
    RevisionChain,
    #[error("invalid declaration: {0}")]
    InvalidDeclaration(ErrorCode),
    #[error("instrument classification remains unresolved")]
    UnresolvedClassification,
    #[error("multiple logical identities for one source acquisition")]
    DuplicateSourceKey,
    #[error("source identity was reassigned within retained history")]
    SourceReassigned,
    #[error("source acquisition was assigned a different logical identity")]
    AcquisitionReassigned,
    #[error("source/derived identities collide within retained history")]
    CategoryCollision,
    #[error("removed identity reintroduction requires an approved lifecycle policy")]
    ReintroductionPolicy,
    #[error("retained revisions cannot prove identities before their first parent")]
    IncompleteHistory,
}
#[derive(Debug, Clone, Copy)]
pub struct IdentityLimits {
    pub revisions: usize,
    pub items: usize,
    pub edges: usize,
    pub bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityHistoryScope {
    FromInitialRevision,
    RetainedSuffix {
        first_revision: ConfigRevisionId,
        missing_parent: ConfigRevisionId,
    },
}
/// Historical identity was absent in at least one intervening committed revision.
/// Evidence is retained, but this contract deliberately does not authorize reuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReintroducedIdentity {
    pub identity: StreamRef,
    pub last_seen: ConfigRevisionId,
    pub reintroduced_at: ConfigRevisionId,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedSemanticBoundary {
    pub stream: DerivedStreamId,
    pub before: ConfigRevisionId,
    pub after: ConfigRevisionId,
}
impl DerivedSemanticBoundary {
    pub fn boundary(&self) -> ReconfigurationBoundary {
        ReconfigurationBoundary::NewDerivedGeneration
    }
    /// WP-11/23/24 supply their actual owner generation. This validates a
    /// transition; it neither allocates nor mutates any generation or LogEpoch.
    pub fn validate_generation(
        &self,
        actual: DerivedGeneration,
        expected: DerivedGeneration,
        next: DerivedGeneration,
    ) -> Result<(), ErrorCode> {
        actual.check_owner(expected)?;
        if expected.checked_next()? != next {
            return Err(ErrorCode::InvalidTransition);
        }
        Ok(())
    }
}
/// Current declarations retain their original IDs, including disabled entries.
/// Private fields prevent reconstruction from creating or dropping runtime claims.
#[derive(Debug, Clone)]
pub struct PersistentIdentityMap {
    revision: ConfigRevisionId,
    sources: Vec<SourceDefinition>,
    derived: Vec<DerivedDefinition>,
    source_index: HashMap<StreamId, usize>,
    key_index: HashMap<SourceStreamKey, usize>,
    derived_index: HashMap<DerivedStreamId, usize>,
    dependency_order: Vec<StreamRef>,
}
impl PersistentIdentityMap {
    pub fn revision(&self) -> ConfigRevisionId {
        self.revision
    }
    pub fn sources(&self) -> &[SourceDefinition] {
        &self.sources
    }
    pub fn derived(&self) -> &[DerivedDefinition] {
        &self.derived
    }
    pub fn source(&self, id: &StreamId) -> Option<&SourceDefinition> {
        self.source_index.get(id).map(|i| &self.sources[*i])
    }
    pub fn acquisition(&self, key: &SourceStreamKey) -> Option<&SourceDefinition> {
        self.key_index
            .get(&key.canonicalized().ok()?)
            .map(|i| &self.sources[*i])
    }
    pub fn derived_stream(&self, id: &DerivedStreamId) -> Option<&DerivedDefinition> {
        self.derived_index.get(id).map(|i| &self.derived[*i])
    }
    pub fn dependency_order(&self) -> &[StreamRef] {
        &self.dependency_order
    }
    fn reconstruct(
        revision: &ConfigurationRevision,
        limits: IdentityLimits,
    ) -> Result<Self, IdentityError> {
        // Reuse the existing pure bounded DAG validator; it has no persistence,
        // compiler, runtime or network effects. This is not economic approval.
        let order = crate::live::config::validation::dependency_order(
            &revision.desired,
            limits.items,
            limits.edges,
        )
        .map_err(|i| IdentityError::InvalidDeclaration(i.code))?;
        let sources = revision.desired.sources.clone();
        let derived = revision.desired.derived.clone();
        let mut source_index = HashMap::new();
        let mut key_index = HashMap::new();
        let mut derived_index = HashMap::new();
        for (i, s) in sources.iter().enumerate() {
            let key = s.key.canonicalized().map_err(|e| {
                if e == ErrorCode::Dependency {
                    IdentityError::UnresolvedClassification
                } else {
                    IdentityError::InvalidDeclaration(e)
                }
            })?;
            source_index.insert(s.id.clone(), i);
            if key_index.insert(key, i).is_some() {
                return Err(IdentityError::DuplicateSourceKey);
            }
        }
        for (i, d) in derived.iter().enumerate() {
            derived_index.insert(d.id.clone(), i);
        }
        Ok(Self {
            revision: revision.id,
            sources,
            derived,
            source_index,
            key_index,
            derived_index,
            dependency_order: order,
        })
    }
}
#[derive(Debug, Clone)]
pub struct PersistentIdentityEvidence {
    current: PersistentIdentityMap,
    scope: IdentityHistoryScope,
    reintroductions: Vec<ReintroducedIdentity>,
    derived_boundaries: Vec<DerivedSemanticBoundary>,
}
impl PersistentIdentityEvidence {
    pub fn current(&self) -> &PersistentIdentityMap {
        &self.current
    }
    pub fn scope(&self) -> &IdentityHistoryScope {
        &self.scope
    }
    pub fn reintroductions(&self) -> &[ReintroducedIdentity] {
        &self.reintroductions
    }
    pub fn derived_boundaries(&self) -> &[DerivedSemanticBoundary] {
        &self.derived_boundaries
    }
    /// Does not turn a truncated history or unresolved reintroduction into an
    /// all-time identity guarantee. WP-09 decides whether a proposal is provable.
    pub fn require_complete_identity_proof(&self) -> Result<(), IdentityError> {
        if !self.reintroductions.is_empty() {
            return Err(IdentityError::ReintroductionPolicy);
        }
        if self.scope != IdentityHistoryScope::FromInitialRevision {
            return Err(IdentityError::IncompleteHistory);
        }
        Ok(())
    }
    pub fn reconstruct(
        revisions: &[ConfigurationRevision],
        limits: IdentityLimits,
    ) -> Result<Self, IdentityError> {
        if limits.revisions == 0
            || limits.items == 0
            || limits.bytes == 0
            || revisions.is_empty()
            || revisions.len() > limits.revisions
        {
            return Err(IdentityError::Limit);
        }
        check_size(revisions, limits.bytes)?;
        let first = &revisions[0];
        let scope = match (first.id.0, first.parent) {
            (1, None) => IdentityHistoryScope::FromInitialRevision,
            (id, Some(parent)) if id > 1 && parent.0 == id - 1 => {
                IdentityHistoryScope::RetainedSuffix {
                    first_revision: first.id,
                    missing_parent: parent,
                }
            }
            _ => return Err(IdentityError::RevisionChain),
        };
        let mut source_keys = HashMap::<StreamId, SourceStreamKey>::new();
        let mut source_ids = HashMap::<SourceStreamKey, StreamId>::new();
        let mut categories = HashMap::<String, bool>::new();
        let mut last_seen = HashMap::<StreamRef, ConfigRevisionId>::new();
        let mut previous: Option<PersistentIdentityMap> = None;
        let mut reintroductions = vec![];
        let mut derived_boundaries = vec![];
        for revision in revisions {
            if let Some(previous) = &previous {
                if revision.parent != Some(previous.revision)
                    || previous.revision.0.checked_add(1) != Some(revision.id.0)
                {
                    return Err(IdentityError::RevisionChain);
                }
            }
            let current = PersistentIdentityMap::reconstruct(revision, limits)?;
            let mut declarations = Vec::new();
            for source in current.sources() {
                if categories.insert(source.id.0.clone(), false) == Some(true) {
                    return Err(IdentityError::CategoryCollision);
                }
                let key = source
                    .key
                    .canonicalized()
                    .map_err(IdentityError::InvalidDeclaration)?;
                if source_keys.get(&source.id).is_some_and(|old| old != &key) {
                    return Err(IdentityError::SourceReassigned);
                }
                if source_ids.get(&key).is_some_and(|id| id != &source.id) {
                    return Err(IdentityError::AcquisitionReassigned);
                }
                source_keys.insert(source.id.clone(), key.clone());
                source_ids.insert(key, source.id.clone());
                declarations.push(StreamRef::Source(source.id.clone()));
            }
            for derived in current.derived() {
                if categories.insert(derived.id.0.clone(), true) == Some(false) {
                    return Err(IdentityError::CategoryCollision);
                }
                declarations.push(StreamRef::Derived(derived.id.clone()));
                if let Some(old) = previous
                    .as_ref()
                    .and_then(|p| p.derived_stream(&derived.id))
                {
                    if old.transformation != derived.transformation {
                        derived_boundaries.push(DerivedSemanticBoundary {
                            stream: derived.id.clone(),
                            before: previous.as_ref().unwrap().revision,
                            after: revision.id,
                        });
                    }
                }
            }
            for identity in declarations {
                if let Some(last) = last_seen.insert(identity.clone(), revision.id) {
                    if Some(last) != previous.as_ref().map(|p| p.revision) {
                        reintroductions.push(ReintroducedIdentity {
                            identity,
                            last_seen: last,
                            reintroduced_at: revision.id,
                        });
                    }
                }
            }
            previous = Some(current);
        }
        Ok(Self {
            current: previous.unwrap(),
            scope,
            reintroductions,
            derived_boundaries,
        })
    }
}

fn check_size<T: serde::Serialize + ?Sized>(
    value: &T,
    max_bytes: usize,
) -> Result<(), IdentityError> {
    struct Counter {
        used: usize,
        max: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.used = self
                .used
                .checked_add(b.len())
                .filter(|n| *n <= self.max)
                .ok_or_else(|| std::io::Error::other("identity limit"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(
        Counter {
            used: 0,
            max: max_bytes,
        },
        value,
    )
    .map_err(|_| IdentityError::Limit)?;
    Ok(())
}

/// A pure pre-commit result, not a revision receipt or permission to reuse IDs.
#[derive(Debug, Clone)]
pub struct IdentityCandidateRequirements {
    base_revision: ConfigRevisionId,
    scope: IdentityHistoryScope,
    reintroductions: Vec<ReintroducedIdentity>,
    derived_boundaries: Vec<DerivedSemanticBoundary>,
}
impl IdentityCandidateRequirements {
    pub fn base_revision(&self) -> ConfigRevisionId {
        self.base_revision
    }
    pub fn scope(&self) -> &IdentityHistoryScope {
        &self.scope
    }
    pub fn reintroductions(&self) -> &[ReintroducedIdentity] {
        &self.reintroductions
    }
    pub fn derived_boundaries(&self) -> &[DerivedSemanticBoundary] {
        &self.derived_boundaries
    }
    pub fn require_complete_identity_proof(&self) -> Result<(), IdentityError> {
        if !self.reintroductions.is_empty() {
            return Err(IdentityError::ReintroductionPolicy);
        }
        if self.scope != IdentityHistoryScope::FromInitialRevision {
            return Err(IdentityError::IncompleteHistory);
        }
        Ok(())
    }
}
/// WP-09 can call this before durable CAS. The prospective revision number is
/// used only to describe boundaries; no number is allocated/reserved/persisted.
/// A successful result is never proof that a candidate has been accepted.
pub fn validate_candidate_identity(
    history: &[ConfigurationRevision],
    desired: &DesiredConfiguration,
    limits: IdentityLimits,
) -> Result<IdentityCandidateRequirements, IdentityError> {
    PersistentIdentityEvidence::reconstruct(history, limits)?;
    if history.len() >= limits.revisions {
        return Err(IdentityError::Limit);
    }
    check_size(desired, limits.bytes)?;
    let base = history.last().ok_or(IdentityError::RevisionChain)?;
    let next = ConfigRevisionId(base.id.0.checked_add(1).ok_or(IdentityError::Limit)?);
    let mut prospective = history.to_vec();
    prospective.push(ConfigurationRevision {
        id: next,
        parent: Some(base.id),
        created_at_ns: base.created_at_ns,
        desired: desired.clone(),
    });
    let evidence = PersistentIdentityEvidence::reconstruct(&prospective, limits)?;
    Ok(IdentityCandidateRequirements {
        base_revision: base.id,
        scope: evidence.scope,
        reintroductions: evidence.reintroductions,
        derived_boundaries: evidence.derived_boundaries,
    })
}
