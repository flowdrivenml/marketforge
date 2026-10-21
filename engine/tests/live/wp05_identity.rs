//! Persistent identity evidence uses committed configuration snapshots, not a
//! second persistent allocator or reconstruction of obsolete runtime handles.
use marketforge_engine::live::{
    config::store::{FileRevisionJournal, JournalLimits},
    foundation::{
        CancellationToken, FileEpochStore,
        clock::{ManualClock, MonotonicClock},
    },
    model::{
        acceptance::EpochStore,
        configuration::*,
        cursor::*,
        derived::*,
        error::ErrorCode,
        identity::*,
        operation::{ResourceId, ResourceVersion},
        persistent_identity::*,
    },
    ports::{configuration::CompiledConfiguration, context::CallContext},
};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
fn source(id: &str) -> SourceDefinition {
    SourceDefinition {
        id: StreamId(id.into()),
        key: super::wp07_contracts::source_key(),
        enabled: true,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    }
}
fn derived(id: &str, input: &str) -> DerivedDefinition {
    DerivedDefinition {
        id: DerivedStreamId(id.into()),
        enabled: true,
        recording: Override::Inherit,
        transformation: DerivedTransformation::Merge {
            sources: vec![StreamRef::Source(StreamId(input.into()))],
            ordering: MergeOrdering::Arrival,
            initial_window_ns: 0,
            max_window_ns: 0,
            late_events: LateEventPolicy::Fail,
            source_failure: SourceFailurePolicy::Fail,
            max_buffer_events: 10,
            max_buffer_bytes: 1000,
        },
    }
}
fn revision(
    id: u64,
    sources: Vec<SourceDefinition>,
    derived: Vec<DerivedDefinition>,
) -> ConfigurationRevision {
    let mut desired = super::wp07_contracts::config();
    desired.sources = sources;
    desired.derived = derived;
    ConfigurationRevision {
        id: ConfigRevisionId(id),
        parent: if id == 1 {
            None
        } else {
            Some(ConfigRevisionId(id - 1))
        },
        created_at_ns: 100,
        desired,
    }
}
fn limits() -> IdentityLimits {
    IdentityLimits {
        revisions: 10,
        items: 100,
        edges: 100,
        bytes: 100000,
    }
}
fn reconstruct(
    revisions: &[ConfigurationRevision],
) -> Result<PersistentIdentityEvidence, IdentityError> {
    PersistentIdentityEvidence::reconstruct(revisions, limits())
}
#[test]
fn wp05_identity_canonical_dedup_includes_all_acquisition_semantics() {
    let source = source("s");
    let mut case = source.clone();
    case.key.instrument.exchange = "BYBIT".into();
    assert_ne!(source.key, case.key);
    assert_eq!(
        source.key.canonicalized().unwrap(),
        case.key.canonicalized().unwrap()
    );
    let error = reconstruct(&[revision(
        1,
        vec![
            source.clone(),
            SourceDefinition {
                id: StreamId("other".into()),
                ..case.clone()
            },
        ],
        vec![],
    )])
    .unwrap_err();
    assert_eq!(error, IdentityError::DuplicateSourceKey);
    for depth in [1, 5, 50, 100] {
        for interval in [None, Some(1), Some(100)] {
            let mut a = source.clone();
            a.key.channel.depth = Some(depth);
            a.key.channel.interval_ms = interval;
            let mut b = a.clone();
            b.key.channel.depth = Some(depth + 1);
            b.id = StreamId("b".into());
            let map = reconstruct(&[revision(1, vec![a.clone(), b.clone()], vec![])]).unwrap();
            assert_eq!(map.current().acquisition(&a.key).unwrap().id, a.id);
            assert_eq!(map.current().acquisition(&b.key).unwrap().id, b.id);
            let set: HashSet<_> = [
                a.key.canonicalized().unwrap(),
                b.key.canonicalized().unwrap(),
            ]
            .into_iter()
            .collect();
            assert_eq!(set.len(), 2);
        }
    }
    let map = reconstruct(&[revision(1, vec![case], vec![])]).unwrap();
    assert_eq!(
        map.current().acquisition(&source.key).unwrap().id,
        source.id
    );
    let mut wrong = source.key.clone();
    wrong.instrument.symbol = " ".into();
    assert_eq!(wrong.canonicalized(), Err(ErrorCode::InvalidInput));
}
#[test]
fn wp05_identity_duplicate_ids_and_cross_kind_collisions_are_rejected() {
    let a = source("s");
    let mut b = a.clone();
    b.key.channel.depth = Some(100);
    assert!(matches!(
        reconstruct(&[revision(1, vec![a.clone(), b], vec![])]),
        Err(IdentityError::InvalidDeclaration(ErrorCode::InvalidInput))
    ));
    assert!(reconstruct(&[revision(1, vec![a.clone()], vec![derived("s", "s")])]).is_err());
    let original = revision(1, vec![a], vec![]);
    let later = revision(2, vec![], vec![derived("s", "different")]);
    // A valid next snapshot with a reused category is needed to isolate the
    // cross-revision collision from ordinary dependency errors.
    let mut later = later;
    let mut different = source("different");
    different.key.channel.depth = Some(100);
    later.desired.sources.push(different);
    assert_eq!(
        reconstruct(&[original, later]).unwrap_err(),
        IdentityError::CategoryCollision
    );
}
#[test]
fn wp05_identity_reassignment_is_detected_across_revisions_and_removal_gaps() {
    let original = revision(1, vec![source("s")], vec![]);
    let mut changed = revision(2, vec![source("s")], vec![]);
    changed.desired.sources[0].key.channel.depth = Some(100);
    assert_eq!(
        reconstruct(&[original.clone(), changed.clone()]).unwrap_err(),
        IdentityError::SourceReassigned
    );
    changed.desired.sources[0].key = source("s").key;
    changed.desired.sources[0].id = StreamId("other".into());
    assert_eq!(
        reconstruct(&[original.clone(), changed]).unwrap_err(),
        IdentityError::AcquisitionReassigned
    );
    let mut third = revision(3, vec![source("s")], vec![]);
    third.desired.sources[0].key.channel.depth = Some(100);
    assert_eq!(
        reconstruct(&[original, revision(2, vec![], vec![]), third]).unwrap_err(),
        IdentityError::SourceReassigned
    );
}
#[test]
fn wp05_identity_disabled_and_operational_changes_keep_identity_without_runtime_claim_effects() {
    let original = revision(1, vec![source("s")], vec![derived("d", "s")]);
    let mut next = original.clone();
    next.id = ConfigRevisionId(2);
    next.parent = Some(original.id);
    next.desired.sources[0].enabled = false;
    next.desired.sources[0].recording = Override::Set(RecordingSettings {
        enabled: true,
        ..next.desired.global.recording.clone()
    });
    next.desired.sources[0].metrics = Override::Set(MetricsSettings {
        enabled: false,
        ..next.desired.global.metrics.clone()
    });
    next.desired.sources[0].recovery = Override::Set(RecoveryPolicy {
        enabled: false,
        ..next.desired.global.recovery.clone()
    });
    next.desired.derived[0].enabled = false;
    let restored = reconstruct(&[original.clone(), next.clone()]).unwrap();
    restored.require_complete_identity_proof().unwrap();
    assert!(
        !restored
            .current()
            .source(&StreamId("s".into()))
            .unwrap()
            .enabled
    );
    assert_eq!(
        restored
            .current()
            .acquisition(&original.desired.sources[0].key)
            .unwrap()
            .id,
        StreamId("s".into())
    );
    assert!(restored.derived_boundaries().is_empty());
    // Independent demands are deliberately absent from the reconstruction API;
    // removal is a change to persistent declarations, not a release instruction.
    let removed = reconstruct(&[original, next, revision(3, vec![], vec![])]).unwrap();
    assert!(removed.current().sources().is_empty());
    assert!(removed.current().derived().is_empty());
}
#[test]
fn wp05_identity_reintroduction_and_truncated_history_do_not_invent_tombstones() {
    let source = source("s");
    let evidence = reconstruct(&[
        revision(1, vec![source.clone()], vec![]),
        revision(2, vec![], vec![]),
        revision(3, vec![source.clone()], vec![]),
    ])
    .unwrap();
    assert_eq!(evidence.reintroductions().len(), 1);
    assert_eq!(evidence.reintroductions()[0].last_seen, ConfigRevisionId(1));
    assert_eq!(
        evidence.reintroductions()[0].reintroduced_at,
        ConfigRevisionId(3)
    );
    assert_eq!(
        evidence.require_complete_identity_proof(),
        Err(IdentityError::ReintroductionPolicy)
    );
    let truncated = reconstruct(&[revision(3, vec![source], vec![])]).unwrap();
    assert_eq!(
        truncated.scope(),
        &IdentityHistoryScope::RetainedSuffix {
            first_revision: ConfigRevisionId(3),
            missing_parent: ConfigRevisionId(2)
        }
    );
    assert_eq!(
        truncated.require_complete_identity_proof(),
        Err(IdentityError::IncompleteHistory)
    );
    assert!(truncated.reintroductions().is_empty());
}
#[test]
fn wp05_identity_derived_changes_require_fenced_checked_generation_boundaries() {
    let original = revision(1, vec![source("s")], vec![derived("d", "s")]);
    let mut next = original.clone();
    next.id = ConfigRevisionId(2);
    next.parent = Some(original.id);
    if let DerivedTransformation::Merge { ordering, .. } =
        &mut next.desired.derived[0].transformation
    {
        *ordering = MergeOrdering::AdaptiveEventTime;
    }
    let evidence = reconstruct(&[original, next]).unwrap();
    assert_eq!(evidence.derived_boundaries().len(), 1);
    let boundary = &evidence.derived_boundaries()[0];
    assert_eq!(
        boundary.boundary(),
        ReconfigurationBoundary::NewDerivedGeneration
    );
    assert_eq!(boundary.stream, DerivedStreamId("d".into()));
    boundary
        .validate_generation(
            DerivedGeneration(4),
            DerivedGeneration(4),
            DerivedGeneration(5),
        )
        .unwrap();
    assert_eq!(
        boundary.validate_generation(
            DerivedGeneration(4),
            DerivedGeneration(3),
            DerivedGeneration(5)
        ),
        Err(ErrorCode::StaleGeneration)
    );
    assert_eq!(
        boundary.validate_generation(
            DerivedGeneration(4),
            DerivedGeneration(5),
            DerivedGeneration(6)
        ),
        Err(ErrorCode::StaleGeneration)
    );
    assert_eq!(
        boundary.validate_generation(
            DerivedGeneration(4),
            DerivedGeneration(4),
            DerivedGeneration(4)
        ),
        Err(ErrorCode::InvalidTransition)
    );
    assert_eq!(
        boundary.validate_generation(
            DerivedGeneration(u64::MAX),
            DerivedGeneration(u64::MAX),
            DerivedGeneration(0)
        ),
        Err(ErrorCode::Limit)
    );
}
#[test]
fn wp05_identity_validated_dependencies_revision_chain_and_bounds() {
    let mut config = revision(1, vec![source("s")], vec![derived("d", "missing")]);
    assert!(reconstruct(&[config.clone()]).is_err());
    config.desired.derived[0] = derived("d", "s");
    assert_eq!(
        reconstruct(&[config.clone()])
            .unwrap()
            .current()
            .dependency_order(),
        &[
            StreamRef::Source(StreamId("s".into())),
            StreamRef::Derived(DerivedStreamId("d".into()))
        ]
    );
    let mut second = config.clone();
    second.id = ConfigRevisionId(2);
    second.parent = None;
    assert_eq!(
        reconstruct(&[config.clone(), second]).unwrap_err(),
        IdentityError::RevisionChain
    );
    let mut limited = limits();
    limited.bytes = 1;
    assert_eq!(
        PersistentIdentityEvidence::reconstruct(&[config.clone()], limited).unwrap_err(),
        IdentityError::Limit
    );
    limited = limits();
    limited.items = 0;
    assert_eq!(
        PersistentIdentityEvidence::reconstruct(&[config], limited).unwrap_err(),
        IdentityError::Limit
    );
}
#[test]
fn wp05_identity_real_configuration_journal_restart_and_epoch_namespace_compatibility() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let owner = ResourceVersion {
        resource: ResourceId::Configuration,
        generation: 1,
    };
    let context = CallContext {
        cancellation: CancellationToken::new(),
        deadline: clock.now() + Duration::from_secs(60),
        expected: Some(owner.clone()),
    };
    let path = dir.path().join("configuration.json");
    let initial = revision(1, vec![source("s")], vec![derived("d", "s")]);
    let limits = JournalLimits {
        bytes: 100000,
        history: 8,
        items: 100,
        edges: 100,
    };
    let plan = CompiledConfiguration {
        base_revision: ConfigRevisionId(0),
        resolved: initial.desired.resolve(100).unwrap(),
        desired: initial.desired.clone(),
        impacts: vec![],
        boundaries: vec![],
    };
    let journal = FileRevisionJournal::create(
        &path,
        &plan,
        limits,
        clock.clone(),
        clock.clone(),
        owner.clone(),
    )
    .unwrap();
    let committed = journal.load(&context).unwrap();
    let original = reconstruct(&[committed.clone()]).unwrap();
    let bytes = serde_json::to_vec(&committed).unwrap();
    let deserialized: ConfigurationRevision = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(deserialized, committed);
    assert_eq!(
        serde_json::to_value(&initial.desired.sources[0].id).unwrap(),
        "s"
    );
    assert_eq!(
        serde_json::to_value(&initial.desired.derived[0].id).unwrap(),
        "d"
    );
    drop(journal);
    let reopened = FileRevisionJournal::open(&path, limits, clock.clone(), clock, owner).unwrap();
    let restored = reconstruct(&reopened.history(&context, None, 8).unwrap()).unwrap();
    assert_eq!(original.current().sources(), restored.current().sources());
    assert_eq!(original.current().derived(), restored.current().derived());
    let id = restored.current().sources()[0].id.clone();
    let epoch_path = dir.path().join("epochs.json");
    let mut epochs = FileEpochStore::open(&epoch_path).unwrap();
    let first = epochs.fresh_epoch(&id).unwrap();
    drop(epochs);
    let mut epochs = FileEpochStore::open(&epoch_path).unwrap();
    let second = epochs.fresh_epoch(&id).unwrap();
    assert!(second.0 > first.0);
    assert_eq!(
        StreamCursor::new(id.clone(), first, 1).compare_position(&StreamCursor::new(id, second, 1)),
        None
    );
}

#[test]
fn wp05_identity_real_bounded_journal_exposes_unprovable_earlier_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("configuration.json");
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let owner = ResourceVersion {
        resource: ResourceId::Configuration,
        generation: 1,
    };
    let context = CallContext {
        cancellation: CancellationToken::new(),
        deadline: clock.now() + Duration::from_secs(10),
        expected: Some(owner.clone()),
    };
    let limits = JournalLimits {
        bytes: 100000,
        history: 1,
        items: 100,
        edges: 100,
    };
    let initial = revision(1, vec![source("s")], vec![]);
    let mut plan = CompiledConfiguration {
        base_revision: ConfigRevisionId(0),
        resolved: initial.desired.resolve(100).unwrap(),
        desired: initial.desired,
        impacts: vec![],
        boundaries: vec![],
    };
    let journal = FileRevisionJournal::create(
        &path,
        &plan,
        limits,
        clock.clone(),
        clock.clone(),
        owner.clone(),
    )
    .unwrap();
    plan.base_revision = ConfigRevisionId(1);
    plan.desired.sources[0].enabled = false;
    plan.resolved = plan.desired.resolve(100).unwrap();
    assert_eq!(
        journal.commit(&context, &plan).unwrap().id,
        ConfigRevisionId(2)
    );
    drop(journal);
    let reopened = FileRevisionJournal::open(&path, limits, clock.clone(), clock, owner).unwrap();
    let revisions = reopened.history(&context, None, 1).unwrap();
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].id, ConfigRevisionId(2));
    let evidence = reconstruct(&revisions).unwrap();
    assert!(!evidence.current().sources()[0].enabled);
    assert_eq!(evidence.current().sources()[0].id, StreamId("s".into()));
    assert_eq!(
        evidence.scope(),
        &IdentityHistoryScope::RetainedSuffix {
            first_revision: ConfigRevisionId(2),
            missing_parent: ConfigRevisionId(1)
        }
    );
    assert_eq!(
        evidence.require_complete_identity_proof(),
        Err(IdentityError::IncompleteHistory)
    );
}
#[test]
fn wp05_identity_derived_cycles_and_unresolved_families_never_become_proven_mappings() {
    let mut cyclic = revision(1, vec![source("s")], vec![derived("d", "s")]);
    if let DerivedTransformation::Merge { sources, .. } =
        &mut cyclic.desired.derived[0].transformation
    {
        *sources = vec![StreamRef::Derived(DerivedStreamId("d".into()))];
    }
    assert_eq!(
        reconstruct(&[cyclic]).unwrap_err(),
        IdentityError::InvalidDeclaration(ErrorCode::Dependency)
    );
    let mut unresolved = source("s");
    unresolved.key.instrument.instrument_kind = marketforge_engine::job::InstrumentKind::Perpetual;
    unresolved.key.market = MarketKind::Linear;
    assert_eq!(
        reconstruct(&[revision(1, vec![unresolved], vec![])]).unwrap_err(),
        IdentityError::UnresolvedClassification
    );
}

#[test]
fn wp05_identity_candidate_validation_is_pure_and_reuses_the_reconstruction_contract() {
    let base = revision(1, vec![source("s")], vec![derived("d", "s")]);
    let original = base.clone();
    let mut desired = base.desired.clone();
    desired.sources[0].enabled = false;
    let requirements = validate_candidate_identity(&[base.clone()], &desired, limits()).unwrap();
    requirements.require_complete_identity_proof().unwrap();
    assert_eq!(requirements.base_revision(), base.id);
    assert!(requirements.derived_boundaries().is_empty());
    assert_eq!(base, original);
    desired.sources[0].key.channel.depth = Some(100);
    assert_eq!(
        validate_candidate_identity(&[base.clone()], &desired, limits()).unwrap_err(),
        IdentityError::SourceReassigned
    );
    assert_eq!(base, original);
    desired = base.desired.clone();
    if let DerivedTransformation::Merge { ordering, .. } = &mut desired.derived[0].transformation {
        *ordering = MergeOrdering::AdaptiveEventTime;
    }
    let requirements = validate_candidate_identity(&[base.clone()], &desired, limits()).unwrap();
    assert_eq!(requirements.derived_boundaries().len(), 1);
    assert_eq!(
        requirements.derived_boundaries()[0].after,
        ConfigRevisionId(2)
    );
    assert_eq!(base.id, ConfigRevisionId(1));
    let removed = revision(2, vec![], vec![]);
    let requirements =
        validate_candidate_identity(&[base.clone(), removed], &base.desired, limits()).unwrap();
    assert_eq!(requirements.reintroductions().len(), 2);
    assert_eq!(
        requirements.require_complete_identity_proof(),
        Err(IdentityError::ReintroductionPolicy)
    );
    let mut exhausted = base;
    exhausted.id = ConfigRevisionId(u64::MAX);
    exhausted.parent = Some(ConfigRevisionId(u64::MAX - 1));
    assert_eq!(
        validate_candidate_identity(&[exhausted.clone()], &exhausted.desired, limits())
            .unwrap_err(),
        IdentityError::Limit
    );
}
