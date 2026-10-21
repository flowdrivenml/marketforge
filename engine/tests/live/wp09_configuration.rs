//! Tests pure candidate/DAG behavior; no persistence or capability certification
//! is inferred from these independent configuration primitives.
use marketforge_engine::live::{
    config::{compiler::build_candidate, validation::dependency_order},
    model::{configuration::*, derived::*, error::ErrorCode, identity::*},
};
fn base() -> ConfigurationRevision {
    ConfigurationRevision {
        id: ConfigRevisionId(4),
        parent: Some(ConfigRevisionId(3)),
        created_at_ns: 0,
        desired: super::wp07_contracts::config(),
    }
}
fn source(id: &str) -> SourceDefinition {
    SourceDefinition {
        id: StreamId(id.into()),
        key: super::wp07_contracts::source_key(),
        enabled: false,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    }
}
fn merge(id: &str, sources: Vec<StreamRef>) -> DerivedDefinition {
    DerivedDefinition {
        id: DerivedStreamId(id.into()),
        enabled: true,
        recording: Override::Inherit,
        transformation: DerivedTransformation::Merge {
            sources,
            ordering: MergeOrdering::Arrival,
            initial_window_ns: 0,
            max_window_ns: 0,
            late_events: LateEventPolicy::Fail,
            source_failure: SourceFailurePolicy::Fail,
            max_buffer_events: 10,
            max_buffer_bytes: 1024,
        },
    }
}
fn changes(changes: Vec<ConfigChange>) -> ConfigurationProposal {
    ConfigurationProposal::Changes(ConfigUpdate {
        expected_revision: ConfigRevisionId(4),
        changes,
    })
}
#[test]
fn all_hot_global_categories_and_explicit_inheritance_have_no_base_effects() {
    let base = base();
    let original = base.clone();
    let mut metrics = base.desired.global.metrics.clone();
    metrics.enabled = !metrics.enabled;
    let candidate = build_candidate(
        &base,
        &changes(vec![
            ConfigChange::RegisterSource {
                definition: source("s"),
            },
            ConfigChange::SetLogging {
                level: LogLevel::Trace,
            },
            ConfigChange::SetMetrics {
                settings: metrics.clone(),
            },
            ConfigChange::SetRecordingBatch {
                batch_events: 77,
                flush_interval_ns: 8,
            },
            ConfigChange::SetMaxSubscribers { value: 9 },
        ]),
        100,
        100,
    )
    .unwrap();
    assert_eq!(base, original);
    assert_eq!(candidate.resolved.sources[0].metrics, metrics);
    assert_eq!(candidate.resolved.sources[0].recording.batch_events, 77);
    assert_eq!(candidate.resolved.global.max_subscribers, 9);
    assert_eq!(candidate.desired.sources[0].metrics, Override::Inherit);
    assert_eq!(
        ConfigChange::SetRecordingBatch {
            batch_events: 77,
            flush_interval_ns: 8
        }
        .boundary(),
        ReconfigurationBoundary::NextBatch
    );
}
#[test]
fn candidate_conflicts_unknown_resources_and_invalid_last_change_are_atomic() {
    let base = base();
    let bad = changes(vec![
        ConfigChange::SetLogging {
            level: LogLevel::Trace,
        },
        ConfigChange::SetSourceEnabled {
            stream: StreamId("missing".into()),
            enabled: true,
        },
    ]);
    assert_eq!(
        build_candidate(&base, &bad, 100, 100).unwrap_err().issues[0].code,
        ErrorCode::NotFound
    );
    assert_ne!(base.desired.global.logging, LogLevel::Trace);
    let stale = ConfigurationProposal::Changes(ConfigUpdate {
        expected_revision: ConfigRevisionId(3),
        changes: vec![],
    });
    assert_eq!(
        build_candidate(&base, &stale, 100, 100).unwrap_err().issues[0].code,
        ErrorCode::RevisionConflict
    );
}
#[test]
fn dependency_first_order_missing_cycles_duplicates_and_budget() {
    let mut config = base().desired;
    config.sources.push(source("s"));
    config.derived = vec![
        merge("b", vec![StreamRef::Derived(DerivedStreamId("a".into()))]),
        merge("a", vec![StreamRef::Source(StreamId("s".into()))]),
    ];
    let order = dependency_order(&config, 100, 100).unwrap();
    assert_eq!(
        order,
        vec![
            StreamRef::Source(StreamId("s".into())),
            StreamRef::Derived(DerivedStreamId("a".into())),
            StreamRef::Derived(DerivedStreamId("b".into()))
        ]
    );
    assert_eq!(
        dependency_order(&config, 100, 1).unwrap_err().code,
        ErrorCode::Limit
    );
    config.derived[1] = merge("a", vec![StreamRef::Derived(DerivedStreamId("b".into()))]);
    assert_eq!(
        dependency_order(&config, 100, 100).unwrap_err().code,
        ErrorCode::Dependency
    );
    config.derived[1] = merge("a", vec![StreamRef::Source(StreamId("missing".into()))]);
    assert_eq!(
        dependency_order(&config, 100, 100).unwrap_err().code,
        ErrorCode::NotFound
    );
    config.derived[1] = merge("a", vec![StreamRef::Source(StreamId("s".into())); 2]);
    assert_eq!(
        dependency_order(&config, 100, 100).unwrap_err().code,
        ErrorCode::InvalidInput
    );
}
#[test]
fn restoration_uses_contents_without_rewinding_accepted_identity() {
    let base = base();
    let mut old = base.clone();
    old.id = ConfigRevisionId(1);
    old.desired.global.logging = LogLevel::Trace;
    let candidate = build_candidate(
        &base,
        &ConfigurationProposal::Restore {
            expected_revision: base.id,
            contents: old,
        },
        100,
        100,
    )
    .unwrap();
    assert_eq!(candidate.desired.global.logging, LogLevel::Trace);
    assert_eq!(base.id, ConfigRevisionId(4));
}
#[test]
fn iterative_graph_handles_long_chains_without_recursive_stack_growth() {
    let mut config = base().desired;
    config.sources.push(source("s"));
    for i in 0..1000 {
        config.derived.push(merge(
            &format!("d{i}"),
            vec![if i == 0 {
                StreamRef::Source(StreamId("s".into()))
            } else {
                StreamRef::Derived(DerivedStreamId(format!("d{}", i - 1)))
            }],
        ));
    }
    assert_eq!(dependency_order(&config, 1001, 1000).unwrap().len(), 1001);
}
