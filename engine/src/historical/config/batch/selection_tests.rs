//! Configuration/routing components only. No normalizer execution or real input.
use super::*;
pub(crate) fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .into()
}
pub(crate) fn runtime() -> ProcessingConfig {
    let mut wire: toml::Value = toml::from_str(include_str!(
        "../../../shared/historical/config/fixtures/runtime.toml"
    ))
    .unwrap();
    wire.as_table_mut().unwrap().remove("schema_version");
    wire.try_into().unwrap()
}
pub(crate) fn batch() -> PreparedBatch {
    let mut instruments = Vec::new();
    for (id, symbol) in [(11, "BTC-USD_UM-261030"), (12, "BTC-USD_UM-261225")] {
        instruments.push(json!({"instrument_id":id,"exchange":"okx","instrument_type":"future","market_category":"linear","symbol":symbol,"base_asset":"BTC","quote_asset":"USD","instrument":{"tick_size":"0.1","contract_value":"0.01","contract_value_asset":"BTC"}}));
    }
    let tasks=[900,1].into_iter().enumerate().map(|(n,id)|json!({"task_id":id,"input_path":format!("synthetic-{n}.csv"),"exchange":"okx","instrument_type":"future","market_category":"linear","base_asset":"BTC","quote_asset":"USD","symbol":null,"instrument":null,"instrument_id":null,"stream_id":"synthetic.family","stream_rank":5,"scope":{"kind":"family","identifier":"BTC-USD_UM"},"instrument_selection":{"mode":"all"}})).collect::<Vec<_>>();
    serde_json::from_value(json!({"schema_version":4,"kind":"normalization_batch","status":"prepared","run_id":"synthetic-selection","run_directory":"synthetic-run","batch_path":"synthetic-batch","ranking_snapshot":null,"ranking_policy":"synthetic","requested_interval":{"start":"1970-01-01T00:00:00Z","end":"1970-01-02T00:00:00Z"},"instrument_namespace":"synthetic-selection-authority","instruments":instruments,"jobs":[{"operation":"normalize","job_id":41,"dataset_id":51,"tasks":tasks,"ordering":{"primary":"event_timestamp_ns","tie_break":"stream_rank"},"output":{"content_type":"trades","staging_path":"unused-stage","dataset_path":"unused-final"}}]})).unwrap()
}
pub(crate) fn resolved(b: &PreparedBatch) -> ProcessingJob {
    b.validate(&root()).unwrap();
    b.resolve_job(
        &b.jobs[0],
        &FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap(),
        &runtime(),
        &root(),
    )
    .unwrap()
}
#[test]
fn strict_selection_modes_and_retired_fields() {
    for invalid in [
        json!({"mode":"all","symbols":["A"]}),
        json!({"mode":"unknown"}),
        json!({"mode":"include","symbols":[]}),
        json!({"mode":"include","symbols":[""]}),
        json!({"mode":"include","symbols":[" "]}),
        json!({"mode":"include","symbols":["A","A"]}),
        json!({"mode":"include","symbols":["A"],"extra":1}),
    ] {
        let result = serde_json::from_value::<InstrumentSelection>(invalid.clone());
        assert!(
            result.is_err() || result.unwrap().validate().is_err(),
            "{invalid}"
        );
    }
    let mut b = batch();
    b.jobs[0].tasks[0].instrument_selection = None;
    assert!(
        b.validate(&root())
            .unwrap_err()
            .to_string()
            .contains("explicit")
    );
    for removed in [
        Value::Null,
        json!({"start_timestamp_ns":1,"end_timestamp_ns":2}),
    ] {
        let mut task = serde_json::to_value(&batch().jobs[0].tasks[0]).unwrap();
        task["source_time_bounds"] = removed;
        assert!(serde_json::from_value::<PreparedTask>(task).is_err());
    }
    b = batch();
    b.schema_version = 3;
    assert!(b.validate(&root()).is_err());
}
#[test]
fn single_symbol_omission_is_compatible_and_all_is_not_ambiguous() {
    let mut b = batch();
    let i = b.instruments[0].clone();
    let task = &mut b.jobs[0].tasks[0];
    task.scope = None;
    task.symbol = Some(i.symbol.clone());
    task.instrument = Some(i.instrument);
    task.instrument_id = Some(i.instrument_id);
    task.instrument_selection = None;
    b.jobs[0].tasks.truncate(1);
    b.validate(&root()).unwrap();
    b.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::All);
    assert!(b.validate(&root()).is_err());
    b.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::Include {
        symbols: vec![i.symbol],
    });
    b.validate(&root()).unwrap();
}
#[test]
fn expansion_preserves_original_priority_and_is_deterministic() {
    let b = batch();
    let a = resolved(&b);
    assert_eq!(a.tasks.len(), 4);
    for t in &a.tasks {
        let source = &t.raw_schema["marketforge_source"];
        assert_eq!(
            source["source_priority"],
            if source["task_id"] == 900 {
                json!(0)
            } else {
                json!(1)
            }
        );
    }
    let mut changed = b.clone();
    changed.instruments.reverse();
    changed.jobs[0].tasks[0]
        .scope
        .as_mut()
        .unwrap()
        .members
        .reverse();
    let mut config = runtime();
    config.resources.workers = 3;
    let other = changed
        .resolve_job(
            &changed.jobs[0],
            &FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap(),
            &config,
            &root(),
        )
        .unwrap();
    assert_eq!(a.streams, other.streams);
    assert_eq!(
        a.tasks
            .iter()
            .map(|t| (&t.symbol, t.instrument_id, t.task_id))
            .collect::<Vec<_>>(),
        other
            .tasks
            .iter()
            .map(|t| (&t.symbol, t.instrument_id, t.task_id))
            .collect::<Vec<_>>()
    );
    let groups = crate::historical::normalize::streams::plan(&a).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0]
            .tasks
            .iter()
            .map(|(i, t)| crate::historical::normalize::streams::priority(*i, t))
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 1]
    );
}
#[test]
fn explicit_concrete_ranks_are_preserved_when_family_expands() {
    let mut b = batch();
    let mut concrete = b.jobs[0].tasks[0].clone();
    let i = &b.instruments[0];
    concrete.task_id = TaskId(77);
    concrete.stream_id = StreamId("explicit.concrete".into());
    concrete.stream_rank = 1;
    concrete.scope = None;
    concrete.symbol = Some(i.symbol.clone());
    concrete.instrument_id = Some(i.instrument_id);
    concrete.instrument = Some(i.instrument.clone());
    concrete.instrument_selection = None;
    b.jobs[0].tasks.push(concrete);
    let job = resolved(&b);
    assert_eq!(
        job.streams
            .iter()
            .find(|s| s.stream_id.0 == "explicit.concrete")
            .unwrap()
            .stream_rank,
        1
    );
    let ranks = job
        .streams
        .iter()
        .map(|s| s.stream_rank)
        .collect::<BTreeSet<_>>();
    assert_eq!(ranks.len(), job.streams.len());
}
#[test]
fn metadata_is_deferred_for_unencountered_family_candidates() {
    let mut b = batch();
    b.instruments[1].instrument.contract_value = None;
    b.instruments[1].instrument.contract_value_asset = None;
    let job = resolved(&b);
    assert!(
        job.tasks
            .iter()
            .filter(|t| t.instrument_id == 12)
            .all(|t| t.raw_schema["marketforge_source"]["deferred_instrument_error"].is_string())
    );
    let mut b = batch();
    b.instruments.pop();
    b.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::Include {
        symbols: vec!["BTC-USD_UM-261225".into()],
    });
    assert!(
        b.resolve_job(
            &b.jobs[0],
            &FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap(),
            &runtime(),
            &root()
        )
        .unwrap_err()
        .to_string()
        .contains("authoritative offline metadata")
    );
}
#[test]
fn active_fixture_contract_has_no_bounds_and_explicit_family_intent() {
    let path = super::super::normalization_fixture::normalization_fixture_path();
    let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value["schema_version"], 4);
    let mut count = 0;
    for job in value["jobs"].as_array().unwrap() {
        for task in job["tasks"].as_array().unwrap() {
            count += 1;
            assert!(task.get("source_time_bounds").is_none());
            if !task["scope"].is_null() {
                assert_eq!(task["instrument_selection"], json!({"mode":"all"}));
            }
        }
    }
    assert_eq!(count, 357);
    serde_json::from_value::<PreparedBatch>(value)
        .unwrap()
        .validate(&root())
        .unwrap();
}

#[test]
fn family_reservation_counts_one_shared_buffer_and_one_active_writer() {
    let mut job = resolved(&batch());
    let file = tempfile::NamedTempFile::new().unwrap();
    for task in &mut job.tasks {
        task.input_path = file.path().into();
    }
    let config = runtime();
    let groups = crate::historical::normalize::streams::plan(&job).unwrap();
    let estimate = crate::historical::normalize::streams::estimate(&groups[0], &config).unwrap();
    let maximum = job
        .tasks
        .iter()
        .map(|task| {
            crate::historical::normalize::resources::estimate_task_resources_with_limits(
                task,
                &config.resources.parquet,
                &config.resources.normalization_limits,
            )
            .unwrap()
            .memory_bytes
        })
        .max()
        .unwrap();
    let metadata = (job.tasks.len()
        * (std::mem::size_of::<crate::historical::normalize::streams::SourceSpan>()
            + std::mem::size_of::<(usize, crate::historical::normalize::task::TaskExecution)>()))
        as u64;
    assert_eq!(
        estimate.memory_bytes,
        maximum + metadata + config.resources.normalization_limits.reconciliation_bytes
    );
    let fan_in = config
        .merge_fan_in
        .min(
            config
                .resources
                .normalization_limits
                .open_files
                .saturating_sub(6) as usize,
        )
        .max(2);
    assert_eq!(estimate.open_files, fan_in as u64 + 6);
    let mut limits = config.resources.normalization_limits;
    limits.reconciliation_bytes = 0;
    assert!(limits.validate().is_err());
}

#[test]
fn family_expansion_cannot_allocate_an_unadmitted_fanout() {
    let b = batch();
    let mut config = runtime();
    config.resources.memory_budget_bytes = 1;
    let error = b
        .resolve_job(
            &b.jobs[0],
            &FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap(),
            &config,
            &root(),
        )
        .unwrap_err();
    assert!(
        matches!(error.processing_cause(),crate::shared::error::MarketForgeError::ResourceLimit(e) if e.resource=="prepared expansion bytes" && e.scope.contains("job=41") && e.scope.contains("source task=900"))
    );
}

#[test]
fn canonical_samples_keep_authority_and_are_independent_of_task_order() {
    use super::super::normalization_fixture::{
        canonical_normalization_batch, select_normalization_sample,
    };
    let mut batch = canonical_normalization_batch().unwrap();
    let stream = "bitget.linear.perpetual.BTCUSDT.trades";
    let sample = |batch: &PreparedBatch| {
        select_normalization_sample(batch, stream, ContentType::Trades, 2, |task| {
            task.stream_id.0 == stream
        })
        .unwrap()
    };
    let selected = sample(&batch);
    assert_eq!(selected.instrument_namespace, batch.instrument_namespace);
    assert_eq!(selected.instruments.len(), batch.instruments.len());
    assert_eq!(selected.families.len(), batch.families.len());
    let expected_ids = selected.jobs[0]
        .tasks
        .iter()
        .map(|task| task.task_id)
        .collect::<Vec<_>>();
    let resolved = resolved(&selected);
    for (input, task) in selected.jobs[0].tasks.iter().zip(&resolved.tasks) {
        assert_eq!(task.input_path, input.input_path);
        assert_eq!(task.instrument_id, input.instrument_id.unwrap());
        assert_eq!(task.stream_id.0, stream);
        assert_eq!(
            task.raw_schema["marketforge_source"]["instrument_namespace"],
            selected.instrument_namespace
        );
    }
    batch.jobs.reverse();
    for job in &mut batch.jobs {
        job.tasks.reverse();
    }
    assert_eq!(
        serde_json::to_value(sample(&batch)).unwrap(),
        serde_json::to_value(selected).unwrap()
    );
    assert_eq!(
        resolved
            .tasks
            .iter()
            .map(|task| task.task_id)
            .collect::<Vec<_>>(),
        expected_ids
    );
    let error = select_normalization_sample(
        &batch,
        "missing exchange and stream",
        ContentType::Trades,
        1,
        |_| false,
    )
    .unwrap_err();
    assert!(error.contains("normalize.json"));
    assert!(error.contains("missing exchange and stream"));
    assert!(error.contains("maximum 0"));

    let family = batch
        .families
        .iter()
        .find(|f| f.exchange == Exchange::Okx && f.identifier == "BTC-USD_UM")
        .unwrap();
    let derived =
        select_normalization_sample(&batch, "OKX BTC-USD_UM", ContentType::Trades, 1, |task| {
            task.scope.as_ref().and_then(|scope| scope.family_id) == Some(family.family_id)
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(&derived.families).unwrap(),
        serde_json::to_value(&batch.families).unwrap()
    );
    assert!(derived.jobs[0].tasks[0].instrument_selection.is_some());
}
