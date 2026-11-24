use super::*;
use crate::historical::config::selection_test_support as support;
use std::io::Write;
fn root() -> PathBuf {
    support::root()
}
fn directory() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}
fn wire() -> Value {
    use super::super::normalization_fixture::{
        canonical_normalization_batch, select_normalization_sample,
    };

    let canonical = canonical_normalization_batch().unwrap();
    let family = canonical
        .families
        .iter()
        .find(|family| family.exchange == Exchange::Okx && family.identifier == "BTC-USD_UM")
        .expect("normalize.json must bind the OKX BTC-USD_UM family");
    let selected = select_normalization_sample(
        &canonical,
        "OKX BTC-USD_UM",
        ContentType::Trades,
        1,
        |task| task.scope.as_ref().and_then(|scope| scope.family_id) == Some(family.family_id),
    )
    .unwrap();
    let mut value = serde_json::to_value(support::batch()).unwrap();
    // The wire tests exercise the compact family reference and force inheritance
    // for the generated members in the test namespace.
    value["instruments"] = json!([]);
    value["families"] = json!([family]);
    value["jobs"][0]["tasks"] = json!([{
        "task_id": 900,
        "input_path": "unused.tar.gz",
        "family_id": family.family_id,
        "stream_rank": 5,
        "instrument_selection": selected.jobs[0].tasks[0].instrument_selection,
    }]);
    value
}
fn batch(value: Value) -> PreparedBatch {
    serde_json::from_value(value).unwrap()
}
fn catalog() -> FormatCatalog {
    FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap()
}
fn resolve(batch: &PreparedBatch) -> Result<ProcessingJob> {
    batch.validate(&root())?;
    batch.resolve_job(&batch.jobs[0], &catalog(), &support::runtime(), &root())
}
fn tar(path: &Path, members: &[(&str, &str)]) {
    let encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(path).unwrap(),
        flate2::Compression::fast(),
    );
    let mut builder = tar::Builder::new(encoder);
    for (name, payload) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, payload.as_bytes())
            .unwrap();
    }
    builder.into_inner().unwrap().finish().unwrap();
}
fn okx_depth(path: &Path) -> PreparedBatch {
    let mut value = wire();
    value["jobs"][0]["output"]["content_type"] = json!("depth");
    value["jobs"][0]["tasks"][0]["input_path"] = json!(path);
    batch(value)
}
#[test]
fn every_active_migrated_fixture_parses_without_opening_archives() {
    let b = super::super::normalization_fixture::canonical_normalization_batch().unwrap();
    assert_eq!(b.schema_version, 4);
    for family in &b.families {
        assert!(family.family_id > 9_007_199_254_740_992);
    }
    // Concrete preparation remains independent of archive availability.
    for job in &b.jobs {
        let mut concrete = job.clone();
        concrete.tasks.retain(|t| t.scope.is_none());
        if !concrete.tasks.is_empty() {
            b.resolve_job(&concrete, &catalog(), &support::runtime(), &root())
                .unwrap();
        }
    }
}
#[test]
fn family_references_are_exact_and_do_not_compete_with_identity_fields() {
    let b = batch(wire());
    b.validate(&root()).unwrap();
    assert_eq!(
        b.jobs[0].tasks[0].scope.as_ref().unwrap().family_id,
        Some(5134923400617818826)
    );
    assert_eq!(b.jobs[0].tasks[0].task_id.0, 900);
    assert_eq!(b.jobs[0].tasks[0].stream_rank, 5);
    for field in [
        "exchange",
        "instrument_type",
        "market_category",
        "base_asset",
        "quote_asset",
        "symbol",
        "instrument",
        "instrument_id",
        "scope",
        "stream_id",
    ] {
        let mut v = wire();
        v["jobs"][0]["tasks"][0][field] = Value::Null;
        assert!(
            serde_json::from_value::<PreparedBatch>(v).is_err(),
            "{field}"
        );
    }
    for id in [
        json!(999),
        json!(1.5),
        json!("5134923400617818826"),
        json!(0),
    ] {
        let mut v = wire();
        v["jobs"][0]["tasks"][0]["family_id"] = id;
        assert!(serde_json::from_value::<PreparedBatch>(v).is_err());
    }
    for mutation in 0..7 {
        let mut v = wire();
        match mutation {
            0 => {
                let f = v["families"][0].clone();
                v["families"].as_array_mut().unwrap().push(f);
            }
            1 => {
                let mut f = v["families"][0].clone();
                f["family_id"] = json!(44);
                v["families"].as_array_mut().unwrap().push(f);
            }
            2 => v["families"][0]["instrument_type"] = json!("spot"),
            3 => v["families"][0]["instrument"]["tick_size"] = json!("0"),
            4 => v["families"][0]["quote_asset"] = json!("BTC"),
            5 => v["families"][0]["instrument"]["contract_value_asset"] = Value::Null,
            _ => v["families"][0]["identifier"] = json!(""),
        }
        assert!(
            serde_json::from_value::<PreparedBatch>(v).is_err(),
            "mutation {mutation}"
        );
    }
}
#[test]
fn four_okx_members_discover_route_and_keep_stable_concrete_identities() {
    let temp = directory();
    let path = temp.path().join("futureschain.tar.gz");
    let symbols = [
        "BTC-USD_UM-261030",
        "BTC-USD_UM-261127",
        "BTC-USD_UM-261225",
        "BTC-USD_UM-270326",
    ];
    let names = symbols.map(|s| format!("{s}-L2orderbook-5000lv-2026-10-27.data"));
    let payloads=symbols.map(|s|format!("{{\"instId\":\"{s}\",\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\",\"1\"]],\"asks\":[[\"101\",\"1\",\"1\"]]}}\n"));
    let members = (0..4)
        .map(|i| (names[i].as_str(), payloads[i].as_str()))
        .collect::<Vec<_>>();
    tar(&path, &members);
    let b = okx_depth(&path);
    let job = resolve(&b).unwrap();
    assert_eq!(job.tasks.len(), 4);
    let identities = job
        .tasks
        .iter()
        .map(|t| (t.symbol.clone(), t.instrument_id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(identities.values().collect::<BTreeSet<_>>().len(), 4);
    assert!(
        job.tasks
            .iter()
            .all(|t| t.instrument_id != b.families[0].family_id)
    );
    let groups = crate::historical::normalize::streams::plan(&job).unwrap();
    assert_eq!(groups.len(), 1);
    let config = support::runtime_in(temp.path());
    let quota = crate::historical::normalize::reconcile::Quota::new(
        config.resources.normalization_limits.reconciliation_bytes,
    );
    let routed = crate::historical::normalize::family::route(&groups[0], &quota, &config);
    assert!(
        routed.failure.is_none(),
        "{:?}",
        routed.failure.as_ref().map(|(_, e)| e)
    );
    assert_eq!(routed.inputs.len(), 4);
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 4);
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 4);
    drop(routed);
    drop(groups);
    for workers in [1, 3] {
        let mut publish_job = job.clone();
        let mut config = support::runtime();
        config.resources.workers = workers;
        config.resources.scratch_path = temp.path().join(format!("scratch-{workers}"));
        publish_job.resources = config.resources.clone();
        publish_job.output.staging_path = temp.path().join(format!("stage-{workers}"));
        publish_job.output.dataset_path = temp.path().join(format!("dataset-{workers}"));
        let result =
            crate::historical::normalize::execute_processing_job(&publish_job, &config).unwrap();
        assert_eq!(
            result.status,
            crate::historical::normalize::ProcessingStatus::Complete,
            "{:?}",
            result.failure
        );
        let manifest: crate::historical::normalize::manifest::DatasetManifest =
            serde_json::from_slice(
                &std::fs::read(publish_job.output.dataset_path.join("manifest.json")).unwrap(),
            )
            .unwrap();
        assert_eq!(manifest.resolved_instruments.len(), 4);
        assert_eq!(manifest.events_written, 8);
        assert_eq!(
            manifest.instrument_namespace.as_deref(),
            Some(b.instrument_namespace.as_str())
        );
        assert!(manifest.resolved_instruments.iter().all(|i| {
            i["metadata_origin"] == "family_assertion"
                && i["family_specification_sha256"]
                    .as_str()
                    .is_some_and(|v| v.len() == 64)
        }));
        assert_eq!(
            manifest
                .resolved_instruments
                .iter()
                .map(|i| (
                    i["instrument"]["symbol"].as_str().unwrap().to_owned(),
                    i["instrument"]["instrument_id"].as_i64().unwrap()
                ))
                .collect::<BTreeMap<_, _>>(),
            identities
        );
    }

    let mut reverse = members.clone();
    reverse.reverse();
    tar(&path, &reverse);
    let reversed = resolve(&b).unwrap();
    assert_eq!(
        identities,
        reversed
            .tasks
            .iter()
            .map(|t| (t.symbol.clone(), t.instrument_id))
            .collect()
    );
    tar(&path, &members[..3]);
    let smaller = resolve(&b).unwrap();
    for t in smaller.tasks {
        assert_eq!(identities[&t.symbol], t.instrument_id);
    }
    let mut included = b.clone();
    included.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::Include {
        symbols: vec![symbols[0].into()],
    });
    assert!(
        resolve(&included)
            .unwrap()
            .tasks
            .iter()
            .all(|t| t.symbol == symbols[0])
    );
}
#[test]
fn exact_catalog_precedence_conflicts_and_namespace_collisions() {
    let temp = directory();
    let path = temp.path().join("futureschain.tar.gz");
    tar(
        &path,
        &[(
            "BTC-USD_UM-261127-L2orderbook-5000lv-2026-10-27.data",
            "{\"instId\":\"BTC-USD_UM-261127\"}\n",
        )],
    );
    let mut b = okx_depth(&path);
    let derived = resolve(&b).unwrap();
    let mut exact: OfflineInstrument = serde_json::from_value(
        derived.tasks[0].raw_schema["marketforge_source"]["instrument"].clone(),
    )
    .unwrap();
    exact.instrument_id = 99;
    b.instruments.push(exact.clone());
    assert_eq!(resolve(&b).unwrap().tasks[0].instrument_id, 99);
    b.instruments[0].instrument.tick_size = Decimal::ONE;
    assert!(resolve(&b).unwrap_err().to_string().contains("conflicts"));
    b.instruments[0] = exact;
    b.instruments[0].symbol = "BTC-USD_UM-261225".into();
    b.instruments[0].instrument_id = derived.tasks[0].instrument_id;
    assert!(resolve(&b).unwrap_err().to_string().contains("collision"));
}
#[test]
fn bybit_native_and_binance_filename_discovery_preserve_null_contract_values() {
    let temp = directory();
    for exchange in ["binance", "bybit"] {
        let mut v = wire();
        v["families"][0]["exchange"] = json!(exchange);
        v["families"][0]["identifier"] = json!("BTCUSDT");
        v["families"][0]["quote_asset"] = json!("USDT");
        v["families"][0]["instrument"]["contract_value"] = Value::Null;
        v["families"][0]["instrument"]["contract_value_asset"] = Value::Null;
        let path = if exchange == "binance" {
            let p = temp.path().join("BTCUSDT_261225-trades-2026-10-27.zip");
            let mut writer = zip::ZipWriter::new(std::fs::File::create(&p).unwrap());
            writer
                .start_file(
                    "BTCUSDT_261225-trades-2026-10-27.csv",
                    zip::write::SimpleFileOptions::default(),
                )
                .unwrap();
            writer
                .write_all(b"id,price,qty,quote_qty,time,is_buyer_maker\n1,100,1,100,1000,true\n")
                .unwrap();
            writer.finish().unwrap();
            p
        } else {
            let p = temp.path().join("BTCUSDT-25DEC262026-10-27.csv.gz");
            let mut writer = flate2::write::GzEncoder::new(
                std::fs::File::create(&p).unwrap(),
                flate2::Compression::fast(),
            );
            writer.write_all(b"timestamp,symbol,side,size,price,trdMatchID\n1,BTCUSDT-25DEC26,Buy,1,100,native\n").unwrap();
            writer.finish().unwrap();
            p
        };
        v["jobs"][0]["tasks"][0]["input_path"] = json!(path);
        let job = resolve(&batch(v)).unwrap();
        assert_eq!(job.tasks.len(), 1);
        assert_eq!(job.tasks[0].instrument.contract_value, None);
        assert_eq!(
            family_of(
                job.tasks[0].exchange,
                InstrumentKind::Future,
                &job.tasks[0].symbol
            ),
            Some("BTCUSDT")
        );
        let groups = crate::historical::normalize::streams::plan(&job).unwrap();
        let config = support::runtime_in(temp.path());
        let quota = crate::historical::normalize::reconcile::Quota::new(
            config.resources.normalization_limits.reconciliation_bytes,
        );
        let routed = crate::historical::normalize::family::route(&groups[0], &quota, &config);
        assert!(
            routed.failure.is_none(),
            "{:?}",
            routed.failure.as_ref().map(|(_, e)| e)
        );
        assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 1);
    }
}
#[test]
fn duplicate_members_mismatch_and_truncation_are_rejected() {
    let temp = directory();
    let path = temp.path().join("futureschain.tar.gz");
    let name = "BTC-USD_UM-261127-L2orderbook-5000lv-2026-10-27.data";
    let good = "{\"instId\":\"BTC-USD_UM-261127\"}\n";
    tar(&path, &[(name, good), (name, good)]);
    assert!(
        resolve(&okx_depth(&path))
            .unwrap_err()
            .to_string()
            .contains("duplicate archive member")
    );
    tar(&path, &[(name, "{\"instId\":\"BTC-USD_UM-261225\"}\n")]);
    assert!(
        resolve(&okx_depth(&path))
            .unwrap_err()
            .to_string()
            .contains("identity conflict")
    );
    tar(&path, &[(name, good)]);
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(std::fs::metadata(&path).unwrap().len() - 5)
        .unwrap();
    assert!(resolve(&okx_depth(&path)).is_err());
}

#[test]
fn mixed_trade_member_and_depth_members_share_ids_and_absence_is_permitted_per_source() {
    let temp = directory();
    let depth = temp.path().join("futureschain.tar.gz");
    tar(
        &depth,
        &[
            ("one.data", "{\"instId\":\"BTC-USD_UM-261127\"}\n"),
            ("two.data", "{\"instId\":\"BTC-USD_UM-261127\"}\n"),
        ],
    );
    let depths = resolve(&okx_depth(&depth)).unwrap();
    assert_eq!(depths.tasks.len(), 2);
    assert_eq!(depths.tasks[0].instrument_id, depths.tasks[1].instrument_id);
    let path = temp.path().join("futureschain.zip");
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
    zip.start_file("trades.csv", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"instrument_name\nBTC-USD_UM-261225\nBTC-USD_UM-261127\n")
        .unwrap();
    zip.finish().unwrap();
    let mut value = wire();
    value["jobs"][0]["tasks"][0]["input_path"] = json!(path);
    let trades = resolve(&batch(value.clone())).unwrap();
    assert_eq!(trades.tasks.len(), 2);
    assert_eq!(
        depths.tasks[0].instrument_id,
        trades
            .tasks
            .iter()
            .find(|t| t.symbol == "BTC-USD_UM-261127")
            .unwrap()
            .instrument_id
    );
    value["jobs"][0]["tasks"][0]["instrument_selection"] =
        json!({"mode":"include","symbols":["BTC-USD_UM-270326"]});
    let absent = resolve(&batch(value)).unwrap();
    assert_eq!(absent.tasks.len(), 1);
    let config = support::runtime_in(temp.path());
    let groups = crate::historical::normalize::streams::plan(&absent).unwrap();
    let quota = crate::historical::normalize::reconcile::Quota::new(
        config.resources.normalization_limits.reconciliation_bytes,
    );
    let routed = crate::historical::normalize::family::route(&groups[0], &quota, &config);
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 0);
    assert_eq!(routed.stats.values().map(|s| s.filtered).sum::<u64>(), 2);
    let mut report = crate::historical::normalize::metrics::ProcessingMetricsReport::default();
    for (i, task) in absent.tasks.iter().enumerate() {
        let metrics = crate::historical::normalize::metrics::TaskMetrics {
            source: Some(crate::historical::normalize::reconcile::SourceObservation {
                observed_selected_records: routed.stats[&i].selected,
                ..Default::default()
            }),
            ..Default::default()
        };
        report.tasks.push(metrics.into_report(task));
    }
    assert!(
        crate::historical::normalize::family::validate_selection(&absent, &report)
            .unwrap_err()
            .to_string()
            .contains("never observed")
    );
}

#[test]
fn filtering_members_preserves_physical_rows_and_archive_inventory_limits() {
    let temp = directory();
    let path = temp.path().join("futureschain.tar.gz");
    tar(
        &path,
        &[
            (
                "BTC-USD_UM-261127-L2orderbook-5000lv-2026-10-27.data",
                "{\"instId\":\"BTC-USD_UM-261127\"}\n",
            ),
            (
                "BTC-USD_UM-261225-L2orderbook-5000lv-2026-10-27.data",
                "{\"instId\":\"BTC-USD_UM-261225\"}\n",
            ),
        ],
    );
    let mut b = okx_depth(&path);
    b.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::Include {
        symbols: vec!["BTC-USD_UM-261127".into()],
    });
    let job = resolve(&b).unwrap();
    let config = support::runtime_in(temp.path());
    let groups = crate::historical::normalize::streams::plan(&job).unwrap();
    let quota = crate::historical::normalize::reconcile::Quota::new(
        config.resources.normalization_limits.reconciliation_bytes,
    );
    let routed = crate::historical::normalize::family::route(&groups[0], &quota, &config);
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 2);
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 1);
    assert_eq!(routed.stats.values().map(|s| s.filtered).sum::<u64>(), 1);
    let mut config = support::runtime();
    config.resources.normalization_limits.metric_entries = 1;
    let error = b
        .resolve_job(&b.jobs[0], &catalog(), &config, &root())
        .unwrap_err();
    assert!(matches!(
        error.processing_cause(),
        crate::shared::error::MarketForgeError::ResourceLimit(_)
    ));
}

#[test]
fn empty_exchange_projection_jobs_do_not_enter_execution() {
    let temp = directory();
    let path = temp.path().join("futureschain.tar.gz");
    tar(
        &path,
        &[("data.jsonl", "{\"instId\":\"BTC-USD_UM-261127\"}\n")],
    );
    let mut batch = okx_depth(&path);
    let mut empty = batch.jobs[0].clone();
    empty.tasks.clear();
    empty.job_id.0 += 1;
    empty.dataset_id.0 += 1;
    empty.output.staging_path = "empty-stage".into();
    empty.output.dataset_path = "empty-output".into();
    batch.jobs.push(empty);
    assert_eq!(
        batch
            .resolve_jobs(&catalog(), &support::runtime(), &root())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn inherited_mixed_family_trade_policy_publishes_original_physical_ordinals() {
    let temp = directory();
    let path = temp.path().join("futureschain.zip");
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
    zip.start_file("trades.csv", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"instrument_name,trade_id,side,price,size,created_time,source\nBTC-USD_UM-261127,1,buy,100,1,1000,0\nBTC-USD_UM-261225,2,buy,100,1,1000,0\nBTC-USD_UM-261127,3,buy,100,1,1001,0\n").unwrap();
    zip.finish().unwrap();
    let mut v = wire();
    v["jobs"][0]["tasks"][0]["input_path"] = json!(path);
    let b = batch(v);
    let mut config = support::runtime();
    config.resources.scratch_path = temp.path().join("scratch");
    config.timestamp_regression.trades.policy =
        crate::shared::historical::policies::TradeTimestamp::Preserve;
    let mut job = b
        .resolve_job(&b.jobs[0], &catalog(), &config, &root())
        .unwrap();
    job.output.dataset_path = temp.path().join("dataset");
    job.output.staging_path = temp.path().join("staging");
    let result = crate::historical::normalize::execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        crate::historical::normalize::ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m: crate::historical::normalize::manifest::DatasetManifest = serde_json::from_reader(
        std::fs::File::open(job.output.dataset_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(m.events_written, 3);
    assert_eq!(m.resolved_instruments.len(), 2);
    assert_eq!(m.protocol_version, 5);
    let mut ordinals = Vec::new();
    for stream in m.streams {
        let text = std::fs::read_to_string(
            job.output
                .dataset_path
                .join(stream.directory)
                .join("sources.jsonl"),
        )
        .unwrap();
        for line in text.lines() {
            ordinals.push(
                serde_json::from_str::<Value>(line).unwrap()["source_record"]
                    .as_u64()
                    .unwrap(),
            );
        }
    }
    ordinals.sort_unstable();
    assert_eq!(ordinals, [1, 2, 3]);
}
