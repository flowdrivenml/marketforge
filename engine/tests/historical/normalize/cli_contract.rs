//! Real CLI processes over tiny deterministic sources, with retained artifacts.
use crate::support::{
    self,
    fixtures::{concrete_stream, workload},
    runtime::TestRun,
};
use marketforge_engine::{
    historical::{
        config::PreparedBatch,
        job::{ContentType, InstrumentKind},
        normalize::{
            ProcessingErrorCode, ProcessingResult, ProcessingStatus,
            response::{NormalizationError, NormalizationResponse},
        },
    },
    shared::domain::canonical::Exchange,
};
use serde_json::{Value, json};
use std::{fs, io::Write, path::Path, process::Command};
fn fixture(multi: bool) -> (TestRun, PreparedBatch) {
    let mut run = TestRun::new("execution/cli_contract").unwrap();
    let all = workload().unwrap();
    let pick = |kind| {
        let mut selected =
            concrete_stream(&all, Exchange::Bybit, InstrumentKind::Spot, kind, 1).unwrap();
        selected.jobs[0].tasks.truncate(1);
        selected
    };
    let mut b = pick(ContentType::Trades);
    if multi {
        b.jobs.extend(pick(ContentType::Depth).jobs);
    }
    b.jobs[0].job_id.0 = 9007199254740993;
    b.jobs[0].dataset_id.0 = 9007199254740995;
    let input = run.path.join("trade.csv.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        fs::File::create(&input).unwrap(),
        flate2::Compression::default(),
    );
    gzip.write_all(b"id,timestamp,price,volume,side,rpi\n7,1250,200,3,buy,0\n")
        .unwrap();
    gzip.finish().unwrap();
    b.jobs[0].tasks[0].input_path = input;
    if multi {
        b.jobs[1].tasks[0].input_path = run.path.join("unavailable-depth.zip");
    }
    let mut config = run.runtime(Default::default()).unwrap();
    // These CLI contract sources contain only a handful of tiny records. Give
    // their explicit synthetic runtime matching safety bounds, rather than the
    // production metadata allowances that exceed its 1 GiB test reservation.
    config.resources.normalization_limits.record_bytes = 64 * 1024;
    config.resources.normalization_limits.metric_entries = 256;
    config.resources.normalization_limits.depth_levels = 4096;
    config.resources.normalization_limits.reconciliation_bytes = 8 * 1024 * 1024;
    let mut value: toml::Value = toml::from_str(include_str!(
        "../../../../data/.config/historical/general.toml"
    ))
    .unwrap();
    value["resources"] = toml::Value::try_from(&config.resources).unwrap();
    value["schema_version"] = toml::Value::Integer(1);
    fs::write(
        run.path.join("runtime.toml"),
        toml::to_string(&value).unwrap(),
    )
    .unwrap();
    run.prepare(&b, &config).unwrap();
    // prepare redirects a copy; persist the resulting authoritative subset.
    b = marketforge_engine::historical::config::read_prepared_batch(
        run.path.join("selected-batch.json"),
        support::root(),
    )
    .unwrap();
    (run, b)
}
fn invoke(run: &TestRun, args: &[&str]) -> (std::process::Output, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_marketforge-normalize"))
        .current_dir("/")
        .arg("--project-root")
        .arg(support::root())
        .arg("--config")
        .arg(run.path.join("runtime.toml"))
        .args(args)
        .output()
        .unwrap();
    let value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON stdout: {e}; stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, value)
}
fn batch_path(run: &TestRun) -> String {
    run.path.join("selected-batch.json").display().to_string()
}
#[test]
fn batch_cli_commits_exact_identities_and_clean_json() {
    let (mut run, b) = fixture(false);
    let (o, v) = invoke(&run, &["--batch", &batch_path(&run)]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(v["kind"], "normalization_result");
    assert_eq!(v["schema_version"], 1);
    assert_eq!(v["run_id"], b.run_id);
    assert_eq!(v["status"], "complete");
    assert_eq!(v["jobs"][0]["job_id"], 9007199254740993u64);
    assert_eq!(v["jobs"][0]["dataset_id"], 9007199254740995u64);
    assert_eq!(v["jobs"][0]["events_written"], 1);
    assert!(v["jobs"][0]["failure_report_path"].is_null());
    let r: NormalizationResponse = serde_json::from_value(v).unwrap();
    let internal:ProcessingResult=serde_json::from_value(json!({"protocol_version":1,"job_id":r.jobs[0].job_id,"dataset_id":r.jobs[0].dataset_id,"status":r.jobs[0].status,"manifest_path":r.jobs[0].manifest_path,"failure_report_path":null,"events_written":r.jobs[0].events_written,"files_written":r.jobs[0].files_written,"start_timestamp_ns":r.jobs[0].start_timestamp_ns,"end_timestamp_ns":r.jobs[0].end_timestamp_ns,"failure":null})).unwrap();
    support::assertions::committed(&run, &b.jobs[0], &internal).unwrap();
    let snapshot: Value = serde_json::from_slice(
        &fs::read(b.jobs[0].output.dataset_path.join("processing-job.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        snapshot["tasks"][0]["raw_schema"]["marketforge_source"]["instrument_namespace"],
        b.instrument_namespace
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn mixed_batch_nonzero_preserves_success_and_failure_report() {
    let (mut run, b) = fixture(true);
    let (o, v) = invoke(&run, &["--batch", &batch_path(&run)]);
    assert!(!o.status.success());
    assert_eq!(v["status"], "failed");
    assert_eq!(
        v["summary"],
        json!({"jobs_total":2,"jobs_complete":1,"jobs_failed":1,"jobs_not_reported":0})
    );
    assert_eq!(v["jobs"][0]["status"], "complete");
    assert_eq!(v["jobs"][1]["status"], "failed");
    assert!(v["jobs"][1]["manifest_path"].is_null());
    assert!(Path::new(v["jobs"][1]["failure_report_path"].as_str().unwrap()).is_file());
    assert!(!b.jobs[1].output.dataset_path.exists());
    let failure: Value = serde_json::from_slice(
        &fs::read(v["jobs"][1]["failure_report_path"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(failure["error_code"], "missing_source");
    assert_eq!(failure["job_id"], b.jobs[1].job_id.0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn legacy_modes_use_envelope_and_consistent_redirection() {
    let (mut run, b) = fixture(false);
    let config = run.runtime(Default::default()).unwrap();
    let resolved = run.prepare(&b, &config).unwrap();
    let directory = run.path.join("jobs");
    fs::create_dir(&directory).unwrap();
    let path = directory.join("one.json");
    fs::write(&path, serde_json::to_vec(&resolved.jobs[0]).unwrap()).unwrap();
    for (mode, input, suffix) in [
        ("--job", path.as_path(), "single"),
        ("--jobs-dir", directory.as_path(), "directory"),
    ] {
        let root = run.path.join(suffix);
        let (o, v) = invoke(
            &run,
            &[
                mode,
                input.to_str().unwrap(),
                "--output-root",
                root.to_str().unwrap(),
            ],
        );
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        assert_eq!(v["status"], "complete");
        assert!(v["run_id"].is_null());
        assert_eq!(v["summary"]["jobs_total"], 1);
        assert!(Path::new(v["jobs"][0]["manifest_path"].as_str().unwrap()).starts_with(&root));
    }
    run.finish(&Ok(())).unwrap();
}
#[test]
fn invalid_inputs_and_runtime_return_nonzero_without_fabricated_jobs() {
    let (mut run, mut b) = fixture(false);
    b.schema_version = 99;
    let bad = run.path.join("bad.json");
    fs::write(&bad, serde_json::to_vec(&b).unwrap()).unwrap();
    let (o, v) = invoke(&run, &["--batch", bad.to_str().unwrap()]);
    assert!(!o.status.success());
    assert_eq!(v["status"], "failed");
    assert_eq!(v["jobs"], json!([]));
    assert!(!v["error"].is_null());
    fs::write(run.path.join("runtime.toml"), "schema_version=99").unwrap();
    let (o, v) = invoke(&run, &["--batch", &batch_path(&run)]);
    assert!(!o.status.success());
    assert_eq!(v["summary"]["jobs_not_reported"], 1);
    assert_eq!(v["run_id"], b.run_id);
    for args in [
        vec![],
        vec!["--batch", "one", "--job", "two"],
        vec!["--batch", "one", "--jobs-dir", "two"],
        vec!["--job", "one", "--jobs-dir", "two"],
    ] {
        let o = Command::new(env!("CARGO_BIN_EXE_marketforge-normalize"))
            .args(args)
            .output()
            .unwrap();
        assert!(!o.status.success());
    }
    run.finish(&Ok(())).unwrap();
}
#[test]
fn relative_batch_input_and_existing_dataset_protection() {
    let (mut run, b) = fixture(false);
    let relative = run
        .path
        .join("selected-batch.json")
        .strip_prefix(support::root())
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let (o, v) = invoke(&run, &["--batch", &relative]);
    assert!(o.status.success());
    let manifest = v["jobs"][0]["manifest_path"].as_str().unwrap();
    let before = fs::read(manifest).unwrap();
    let (o, v) = invoke(&run, &["--batch", &relative]);
    assert!(!o.status.success());
    assert_eq!(v["status"], "failed");
    assert_eq!(fs::read(manifest).unwrap(), before);
    assert_eq!(b.jobs.len(), 1);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn response_accounts_for_unreported_jobs_and_exact_failures() {
    use marketforge_engine::historical::job::{DatasetId, JobId};
    let failed = ProcessingResult::failed(
        JobId(u64::MAX),
        DatasetId(u64::MAX),
        ProcessingErrorCode::OutputFailure,
        "synthetic failure",
    );
    let response = NormalizationResponse::new(
        Some("run".into()),
        3,
        vec![failed],
        Some(NormalizationError {
            code: "execution_error".into(),
            message: "session stopped".into(),
        }),
    );
    assert_eq!(response.status, ProcessingStatus::Failed);
    assert_eq!(response.summary.jobs_not_reported, 2);
    let v = serde_json::to_value(response).unwrap();
    assert_eq!(v["jobs"][0]["job_id"], u64::MAX);
    assert!(v["jobs"][0]["failure_report_path"].is_null());
}
#[cfg(unix)]
#[test]
fn manifest_discovery_rejects_symlink_artifacts() {
    let (mut run, _) = fixture(false);
    let directory = run.path.join("malicious/trades");
    fs::create_dir_all(&directory).unwrap();
    std::os::unix::fs::symlink(
        run.path.join("trade.csv.gz"),
        directory.join("part-0.parquet"),
    )
    .unwrap();
    assert!(
        marketforge_engine::historical::normalize::manifest::inspect_parquet_files(
            directory.parent().unwrap()
        )
        .unwrap_err()
        .to_string()
        .contains("symlink")
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn successful_depth_batch_preserves_auxiliary_artifacts() {
    let (mut run, b) = fixture(true);
    let input = &b.jobs[1].tasks[0].input_path;
    let mut zip = zip::ZipWriter::new(fs::File::create(input).unwrap());
    zip.start_file("sample.data", zip::write::SimpleFileOptions::default())
        .unwrap();
    let snapshot = json!({"type":"snapshot","ts":1250,"cts":1250,"data":{"s":b.jobs[1].tasks[0].symbol.as_ref().unwrap(),"u":10,"seq":1,"b":[["200","2"]],"a":[["400","3"]]}});
    writeln!(zip, "{snapshot}").unwrap();
    zip.finish().unwrap();
    let (o, v) = invoke(&run, &["--batch", &batch_path(&run)]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(v["summary"]["jobs_complete"], 2);
    let path = v["jobs"][1]["manifest_path"].as_str().unwrap();
    let manifest: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(manifest["events_written"], 2);
    assert_eq!(manifest["auxiliary_files"].as_array().unwrap().len(), 3);
    for file in manifest["auxiliary_files"].as_array().unwrap() {
        assert!(
            Path::new(path)
                .parent()
                .unwrap()
                .join(file["path"].as_str().unwrap())
                .is_file()
        );
    }
    run.finish(&Ok(())).unwrap();
}

#[test]
fn batch_cli_diagnostics_keep_stdout_structured() {
    let (mut run, batch) = fixture(false);
    let path = run.path.join("runtime.toml");
    let mut value: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    value["diagnostics"] = toml::Value::try_from(
        marketforge_engine::shared::historical::config::DiagnosticsConfig {
            mode: marketforge_engine::shared::historical::config::DiagnosticsMode::Basic,
            interval_ms: 100,
        },
    )
    .unwrap();
    fs::write(&path, toml::to_string(&value).unwrap()).unwrap();
    let (output, response) = invoke(&run, &["--batch", &batch_path(&run)]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(response["status"], "complete");
    assert!(String::from_utf8_lossy(&output.stderr).contains("normalization diagnostics started"));
    let parent = batch.jobs[0].output.staging_path.parent().unwrap();
    let report = fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("performance-"))
        .unwrap()
        .path()
        .join("performance.json");
    let report: Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    assert_eq!(report["status"], "committed");
    assert_eq!(report["schema_version"], 1);
    run.finish(&Ok(())).unwrap();
}
