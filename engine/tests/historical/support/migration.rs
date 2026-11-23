//! Explicit synthetic cases and retained output ownership. Real archive tests
//! select exclusively from the authoritative prepared batch.
use super::{
    root,
    runtime::{Overrides, TestRun},
};
use marketforge_engine::{
    historical::{config::*, job::*},
    shared::{
        error::{MarketForgeError, Result},
        historical::ProcessingConfig,
    },
};
use serde_json::json;
use std::path::{Path, PathBuf};
fn error(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
fn case(label: &str) -> Result<(&str, &str, &str, &str, &str)> {
    let (profile, symbol) = label
        .split_once(':')
        .ok_or_else(|| error("synthetic case requires profile:symbol"))?;
    let mut parts = profile.split('.');
    let (Some(exchange), Some(instrument), Some(category), Some(content)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(error("invalid synthetic profile identity"));
    };
    if parts.next().is_some() || symbol.is_empty() {
        return Err(error("invalid synthetic case identity"));
    }
    Ok((exchange, instrument, category, symbol, content))
}
fn resolve(mut b: PreparedBatch) -> Result<ProcessingJob> {
    let mut run = TestRun::new("retained_case_resolution").map_err(error)?;
    for j in &mut b.jobs {
        j.output.staging_path = run.path.join("staging");
        j.output.dataset_path = run.path.join("dataset");
    }
    b.validate(&root())?;
    let c = run.runtime(Overrides::default()).map_err(error)?;
    let catalog = FormatCatalog::load(root().join("data/.config/engine/normalize"))?;
    let [job] = b.jobs.as_slice() else {
        return Err(error("retained case must select exactly one job"));
    };
    let result = b.resolve_job(job, &catalog, &c, &root());
    run.finish(&result.as_ref().map(|_| ()).map_err(|e| e.to_string()))
        .map_err(error)?;
    result
}
/// Source-only retained tests use production profiles and explicit synthetic
/// metadata. They never borrow raw records or specifications from a family file.
pub fn template(label: impl AsRef<str>) -> Result<ProcessingJob> {
    let (ex, kind, category, symbol, data_type) = case(label.as_ref())?;
    let (instrument_id, expected_symbol) = super::profile_cases::identity(ex, kind, category);
    if symbol != expected_symbol {
        return Err(error(
            "synthetic symbol does not match its explicit unit namespace",
        ));
    }
    let (value, asset) = match (kind, category) {
        ("spot", "spot") => (None, None),
        ("future", "linear") => (Some("0.01"), Some("BTC")),
        _ => return Err(error("undeclared synthetic template contract")),
    };
    let entry = json!({
        "instrument_id": instrument_id,
        "exchange": ex,
        "instrument_type": kind,
        "market_category": category,
        "symbol": symbol,
        "base_asset": "BTC",
        "quote_asset": if kind == "spot" { "USDT" } else { "USD" },
        "instrument": {
            "tick_size": "0.1",
            "contract_value": value,
            "contract_value_asset": asset
        }
    });
    let tasks: Vec<_> = (1..=3)
        .map(|id| {
            json!({
                "task_id": id,
                "input_path": format!("unavailable-synthetic-input-{id}"),
                "exchange": ex,
                "instrument_type": kind,
                "market_category": category,
                "symbol": symbol,
                "instrument_id": instrument_id,
                "base_asset": entry["base_asset"],
                "quote_asset": entry["quote_asset"],
                "instrument": entry["instrument"],
                "stream_id": format!("synthetic.{ex}.{kind}.{category}.{symbol}.{data_type}"),
                "stream_rank": 1
            })
        })
        .collect();
    let batch: PreparedBatch = serde_json::from_value(json!({
        "schema_version": 4,
        "kind": "normalization_batch",
        "status": "prepared",
        "run_id": "synthetic-template",
        "run_directory": "unit-run",
        "batch_path": "unit-batch",
        "ranking_policy": "turnover",
        "ranking_snapshot": null,
        "requested_interval": {
            "start": "1970-01-01T00:00:00Z",
            "end": "1970-01-02T00:00:00Z"
        },
        "instrument_namespace": "marketforge-profile-unit-v1",
        "instruments": [entry],
        "jobs": [{
            "operation": "normalize",
            "job_id": 1,
            "dataset_id": 1,
            "tasks": tasks,
            "ordering": { "primary": "event_timestamp_ns", "tie_break": "stream_rank" },
            "output": {
                "content_type": data_type,
                "staging_path": "unit-staging",
                "dataset_path": "unit-dataset"
            }
        }]
    }))
    .map_err(|e| error(e.to_string()))?;
    resolve(batch)
}
pub fn configuration(_path: impl AsRef<Path>, _root: impl AsRef<Path>) -> Result<ProcessingConfig> {
    let mut run = TestRun::new("retained_runtime").map_err(error)?;
    let result = run.runtime(Default::default());
    run.finish(&result.as_ref().map(|_| ()).map_err(Clone::clone))
        .map_err(error)?;
    result.map_err(error)
}
/// Explicit threshold fixture for evaluator/config tests; never production values.
pub fn threshold_fixture() -> ProcessingConfig {
    let mut c = configuration("unused", root()).unwrap();
    c.integrity_policy.invalid_record.action = IntegrityAction::Degrade;
    c.integrity_policy.invalid_record.max_count = Some(10000);
    c.integrity_policy.invalid_record.max_rate = Some(0.001);
    c.integrity_policy.invalid_record.minimum_samples = 1000;
    c.integrity_policy.invalid_record.windows.daily = Some(IntegrityWindowRule {
        max_rate: 0.005,
        minimum_samples: 1000,
    });
    c.integrity_policy.invalid_record.windows.hourly = Some(IntegrityWindowRule {
        max_rate: 0.02,
        minimum_samples: 100,
    });
    c
}
/// Retained artifact tests keep the guard until assertions finish. Drop finalizes
/// metadata on normal completion/unwind; it never deletes any artifact.
pub struct PersistentDirectory {
    pub path: PathBuf,
    run: TestRun,
}
impl PersistentDirectory {
    #[track_caller]
    pub fn new() -> Self {
        let caller = std::panic::Location::caller();
        let name = Path::new(caller.file())
            .file_stem()
            .unwrap()
            .to_string_lossy();
        let run = TestRun::new(&format!("generic_{name}_{}", caller.line())).unwrap();
        Self {
            path: run.path.clone(),
            run,
        }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}
impl std::ops::Deref for PersistentDirectory {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}
impl AsRef<Path> for PersistentDirectory {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
impl Drop for PersistentDirectory {
    fn drop(&mut self) {
        let result = if std::thread::panicking() {
            Err("test assertion panicked; retained artifacts contain diagnostics".into())
        } else {
            Ok(())
        };
        if let Err(error) = self.run.finish(&result) {
            eprintln!("cannot finalize retained test metadata: {error}");
        }
    }
}
pub struct PersistentTuple(pub PathBuf, PersistentDirectory);
impl PersistentTuple {
    #[track_caller]
    pub fn new() -> Self {
        let directory = PersistentDirectory::new();
        Self(directory.path.clone(), directory)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// Exclusive retained artifact root for legacy guards; never automatically removed.
pub fn persistent_path(label: String) -> PathBuf {
    let mut run = TestRun::new(&format!("execution/{label}")).expect("persistent test directory");
    run.metadata["purpose"] =
        json!("retained integration scope; inspect artifacts and Cargo result");
    run.finish(&Ok(()))
        .expect("record allocated persistent scope");
    run.path
}
