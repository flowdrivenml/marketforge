//! Narrow adapters for retained tests: labels identify reviewed cases, never files
//! to load. Production batch validation/resolution remains the only interpreter.
use super::{
    fixtures::workload,
    root,
    runtime::{Overrides, TestRun},
};
use marketforge_engine::{
    historical::{config::*, job::*},
    shared::{
        domain::canonical::Exchange,
        error::{MarketForgeError, Result},
        historical::ProcessingConfig,
    },
};
use serde_json::json;
use std::path::{Path, PathBuf};
fn error(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
fn case(label: &Path) -> Result<(String, String, String, String, String, String)> {
    let name = label
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| error("invalid test case label"))?;
    let (identity, period, kind) = if let Some((a, b)) = name.split_once("-trade-") {
        (a, b, "trades")
    } else if let Some((a, b)) = name.split_once("-l2-") {
        (a, b, "depth")
    } else {
        return Err(error(format!("unknown retained test case {name}")));
    };
    let mut parts = identity.splitn(4, '-');
    let exchange = parts.next().unwrap_or_default();
    let instrument = parts.next().unwrap_or_default();
    let category = parts.next().unwrap_or_default();
    let symbol = parts.next().unwrap_or_default();
    Ok((
        exchange.into(),
        instrument.into(),
        category.into(),
        symbol.into(),
        kind.into(),
        period.into(),
    ))
}
fn resolve(mut b: PreparedBatch) -> Result<ProcessingJob> {
    let mut run = TestRun::new("retained_case_resolution").map_err(error)?;
    for j in &mut b.jobs {
        j.output.staging_path = run.path.join("staging");
        j.output.dataset_path = run.path.join("dataset");
    }
    b.validate(&root())?;
    let c = run.runtime(Overrides::default()).map_err(error)?;
    let catalog = FormatCatalog::load(root().join("data/.config/historical"))?;
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
pub fn template(label: impl AsRef<Path>) -> Result<ProcessingJob> {
    let (ex, kind, category, symbol, data_type, _) = case(label.as_ref())?;
    let mut b = workload().map_err(error)?;
    if ex == "okx"
        && kind == "future"
        && category == "linear"
        && symbol == "BTC-USD_UM-261225"
        && !b
            .instruments
            .iter()
            .any(|i| i.symbol == symbol && i.exchange == Exchange::Okx)
    {
        // Explicit persisted test authority; synthetic specs, no real-data claim.
        b.instrument_namespace = "marketforge-synthetic-unit-v1".into();
        b.instruments.push(serde_json::from_value(json!({"instrument_id":1000001,"exchange":"okx","instrument_type":"future","market_category":"linear","symbol":symbol,"base_asset":"BTC","quote_asset":"USD","instrument":{"tick_size":"0.1","contract_value":"0.01","contract_value_asset":"BTC"}})).map_err(|e|error(e.to_string()))?);
    }
    let entry=b.instruments.iter().find(|i|serde_json::to_value(i.exchange).unwrap()==ex&&serde_json::to_value(i.instrument_type).unwrap()==kind&&serde_json::to_value(i.market_category).unwrap()==category&&i.symbol==symbol).cloned().ok_or_else(||error(format!("source-only case requires explicit synthetic metadata: {ex}.{kind}.{category}.{symbol}")))?;
    b.jobs
        .retain(|j| serde_json::to_value(j.output.content_type).unwrap() == data_type);
    let job = b
        .jobs
        .first_mut()
        .ok_or_else(|| error("missing job type"))?;
    // Three source units preserve multi-task validation tests, but never refer to
    // downloaded sources. Execution tests replace these with explicit synthetic inputs.
    job.tasks = (1..=3)
        .map(|id| PreparedTask {
            task_id: TaskId(id),
            input_path: PathBuf::from(format!("unavailable-synthetic-input-{id}")),
            exchange: entry.exchange,
            instrument_type: entry.instrument_type,
            market_category: entry.market_category,
            base_asset: Some(entry.base_asset.clone()),
            quote_asset: Some(entry.quote_asset.clone()),
            symbol: Some(entry.symbol.clone()),
            instrument: Some(entry.instrument.clone()),
            instrument_id: Some(entry.instrument_id),
            stream_id: StreamId(format!(
                "synthetic.{ex}.{kind}.{category}.{symbol}.{data_type}"
            )),
            stream_rank: 1,
            scope: None,
            source_options: None,
        })
        .collect();
    resolve(b)
}
/// Retained archive-specific oracles require their original requested interval.
/// They fail explicitly when normalize.json has no such prepared sources.
pub fn archive_case(label: impl AsRef<Path>) -> Result<ProcessingJob> {
    let (ex, kind, category, symbol, data_type, period) = case(label.as_ref())?;
    let mut b = workload().map_err(error)?;
    let date = period.split('-').next().unwrap_or_default();
    let requested = if date.len() == 8 {
        format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..8])
    } else {
        return Err(error("invalid retained archive period"));
    };
    if !b.requested_interval.start.starts_with(&requested) {
        return Err(error(format!(
            "unavailable retained source oracle: {ex}.{kind}.{category}.{symbol}.{data_type} requires prepared interval starting {requested}; authoritative normalize.json starts {}. No archive substitution is permitted",
            b.requested_interval.start
        )));
    }
    b.jobs.retain_mut(|j| {
        if serde_json::to_value(j.output.content_type).unwrap() != data_type {
            return false;
        }
        j.tasks.retain(|t| {
            serde_json::to_value(t.exchange).unwrap() == ex
                && serde_json::to_value(t.instrument_type).unwrap() == kind
                && serde_json::to_value(t.market_category).unwrap() == category
                && t.symbol.as_deref() == Some(&symbol)
        });
        !j.tasks.is_empty()
    });
    resolve(b)
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
/// September-specific aggregate oracles cannot be substituted with October files.
pub fn retained_trade_jobs() -> Vec<ProcessingJob> {
    let b = workload().expect("authoritative normalization fixture");
    assert!(
        b.requested_interval.start.starts_with("2026-09-01"),
        "unavailable real-data prerequisite: retained aggregate oracles require a prepared September 2026 interval, found {}",
        b.requested_interval.start
    );
    b.jobs
        .iter()
        .filter(|j| j.output.content_type == ContentType::Trades)
        .map(|j| {
            let mut selected = b.clone();
            selected.jobs = vec![j.clone()];
            resolve(selected).expect("resolve retained trade job")
        })
        .collect()
}
