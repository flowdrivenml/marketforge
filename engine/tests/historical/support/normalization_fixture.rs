//! Typed fixture access shared by historical library and integration tests.
use super::fixture_engine::historical::{
    config::{PreparedBatch, PreparedJob, PreparedTask, read_prepared_batch},
    job::ContentType,
};
use std::path::{Path, PathBuf};

pub(crate) fn normalization_fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/historical/fixtures/normalize.json")
}

pub(crate) fn canonical_normalization_batch() -> Result<PreparedBatch, String> {
    let path = normalization_fixture_path();
    read_prepared_batch(&path, project_root()).map_err(|error| {
        format!(
            "cannot load authoritative normalization fixture {}: {error}",
            path.display()
        )
    })
}

pub(crate) fn select_normalization_tasks(
    batch: &PreparedBatch,
    content_type: ContentType,
    matches: impl Fn(&PreparedTask) -> bool,
) -> PreparedBatch {
    let mut selected = batch.clone();
    selected.jobs.retain_mut(|job| {
        if job.output.content_type != content_type {
            return false;
        }
        job.tasks.retain(&matches);
        !job.tasks.is_empty()
    });
    selected
}

/// Choose an exact number of sources from a single canonical job. Task identity
/// fixes the sample independently of the order of the input JSON arrays.
pub(crate) fn select_normalization_sample(
    batch: &PreparedBatch,
    description: &str,
    content_type: ContentType,
    count: usize,
    matches: impl Fn(&PreparedTask) -> bool,
) -> Result<PreparedBatch, String> {
    let selected = select_normalization_tasks(batch, content_type, matches);
    let mut jobs = selected.jobs;
    jobs.sort_by_key(|job| job.job_id.0);
    let available = jobs.iter().map(|job| job.tasks.len()).max().unwrap_or(0);
    let mut job: PreparedJob = jobs
        .into_iter()
        .find(|job| count > 0 && job.tasks.len() >= count)
        .ok_or_else(|| format!("normalize.json has no {description} {content_type:?} job with {count} matching sources (maximum {available})"))?;
    job.tasks.sort_by_key(|task| task.task_id.0);
    job.tasks.truncate(count);
    let subset = PreparedBatch {
        jobs: vec![job],
        ..selected
    };
    subset
        .validate(project_root())
        .map_err(|error| format!("invalid {description} subset of normalize.json: {error}"))?;
    Ok(subset)
}

#[allow(
    dead_code,
    reason = "used by library normalization tests but not every integration target"
)]
pub(crate) fn bitget_trade_sources(spot: bool, count: usize) -> Result<PreparedBatch, String> {
    let stream = if spot {
        "bitget.spot.spot.BTCUSDT.trades"
    } else {
        "bitget.linear.perpetual.BTCUSDT.trades"
    };
    select_normalization_sample(
        &canonical_normalization_batch()?,
        stream,
        ContentType::Trades,
        count,
        |task| task.stream_id.0 == stream,
    )
}

fn project_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

pub(crate) fn bybit_spot_trade_job(
    config: &super::fixture_engine::shared::historical::ProcessingConfig,
) -> Result<super::fixture_engine::historical::job::ProcessingJob, String> {
    use super::fixture_engine::historical::config::FormatCatalog;
    let stream = "bybit.spot.spot.BTCUSDT.trades";
    let batch = select_normalization_sample(
        &canonical_normalization_batch()?,
        stream,
        ContentType::Trades,
        1,
        |task| task.stream_id.0 == stream,
    )?;
    let root = project_root();
    let catalog = FormatCatalog::load(root.join("data/.config/historical"))
        .map_err(|error| error.to_string())?;
    batch
        .resolve_job(&batch.jobs[0], &catalog, config, root)
        .map_err(|error| format!("cannot resolve {stream} from normalize.json: {error}"))
}
