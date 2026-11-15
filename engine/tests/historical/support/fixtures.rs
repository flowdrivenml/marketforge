use super::{TestResult, root};
use marketforge_engine::{
    historical::{
        config::{Feed, PreparedBatch, PreparedTask, read_prepared_batch},
        job::{ContentType, InstrumentKind},
    },
    shared::domain::canonical::Exchange,
};
#[derive(Debug, Clone)]
pub struct Selection<'a> {
    pub exchange: Exchange,
    pub instrument_type: InstrumentKind,
    pub market_category: &'a str,
    pub content_type: ContentType,
    pub symbol: Option<&'a str>,
    pub family: Option<&'a str>,
    pub feed: Option<Feed>,
    pub filename: Option<&'a str>,
}
pub fn workload() -> TestResult<PreparedBatch> {
    read_prepared_batch("engine/tests/historical/fixtures/normalize.json", root())
        .map_err(|e| e.to_string())
}
impl Selection<'_> {
    fn matches(&self, t: &PreparedTask) -> bool {
        t.exchange == self.exchange
            && t.instrument_type == self.instrument_type
            && serde_json::to_value(t.market_category).unwrap().as_str()
                == Some(self.market_category)
            && t.symbol.as_deref() == self.symbol
            && t.scope.as_ref().map(|s| s.identifier.as_str()) == self.family
            && t.source_options.as_ref().map(|o| o.feed) == self.feed
            && self
                .filename
                .is_none_or(|name| t.input_path.file_name().and_then(|s| s.to_str()) == Some(name))
    }
}
pub fn select(
    batch: &PreparedBatch,
    name: &str,
    selection: &Selection<'_>,
    expected: usize,
) -> TestResult<PreparedBatch> {
    let mut subset = batch.clone();
    subset.jobs.retain_mut(|j| {
        if j.output.content_type != selection.content_type {
            return false;
        }
        j.tasks.retain(|t| selection.matches(t));
        !j.tasks.is_empty()
    });
    let count: usize = subset.jobs.iter().map(|j| j.tasks.len()).sum();
    if expected == 0 || count != expected {
        return Err(format!(
            "test {name}: {selection:?}; expected {expected} tasks, found {count}; matches: {:?}",
            subset
                .jobs
                .iter()
                .flat_map(|j| j.tasks.iter().map(|t| (&t.stream_id, &t.input_path)))
                .collect::<Vec<_>>()
        ));
    }
    Ok(subset)
}

/// Select one concrete stream from current prepared metadata. The minimum is a
/// coverage prerequisite (for example, multi-archive execution), not a fixture count.
pub fn concrete_stream(
    batch: &PreparedBatch,
    exchange: Exchange,
    instrument_type: InstrumentKind,
    content_type: ContentType,
    minimum_sources: usize,
) -> TestResult<PreparedBatch> {
    let mut candidates = std::collections::BTreeMap::new();
    for job in &batch.jobs {
        if job.output.content_type != content_type {
            continue;
        }
        for task in &job.tasks {
            if task.exchange == exchange
                && task.instrument_type == instrument_type
                && task.scope.is_none()
                && task.symbol.is_some()
            {
                *candidates
                    .entry((job.job_id.0, task.stream_id.0.clone()))
                    .or_insert(0usize) += 1;
            }
        }
    }
    let ((job_id,stream_id),_) = candidates.into_iter().find(|(_,count)| *count>=minimum_sources)
        .ok_or_else(||format!("prepared batch has no {exchange:?}/{instrument_type:?}/{content_type:?} concrete stream with at least {minimum_sources} sources"))?;
    let mut subset = batch.clone();
    subset.jobs.retain_mut(|job| {
        if job.job_id.0 != job_id {
            return false;
        }
        job.tasks
            .retain(|task| task.stream_id.0 == stream_id && task.scope.is_none());
        !job.tasks.is_empty()
    });
    Ok(subset)
}

/// Select profile metadata for synthetic payload tests, independently of archive
/// names and concrete instrument identities. The caller replaces the source.
pub fn profile_source(
    batch: &PreparedBatch,
    exchange: Exchange,
    instrument_type: InstrumentKind,
    market_category: &str,
    content_type: ContentType,
    feed: Option<Feed>,
    family: bool,
) -> TestResult<PreparedBatch> {
    let mut subset = batch.clone();
    subset.jobs.retain_mut(|job| {
        if job.output.content_type != content_type {
            return false;
        }
        job.tasks.retain(|task| {
            task.exchange == exchange
                && task.instrument_type == instrument_type
                && serde_json::to_value(task.market_category).unwrap().as_str()
                    == Some(market_category)
                && task.source_options.as_ref().map(|o| o.feed) == feed
                && task.scope.is_some() == family
        });
        !job.tasks.is_empty()
    });
    subset.jobs.truncate(1);
    let job = subset
        .jobs
        .first_mut()
        .ok_or("prepared batch lacks the required synthetic test profile")?;
    job.tasks.truncate(1);
    Ok(subset)
}
