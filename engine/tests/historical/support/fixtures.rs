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
