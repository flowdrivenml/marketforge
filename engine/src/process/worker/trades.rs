use crate::{
    decode::CsvDecoder,
    error::{MarketForgeError, Result},
    formats::trades::{
        IdentityDecision, InstrumentMatcher, TradeContext, TradeProcessor, resolve_trade_headers,
    },
    job::{TargetSchema, WorkTask},
    source::with_source_reader,
};

use super::sink::TradeSink;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradeWorkerMetrics {
    pub records_read: u64,
    pub records_matched: u64,
    pub records_skipped_instrument: u64,

    pub trades_normalized: u64,
    pub trades_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

pub fn process_trade_task<S: TradeSink>(
    task: &WorkTask,
    sink: &mut S,
) -> Result<TradeWorkerMetrics> {
    let normalization = task
        .normalizations
        .iter()
        .find(|normalization| normalization.target_schema == TargetSchema::Trade)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "trade task does not contain trade normalization".to_owned(),
            )
        })?
        .clone();

    let has_headers = task
        .raw_schema
        .get("header")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "trade raw schema requires boolean header field".to_owned(),
            )
        })?;

    let context = TradeContext {
        exchange: task.exchange,
        instrument_id: task.instrument_id,
        symbol: task.symbol.clone(),
        stream_id: task.stream_id.0.clone(),
    };

    let metrics = with_source_reader(
        &task.input_path,
        task.source_compression,
        task.archive_member.as_deref(),
        |reader| {
            let mut decoder = CsvDecoder::with_headers(reader, has_headers);

            let actual_headers = if has_headers {
                Some(decoder.headers().map_err(|error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "failed to read CSV headers: {error}",
                    ))
                })?)
            } else {
                None
            };

            let headers = resolve_trade_headers(&task.raw_schema, actual_headers.as_ref())?;

            let processor =
                TradeProcessor::new(&headers, normalization, task.instrument.clone(), context)?;

            let mut matcher = InstrumentMatcher::from_task(&headers, task)?;

            let mut metrics = TradeWorkerMetrics::default();

            for record in decoder.records() {
                let record = record.map_err(|error| {
                    MarketForgeError::InvalidCanonical(format!(
                        "failed to decode CSV record: {error}",
                    ))
                })?;

                match matcher.check(&record)? {
                    IdentityDecision::Skip => continue,
                    IdentityDecision::Match => {}
                }

                let trade = processor.process_record(&record)?;

                metrics.trades_normalized += 1;

                let timestamp = trade.envelope.event_timestamp_ns;

                metrics.start_timestamp_ns = Some(
                    metrics
                        .start_timestamp_ns
                        .map_or(timestamp, |current| current.min(timestamp)),
                );

                metrics.end_timestamp_ns = Some(
                    metrics
                        .end_timestamp_ns
                        .map_or(timestamp, |current| current.max(timestamp)),
                );

                sink.write_trade(trade)?;

                metrics.trades_written += 1;
            }

            matcher.finish()?;

            let identity = matcher.metrics();

            metrics.records_read = identity.records_read;
            metrics.records_matched = identity.records_matched;
            metrics.records_skipped_instrument = identity.records_skipped_instrument;

            Ok(metrics)
        },
    )?;

    Ok(metrics)
}
