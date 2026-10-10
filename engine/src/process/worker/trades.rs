use crate::{
    decode::CsvDecoder,
    error::{MarketForgeError, Result},
    formats::trades::{
        IdentityDecision, InstrumentMatcher, TradeContext, TradeProcessor, resolve_trade_headers,
    },
    source::with_source_reader,
};

use super::sink::TradeSink;
use crate::{
    job::{IntegrityPolicy, TargetSchema, WorkTask},
    process::metrics::{
        IntegrityCategory, IntegrityDiagnostic, IntegrityScope, ScopedIntegrityMetrics,
        TaskMetrics, enforce_immediate_integrity,
    },
};

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

pub fn process_trade_task_with_metrics<S: TradeSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
) -> Result<()> {
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

    let scope = IntegrityScope {
        task_id: task.task_id.0,
        stream_id: task.stream_id.0.clone(),
        source_file: task.input_path.to_string_lossy().into_owned(),
    };

    let result = with_source_reader(
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

            for (index, record) in decoder.records().enumerate() {
                let source_record = u64::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "source record index overflow".to_owned(),
                        )
                    })?;

                // ---------------------------------------------------------
                // Decode source record
                // ---------------------------------------------------------

                let record = match record {
                    Ok(record) => record,

                    Err(error) => {
                        // Source I/O and decompression failures are always fatal.
                        if error.is_io_error() {
                            return Err(MarketForgeError::InvalidCanonical(format!(
                                "source CSV I/O failure: {error}"
                            )));
                        }

                        increment(&mut metrics.counters.records_read, "records_read")?;

                        record_trade_integrity_error(
                            task,
                            &scope,
                            metrics,
                            IntegrityCategory::ParseFailure,
                            source_record,
                            error.to_string(),
                        )?;

                        increment(
                            &mut metrics.counters.records_rejected_unmatched,
                            "records_rejected_unmatched",
                        )?;

                        enforce_trade_integrity(
                            policy,
                            metrics,
                            previous_job_integrity,
                            IntegrityCategory::ParseFailure,
                            &format!("CSV parse failure at record {source_record}: {error}"),
                        )?;

                        continue;
                    }
                };

                increment(&mut metrics.counters.records_read, "records_read")?;

                // ---------------------------------------------------------
                // Instrument identity
                // ---------------------------------------------------------

                match matcher.check(&record)? {
                    IdentityDecision::Skip => {
                        increment(
                            &mut metrics.counters.records_skipped_instrument,
                            "records_skipped_instrument",
                        )?;

                        continue;
                    }

                    IdentityDecision::Match => {
                        increment(&mut metrics.counters.records_matched, "records_matched")?;
                    }
                }

                // ---------------------------------------------------------
                // Normalize trade
                // ---------------------------------------------------------

                let trade = match processor.process_record(&record) {
                    Ok(trade) => trade,

                    Err(MarketForgeError::RecordIntegrity { category, message }) => {
                        record_trade_integrity_error(
                            task,
                            &scope,
                            metrics,
                            category,
                            source_record,
                            message.clone(),
                        )?;

                        enforce_trade_integrity(
                            policy,
                            metrics,
                            previous_job_integrity,
                            category,
                            &format!("record {source_record}: {message}"),
                        )?;

                        // Degradable record: skip and continue.
                        continue;
                    }

                    Err(error) => return Err(error),
                };

                increment(&mut metrics.counters.events_normalized, "events_normalized")?;

                increment(&mut metrics.counters.records_processed, "records_processed")?;

                let timestamp = trade.envelope.event_timestamp_ns;

                metrics.record_timestamp(timestamp);

                // ---------------------------------------------------------
                // Record successful integrity observations
                // ---------------------------------------------------------

                for category in [
                    IntegrityCategory::ParseFailure,
                    IntegrityCategory::InvalidRecord,
                    IntegrityCategory::TransformationFailure,
                ] {
                    metrics
                        .scoped_integrity
                        .record(&scope, category, false, Some(timestamp))?;
                }

                // ---------------------------------------------------------
                // Write canonical trade
                // ---------------------------------------------------------

                sink.write_trade(trade)?;

                increment(&mut metrics.counters.events_written, "events_written")?;
            }

            matcher.finish()?;

            Ok(())
        },
    );

    match result {
        Ok(()) => {
            metrics.counters.tasks_completed += 1;

            metrics.counters.validate()?;

            Ok(())
        }

        Err(error) => {
            metrics.counters.tasks_failed += 1;

            Err(error)
        }
    }
}

fn increment(counter: &mut u64, name: &str) -> Result<()> {
    *counter = counter.checked_add(1).ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!("processing counter overflow: {name}"))
    })?;

    Ok(())
}

fn record_trade_integrity_error(
    task: &WorkTask,
    scope: &IntegrityScope,
    metrics: &mut TaskMetrics,
    category: IntegrityCategory,
    source_record: u64,
    message: String,
) -> Result<()> {
    // Count every rejected record.
    increment(&mut metrics.counters.records_rejected, "records_rejected")?;

    metrics.integrity.record(IntegrityDiagnostic {
        category,
        task_id: task.task_id.0,
        source_record: Some(source_record),
        event_timestamp_ns: None,
        message,
    })?;

    // Record the integrity violation.
    metrics
        .scoped_integrity
        .record(scope, category, true, None)?;

    Ok(())
}

fn enforce_trade_integrity(
    policy: &IntegrityPolicy,
    metrics: &TaskMetrics,
    previous_job_integrity: &ScopedIntegrityMetrics,
    category: IntegrityCategory,
    message: &str,
) -> Result<()> {
    let mut combined = previous_job_integrity.clone();

    combined.merge(&metrics.scoped_integrity)?;

    let evaluation = enforce_immediate_integrity(policy, &combined, category)?;

    if evaluation.is_failed() {
        return Err(MarketForgeError::RecordIntegrity {
            category,
            message: format!("immediate integrity policy violation: {message}"),
        });
    }

    Ok(())
}
