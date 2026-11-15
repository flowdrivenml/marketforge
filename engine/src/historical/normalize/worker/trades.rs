use super::sink::TradeSink;
use crate::historical::normalize::progress::WorkerProgress;
use crate::{
    historical::normalize::worker::decoded::with_source_reader,
    shared::error::{MarketForgeError, Result},
    shared::formats::trades::{
        IdentityDecision, InstrumentMatcher, TradeContext, TradeProcessor, resolve_trade_headers,
    },
    shared::io::decode::CsvDecoder,
};
use {
    crate::historical::job::IntegrityPolicy, crate::historical::job::WorkTask,
    crate::historical::normalize::metrics::IntegrityCategory,
    crate::historical::normalize::metrics::IntegrityDiagnostic,
    crate::historical::normalize::metrics::IntegrityScope,
    crate::historical::normalize::metrics::ScopedIntegrityMetrics,
    crate::historical::normalize::metrics::TaskMetrics,
    crate::historical::normalize::metrics::enforce_immediate_integrity,
    crate::shared::normalization::TargetSchema,
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
    let _limits = super::resource_scope(task);
    super::decoded::ensure_routed(task)?;
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
                Some(
                    decoder
                        .headers()
                        .map_err(|error| super::integrity::csv_source_error(task, error))?,
                )
            } else {
                None
            };

            let headers = resolve_trade_headers(&task.raw_schema, actual_headers.as_ref())?;

            let processor =
                TradeProcessor::new(&headers, normalization, task.instrument.clone(), context)?;

            let mut matcher = InstrumentMatcher::from_task(&headers, task)?;
            super::ordering::supported(task)?;
            let mut previous_timestamp = None;

            let mut metrics = TradeWorkerMetrics::default();

            for record in decoder.records() {
                let record =
                    record.map_err(|error| super::integrity::csv_source_error(task, error))?;

                match matcher.check(&record)? {
                    IdentityDecision::Skip => continue,
                    IdentityDecision::Match => {}
                }

                let trade = processor.process_record(&record)?;
                let timestamp = trade.envelope.event_timestamp_ns;
                if super::ordering::regresses(previous_timestamp, timestamp) {
                    return Err(MarketForgeError::RecordIntegrity {
                        category: IntegrityCategory::TimestampRegression,
                        message: format!(
                            "stream {} timestamp regressed at record {}",
                            task.stream_id.0, metrics.records_read
                        ),
                    });
                }

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
                previous_timestamp = Some(timestamp);

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
    let _limits = super::resource_scope(task);
    process_trade_task_with_metrics_impl(task, sink, metrics, policy, previous_job_integrity, None)
}

pub fn process_trade_task_with_metrics_and_progress<S: TradeSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    progress: &mut WorkerProgress,
) -> Result<()> {
    let _limits = super::resource_scope(task);
    process_trade_task_with_metrics_impl(
        task,
        sink,
        metrics,
        policy,
        previous_job_integrity,
        Some(progress),
    )
}

fn process_trade_task_with_metrics_impl<S: TradeSink>(
    task: &WorkTask,
    sink: &mut S,
    metrics: &mut TaskMetrics,
    policy: &IntegrityPolicy,
    previous_job_integrity: &ScopedIntegrityMetrics,
    mut progress: Option<&mut WorkerProgress>,
) -> Result<()> {
    let _limits = super::resource_scope(task);
    super::decoded::ensure_routed(task)?;
    super::begin_observation(task, metrics);
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

            let cached = super::decoded::take_csv();
            let actual_headers = if let Some((headers, _)) = &cached {
                headers.clone()
            } else if has_headers {
                Some(
                    decoder
                        .headers()
                        .map_err(|error| super::integrity::csv_source_error(task, error))?,
                )
            } else {
                None
            };

            let headers = resolve_trade_headers(&task.raw_schema, actual_headers.as_ref())?;

            let processor =
                TradeProcessor::new(&headers, normalization, task.instrument.clone(), context)?;

            let mut matcher = InstrumentMatcher::from_task(&headers, task)?;
            let mut ordering =
                super::ordering::TimestampOrder::new(task, &scope, policy, previous_job_integrity)?;

            let rows: Box<
                dyn Iterator<
                        Item = (
                            Option<u64>,
                            std::result::Result<csv::ByteRecord, csv::Error>,
                        ),
                    > + '_,
            > = if let Some((_, rows)) = cached {
                Box::new(rows.map(|row| match row {
                    Ok((ordinal, row)) => (Some(ordinal), Ok(row)),
                    Err(e) => (None, Err(e)),
                }))
            } else {
                Box::new(decoder.records().map(|row| (None, row)))
            };
            for (index, (physical_ordinal, record)) in rows.enumerate() {
                if let Some(reporter) = progress.as_deref_mut() {
                    reporter.maybe_update(&metrics.counters);
                }
                let source_record = physical_ordinal.map(Ok).unwrap_or_else(|| {
                    u64::try_from(index)
                        .ok()
                        .and_then(|value| value.checked_add(1))
                        .ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(
                                "source record index overflow".to_owned(),
                            )
                        })
                })?;

                // ---------------------------------------------------------
                // Decode source record
                // ---------------------------------------------------------

                let record = match record {
                    Ok(record) => record,

                    Err(error) => {
                        // Source I/O and decompression failures are always fatal.
                        if error.is_io_error() {
                            return Err(super::integrity::csv_source_error(task, error));
                        }

                        increment(&mut metrics.counters.records_read, "records_read")?;

                        record_trade_integrity_error(
                            task,
                            &scope,
                            metrics,
                            IntegrityCategory::ParseFailure,
                            source_record,
                            None,
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

                let mut trace = crate::shared::validation::IntegrityObservationTrace::default();
                trace.passed(IntegrityCategory::ParseFailure);

                // ---------------------------------------------------------
                // Instrument identity
                // ---------------------------------------------------------

                let identity = matcher.check(&record);
                if let Err(error) = identity.as_ref() {
                    trace.event_timestamp_ns = processor.event_timestamp(&record).ok();
                    let mut file_scope = scope.clone();
                    file_scope.stream_id.clear();
                    super::integrity::record_trace(metrics, &file_scope, &trace, None)?;
                    return Err(MarketForgeError::InvalidConfiguration(error.to_string()));
                }
                match identity? {
                    IdentityDecision::Skip => {
                        trace.event_timestamp_ns = processor.event_timestamp(&record).ok();
                        let mut file_scope = scope.clone();
                        file_scope.stream_id.clear();
                        super::integrity::record_trace(metrics, &file_scope, &trace, None)?;
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

                let result = processor.process_record_observed(&record, &mut trace);
                super::integrity::record_trace(metrics, &scope, &trace, result.as_ref().err())?;
                let trade = match result {
                    Ok(trade) => trade,

                    Err(MarketForgeError::RecordIntegrity { category, message }) => {
                        record_trade_integrity_error(
                            task,
                            &scope,
                            metrics,
                            category,
                            source_record,
                            trace.event_timestamp_ns,
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

                let timestamp = trade.envelope.event_timestamp_ns;
                if let Err(error) = ordering.check(metrics, source_record, timestamp) {
                    increment(&mut metrics.counters.records_rejected, "records_rejected")?;
                    return Err(error);
                }
                increment(&mut metrics.counters.events_normalized, "events_normalized")?;

                increment(&mut metrics.counters.records_processed, "records_processed")?;

                let timestamp = trade.envelope.event_timestamp_ns;

                metrics.record_timestamp(timestamp);

                // ---------------------------------------------------------
                // Write canonical trade
                // ---------------------------------------------------------

                sink.write_trade_at(trade, source_record)?;
                if let Some(source) = metrics.source.as_mut() {
                    source.observe(timestamp);
                }
                ordering.accept(timestamp);

                increment(&mut metrics.counters.events_written, "events_written")?;
            }

            matcher.finish()?;

            Ok(())
        },
    );

    let outcome = match result {
        Ok(()) => {
            metrics.counters.tasks_completed += 1;

            metrics.counters.validate()?;

            Ok(())
        }

        Err(error) => {
            metrics.counters.tasks_failed += 1;

            Err(error)
        }
    };

    // Publish the final counters, including partial progress on failure.
    if let Some(reporter) = progress {
        reporter.finish(&metrics.counters);
    }

    super::finish_observation(metrics);
    outcome
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
    event_timestamp_ns: Option<i64>,
    message: String,
) -> Result<()> {
    // Count every rejected record.
    increment(&mut metrics.counters.records_rejected, "records_rejected")?;

    metrics.integrity.record(IntegrityDiagnostic {
        category,
        task_id: task.task_id.0,
        source_record: Some(source_record),
        event_timestamp_ns,
        message,
    })?;

    // Record the integrity violation.
    metrics
        .scoped_integrity
        .record(scope, category, true, event_timestamp_ns)?;

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
