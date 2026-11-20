//! Deterministic complete physical inputs. No rows or manifests are copied from recordings.
use std::{fs, path::Path, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt32Array, UInt64Array, new_null_array,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::json;

use marketforge_engine::{
    historical::{
        job::{MergeParquetConfig, MergeTimeRange, MergeTimelineOrdering, ProcessingJob},
        merge::{
            MergeInputDataset, MergeTimelineFile, PhysicalInitialReconstruction,
            PhysicalMergeManifest, PhysicalReconstructionAvailability, UnifiedMergeMode,
            UnifiedMergeStream, UnifiedMergeStreamKind, physical_causal_schema,
            physical_combined_schema, physical_depth_schema, physical_trade_schema,
        },
        normalize::manifest::file_sha256,
    },
    shared::domain::instrument::InstrumentCatalog,
};

use crate::depth_fixture::{decimal_array, source_batch};

pub const SYMBOLS: [(&str, &str, &str); 4] = [
    ("BTCUSDT", "BTC", "USDT"),
    ("BTCUSDC", "BTC", "USDC"),
    ("ETHUSDT", "ETH", "USDT"),
    ("ETHBTC", "ETH", "BTC"),
];

#[derive(Clone, Copy)]
struct Event {
    symbol: usize,
    depth: bool,
    timestamp: i64,
    price: i128,
}

const fn trade(symbol: usize, timestamp: i64, price: i128) -> Event {
    Event {
        symbol,
        depth: false,
        timestamp,
        price,
    }
}

const fn depth(symbol: usize, timestamp: i64, price: i128) -> Event {
    Event {
        symbol,
        depth: true,
        timestamp,
        price,
    }
}

const EVENTS: [Event; 12] = [
    depth(0, 90, 6),
    trade(0, 100, 6),
    trade(1, 100, 2),
    trade(2, 100, 7),
    trade(3, 105, 2),
    trade(0, 120, 10),
    depth(3, 110, 2),
    trade(1, 121, 4),
    depth(1, 122, 8),
    depth(2, 123, 7),
    depth(3, 124, 0),
    depth(0, 125, 10),
];

pub fn parquet() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 1,
    }
}

pub fn stream_id(symbol: usize, depth: bool) -> String {
    format!(
        "bybit.spot.spot.{}.{}",
        SYMBOLS[symbol].0,
        if depth { "depth" } else { "trades" }
    )
}

pub fn catalog(directory: &Path) -> InstrumentCatalog {
    let path = write_catalog(directory);
    InstrumentCatalog::load(&path).unwrap()
}

pub fn write_catalog(directory: &Path) -> std::path::PathBuf {
    let path = directory.join("instruments.json");
    let instruments = SYMBOLS.map(|(symbol, base, quote)| {
        json!({
            "exchange": "bybit", "symbol": symbol, "instrument_kind": "spot",
            "contract_kind": null, "base_currency": base, "quote_currency": quote,
        })
    });
    fs::write(
        &path,
        serde_json::to_vec(&json!({"version": 1, "instruments": instruments})).unwrap(),
    )
    .unwrap();
    path
}

fn rank(event: Event, mode: UnifiedMergeMode) -> u32 {
    if mode == UnifiedMergeMode::Combined {
        event.symbol as u32 * 2 + u32::from(event.depth)
    } else {
        event.symbol as u32
    }
}

pub fn create(directory: &Path, mode: UnifiedMergeMode, causal: bool) -> PhysicalMergeManifest {
    create_with_omitted_trades(directory, mode, causal, &[])
}

pub fn create_with_omitted_trades(
    directory: &Path,
    mode: UnifiedMergeMode,
    causal: bool,
    omitted_symbols: &[usize],
) -> PhysicalMergeManifest {
    let mut events = EVENTS
        .into_iter()
        .filter(|event| event.depth || !omitted_symbols.contains(&event.symbol))
        .filter(|event| match mode {
            UnifiedMergeMode::Trades => !event.depth,
            UnifiedMergeMode::Depth => event.depth,
            UnifiedMergeMode::Combined => true,
        })
        .collect::<Vec<_>>();
    if !causal {
        events.sort_by_key(|event| (event.timestamp, rank(*event, mode)));
    }
    let start_ns = if causal { 100 } else { 90 };
    let end_ns = 140;
    let path = "timeline/partition-000000/part-000000.parquet";
    let file = directory.join(path);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    let mut streams = Vec::new();
    let mut inputs = Vec::new();
    for is_depth in [false, true] {
        let dataset_id = if is_depth { 102 } else { 101 };
        let selected = events
            .iter()
            .copied()
            .filter(|event| event.depth == is_depth)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            continue;
        }
        let data = directory.join(format!("normalized-{dataset_id}"));
        fs::create_dir(&data).unwrap();
        let mut tasks = Vec::new();
        let mut declared = Vec::new();
        for (symbol, &(symbol_name, _, _)) in SYMBOLS.iter().enumerate() {
            let Some(event) = selected
                .iter()
                .copied()
                .find(|event| event.symbol == symbol)
            else {
                continue;
            };
            let id = stream_id(symbol, is_depth);
            let instrument_id = symbol as i64 + 1;
            streams.push(UnifiedMergeStream {
                stream_rank: rank(event, mode),
                dataset_id,
                kind: if is_depth {
                    UnifiedMergeStreamKind::Depth
                } else {
                    UnifiedMergeStreamKind::Trade
                },
                exchange: "bybit".into(),
                symbol: symbol_name.into(),
                instrument_id,
                stream_id: id.clone(),
                base_currency: None,
                quote_currency: None,
            });
            declared.push(json!({"stream_id": id, "stream_rank": symbol}));
            let raw = data.join(format!("{symbol_name}.csv"));
            fs::write(&raw, format!("{symbol_name}\n")).unwrap();
            for task_id in [symbol * 2 + 1, symbol * 2 + 2] {
                tasks.push(json!({
                    "task_id": task_id, "stream_id": id, "input_path": raw,
                    "format_code": "CONVERSION-TEST", "exchange": "bybit",
                    "instrument_id": instrument_id, "symbol": symbol_name, "raw_schema": {},
                    "instrument": {"instrument_kind": "spot", "tick_size": "0.1"},
                    "normalizations": [{"timestamp_encoding": "nanoseconds", "quantity_encoding": "base",
                        "target_schema": if is_depth { "depth" } else { "trade" }, "rules": {}}],
                    "source_ordering": "source_ordered", "source_compression": "plain",
                }));
            }
        }
        let rule = json!({"action": "fail"});
        let job: ProcessingJob = serde_json::from_value(json!({
            "protocol_version": 1, "job_id": dataset_id + 1000, "dataset_id": dataset_id,
            "operation": "process", "tasks": tasks, "streams": declared,
            "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
            "integrity_policy": {"enabled": true, "parse_failure": rule, "invalid_record": rule,
                "sequence_gap": rule, "timestamp_regression": rule, "missing_snapshot": rule,
                "invalid_book": rule, "transformation_failure": rule},
            "resources": {"workers": 1, "memory_budget_bytes": 268435456, "scratch_path": data.join("scratch"),
                "scratch_budget_bytes": 268435456,
                "parquet": {"row_group_target_bytes": 1048576, "file_target_bytes": 4194304}},
            "output": {"content_type": if is_depth {"depth"} else {"trades"},
                "staging_path": data.with_extension("staging"), "dataset_path": data}
        })).unwrap();
        let snapshot = data.join("processing-job.json");
        fs::write(&snapshot, serde_json::to_vec(&job).unwrap()).unwrap();
        fs::write(
            data.join("processing-job.sha256"),
            format!("{}  processing-job.json\n", file_sha256(&snapshot).unwrap()),
        )
        .unwrap();
        inputs.push(MergeInputDataset {
            dataset_id,
            dataset_path: data.to_str().unwrap().into(),
            manifest_version: 3,
        });
    }
    streams.sort_by_key(|stream| stream.stream_rank);

    let schema = match mode {
        UnifiedMergeMode::Combined => physical_combined_schema(),
        UnifiedMergeMode::Trades => physical_trade_schema(),
        UnifiedMergeMode::Depth => physical_depth_schema(),
    };
    let schema = if causal {
        physical_causal_schema(&schema)
    } else {
        schema
    };
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&file).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    let mut canonical_rows = 0;
    let mut offsets = [0; 8];
    let mut ordinals = [0; 8];
    for (sequence, event) in events.iter().copied().enumerate() {
        let stream_rank = rank(event, mode);
        let index = stream_rank as usize;
        let count = u64::from(!event.depth || event.price > 0);
        let dataset_id = if event.depth { 102 } else { 101 };
        let template = source_batch();
        let changes = template
            .column_by_name("changes")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let changes = if count == 0 {
            changes.slice(1, 1)
        } else {
            let levels = changes.value(2);
            let levels = levels.as_any().downcast_ref::<StructArray>().unwrap();
            let mut values = levels.columns().to_vec();
            values[1] = decimal_array(&[Some(event.price)]);
            values[2] = decimal_array(&[Some(1)]);
            values[3] = decimal_array(&[Some(event.price)]);
            let values = StructArray::new(levels.fields().clone(), values, None);
            ListArray::new(
                match changes.data_type() {
                    arrow_schema::DataType::List(field) => field.clone(),
                    _ => unreachable!(),
                },
                arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
                Arc::new(values),
                None,
            )
        };
        let changes: ArrayRef = Arc::new(changes);
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let trade_field = |name| -> Option<ArrayRef> {
                    match name {
                        "side" => Some(Arc::new(StringArray::from(vec!["buy"]))),
                        "price" => Some(decimal_array(&[Some(event.price)])),
                        "quantity_base" => Some(decimal_array(&[Some(1)])),
                        "quantity_quote" => Some(decimal_array(&[Some(event.price)])),
                        _ => None,
                    }
                };
                let depth_field = |name| -> Option<ArrayRef> {
                    match name {
                        "changes" => Some(changes.clone()),
                        "event_boundary" | "source_operation" => Some(Arc::new(UInt8Array::from(
                            vec![u8::from(ordinals[index] > 0)],
                        ))),
                        _ => None,
                    }
                };
                match field.name().as_str() {
                    "event_timestamp_ns" => {
                        Arc::new(Int64Array::from(vec![event.timestamp])) as ArrayRef
                    }
                    "exchange" => Arc::new(StringArray::from(vec!["bybit"])),
                    "instrument_id" => Arc::new(Int64Array::from(vec![event.symbol as i64 + 1])),
                    "symbol" => Arc::new(StringArray::from(vec![SYMBOLS[event.symbol].0])),
                    "stream_id" => Arc::new(StringArray::from(vec![stream_id(
                        event.symbol,
                        event.depth,
                    )])),
                    "event_kind" => Arc::new(UInt8Array::from(vec![u8::from(event.depth)])),
                    "stream_rank" => Arc::new(UInt32Array::from(vec![stream_rank])),
                    "dataset_id" => Arc::new(UInt64Array::from(vec![dataset_id])),
                    "task_id" => Arc::new(UInt64Array::from(vec![event.symbol as u64 * 2 + 1])),
                    "event_ordinal" => Arc::new(UInt64Array::from(vec![ordinals[index]])),
                    "canonical_row_offset" => Arc::new(UInt64Array::from(vec![offsets[index]])),
                    "canonical_row_count" => Arc::new(UInt64Array::from(vec![count])),
                    "timeline_sequence" => Arc::new(UInt64Array::from(vec![sequence as u64])),
                    "reconstruction_only" => {
                        Arc::new(BooleanArray::from(vec![event.timestamp < start_ns]))
                    }
                    name @ ("trade" | "depth") if (name == "depth") == event.depth => {
                        let arrow_schema::DataType::Struct(fields) = field.data_type() else {
                            unreachable!()
                        };
                        let children = fields
                            .iter()
                            .map(|child| {
                                let supplied = if event.depth {
                                    depth_field(child.name())
                                } else {
                                    trade_field(child.name())
                                };
                                supplied.unwrap_or_else(|| {
                                    assert!(child.is_nullable(), "{}", child.name());
                                    new_null_array(child.data_type(), 1)
                                })
                            })
                            .collect();
                        Arc::new(StructArray::try_new(fields.clone(), children, None).unwrap())
                    }
                    name => {
                        let supplied = if mode == UnifiedMergeMode::Trades {
                            trade_field(name)
                        } else if mode == UnifiedMergeMode::Depth {
                            depth_field(name)
                        } else {
                            None
                        };
                        supplied.unwrap_or_else(|| {
                            assert!(field.is_nullable(), "{name}");
                            new_null_array(field.data_type(), 1)
                        })
                    }
                }
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
        ordinals[index] += 1;
        offsets[index] += count;
        canonical_rows += count;
    }
    let footer = writer.close().unwrap();
    let rows = events.len() as u64;
    let trade_events = events.iter().filter(|event| !event.depth).count() as u64;
    let first = events.iter().map(|event| event.timestamp).min();
    let last = events.iter().map(|event| event.timestamp).max();
    let manifest = PhysicalMergeManifest {
        protocol_version: 1,
        physical_schema_version: if causal { 2 } else { 1 },
        timeline_ordering: if causal {
            MergeTimelineOrdering::Causal
        } else {
            MergeTimelineOrdering::Chronological
        },
        reconstruction_events: events
            .iter()
            .filter(|event| event.timestamp < start_ns)
            .count() as u64,
        job_id: 901,
        mode,
        input_datasets: inputs,
        time_range: MergeTimeRange { start_ns, end_ns },
        streams,
        events_written: rows,
        trade_events,
        depth_events: rows - trade_events,
        canonical_rows,
        files_written: 1,
        row_groups: footer.num_row_groups() as u64,
        start_timestamp_ns: first,
        end_timestamp_ns: last,
        files: vec![MergeTimelineFile {
            path: path.into(),
            size_bytes: fs::metadata(&file).unwrap().len(),
            partition_id: Some(0),
            rows: Some(rows),
            start_timestamp_ns: first,
            end_timestamp_ns: last,
        }],
        boundary_links: vec![],
        skipped_initializations: vec![],
        checkpoints: vec![],
        initial_reconstruction: if !causal && mode != UnifiedMergeMode::Trades {
            vec![PhysicalInitialReconstruction {
                dataset_id: 102,
                availability: PhysicalReconstructionAvailability::Unavailable,
                first_physical_checkpoint_ns: None,
                prior_canonical_checkpoint_ns: None,
            }]
        } else {
            vec![]
        },
    };
    manifest.validate().unwrap();
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    manifest
}
