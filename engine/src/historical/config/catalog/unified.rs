//! Schema-2 self-contained profiles compile into the established processor specs.
use super::*;
use serde_json::json;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Unified {
    schema_version: u32,
    kind: String,
    id: String,
    format_code: String,
    exchange: Exchange,
    instrument_type: InstrumentKind,
    market_category: MarketCategory,
    source: Source,
    fields: BTreeMap<String, Value>,
    normalization: Value,
    unsupported_reason: Option<String>,
    #[serde(default)]
    feeds: BTreeMap<String, FeedDefinition>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedDefinition {
    source: Source,
    fields: BTreeMap<String, Value>,
    normalization: Value,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Source {
    container: SourceContainer,
    format: PhysicalRecord,
    record_format: RecordFormat,
    header: Option<bool>,
    ordering: String,
    granularity: String,
    archive_day_boundary: Option<String>,
    daily_packaging: Option<String>,
    maximum_depth_per_side: Option<u64>,
    workbook_sheets: Option<u64>,
}

pub(super) fn compile(
    text: &str,
    path: &Path,
    data_type: &str,
) -> Result<(Profile, PhysicalFormat)> {
    let wire: Unified = toml::from_str(text).map_err(|e| invalid(e.to_string()))?;
    compile_wire(wire, path, data_type)
}
fn compile_wire(wire: Unified, path: &Path, data_type: &str) -> Result<(Profile, PhysicalFormat)> {
    if wire.schema_version != 2 || wire.kind != "profile" {
        return Err(invalid(
            "expected profile schema_version=2 and kind=profile",
        ));
    }
    if path.file_stem().and_then(|s| s.to_str()) != Some(&wire.id) {
        return Err(invalid(format!(
            "profile filename must match ID {}",
            wire.id
        )));
    }
    if !matches!(
        wire.source.granularity.as_str(),
        "daily" | "hourly" | "monthly"
    ) || wire
        .source
        .archive_day_boundary
        .as_deref()
        .is_some_and(|s| !matches!(s, "UTC" | "UTC+8"))
        || wire
            .source
            .daily_packaging
            .as_deref()
            .is_some_and(|s| s != "numbered_zip_chunks")
        || wire.source.maximum_depth_per_side == Some(0)
    {
        return Err(invalid("unsupported source packaging metadata"));
    }
    let ordering = match wire.source.ordering.as_str() {
        "chronological" => SourceOrdering::SourceOrdered,
        "timestamp_sortable" | "not_chronological" => SourceOrdering::TimestampSortable,
        _ => return Err(invalid("unsupported source.ordering")),
    };
    if matches!(
        wire.source.format,
        PhysicalRecord::Csv | PhysicalRecord::Xlsx
    ) && wire.source.header.is_none()
    {
        return Err(invalid("source.header is required for CSV/XLSX"));
    }
    let mut compiled_feeds = serde_json::Map::new();
    for (feed, definition) in &wire.feeds {
        if wire.exchange != Exchange::GateIo
            || data_type != "depth"
            || !matches!(feed.as_str(), "snapshot" | "incremental")
        {
            return Err(invalid("unsupported profile feed"));
        }
        let (profile, physical) = compile_wire(
            Unified {
                schema_version: 2,
                kind: "profile".into(),
                id: wire.id.clone(),
                format_code: wire.format_code.clone(),
                exchange: wire.exchange,
                instrument_type: wire.instrument_type,
                market_category: wire.market_category,
                source: definition.source.clone(),
                fields: definition.fields.clone(),
                normalization: definition.normalization.clone(),
                unsupported_reason: wire.unsupported_reason.clone(),
                feeds: BTreeMap::new(),
            },
            path,
            data_type,
        )?;
        let normalizations: Vec<_> = profile
            .rules
            .into_iter()
            .map(|r| NormalizationConfig {
                timestamp_encoding: r.timestamp_encoding.unwrap(),
                quantity_encoding: r.quantity_encoding.unwrap(),
                target_schema: TargetSchema::Depth,
                rules: r.rules,
            })
            .collect();
        for n in &normalizations {
            DepthSpec::from_normalization(n)?;
        }
        compiled_feeds.insert(feed.clone(),json!({"physical":physical,"normalizations":normalizations,"ordering":profile.source_ordering}));
    }
    let mut fields = vec![];
    for (name, definition) in &wire.fields {
        let mut field = definition
            .as_object()
            .cloned()
            .ok_or_else(|| invalid("field definition must be a table"))?;
        for key in field.keys() {
            if ![
                "type",
                "encoding",
                "position",
                "precision",
                "fractional_digits",
                "observed_effective_precision",
                "values",
                "level_format",
                "ordering",
                "decode",
                "update_precision",
                "snapshot_precision",
                "level_fields",
            ]
            .contains(&key.as_str())
            {
                return Err(invalid(format!("unknown field metadata {name}.{key}")));
            }
        }
        if field.get("decode").is_some_and(|v| v != "json") {
            return Err(invalid("unsupported field decode"));
        }
        if let Some(levels) = field.get("level_format") {
            if levels != &json!(["price", "quantity"])
                && levels != &json!(["price", "quantity", "order_count"])
            {
                return Err(invalid("unsupported level_format"));
            }
        }
        field.insert("name".into(), Value::String(name.clone()));
        fields.push(Value::Object(field));
    }
    fields.sort_by_key(|v| v["position"].as_u64().unwrap_or(u64::MAX));
    let mut raw = json!({"fields":fields});
    if !compiled_feeds.is_empty() {
        raw["marketforge_feeds"] = Value::Object(compiled_feeds);
    }
    if let Some(header) = wire.source.header {
        raw["header"] = header.into();
    }
    if let Some(sheets) = wire.source.workbook_sheets {
        raw["workbook_sheets"] = sheets.into();
    }
    if matches!(wire.source.format, PhysicalRecord::Xlsx) {
        if wire.source.workbook_sheets != Some(1)
            || wire.source.header != Some(true)
            || !matches!(ordering, SourceOrdering::TimestampSortable)
            || wire.fields.keys().map(String::as_str).collect::<Vec<_>>()
                != ["asks", "bids", "timestamp"]
            || wire.fields["timestamp"]["encoding"] != "unix_seconds"
            || wire.normalization["input_ordering"]
                != json!({"order":"ascending","sort_by":"timestamp"})
            || wire.normalization["event_timestamp"]["field"] != "timestamp"
            || wire.normalization["asks"]["field"] != "asks"
            || wire.normalization["bids"]["field"] != "bids"
        {
            return Err(invalid(
                "XLSX requires one header worksheet, timestamp/asks/bids columns, seconds and ascending timestamp sorting",
            ));
        }
    } else if wire.source.workbook_sheets.is_some() {
        return Err(invalid("workbook_sheets requires XLSX"));
    }
    let mut common = wire
        .normalization
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("normalization must be a table"))?;
    let quantity: QuantityEncoding = serde_json::from_value(
        common
            .remove("quantity_encoding")
            .ok_or_else(|| invalid("missing quantity_encoding"))?,
    )
    .map_err(|e| invalid(e.to_string()))?;
    let target = common.remove("target");
    let variants = common.remove("rules");
    let book = common.remove("book");
    validate_book(book.as_ref(), &common)?;
    let variants = match (target, variants) {
        (Some(target), None) => vec![json!({"target":target})],
        (None, Some(Value::Array(rules))) if !rules.is_empty() => rules,
        _ => return Err(invalid("normalization requires target or nonempty rules")),
    };
    let mut rules = vec![];
    for variant in variants {
        let mut v = variant
            .as_object()
            .cloned()
            .ok_or_else(|| invalid("rule must be a table"))?;
        let target: RuleTarget = serde_json::from_value(
            v.remove("target")
                .ok_or_else(|| invalid("missing rule target"))?,
        )
        .map_err(|e| invalid(e.to_string()))?;
        let mut effective = common.clone();
        for (key, value) in v {
            if effective.insert(key.clone(), value).is_some() {
                return Err(invalid(format!("conflicting normalization rule {key}")));
            }
        }
        let timestamp = effective
            .get("event_timestamp")
            .and_then(|v| v["field"].as_str())
            .ok_or_else(|| invalid("missing event_timestamp.field"))?;
        let encoding = match wire
            .fields
            .get(timestamp)
            .and_then(|v| v["encoding"].as_str())
        {
            Some("unix_seconds") => TimestampEncoding::Seconds,
            Some("unix_milliseconds") => TimestampEncoding::Milliseconds,
            Some("unix_microseconds") => TimestampEncoding::Microseconds,
            Some("unix_nanoseconds") => TimestampEncoding::Nanoseconds,
            _ => return Err(invalid("unsupported or missing timestamp field encoding")),
        };
        let mut effective = Value::Object(effective);
        translate(&mut effective, &wire.fields)?;
        rules.push(Rule {
            target,
            timestamp_encoding: Some(encoding),
            quantity_encoding: Some(quantity),
            rules: effective,
        });
    }
    let profile = Profile {
        schema_version: 1,
        id: wire.id.clone(),
        physical: wire.id.clone(),
        format_code: wire.format_code,
        exchange: wire.exchange,
        instrument_type: wire.instrument_type,
        market_category: wire.market_category,
        source_ordering: Some(ordering),
        unsupported_reason: wire.unsupported_reason,
        rules,
    };
    let physical = PhysicalFormat {
        schema_version: 1,
        id: wire.id,
        container_format: wire.source.format,
        record_format: wire.source.record_format,
        supported_containers: vec![wire.source.container],
        raw_schema: raw,
    };
    validate_physical(&physical)?;
    if (data_type == "trades") != matches!(physical.record_format, RecordFormat::Rows) {
        return Err(invalid("record format/data type mismatch"));
    }
    Ok((profile, physical))
}
fn translate(value: &mut Value, fields: &BTreeMap<String, Value>) -> Result<()> {
    match value {
        Value::Object(map) => {
            if let Some(field) = map.remove("field") {
                let name = field
                    .as_str()
                    .ok_or_else(|| invalid("field reference must be a string"))?;
                let definition = fields
                    .get(name)
                    .ok_or_else(|| invalid(format!("unknown source field {name}")))?;
                if let Some(levels) = definition["level_format"].as_array() {
                    let index = |label| {
                        levels
                            .iter()
                            .position(|v| v.as_str() == Some(label))
                            .map(|i| json!(i))
                            .ok_or_else(|| invalid("level_format requires price and quantity"))
                    };
                    map.insert("price_index".into(), index("price")?);
                    map.insert("quantity_index".into(), index("quantity")?);
                    if let Some(i) = levels.iter().position(|v| v == "order_count") {
                        map.insert("order_count_index".into(), json!(i));
                    }
                }
                if let Some(object) = definition["level_fields"].as_object() {
                    if object.len() != 2 {
                        return Err(invalid("level_fields requires price and quantity"));
                    }
                    map.insert(
                        "price_field".into(),
                        object
                            .get("price")
                            .cloned()
                            .ok_or_else(|| invalid("missing object price field"))?,
                    );
                    map.insert(
                        "quantity_field".into(),
                        object
                            .get("quantity")
                            .cloned()
                            .ok_or_else(|| invalid("missing object quantity field"))?,
                    );
                }
                map.insert("source".into(), field);
                // XLSX decoder already decodes JSON cells; JSON/CSV processors handle arrays.
            }
            for (key, v) in map.iter_mut() {
                if key != "values" {
                    translate(v, fields)?;
                }
            }
        }
        Value::Array(array) => {
            for v in array {
                translate(v, fields)?;
            }
        }
        _ => (),
    }
    Ok(())
}
fn validate_book(book: Option<&Value>, common: &serde_json::Map<String, Value>) -> Result<()> {
    let Some(book) = book else {
        return Ok(());
    };
    let map = book
        .as_object()
        .ok_or_else(|| invalid("book metadata must be a table"))?;
    for (key, value) in map {
        match key.as_str() {
            "update_id" | "cross_sequence" if value.is_object() => {
                if key == "update_id"
                    && value != &common.get("sequence_start").cloned().unwrap_or(Value::Null)
                {
                    return Err(invalid("book.update_id must match sequence_start"));
                }
            }
            "update_id" | "sequence_id" if value == false => (),
            "snapshot_resets_book" if value == true => (),
            "snapshot_depth" | "update_frequency" | "snapshot_frequency" => (), // descriptive source facts, not truncation controls
            "sequence"
                if value["continuity_rule"]
                    == "next.begin_id = current.begin_id + current.merged_count"
                    && value["begin_id"]
                        == common.get("sequence_start").cloned().unwrap_or(Value::Null)
                    && value["merged_count"]
                        == common.get("sequence_count").cloned().unwrap_or(Value::Null) =>
            {
                ()
            }
            _ => return Err(invalid(format!("unsupported book setting {key}"))),
        }
    }
    Ok(())
}
