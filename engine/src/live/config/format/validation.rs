use super::{
    common::{FieldMappings, FieldTypes},
    depth::DepthFormatConfig,
    trade::TradeFormatConfig,
};

// ============================================================
// SHARED FIELD VALIDATION
// ============================================================

fn validate_field_types(fields: &FieldMappings, types: &FieldTypes) -> Result<(), String> {
    for name in fields.keys() {
        if !types.contains_key(name) {
            return Err(format!(
                "field '{name}' has no corresponding type declaration"
            ));
        }
    }

    for name in types.keys() {
        if !fields.contains_key(name) {
            return Err(format!("type '{name}' has no corresponding field mapping"));
        }
    }

    Ok(())
}

fn require_field(fields: &FieldMappings, name: &str, context: &str) -> Result<(), String> {
    if !fields.contains_key(name) {
        return Err(format!("{context} references missing field '{name}'"));
    }

    Ok(())
}

fn require_nonempty(value: &str, context: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{context} cannot be empty"));
    }

    Ok(())
}

// ============================================================
// TRADE VALIDATION
// ============================================================

pub fn validate_trade(config: &TradeFormatConfig) -> Result<(), String> {
    validate_field_types(&config.fields, &config.types)?;

    // --------------------------------------------------------
    // TIMESTAMP SOURCE
    // --------------------------------------------------------

    require_field(
        &config.fields,
        &config.timestamps.event_source,
        "timestamps.event_source",
    )?;

    // --------------------------------------------------------
    // PRICE AND QUANTITY
    // --------------------------------------------------------

    if let Some(source) = &config.normalization.price.source {
        require_field(&config.fields, source, "normalization.price.source")?;
    }

    if let Some(source) = &config.normalization.quantity.source {
        require_field(&config.fields, source, "normalization.quantity.source")?;
    }

    // --------------------------------------------------------
    // NATIVE TRADE IDENTITY
    // --------------------------------------------------------

    require_field(&config.fields, &config.identity.source, "identity.source")?;

    if config.identity.deduplication_key.is_empty() {
        return Err("identity.deduplication_key cannot be empty".into());
    }

    // --------------------------------------------------------
    // AGGRESSOR SIDE
    // --------------------------------------------------------

    if let Some(side) = &config.normalization.side {
        if let Some(source) = &side.source {
            require_field(&config.fields, source, "normalization.side.source")?;
        }

        if side.mappings.is_empty() {
            return Err("normalization.side requires native side mappings".into());
        }
    }

    // --------------------------------------------------------
    // AGGREGATION
    // --------------------------------------------------------

    if let Some(aggregation) = &config.aggregation {
        if aggregation.kind == "exchange_aggregated"
            && aggregation.expand_to_individual_trades == Some(true)
        {
            return Err("exchange-aggregated trades cannot be expanded \
                 into fabricated individual executions"
                .into());
        }
    }

    // --------------------------------------------------------
    // OPTIONAL NATIVE ATTRIBUTES
    // --------------------------------------------------------

    if let Some(attributes) = &config.attributes {
        for (name, attribute) in attributes {
            require_field(
                &config.fields,
                &attribute.source,
                &format!("attributes.{name}.source"),
            )?;
        }
    }

    // --------------------------------------------------------
    // OPTIONAL SEQUENCE METADATA
    // --------------------------------------------------------

    if let Some(sequence) = &config.sequence {
        require_field(&config.fields, &sequence.source, "sequence.source")?;
    }

    // --------------------------------------------------------
    // OUTPUT
    // --------------------------------------------------------

    if config.output.event_type != "trade" {
        return Err("trade output must declare event_type='trade'".into());
    }

    Ok(())
}

// ============================================================
// DEPTH VALIDATION
// ============================================================

pub fn validate_depth(config: &DepthFormatConfig) -> Result<(), String> {
    validate_field_types(&config.fields, &config.types)?;

    // --------------------------------------------------------
    // TIMESTAMP SOURCE
    // --------------------------------------------------------

    require_field(
        &config.fields,
        &config.timestamps.event_source,
        "timestamps.event_source",
    )?;

    // --------------------------------------------------------
    // BID / ASK LEVELS
    // --------------------------------------------------------

    if config.levels.bids.side != "bid" {
        return Err("levels.bids.side must be 'bid'".into());
    }

    if config.levels.asks.side != "ask" {
        return Err("levels.asks.side must be 'ask'".into());
    }

    // --------------------------------------------------------
    // SEQUENCE REFERENCES
    // --------------------------------------------------------

    let sequence = &config.sequence;

    for (context, field) in [
        ("sequence.first_field", &sequence.first_field),
        ("sequence.last_field", &sequence.last_field),
        ("sequence.previous_field", &sequence.previous_field),
        ("sequence.sequence_field", &sequence.sequence_field),
        (
            "sequence.previous_sequence_field",
            &sequence.previous_sequence_field,
        ),
        ("sequence.update_id_field", &sequence.update_id_field),
        (
            "sequence.cross_sequence_field",
            &sequence.cross_sequence_field,
        ),
    ] {
        if let Some(name) = field {
            require_field(&config.fields, name, context)?;
        }
    }

    // --------------------------------------------------------
    // SNAPSHOT SYNCHRONIZATION
    // --------------------------------------------------------

    let synchronization = &config.synchronization;

    require_nonempty(&synchronization.strategy, "synchronization.strategy")?;

    match synchronization.snapshot_source.as_str() {
        "rest" => {
            if synchronization.snapshot_endpoint.is_none() {
                return Err("REST synchronization requires snapshot_endpoint".into());
            }
        }

        "websocket" => {
            if synchronization.require_initial_snapshot != Some(true) {
                return Err("WebSocket synchronization requires initial snapshot".into());
            }
        }

        other => {
            return Err(format!(
                "unsupported synchronization.snapshot_source '{other}'"
            ));
        }
    }

    // --------------------------------------------------------
    // RECOVERY
    // --------------------------------------------------------

    if !config.recovery.require_generation_fencing {
        return Err("depth recovery requires generation fencing".into());
    }

    // --------------------------------------------------------
    // OUTPUT
    // --------------------------------------------------------

    if !config.output.atomic_updates {
        return Err("depth output requires atomic updates".into());
    }

    Ok(())
}
