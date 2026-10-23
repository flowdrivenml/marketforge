use super::derived::{CreateDerivedStreamRequest, DerivedOperation, MergeOrdering};
use super::model::{CreateSourceStreamRequest, QuantityType};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::str::FromStr;

// ============================================================
// PUBLIC VALIDATION ENTRY POINT
// ============================================================

pub fn validate(request: &CreateSourceStreamRequest) -> Result<(), String> {
    validate_instrument_identity(request)?;
    validate_assets(request)?;
    validate_contract_specification(request)?;

    Ok(())
}

// ============================================================
// INSTRUMENT IDENTITY
// ============================================================

fn validate_instrument_identity(request: &CreateSourceStreamRequest) -> Result<(), String> {
    if request.instrument_id <= 0 {
        return Err("instrument_id must be greater than zero".into());
    }

    validate_identifier(&request.symbol, "symbol")?;

    validate_identifier(&request.instrument_type, "instrument_type")?;

    Ok(())
}

// ============================================================
// ASSET IDENTIFIERS
// ============================================================

fn validate_assets(request: &CreateSourceStreamRequest) -> Result<(), String> {
    validate_optional_identifier(request.base_asset.as_deref(), "base_asset")?;

    validate_optional_identifier(request.quote_asset.as_deref(), "quote_asset")?;

    validate_optional_identifier(
        request.contract_value_asset.as_deref(),
        "contract_value_asset",
    )?;
    validate_optional_identifier(request.settlement_asset.as_deref(), "settlement_asset")?;
    // Asset identity is supplied by the orchestration layer.
    //
    // Do not require base_asset != quote_asset here.
    // Do not assume settlement or denomination semantics.
    //
    // Those checks belong to instrument-specific normalization.

    Ok(())
}

// ============================================================
// CONTRACT SPECIFICATION
// ============================================================

fn validate_contract_specification(request: &CreateSourceStreamRequest) -> Result<(), String> {
    let contract_value = request.contract_value.as_deref();

    let contract_asset = request.contract_value_asset.as_deref();

    // --------------------------------------------------------
    // Validate exact decimal representation.
    // --------------------------------------------------------

    if let Some(value) = contract_value {
        validate_positive_decimal(value, "contract_value")?;
    }

    // --------------------------------------------------------
    // Contract value and denomination must be paired.
    // --------------------------------------------------------

    match (contract_value, contract_asset) {
        (Some(_), None) => {
            return Err("contract_value_asset is required when \
                 contract_value is provided"
                .into());
        }

        (None, Some(_)) => {
            return Err("contract_value is required when \
                 contract_value_asset is provided"
                .into());
        }

        _ => {}
    }

    // --------------------------------------------------------
    // Quantities expressed in contracts require sufficient
    // contract metadata for normalization.
    // --------------------------------------------------------

    if request.quantity_type == QuantityType::Contracts {
        if contract_value.is_none() {
            return Err("quantity_type 'contracts' requires contract_value".into());
        }

        if contract_asset.is_none() {
            return Err("quantity_type 'contracts' requires contract_value_asset".into());
        }
    }

    Ok(())
}

// ============================================================
// IDENTIFIER VALIDATION
// ============================================================

fn validate_optional_identifier(value: Option<&str>, field: &str) -> Result<(), String> {
    if let Some(value) = value {
        validate_identifier(value, field)?;
    }

    Ok(())
}

fn validate_identifier(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }

    if value.trim() != value {
        return Err(format!(
            "{field} must not contain leading or trailing whitespace"
        ));
    }

    if value.chars().any(char::is_whitespace) {
        return Err(format!("{field} must not contain whitespace"));
    }

    if value.chars().any(char::is_control) {
        return Err(format!("{field} must not contain control characters"));
    }

    Ok(())
}

// ============================================================
// EXACT DECIMAL VALIDATION
// ============================================================

fn validate_positive_decimal(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }

    if value.trim() != value {
        return Err(format!(
            "{field} must not contain leading or trailing whitespace"
        ));
    }

    let decimal = Decimal::from_str(value)
        .map_err(|_| format!("{field} must be a valid exact decimal string"))?;

    if decimal <= Decimal::ZERO {
        return Err(format!("{field} must be greater than zero"));
    }

    Ok(())
}

// ============================================================
// DERIVED STREAM REQUEST VALIDATION
// ============================================================

pub fn validate_derived(request: &CreateDerivedStreamRequest) -> Result<(), String> {
    validate_identifier(&request.name, "name")?;

    validate_derived_inputs(request)?;

    if let Some(asset) = request.target_quote_asset.as_deref() {
        validate_identifier(asset, "target_quote_asset")?;
    }

    match request.operation {
        DerivedOperation::Merge => {
            validate_merge_request(request)?;
        }

        DerivedOperation::Convert => {
            validate_conversion_request(request)?;
        }
    }

    Ok(())
}

// ============================================================
// INPUT VALIDATION
// ============================================================

fn validate_derived_inputs(request: &CreateDerivedStreamRequest) -> Result<(), String> {
    if request.inputs.is_empty() {
        return Err("derived stream requires at least one input".into());
    }

    let mut seen = HashSet::new();

    for input in &request.inputs {
        validate_identifier(input, "inputs")?;

        if !seen.insert(input.as_str()) {
            return Err(format!("duplicate derived stream input: {input}"));
        }
    }

    Ok(())
}

// ============================================================
// MERGE VALIDATION
// ============================================================

fn validate_merge_request(request: &CreateDerivedStreamRequest) -> Result<(), String> {
    if request.inputs.len() < 2 {
        return Err("merge operation requires at least two inputs".into());
    }

    let ordering = request
        .ordering
        .ok_or_else(|| "merge operation requires ordering".to_string())?;

    match ordering {
        MergeOrdering::ArrivalOrder => {
            if request.max_lateness_ns.is_some() {
                return Err("max_lateness_ns is not allowed for arrival_order".into());
            }
        }

        MergeOrdering::EventTime => {
            let lateness = request
                .max_lateness_ns
                .ok_or_else(|| "event_time ordering requires max_lateness_ns".to_string())?;

            if lateness == 0 {
                return Err("max_lateness_ns must be greater than zero".into());
            }
        }
    }

    Ok(())
}

// ============================================================
// CONVERSION VALIDATION
// ============================================================

fn validate_conversion_request(request: &CreateDerivedStreamRequest) -> Result<(), String> {
    if request.inputs.len() != 1 {
        return Err("convert operation requires exactly one input".into());
    }

    if request.ordering.is_some() {
        return Err("ordering is not allowed for convert operation".into());
    }

    if request.max_lateness_ns.is_some() {
        return Err("max_lateness_ns is not allowed for convert operation".into());
    }

    if request.target_quote_asset.is_none() {
        return Err("convert operation requires target_quote_asset".into());
    }

    Ok(())
}

