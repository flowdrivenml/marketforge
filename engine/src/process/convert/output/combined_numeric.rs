use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray, UInt8Array};
use num_bigint::BigInt;
use num_traits::One;

use crate::{
    error::{MarketForgeError, Result},
    process::convert::ConversionDecimal,
};

use super::{
    depth_numeric::reconstruct_conversion_ratio,
    schema::{
        CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CombinedNumericValidationMetrics {
    pub events_checked: u64,
    pub trade_events_checked: u64,
    pub depth_events_checked: u64,

    pub trade_values_checked: u64,
    pub depth_levels_checked: u64,
    pub references_checked: u64,
}

pub fn validate_combined_numeric_batch(
    batch: &RecordBatch,
) -> Result<CombinedNumericValidationMetrics> {
    let kinds = column::<UInt8Array>(batch, "event_kind")?;
    let statuses = column::<UInt8Array>(batch, "conversion_status")?;
    let rates = column::<Decimal256Array>(batch, "conversion_rate")?;
    let references = column::<ListArray>(batch, "conversion_references")?;

    let trades = column::<StructArray>(batch, "trade")?;
    let trade_targets = column::<StructArray>(batch, "trade_target")?;

    let depths = column::<StructArray>(batch, "depth")?;
    let depth_targets = column::<ListArray>(batch, "depth_target")?;

    let source_trade_prices = struct_column::<Decimal256Array>(trades, "price")?;

    let source_trade_quantities = struct_column::<Decimal256Array>(trades, "quantity_quote")?;

    let target_trade_prices = struct_column::<Decimal256Array>(trade_targets, "price_target")?;

    let target_trade_quantities =
        struct_column::<Decimal256Array>(trade_targets, "quantity_quote_target")?;

    let source_changes = struct_column::<ListArray>(depths, "changes")?;

    let source_levels = source_changes
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid source depth levels"))?;

    let target_levels = depth_targets
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid target depth levels"))?;

    let source_depth_prices = struct_column::<Decimal256Array>(source_levels, "price")?;

    let source_depth_quantities =
        struct_column::<Decimal256Array>(source_levels, "quantity_quote")?;

    let target_depth_prices = struct_column::<Decimal256Array>(target_levels, "price_target")?;

    let target_depth_quantities =
        struct_column::<Decimal256Array>(target_levels, "quantity_quote_target")?;

    let reference_values = references
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid combined reference structure"))?;

    let mut metrics = CombinedNumericValidationMetrics::default();

    for row in 0..batch.num_rows() {
        if kinds.is_null(row) || statuses.is_null(row) {
            return invalid("null combined event kind or conversion status");
        }

        let kind = kinds.value(row);
        let status = statuses.value(row);

        // -----------------------------------------------------------------
        // Unavailable conversions
        // -----------------------------------------------------------------

        if status == CONVERSION_STATUS_UNAVAILABLE {
            if !rates.is_null(row) || references.is_null(row) || references.value_length(row) != 0 {
                return invalid("unavailable combined event contains conversion provenance");
            }

            match kind {
                0 => {
                    if trade_targets.is_null(row) {
                        return invalid("trade event missing target payload");
                    }

                    if !target_trade_prices.is_null(row) || !target_trade_quantities.is_null(row) {
                        return invalid("unavailable trade contains converted values");
                    }

                    if !depth_targets.is_null(row) {
                        return invalid("trade event contains depth target");
                    }

                    metrics.trade_events_checked += 1;
                }

                1 => {
                    if depth_targets.is_null(row) {
                        return invalid("depth event missing target payload");
                    }

                    if !trade_targets.is_null(row) {
                        return invalid("depth event contains trade target");
                    }

                    let start = depth_targets.value_offsets()[row] as usize;
                    let end = depth_targets.value_offsets()[row + 1] as usize;

                    for index in start..end {
                        if !target_depth_prices.is_null(index)
                            || !target_depth_quantities.is_null(index)
                        {
                            return invalid("unavailable depth contains converted levels");
                        }
                    }

                    metrics.depth_events_checked += 1;
                }

                _ => return invalid("invalid combined event kind"),
            }

            metrics.events_checked += 1;
            continue;
        }

        // -----------------------------------------------------------------
        // Reconstruct conversion ratio independently
        // -----------------------------------------------------------------

        if rates.is_null(row) || references.is_null(row) {
            return invalid("successful combined conversion missing metadata");
        }

        let reference_start = references.value_offsets()[row] as usize;
        let reference_end = references.value_offsets()[row + 1] as usize;

        let (numerator, denominator) = match status {
            CONVERSION_STATUS_IDENTITY => {
                if reference_start != reference_end {
                    return invalid("identity combined conversion contains references");
                }

                (BigInt::one(), BigInt::one())
            }

            CONVERSION_STATUS_CONVERTED => {
                if reference_start == reference_end {
                    return invalid("converted combined event has no references");
                }

                let ratio =
                    reconstruct_conversion_ratio(reference_values, reference_start, reference_end)?;

                metrics.references_checked += (reference_end - reference_start) as u64;

                ratio
            }

            _ => return invalid("invalid combined conversion status"),
        };

        let expected_rate = ConversionDecimal::from_ratio(numerator.clone(), denominator.clone())?;

        if rates.value(row) != expected_rate.mantissa() {
            return invalid(format!("combined conversion rate mismatch at row {row}",));
        }

        // -----------------------------------------------------------------
        // Validate converted payload
        // -----------------------------------------------------------------

        match kind {
            0 => {
                if trades.is_null(row) || trade_targets.is_null(row) || !depth_targets.is_null(row)
                {
                    return invalid("invalid combined trade payload");
                }

                validate_decimal_pair(
                    source_trade_prices,
                    target_trade_prices,
                    row,
                    &numerator,
                    &denominator,
                    true,
                )?;

                validate_decimal_pair(
                    source_trade_quantities,
                    target_trade_quantities,
                    row,
                    &numerator,
                    &denominator,
                    false,
                )?;

                metrics.trade_events_checked += 1;
                metrics.trade_values_checked += 1;
            }

            1 => {
                if depths.is_null(row) || depth_targets.is_null(row) || !trade_targets.is_null(row)
                {
                    return invalid("invalid combined depth payload");
                }

                let source_start = source_changes.value_offsets()[row] as usize;

                let source_end = source_changes.value_offsets()[row + 1] as usize;

                let target_start = depth_targets.value_offsets()[row] as usize;

                let target_end = depth_targets.value_offsets()[row + 1] as usize;

                if source_end - source_start != target_end - target_start {
                    return invalid(format!("combined depth level count mismatch at row {row}",));
                }

                for offset in 0..(source_end - source_start) {
                    let source_index = source_start + offset;
                    let target_index = target_start + offset;

                    validate_decimal_pair_at(
                        source_depth_prices,
                        target_depth_prices,
                        source_index,
                        target_index,
                        &numerator,
                        &denominator,
                        true,
                    )?;

                    validate_decimal_pair_at(
                        source_depth_quantities,
                        target_depth_quantities,
                        source_index,
                        target_index,
                        &numerator,
                        &denominator,
                        false,
                    )?;

                    metrics.depth_levels_checked += 1;
                }

                metrics.depth_events_checked += 1;
            }

            _ => return invalid("invalid combined event kind"),
        }

        metrics.events_checked += 1;
    }

    Ok(metrics)
}

// -----------------------------------------------------------------------------
// Decimal256 verification
// -----------------------------------------------------------------------------

fn validate_decimal_pair(
    source: &Decimal256Array,
    target: &Decimal256Array,
    row: usize,
    numerator: &BigInt,
    denominator: &BigInt,
    required: bool,
) -> Result<()> {
    validate_decimal_pair_at(source, target, row, row, numerator, denominator, required)
}

fn validate_decimal_pair_at(
    source: &Decimal256Array,
    target: &Decimal256Array,
    source_index: usize,
    target_index: usize,
    numerator: &BigInt,
    denominator: &BigInt,
    required: bool,
) -> Result<()> {
    if source.is_null(source_index) {
        if required {
            return invalid("required source Decimal256 is null");
        }

        if !target.is_null(target_index) {
            return invalid("converted Decimal256 nullability mismatch");
        }

        return Ok(());
    }

    let original = ConversionDecimal::new(source.value(source_index))?;

    let expected = original.checked_mul_ratio(numerator, denominator)?;

    if target.is_null(target_index) || target.value(target_index) != expected.mantissa() {
        return invalid(format!(
            "converted Decimal256 mismatch: source_index={source_index}, target_index={target_index}",
        ));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Arrow helpers
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing combined column: {name}")))
}

fn struct_column<'a, T: Array + 'static>(array: &'a StructArray, name: &str) -> Result<&'a T> {
    array
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing combined struct field: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
