use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray, UInt8Array};

use num_bigint::BigInt;
use num_traits::One;

use {
    crate::historical::convert::ConversionDecimal,
    crate::historical::convert::output::CONVERSION_STATUS_CONVERTED,
    crate::historical::convert::output::CONVERSION_STATUS_IDENTITY,
    crate::historical::convert::output::CONVERSION_STATUS_UNAVAILABLE,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

#[derive(Debug, Clone, Default)]
pub struct DepthNumericValidationMetrics {
    pub events_checked: u64,
    pub levels_checked: u64,
    pub references_checked: u64,
}

pub fn validate_depth_numeric_batch(batch: &RecordBatch) -> Result<DepthNumericValidationMetrics> {
    let original = column::<ListArray>(batch, "changes")?;
    let target = column::<ListArray>(batch, "changes_target")?;
    let statuses = column::<UInt8Array>(batch, "conversion_status")?;
    let rates = column::<Decimal256Array>(batch, "conversion_rate")?;
    let references = column::<ListArray>(batch, "conversion_references")?;

    if original.value_offsets() != target.value_offsets() {
        return invalid("depth numeric validation: mismatched offsets");
    }

    let source_levels = original
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid source depth levels"))?;

    let target_levels = target
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid target depth levels"))?;

    let source_prices = struct_column::<Decimal256Array>(source_levels, "price")?;
    let source_quantities = struct_column::<Decimal256Array>(source_levels, "quantity_quote")?;

    let target_prices = struct_column::<Decimal256Array>(target_levels, "price_target")?;
    let target_quantities =
        struct_column::<Decimal256Array>(target_levels, "quantity_quote_target")?;

    let reference_values = references
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid reference structure"))?;

    let mut metrics = DepthNumericValidationMetrics::default();

    for row in 0..batch.num_rows() {
        let start = original.value_offsets()[row] as usize;
        let end = original.value_offsets()[row + 1] as usize;

        let reference_start = references.value_offsets()[row] as usize;
        let reference_end = references.value_offsets()[row + 1] as usize;

        let status = statuses.value(row);

        if status == CONVERSION_STATUS_UNAVAILABLE {
            if !rates.is_null(row) || reference_start != reference_end {
                return invalid("unavailable depth event contains conversion provenance");
            }

            for index in start..end {
                if !target_prices.is_null(index) || !target_quantities.is_null(index) {
                    return invalid("unavailable depth event contains converted levels");
                }
            }

            metrics.events_checked += 1;
            continue;
        }

        if rates.is_null(row) {
            return invalid("successful depth conversion has null rate");
        }

        let (numerator, denominator) = match status {
            CONVERSION_STATUS_IDENTITY => {
                if reference_start != reference_end {
                    return invalid("identity conversion contains reference provenance");
                }

                (BigInt::one(), BigInt::one())
            }

            CONVERSION_STATUS_CONVERTED => {
                if reference_start == reference_end {
                    return invalid("converted depth event has no references");
                }

                let ratio =
                    reconstruct_conversion_ratio(reference_values, reference_start, reference_end)?;

                metrics.references_checked += (reference_end - reference_start) as u64;

                ratio
            }

            _ => return invalid("invalid depth conversion status"),
        };

        let expected_rate = ConversionDecimal::from_ratio(numerator.clone(), denominator.clone())?;

        if rates.value(row) != expected_rate.mantissa() {
            return invalid("depth conversion rate mismatch");
        }

        for index in start..end {
            if source_prices.is_null(index) {
                return invalid("null source depth price");
            }

            let price = ConversionDecimal::new(source_prices.value(index))?;

            let expected_price = price.checked_mul_ratio(&numerator, &denominator)?;

            if target_prices.is_null(index)
                || target_prices.value(index) != expected_price.mantissa()
            {
                return invalid("depth converted price mismatch");
            }

            if source_quantities.is_null(index) {
                if !target_quantities.is_null(index) {
                    return invalid("depth quote quantity nullability mismatch");
                }
            } else {
                let quantity = ConversionDecimal::new(source_quantities.value(index))?;

                let expected_quantity = quantity.checked_mul_ratio(&numerator, &denominator)?;

                if target_quantities.is_null(index)
                    || target_quantities.value(index) != expected_quantity.mantissa()
                {
                    return invalid("depth converted quote quantity mismatch");
                }
            }

            metrics.levels_checked += 1;
        }

        metrics.events_checked += 1;
    }

    Ok(metrics)
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing depth column: {name}")))
}

fn struct_column<'a, T: Array + 'static>(array: &'a StructArray, name: &str) -> Result<&'a T> {
    array
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing depth struct field: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

pub(crate) fn reconstruct_conversion_ratio(
    references: &StructArray,
    start: usize,
    end: usize,
) -> Result<(BigInt, BigInt)> {
    let prices = struct_column::<Decimal256Array>(references, "reference_price")?;

    let directions = struct_column::<UInt8Array>(references, "direction")?;

    let scale = BigInt::from(10_u8).pow(28);

    let mut numerator = BigInt::one();
    let mut denominator = BigInt::one();

    for index in start..end {
        if prices.is_null(index) || directions.is_null(index) {
            return invalid("null conversion reference");
        }

        let price = ConversionDecimal::new(prices.value(index))?;

        if !price.is_positive() {
            return invalid("nonpositive conversion reference price");
        }

        let mantissa = BigInt::from_signed_bytes_be(&price.mantissa().to_be_bytes());

        match directions.value(index) {
            0 => {
                numerator *= mantissa;
                denominator *= &scale;
            }

            1 => {
                numerator *= &scale;
                denominator *= mantissa;
            }

            _ => {
                return invalid("invalid conversion reference direction");
            }
        }
    }

    Ok((numerator, denominator))
}
