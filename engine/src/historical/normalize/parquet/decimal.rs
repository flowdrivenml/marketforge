use arrow_buffer::i256;
use rust_decimal::Decimal;

use crate::error::{MarketForgeError, Result};

use super::schema::DECIMAL_SCALE;

pub fn decimal_to_i256(value: Decimal) -> Result<i256> {
    let mantissa = value.mantissa();
    let scale = value.scale();

    let target_scale = u32::try_from(DECIMAL_SCALE).map_err(|_| {
        MarketForgeError::InvalidConfiguration("negative canonical decimal scale".to_owned())
    })?;

    if scale > target_scale {
        return Err(MarketForgeError::InvalidCanonical(format!(
            "decimal scale {scale} exceeds target scale {target_scale}"
        )));
    }

    let mut result = i256::from_i128(mantissa);
    let ten = i256::from_i128(10);

    for _ in 0..(target_scale - scale) {
        result = result.checked_mul(ten).ok_or_else(|| {
            MarketForgeError::InvalidCanonical(format!(
                "decimal overflow during Decimal256 conversion: {value}"
            ))
        })?;
    }

    Ok(result)
}

pub fn i256_to_decimal(value: i256) -> Result<Decimal> {
    let mut mantissa = value;

    let mut scale = u32::try_from(DECIMAL_SCALE).map_err(|_| {
        MarketForgeError::InvalidConfiguration("negative canonical decimal scale".to_owned())
    })?;

    let ten = i256::from_i128(10);
    let zero = i256::from_i128(0);

    loop {
        let text = mantissa.to_string();

        // Attempt exact conversion to rust_decimal.
        if let Ok(coefficient) = text.parse::<i128>() {
            if let Ok(decimal) = Decimal::try_from_i128_with_scale(coefficient, scale) {
                return Ok(decimal);
            }
        }

        // Cannot reduce the scale further.
        if scale == 0 {
            break;
        }

        // Only remove trailing zeros.
        // Never truncate significant digits.
        if mantissa % ten != zero {
            break;
        }

        mantissa /= ten;
        scale -= 1;
    }

    Err(MarketForgeError::InvalidCanonical(format!(
        "Decimal256 value cannot be represented exactly by rust_decimal: {value}"
    )))
}
