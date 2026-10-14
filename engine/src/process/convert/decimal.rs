use crate::error::{MarketForgeError, Result};
use arrow_buffer::i256;
use num_bigint::{BigInt, Sign};
use num_traits::{One, Signed, Zero};
use std::sync::LazyLock;

pub const CONVERSION_DECIMAL_PRECISION: u32 = 76;
pub const CONVERSION_DECIMAL_SCALE: u32 = 28;
static DECIMAL_PRECISION_LIMIT: LazyLock<i256> = LazyLock::new(|| {
    let limit = BigInt::from(10u8).pow(CONVERSION_DECIMAL_PRECISION);

    let bytes = limit.to_signed_bytes_be();

    let mut buffer = [0u8; 32];

    buffer[32 - bytes.len()..].copy_from_slice(&bytes);

    i256::from_be_bytes(buffer)
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionDecimal {
    mantissa: i256,
}

impl ConversionDecimal {
    pub fn new(mantissa: i256) -> Result<Self> {
        validate_mantissa(mantissa)?;
        Ok(Self { mantissa })
    }

    pub fn zero() -> Self {
        Self {
            mantissa: i256::from_i128(0),
        }
    }

    pub fn one() -> Self {
        Self {
            mantissa: i256::from_i128(10_i128.pow(CONVERSION_DECIMAL_SCALE)),
        }
    }

    pub fn mantissa(&self) -> i256 {
        self.mantissa
    }

    pub fn is_positive(&self) -> bool {
        self.mantissa > i256::from_i128(0)
    }

    pub fn checked_mul(&self, other: &Self) -> Result<Self> {
        let numerator = self.to_bigint() * other.to_bigint();
        let denominator = scale_factor();

        Self::from_bigint(round_half_even(numerator, denominator)?)
    }

    pub fn checked_div(&self, other: &Self) -> Result<Self> {
        if other.mantissa == i256::from_i128(0) {
            return invalid("Decimal256 division by zero");
        }

        let numerator = self.to_bigint() * scale_factor();
        let denominator = other.to_bigint();

        Self::from_bigint(round_half_even(numerator, denominator)?)
    }

    pub fn checked_inverse(&self) -> Result<Self> {
        Self::one().checked_div(self)
    }

    fn to_bigint(&self) -> BigInt {
        BigInt::from_signed_bytes_be(&self.mantissa.to_be_bytes())
    }

    fn from_bigint(value: BigInt) -> Result<Self> {
        let bytes = value.to_signed_bytes_be();

        if bytes.len() > 32 {
            return invalid("Decimal256 integer overflow");
        }

        let fill = if value.sign() == Sign::Minus {
            0xFF
        } else {
            0x00
        };

        let mut buffer = [fill; 32];

        buffer[32 - bytes.len()..].copy_from_slice(&bytes);

        let mantissa = i256::from_be_bytes(buffer);

        Self::new(mantissa)
    }
    pub fn from_ratio(numerator: BigInt, denominator: BigInt) -> Result<Self> {
        if denominator.is_zero() {
            return invalid("Decimal256 ratio denominator is zero");
        }

        let scaled = numerator * scale_factor();

        Self::from_bigint(round_half_even(scaled, denominator)?)
    }
    pub fn checked_mul_ratio(&self, numerator: &BigInt, denominator: &BigInt) -> Result<Self> {
        if denominator.is_zero() {
            return invalid("conversion ratio denominator is zero");
        }

        // Fast path: perform exact arithmetic using checked i128 operations.
        if let (Some(value), Some(num), Some(den)) = (
            i256_to_i128(self.mantissa),
            i128::try_from(numerator).ok(),
            i128::try_from(denominator).ok(),
        ) {
            if let Some(product) = value.checked_mul(num) {
                if let Some(rounded) = round_half_even_i128(product, den) {
                    return Self::new(i256::from_i128(rounded));
                }
            }
        }

        // Fallback: preserve full arbitrary-precision arithmetic.
        let product = self.to_bigint() * numerator;

        Self::from_bigint(round_half_even(product, denominator.clone())?)
    }
}

fn scale_factor() -> BigInt {
    BigInt::from(10u8).pow(CONVERSION_DECIMAL_SCALE)
}

fn validate_mantissa(value: i256) -> Result<()> {
    let limit = *DECIMAL_PRECISION_LIMIT;

    if value <= -limit || value >= limit {
        return invalid("Decimal256 precision exceeds 76 digits");
    }

    Ok(())
}
fn round_half_even_i128(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator == 0 {
        return None;
    }

    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?;

    if remainder == 0 {
        return Some(quotient);
    }

    let remainder_abs = remainder.unsigned_abs();
    let denominator_abs = denominator.unsigned_abs();

    let halfway = denominator_abs / 2;

    let round_up = if remainder_abs > halfway {
        true
    } else if remainder_abs < halfway {
        false
    } else if denominator_abs % 2 != 0 {
        // Odd denominators cannot produce an exact halfway remainder.
        false
    } else {
        // Exact halfway: round to even.
        quotient % 2 != 0
    };

    if !round_up {
        return Some(quotient);
    }

    let negative = (numerator < 0) != (denominator < 0);

    if negative {
        quotient.checked_sub(1)
    } else {
        quotient.checked_add(1)
    }
}
fn round_half_even(numerator: BigInt, denominator: BigInt) -> Result<BigInt> {
    if denominator.is_zero() {
        return invalid("Decimal256 division by zero");
    }

    let negative = numerator.sign() != denominator.sign() && !numerator.is_zero();

    let numerator = numerator.abs();
    let denominator = denominator.abs();

    // Compute quotient and remainder together.
    let (mut quotient, remainder) = num_integer::Integer::div_rem(&numerator, &denominator);

    // Compare remainder against the halfway point.
    let twice_remainder = &remainder << 1;

    if twice_remainder > denominator
        || (twice_remainder == denominator && (&quotient & BigInt::one()) == BigInt::one())
    {
        quotient += 1u8;
    }

    Ok(if negative { -quotient } else { quotient })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
fn i256_to_i128(value: i256) -> Option<i128> {
    let bytes = value.to_be_bytes();

    let mut lower = [0u8; 16];
    lower.copy_from_slice(&bytes[16..]);

    let result = i128::from_be_bytes(lower);

    // Verify that the upper 128 bits correctly sign-extend the result.
    let expected = if result < 0 { 0xFF } else { 0x00 };

    if bytes[..16].iter().all(|&byte| byte == expected) {
        Some(result)
    } else {
        None
    }
}
