use arrow_buffer::i256;
use num_bigint::{BigInt, Sign};
use num_traits::{One, Signed, Zero};

use crate::error::{MarketForgeError, Result};

pub const CONVERSION_DECIMAL_PRECISION: u32 = 76;
pub const CONVERSION_DECIMAL_SCALE: u32 = 28;

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

        // The input mantissa already has scale 28.
        // Multiplying by a dimensionless rational preserves that scale.
        let product = self.to_bigint() * numerator;

        Self::from_bigint(round_half_even(product, denominator.clone())?)
    }
}

fn scale_factor() -> BigInt {
    BigInt::from(10u8).pow(CONVERSION_DECIMAL_SCALE)
}

fn validate_mantissa(value: i256) -> Result<()> {
    let integer = BigInt::from_signed_bytes_be(&value.to_be_bytes());

    let limit = BigInt::from(10u8).pow(CONVERSION_DECIMAL_PRECISION);

    if integer.abs() >= limit {
        return invalid("Decimal256 precision exceeds 76 digits");
    }

    Ok(())
}

fn round_half_even(numerator: BigInt, denominator: BigInt) -> Result<BigInt> {
    if denominator.is_zero() {
        return invalid("Decimal256 division by zero");
    }

    let negative = numerator.sign() != denominator.sign() && !numerator.is_zero();

    let numerator = numerator.abs();
    let denominator = denominator.abs();

    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;

    let twice_remainder = &remainder * 2u8;

    let rounded = if twice_remainder > denominator
        || (twice_remainder == denominator && (&quotient % 2u8) == BigInt::one())
    {
        quotient + 1u8
    } else {
        quotient
    };

    Ok(if negative { -rounded } else { rounded })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
