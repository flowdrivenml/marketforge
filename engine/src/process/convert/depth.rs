use crate::{
    error::Result,
    process::convert::{ConversionDecimal, reference::EvaluatedConversion},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertedDepthLevel {
    pub price_target: ConversionDecimal,
    pub quantity_quote_target: Option<ConversionDecimal>,
}

/// Convert one canonical depth level using an evaluated conversion path.
///
/// The exact rational conversion is applied directly to the original
/// Decimal256 values, avoiding intermediate rate rounding.
pub fn convert_depth_level(
    price: ConversionDecimal,
    quantity_quote: Option<ConversionDecimal>,
    conversion: &EvaluatedConversion,
) -> Result<ConvertedDepthLevel> {
    let price_target = price.checked_mul_ratio(&conversion.numerator, &conversion.denominator)?;

    let quantity_quote_target = quantity_quote
        .map(|quantity| quantity.checked_mul_ratio(&conversion.numerator, &conversion.denominator))
        .transpose()?;

    Ok(ConvertedDepthLevel {
        price_target,
        quantity_quote_target,
    })
}
