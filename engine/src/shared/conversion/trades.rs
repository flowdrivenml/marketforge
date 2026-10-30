use {
    super::evaluation::EvaluatedConversion, crate::shared::conversion::ConversionDecimal,
    crate::shared::error::Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertedTradeValues {
    pub price_target: ConversionDecimal,
    pub quantity_quote_target: Option<ConversionDecimal>,
}

pub fn convert_trade_values(
    price: ConversionDecimal,
    quantity_quote: Option<ConversionDecimal>,
    conversion: &EvaluatedConversion,
) -> Result<ConvertedTradeValues> {
    let price_target = price.checked_mul_ratio(&conversion.numerator, &conversion.denominator)?;

    let quantity_quote_target = quantity_quote
        .map(|quantity| quantity.checked_mul_ratio(&conversion.numerator, &conversion.denominator))
        .transpose()?;

    Ok(ConvertedTradeValues {
        price_target,
        quantity_quote_target,
    })
}
