use crate::{
    live::config::exchange::{ExchangeId, MarketType},
    shared::domain::canonical::Exchange,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePlanCompatibilityError {
    UnsupportedMarket,
    WrongFormat { expected: String, actual: String },
}

pub struct SourcePlanCompatibility;

impl SourcePlanCompatibility {
    pub fn expected_format_id(
        exchange: Exchange,
        market: &str,
    ) -> Result<String, SourcePlanCompatibilityError> {
        let exchange = match exchange {
            Exchange::Binance => ExchangeId::Binance,
            Exchange::Bybit => ExchangeId::Bybit,
            Exchange::Bitget => ExchangeId::Bitget,
            Exchange::GateIo => ExchangeId::Gateio,
            Exchange::Okx => ExchangeId::Okx,
        };

        let market = match market {
            "spot" => MarketType::Spot,
            "linear" => MarketType::Linear,
            "inverse" => MarketType::Inverse,
            _ => {
                return Err(SourcePlanCompatibilityError::UnsupportedMarket);
            }
        };

        Ok(format!("{}_{}", exchange.as_str(), market.as_str(),))
    }

    /// Validate the compiled plan's format identity
    /// against the authoritative source binding.
    ///
    /// This uses the same canonical format naming rule
    /// enforced by FormatIdentity::validate().
    ///
    /// It does not parse arbitrary substrings from an ID.
    pub fn validate(
        exchange: Exchange,
        market: &str,
        plan_format_id: &str,
    ) -> Result<(), SourcePlanCompatibilityError> {
        let expected = Self::expected_format_id(exchange, market)?;

        if plan_format_id != expected {
            return Err(SourcePlanCompatibilityError::WrongFormat {
                expected,
                actual: plan_format_id.to_owned(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_matching_profile() {
        assert_eq!(
            SourcePlanCompatibility::validate(Exchange::Bybit, "spot", "bybit_spot",),
            Ok(()),
        );
    }

    #[test]
    fn rejects_wrong_exchange() {
        assert!(matches!(
            SourcePlanCompatibility::validate(Exchange::Bybit, "spot", "binance_spot",),
            Err(SourcePlanCompatibilityError::WrongFormat { .. })
        ));
    }

    #[test]
    fn rejects_wrong_market() {
        assert!(matches!(
            SourcePlanCompatibility::validate(Exchange::Bybit, "spot", "bybit_linear",),
            Err(SourcePlanCompatibilityError::WrongFormat { .. })
        ));
    }

    #[test]
    fn rejects_unknown_market() {
        assert_eq!(
            SourcePlanCompatibility::validate(Exchange::Bybit, "options", "bybit_options",),
            Err(SourcePlanCompatibilityError::UnsupportedMarket),
        );
    }

    #[test]
    fn accepts_each_supported_market() {
        for (market, expected) in [
            ("spot", "okx_spot"),
            ("linear", "okx_linear"),
            ("inverse", "okx_inverse"),
        ] {
            assert_eq!(
                SourcePlanCompatibility::expected_format_id(Exchange::Okx, market,).unwrap(),
                expected,
            );
        }
    }
}
