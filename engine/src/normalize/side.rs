use crate::job::IntegrityCategory;
use crate::{
    canonical::TradeSide,
    error::{MarketForgeError, Result},
};

pub fn parse_buy_sell(value: &[u8]) -> Result<TradeSide> {
    match value {
        b"Buy" | b"buy" | b"BUY" => Ok(TradeSide::Buy),

        b"Sell" | b"sell" | b"SELL" => Ok(TradeSide::Sell),

        _ => {
            let value = String::from_utf8_lossy(value);

            Err(MarketForgeError::RecordIntegrity {
                category: IntegrityCategory::InvalidRecord,
                message: format!("invalid trade side: {value}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_buy() {
        assert_eq!(parse_buy_sell(b"Buy").expect("parse side"), TradeSide::Buy,);
    }

    #[test]
    fn parses_sell() {
        assert_eq!(
            parse_buy_sell(b"Sell").expect("parse side"),
            TradeSide::Sell,
        );
    }

    #[test]
    fn rejects_unknown_side() {
        assert!(parse_buy_sell(b"Unknown").is_err());
    }
}
