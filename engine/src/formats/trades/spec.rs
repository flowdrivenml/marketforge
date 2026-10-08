use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::{
    error::{MarketForgeError, Result},
    job::{NormalizationConfig, TargetSchema},
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideTransform {
    Casefold,
    Map,
    SignToSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SideSpec {
    pub source: String,
    pub transform: SideTransform,

    #[serde(default)]
    pub values: HashMap<String, String>,

    pub positive: Option<String>,
    pub negative: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityTransform {
    Abs,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantitySpec {
    pub source: String,
    pub transform: Option<QuantityTransform>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoolTransform {
    Bool,
    Equals,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoolSpec {
    pub source: String,
    pub transform: BoolTransform,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalTradeFields {
    pub is_rpi: Option<BoolSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeSpec {
    pub event_timestamp: FieldSpec,
    pub price: FieldSpec,
    pub quantity: QuantitySpec,
    pub side: SideSpec,

    pub trade_id: Option<FieldSpec>,
    pub sequence: Option<FieldSpec>,

    pub optional: Option<OptionalTradeFields>,
}

impl TradeSpec {
    pub fn from_normalization(config: &NormalizationConfig) -> Result<Self> {
        if config.target_schema != TargetSchema::Trade {
            return Err(MarketForgeError::InvalidConfiguration(
                "normalization target_schema must be trade".to_owned(),
            ));
        }

        let spec: Self = serde_json::from_value(config.rules.clone()).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "invalid trade normalization rules: {error}",
            ))
        })?;

        spec.validate()?;

        Ok(spec)
    }

    fn validate(&self) -> Result<()> {
        if self.event_timestamp.source.trim().is_empty()
            || self.price.source.trim().is_empty()
            || self.quantity.source.trim().is_empty()
            || self.side.source.trim().is_empty()
        {
            return Err(MarketForgeError::InvalidConfiguration(
                "required trade source field cannot be empty".to_owned(),
            ));
        }

        match self.side.transform {
            SideTransform::Map if self.side.values.is_empty() => {
                return Err(MarketForgeError::InvalidConfiguration(
                    "side map requires values".to_owned(),
                ));
            }

            SideTransform::SignToSide
                if self.side.positive.is_none() || self.side.negative.is_none() =>
            {
                return Err(MarketForgeError::InvalidConfiguration(
                    "sign_to_side requires positive and negative mappings".to_owned(),
                ));
            }

            _ => {}
        }

        if let Some(optional) = &self.optional {
            if let Some(rpi) = &optional.is_rpi {
                if matches!(rpi.transform, BoolTransform::Equals) && rpi.value.is_none() {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "equals transform requires value".to_owned(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::job::{QuantityEncoding, TimestampEncoding};

    fn config(rules: Value) -> NormalizationConfig {
        NormalizationConfig {
            timestamp_encoding: TimestampEncoding::Milliseconds,
            quantity_encoding: QuantityEncoding::Base,
            target_schema: TargetSchema::Trade,
            rules,
        }
    }

    #[test]
    fn compiles_bybit_trade_spec() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "timestamp"},
            "price": {"source": "price"},
            "quantity": {"source": "size"},
            "side": {
                "source": "side",
                "transform": "casefold"
            },
            "trade_id": {"source": "trdMatchID"},
            "optional": {
                "is_rpi": {
                    "source": "RPI",
                    "transform": "bool"
                }
            }
        });

        let spec = TradeSpec::from_normalization(&config(rules)).unwrap();

        assert_eq!(spec.quantity.source, "size");
        assert_eq!(spec.trade_id.unwrap().source, "trdMatchID");
        assert_eq!(spec.side.transform, SideTransform::Casefold);
    }

    #[test]
    fn compiles_binance_trade_spec() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "time"},
            "price": {"source": "price"},
            "quantity": {"source": "qty"},
            "side": {
                "source": "is_buyer_maker",
                "transform": "map",
                "values": {
                    "true": "sell",
                    "false": "buy"
                }
            },
            "trade_id": {"source": "id"}
        });

        let spec = TradeSpec::from_normalization(&config(rules)).unwrap();

        assert_eq!(spec.side.values["true"], "sell");
        assert_eq!(spec.quantity.source, "qty");
    }

    #[test]
    fn compiles_gateio_signed_quantity_spec() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "timestamp"},
            "price": {"source": "price"},
            "quantity": {
                "source": "size",
                "transform": "abs"
            },
            "side": {
                "source": "size",
                "transform": "sign_to_side",
                "positive": "buy",
                "negative": "sell"
            },
            "trade_id": {"source": "trade_id"}
        });

        let spec = TradeSpec::from_normalization(&config(rules)).unwrap();

        assert_eq!(spec.side.transform, SideTransform::SignToSide);
        assert_eq!(spec.quantity.transform, Some(QuantityTransform::Abs));
    }

    #[test]
    fn compiles_okx_rpi_spec() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "created_time"},
            "price": {"source": "price"},
            "quantity": {"source": "size"},
            "side": {
                "source": "side",
                "transform": "casefold"
            },
            "optional": {
                "is_rpi": {
                    "source": "source",
                    "transform": "equals",
                    "value": 1
                }
            }
        });

        let spec = TradeSpec::from_normalization(&config(rules)).unwrap();

        assert_eq!(
            spec.optional.unwrap().is_rpi.unwrap().transform,
            BoolTransform::Equals
        );
    }

    #[test]
    fn rejects_unsupported_transform() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "timestamp"},
            "price": {"source": "price"},
            "quantity": {"source": "size"},
            "side": {
                "source": "side",
                "transform": "unknown"
            }
        });

        assert!(TradeSpec::from_normalization(&config(rules)).is_err());
    }

    #[test]
    fn rejects_missing_required_field() {
        let rules = serde_json::json!({
            "event_timestamp": {"source": "timestamp"},
            "quantity": {"source": "size"},
            "side": {
                "source": "side",
                "transform": "casefold"
            }
        });

        assert!(TradeSpec::from_normalization(&config(rules)).is_err());
    }
}
