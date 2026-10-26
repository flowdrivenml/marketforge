use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::live::infrastructure::normalization::extraction::JsonExtractor;
use crate::live::infrastructure::normalization::numeric::NumericConverter;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum DepthLevelError {
    #[error("invalid depth level configuration: {0}")]
    Configuration(String),

    #[error("cannot extract {side} levels at '{path}': {reason}")]
    Extraction {
        side: &'static str,
        path: String,
        reason: String,
    },

    #[error("invalid {side} level {index}: {reason}")]
    InvalidLevel {
        side: &'static str,
        index: usize,
        reason: String,
    },

    #[error("depth message exceeds maximum levels: {actual} > {maximum}")]
    TooManyLevels { actual: usize, maximum: usize },
}

// ============================================================
// NATIVE LEVEL REPRESENTATIONS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeDepthLevelLayout {
    Array {
        price_index: usize,
        quantity_index: usize,
        order_count_index: Option<usize>,
    },

    Object {
        price_field: String,
        quantity_field: String,
        order_count_field: Option<String>,
    },
}

// ============================================================
// ONE SIDE'S EXTRACTION PLAN
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSidePlan {
    pub path: String,
    pub layout: NativeDepthLevelLayout,
}

// ============================================================
// TYPED NATIVE LEVEL
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDepthLevel {
    pub price: Decimal,
    pub quantity: Decimal,
    pub order_count: Option<u64>,
}

// ============================================================
// DECODED BID/ASK LEVELS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDepthLevels {
    pub bids: Vec<NativeDepthLevel>,
    pub asks: Vec<NativeDepthLevel>,
}

// ============================================================
// COMPILED LEVEL EXTRACTION PLAN
// ============================================================

#[derive(Debug, Clone)]
pub struct DepthLevelPlan {
    bids: DepthSidePlan,
    asks: DepthSidePlan,
    max_levels: usize,
}

impl DepthLevelPlan {
    pub fn new(
        bids: DepthSidePlan,
        asks: DepthSidePlan,
        max_levels: usize,
    ) -> Result<Self, DepthLevelError> {
        if max_levels == 0 {
            return Err(DepthLevelError::Configuration(
                "max_levels must be positive".into(),
            ));
        }

        validate_side_plan(&bids)?;
        validate_side_plan(&asks)?;

        Ok(Self {
            bids,
            asks,
            max_levels,
        })
    }

    pub fn bids(&self) -> &DepthSidePlan {
        &self.bids
    }

    pub fn asks(&self) -> &DepthSidePlan {
        &self.asks
    }

    pub fn max_levels(&self) -> usize {
        self.max_levels
    }

    // ========================================================
    // ATOMIC EXTRACTION
    // ========================================================

    pub fn extract(&self, message: &Value) -> Result<NativeDepthLevels, DepthLevelError> {
        self.extract_with_mode(message, false)
    }

    /// For original REST JSON bytes parsed with
    /// serde_json/arbitrary_precision.
    ///
    /// Do not call this on arbitrary Values constructed from
    /// floating-point numbers.
    pub fn extract_lossless_json(
        &self,
        message: &Value,
    ) -> Result<NativeDepthLevels, DepthLevelError> {
        self.extract_with_mode(message, true)
    }

    fn extract_with_mode(
        &self,
        message: &Value,
        lossless_json: bool,
    ) -> Result<NativeDepthLevels, DepthLevelError> {
        let bids = self.extract_side(message, &self.bids, "bid", lossless_json)?;

        let asks = self.extract_side(message, &self.asks, "ask", lossless_json)?;

        let total = bids.len().checked_add(asks.len()).ok_or_else(|| {
            DepthLevelError::Configuration("combined level count overflow".into())
        })?;

        if total > self.max_levels {
            return Err(DepthLevelError::TooManyLevels {
                actual: total,
                maximum: self.max_levels,
            });
        }

        Ok(NativeDepthLevels { bids, asks })
    }

    // ========================================================
    // EXTRACT ONE SIDE
    // ========================================================

    fn extract_side(
        &self,
        message: &Value,
        plan: &DepthSidePlan,
        side: &'static str,
        lossless_json: bool,
    ) -> Result<Vec<NativeDepthLevel>, DepthLevelError> {
        let records = JsonExtractor::array(message, &plan.path).map_err(|error| {
            DepthLevelError::Extraction {
                side,
                path: plan.path.clone(),
                reason: error.to_string(),
            }
        })?;

        if records.len() > self.max_levels {
            return Err(DepthLevelError::TooManyLevels {
                actual: records.len(),
                maximum: self.max_levels,
            });
        }

        let mut result = Vec::with_capacity(records.len());

        for (index, record) in records.iter().enumerate() {
            let level = decode_level(record, &plan.layout, lossless_json).map_err(|reason| {
                DepthLevelError::InvalidLevel {
                    side,
                    index,
                    reason,
                }
            })?;

            result.push(level);
        }

        Ok(result)
    }
}

// ============================================================
// VALIDATION
// ============================================================

fn validate_side_plan(plan: &DepthSidePlan) -> Result<(), DepthLevelError> {
    if plan.path.trim().is_empty() || plan.path.trim() != plan.path {
        return Err(DepthLevelError::Configuration(
            "invalid depth side path".into(),
        ));
    }

    match &plan.layout {
        NativeDepthLevelLayout::Array {
            price_index,
            quantity_index,
            order_count_index,
        } => {
            if price_index == quantity_index {
                return Err(DepthLevelError::Configuration(
                    "price and quantity indices overlap".into(),
                ));
            }

            if let Some(index) = order_count_index {
                if index == price_index || index == quantity_index {
                    return Err(DepthLevelError::Configuration(
                        "order count index overlaps another field".into(),
                    ));
                }
            }
        }

        NativeDepthLevelLayout::Object {
            price_field,
            quantity_field,
            order_count_field,
        } => {
            if price_field.trim().is_empty() || quantity_field.trim().is_empty() {
                return Err(DepthLevelError::Configuration(
                    "object field names cannot be empty".into(),
                ));
            }

            if price_field == quantity_field {
                return Err(DepthLevelError::Configuration(
                    "price and quantity fields overlap".into(),
                ));
            }

            if let Some(field) = order_count_field {
                if field.trim().is_empty() || field == price_field || field == quantity_field {
                    return Err(DepthLevelError::Configuration(
                        "invalid order count field".into(),
                    ));
                }
            }
        }
    }

    Ok(())
}

// ============================================================
// DECODE ONE LEVEL
// ============================================================

fn decode_level(
    record: &Value,
    layout: &NativeDepthLevelLayout,
    lossless_json: bool,
) -> Result<NativeDepthLevel, String> {
    let (price_value, quantity_value, count_value) = match layout {
        NativeDepthLevelLayout::Array {
            price_index,
            quantity_index,
            order_count_index,
        } => {
            let items = record.as_array().ok_or("expected array level")?;

            let price = items.get(*price_index).ok_or("missing price index")?;

            let quantity = items.get(*quantity_index).ok_or("missing quantity index")?;

            let count = match order_count_index {
                Some(index) => Some(items.get(*index).ok_or("missing order count index")?),
                None => None,
            };

            (price, quantity, count)
        }

        NativeDepthLevelLayout::Object {
            price_field,
            quantity_field,
            order_count_field,
        } => {
            let object = record.as_object().ok_or("expected object level")?;

            let price = object.get(price_field).ok_or("missing price field")?;

            let quantity = object.get(quantity_field).ok_or("missing quantity field")?;

            let count = match order_count_field {
                Some(field) => Some(object.get(field).ok_or("missing order count field")?),
                None => None,
            };

            (price, quantity, count)
        }
    };

    let price = if lossless_json {
        NumericConverter::price_lossless_json(price_value)
    } else {
        NumericConverter::price(price_value)
    }
    .map_err(|error| format!("price: {error}"))?;

    let quantity = if lossless_json {
        NumericConverter::quantity_lossless_json(quantity_value)
    } else {
        NumericConverter::quantity(quantity_value)
    }
    .map_err(|error| format!("quantity: {error}"))?;

    let order_count = count_value
        .map(NumericConverter::u64)
        .transpose()
        .map_err(|error| format!("order count: {error}"))?;

    Ok(NativeDepthLevel {
        price,
        quantity,
        order_count,
    })
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn array_side(path: &str) -> DepthSidePlan {
        DepthSidePlan {
            path: path.into(),
            layout: NativeDepthLevelLayout::Array {
                price_index: 0,
                quantity_index: 1,
                order_count_index: None,
            },
        }
    }

    fn array_plan() -> DepthLevelPlan {
        DepthLevelPlan::new(array_side("b"), array_side("a"), 1000).unwrap()
    }

    #[test]
    fn decodes_binance_style_levels() {
        let input = json!({
            "b": [["80000.25", "1.5"]],
            "a": [["80001.50", "0.25"]]
        });

        let levels = array_plan().extract(&input).unwrap();

        assert_eq!(levels.bids.len(), 1);
        assert_eq!(levels.asks.len(), 1);

        assert_eq!(levels.bids[0].price.to_string(), "80000.25",);

        assert_eq!(levels.asks[0].quantity.to_string(), "0.25",);
    }

    #[test]
    fn preserves_zero_quantity_deletion() {
        let input = json!({
            "b": [["80000", "0"]],
            "a": []
        });

        let levels = array_plan().extract(&input).unwrap();

        assert_eq!(levels.bids[0].quantity, Decimal::ZERO,);
    }

    #[test]
    fn decodes_gateio_futures_object_levels() {
        let side = |path: &str| DepthSidePlan {
            path: path.into(),
            layout: NativeDepthLevelLayout::Object {
                price_field: "p".into(),
                quantity_field: "s".into(),
                order_count_field: None,
            },
        };

        let plan = DepthLevelPlan::new(side("result.b"), side("result.a"), 100).unwrap();

        let input = json!({
            "result": {
                "b": [
                    {"p": "80831.8", "s": 62}
                ],
                "a": [
                    {"p": "80832.0", "s": 0}
                ]
            }
        });

        let levels = plan.extract(&input).unwrap();

        assert_eq!(levels.bids[0].quantity, Decimal::from(62),);

        assert_eq!(levels.asks[0].quantity, Decimal::ZERO,);
    }

    #[test]
    fn preserves_okx_order_count() {
        let side = |path: &str| DepthSidePlan {
            path: path.into(),
            layout: NativeDepthLevelLayout::Array {
                price_index: 0,
                quantity_index: 1,
                order_count_index: Some(3),
            },
        };

        let plan = DepthLevelPlan::new(side("bids"), side("asks"), 1000).unwrap();

        let input = json!({
            "bids": [["80892", "0.08589089", "0", "2"]],
            "asks": [["80892.1", "1.36055589", "0", "13"]]
        });

        let levels = plan.extract(&input).unwrap();

        assert_eq!(levels.bids[0].order_count, Some(2));
        assert_eq!(levels.asks[0].order_count, Some(13));
    }

    #[test]
    fn supports_nested_depth_paths() {
        let plan = DepthLevelPlan::new(array_side("data.b"), array_side("data.a"), 100).unwrap();

        let input = json!({
            "data": {
                "b": [["80000", "1"]],
                "a": [["80001", "2"]]
            }
        });

        let levels = plan.extract(&input).unwrap();

        assert_eq!(levels.bids.len(), 1);
        assert_eq!(levels.asks.len(), 1);
    }

    #[test]
    fn rejects_malformed_level_without_partial_result() {
        let input = json!({
            "b": [
                ["80000", "1"],
                ["80001", "invalid"]
            ],
            "a": []
        });

        assert!(array_plan().extract(&input).is_err());
    }

    #[test]
    fn rejects_excessive_levels() {
        let plan = DepthLevelPlan::new(array_side("b"), array_side("a"), 1).unwrap();

        let input = json!({
            "b": [["80000", "1"]],
            "a": [["80001", "1"]]
        });

        assert!(matches!(
            plan.extract(&input),
            Err(DepthLevelError::TooManyLevels {
                actual: 2,
                maximum: 1,
            })
        ));
    }

    #[test]
    fn rejects_overlapping_array_indices() {
        let result = DepthLevelPlan::new(
            DepthSidePlan {
                path: "b".into(),
                layout: NativeDepthLevelLayout::Array {
                    price_index: 0,
                    quantity_index: 0,
                    order_count_index: None,
                },
            },
            array_side("a"),
            100,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_negative_quantities() {
        let input = json!({
            "b": [["80000", "-1"]],
            "a": []
        });

        assert!(array_plan().extract(&input).is_err());
    }
    #[test]
    fn exact_rest_mode_preserves_fractional_numbers() {
        let input: Value = serde_json::from_str(
            r#"{
            "b": [[80895.83, 0.101665]],
            "a": [[80895.84, 0.70207]]
        }"#,
        )
        .unwrap();

        let levels = array_plan().extract_lossless_json(&input).unwrap();

        assert_eq!(levels.bids[0].price.to_string(), "80895.83",);

        assert_eq!(levels.bids[0].quantity.to_string(), "0.101665",);
    }

    #[test]
    fn strict_mode_still_rejects_fractional_json_numbers() {
        let input: Value = serde_json::from_str(r#"{"b":[[80895.83,0.101665]],"a":[]}"#).unwrap();

        assert!(array_plan().extract(&input).is_err());
    }

    #[test]
    fn exact_rest_mode_rejects_malformed_later_level() {
        let input: Value = serde_json::from_str(
            r#"{
            "b": [
                [80895.83, 0.101665],
                [80894.50, -1]
            ],
            "a": []
        }"#,
        )
        .unwrap();

        assert!(array_plan().extract_lossless_json(&input).is_err());
    }
}
