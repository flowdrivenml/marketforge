use super::{DepthLevelError, DepthLevelPlan, DepthSidePlan};

/// Source-specific paths for one REST depth snapshot.
///
/// Paths are evaluated against the complete REST JSON
/// response, not a WebSocket message or extracted record.
#[derive(Debug, Clone)]
pub struct RestDepthLevelMapping {
    pub bids_path: String,
    pub asks_path: String,
}

/// Compiles REST level extraction by reusing the native
/// level layout already validated for WebSocket.
///
/// Only the collection paths change. Array positions,
/// object field names, and order-count positions remain
/// identical when the exchange uses the same native level
/// representation in both transports.
pub fn compile_rest_depth_levels(
    websocket_levels: &DepthLevelPlan,
    mapping: RestDepthLevelMapping,
    max_levels: usize,
) -> Result<DepthLevelPlan, DepthLevelError> {
    let bids = DepthSidePlan {
        path: mapping.bids_path,
        layout: websocket_levels.bids().layout.clone(),
    };

    let asks = DepthSidePlan {
        path: mapping.asks_path,
        layout: websocket_levels.asks().layout.clone(),
    };

    DepthLevelPlan::new(bids, asks, max_levels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::infrastructure::normalization::NativeDepthLevelLayout;
    use serde_json::json;

    #[test]
    fn compiles_binance_rest_array_layout() {
        let websocket = DepthLevelPlan::new(
            DepthSidePlan {
                path: "b".into(),
                layout: NativeDepthLevelLayout::Array {
                    price_index: 0,
                    quantity_index: 1,
                    order_count_index: None,
                },
            },
            DepthSidePlan {
                path: "a".into(),
                layout: NativeDepthLevelLayout::Array {
                    price_index: 0,
                    quantity_index: 1,
                    order_count_index: None,
                },
            },
            1000,
        )
        .unwrap();

        let rest = compile_rest_depth_levels(
            &websocket,
            RestDepthLevelMapping {
                bids_path: "bids".into(),
                asks_path: "asks".into(),
            },
            1000,
        )
        .unwrap();

        let response = json!({
            "lastUpdateId": 123,
            "bids": [["80000", "2"]],
            "asks": [["80001", "3"]]
        });

        let levels = rest.extract(&response).unwrap();

        assert_eq!(levels.bids.len(), 1);
        assert_eq!(levels.asks.len(), 1);
    }

    #[test]
    fn compiles_okx_rest_order_count_layout() {
        let layout = NativeDepthLevelLayout::Array {
            price_index: 0,
            quantity_index: 1,
            order_count_index: Some(3),
        };

        let websocket = DepthLevelPlan::new(
            DepthSidePlan {
                path: "bids".into(),
                layout: layout.clone(),
            },
            DepthSidePlan {
                path: "asks".into(),
                layout,
            },
            1000,
        )
        .unwrap();

        let rest = compile_rest_depth_levels(
            &websocket,
            RestDepthLevelMapping {
                bids_path: "data.0.bids".into(),
                asks_path: "data.0.asks".into(),
            },
            1000,
        )
        .unwrap();

        let response = json!({
            "code": "0",
            "data": [{
                "seqId": 123,
                "ts": "1792806275757",
                "bids": [["80000", "1.5", "0", "4"]],
                "asks": [["80001", "2.5", "0", "7"]]
            }]
        });

        let levels = rest.extract(&response).unwrap();

        assert_eq!(levels.bids[0].order_count, Some(4));
        assert_eq!(levels.asks[0].order_count, Some(7));
    }

    #[test]
    fn rejects_missing_rest_level_collections() {
        let websocket = DepthLevelPlan::new(
            DepthSidePlan {
                path: "b".into(),
                layout: NativeDepthLevelLayout::Array {
                    price_index: 0,
                    quantity_index: 1,
                    order_count_index: None,
                },
            },
            DepthSidePlan {
                path: "a".into(),
                layout: NativeDepthLevelLayout::Array {
                    price_index: 0,
                    quantity_index: 1,
                    order_count_index: None,
                },
            },
            1000,
        )
        .unwrap();

        let rest = compile_rest_depth_levels(
            &websocket,
            RestDepthLevelMapping {
                bids_path: "result.b".into(),
                asks_path: "result.a".into(),
            },
            1000,
        )
        .unwrap();

        assert!(rest.extract(&json!({"result": {}})).is_err());
    }
}
