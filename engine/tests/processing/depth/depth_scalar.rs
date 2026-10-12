use marketforge_engine::formats::depth::{ScalarLevelSpec, extract_scalar_level};

use marketforge_engine::canonical::BookSide;

use serde_json::json;

#[test]
fn extracts_spot_bid() {
    let spec: ScalarLevelSpec = serde_json::from_value(json!({
        "price": {
            "source": "price"
        },
        "quantity": {
            "source": "amount"
        },
        "side": {
            "source": "side",
            "transform": "map",
            "values": {
                "1": "ask",
                "2": "bid"
            }
        }
    }))
    .unwrap();

    let record = json!({
        "price": "78582.8",
        "amount": "0.003844",
        "side": "2"
    });

    let (side, level) = extract_scalar_level(&record, &spec).unwrap();

    assert_eq!(side, BookSide::Bid);
    assert_eq!(level.price.to_string(), "78582.8");
    assert_eq!(level.quantity.to_string(), "0.003844");
}

#[test]
fn extracts_signed_perpetual_ask() {
    let spec: ScalarLevelSpec = serde_json::from_value(json!({
        "price": {
            "source": "price"
        },
        "quantity": {
            "source": "size",
            "transform": "abs"
        },
        "side": {
            "source": "size",
            "transform": "sign_to_book_side",
            "positive": "bid",
            "negative": "ask"
        }
    }))
    .unwrap();

    let record = json!({
        "price": "78542.1",
        "size": "-2153.0"
    });

    let (side, level) = extract_scalar_level(&record, &spec).unwrap();

    assert_eq!(side, BookSide::Ask);
    assert_eq!(level.price.to_string(), "78542.1");
    assert_eq!(level.quantity.to_string(), "2153.0");
}
