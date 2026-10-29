use super::*;
use serde_json::json;
fn selector(symbol: &str) -> NativeMessageSelector {
    NativeMessageSelector::new([
        ("/channel".into(), json!("futures.trades")),
        ("/event".into(), json!("update")),
    ])
    .unwrap()
    .with_payload(NativePayloadSelector {
        path: "/result".into(),
        instrument: "/contract".into(),
        symbol: symbol.into(),
        array: true,
    })
    .unwrap()
}
fn router() -> NativeMessageRouter {
    let mut router = NativeMessageRouter::new();
    router.register("btc", selector("BTC_USDT")).unwrap();
    router.register("eth", selector("ETH_USDT")).unwrap();
    router
}
fn frame(records: Value) -> Message {
    Message::Text(json!({"channel":"futures.trades","event":"update","time_ms":1792858045625_i64,"result":records}).to_string().into())
}
#[test]
fn mixed_records_are_partitioned_without_changing_metadata_or_order() {
    let frames = router()
        .route(frame(json!([
            {"contract":"BTC_USDT","id":1,"price":"80000.000000000000000001"},
            {"contract":"ETH_USDT","id":7,"price":"3000"},
            {"contract":"BTC_USDT","id":2,"price":"80000.000000000000000002"}
        ])))
        .unwrap();
    assert_eq!(frames.len(), 2);
    let btc: Value = serde_json::from_str(frames[0].1.to_text().unwrap()).unwrap();
    assert_eq!(frames[0].0, "btc");
    assert_eq!(btc["time_ms"], 1792858045625_i64);
    assert_eq!(btc["result"].as_array().unwrap().len(), 2);
    assert_eq!(btc["result"][0]["id"], 1);
    assert_eq!(btc["result"][1]["id"], 2);
    assert_eq!(btc["result"][1]["price"], "80000.000000000000000002");
    let eth: Value = serde_json::from_str(frames[1].1.to_text().unwrap()).unwrap();
    assert_eq!(
        eth["result"],
        json!([{"contract":"ETH_USDT","id":7,"price":"3000"}])
    );
}
#[test]
fn invalid_or_unknown_record_rejects_the_entire_batch() {
    for record in [
        json!({}),
        json!({"contract":5}),
        Value::Null,
        json!({"contract":""}),
    ] {
        assert_eq!(
            router().route(frame(json!([{"contract":"BTC_USDT"},record]))),
            Err(NativeMessageRouterError::InvalidPayload)
        );
    }
    assert_eq!(
        router().route(frame(
            json!([{"contract":"BTC_USDT"},{"contract":"DOGE_USDT"}])
        )),
        Err(NativeMessageRouterError::UnknownMessage)
    );
    assert_eq!(
        router().route(frame(json!([]))),
        Err(NativeMessageRouterError::InvalidPayload)
    );
    assert_eq!(
        router().route(frame(json!({"contract":"BTC_USDT"}))),
        Err(NativeMessageRouterError::InvalidPayload)
    );
}
#[test]
fn overlapping_payload_identities_are_ambiguous() {
    let mut router = router();
    let duplicate = NativeMessageSelector::new([("/channel".into(), json!("futures.trades"))])
        .unwrap()
        .with_payload(NativePayloadSelector {
            path: "/result".into(),
            instrument: "/contract".into(),
            symbol: "BTC_USDT".into(),
            array: true,
        })
        .unwrap();
    router.register("alias", duplicate).unwrap();
    assert_eq!(
        router.route(frame(
            json!([{"contract":"BTC_USDT"},{"contract":"ETH_USDT"}])
        )),
        Err(NativeMessageRouterError::AmbiguousMessage)
    );
}
#[test]
fn fanout_has_record_destination_and_envelope_bounds() {
    assert_eq!(
        router().route(frame(json!(vec![json!({"contract":"BTC_USDT"}); 1025]))),
        Err(NativeMessageRouterError::CapacityExceeded)
    );
    let mut router = NativeMessageRouter::new();
    let records: Vec<_> = (0..65)
        .map(|n| {
            let symbol = format!("S{n}");
            router.register(symbol.clone(), selector(&symbol)).unwrap();
            json!({"contract":symbol})
        })
        .collect();
    assert_eq!(
        router.route(frame(json!(records))),
        Err(NativeMessageRouterError::CapacityExceeded)
    );
    let mut large: Value = serde_json::from_str(
        frame(json!([{"contract":"BTC_USDT"},{"contract":"ETH_USDT"}]))
            .to_text()
            .unwrap(),
    )
    .unwrap();
    large["extra"] = "x".repeat(65536).into();
    assert_eq!(
        super::payload_tests::router().route(Message::Text(large.to_string().into())),
        Err(NativeMessageRouterError::CapacityExceeded)
    );
}
#[test]
fn binary_json_retains_binary_fragments() {
    let native = frame(json!([{"contract":"BTC_USDT"},{"contract":"ETH_USDT"}]));
    let results = router().route(Message::Binary(native.into_data())).unwrap();
    assert!(
        results
            .iter()
            .all(|(_, frame)| matches!(frame, Message::Binary(_)))
    );
}
