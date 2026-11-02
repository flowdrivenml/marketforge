use marketforge_engine::stream_protocol::{codec::*, *};
use tokio::io::AsyncWriteExt;
#[test]
fn versioned_cbor_roundtrip_and_hostile_lengths() {
    let e = Envelope::control(
        u64::MAX,
        Message::DescribeStream {
            stream_id: "btc_merged".into(),
        },
    );
    let bytes = encode(&e, 4096).unwrap();
    assert_eq!(decode(&bytes, 4096).unwrap(), e);
    for bytes in [
        vec![0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0x9f, 0xff],
        vec![0xc1, 0x00],
        vec![0xf9, 0, 0],
    ] {
        assert!(decode(&bytes, 4096).is_err());
    }
    let mut unknown = e.clone();
    unknown.schema_version = 99;
    assert!(matches!(
        encode(&unknown, 4096),
        Err(ProtocolError::UnsupportedVersion)
    ));
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&unknown, &mut encoded).unwrap();
    assert!(matches!(
        decode(&encoded, 4096),
        Err(ProtocolError::UnsupportedVersion)
    ));
    assert!(encode(&e, 4).is_err());
}
#[tokio::test]
async fn fragmented_frames_multiple_messages_and_mid_frame_eof() {
    let (mut a, mut b) = tokio::io::duplex(16);
    let e = Envelope::control(u64::MAX, Message::Ping);
    let bytes = encode(&e, 4096).unwrap();
    let task = tokio::spawn(async move {
        for _ in 0..2 {
            for byte in (bytes.len() as u32)
                .to_be_bytes()
                .iter()
                .chain(bytes.iter())
            {
                a.write_all(&[*byte]).await.unwrap();
            }
        }
    });
    assert_eq!(read_frame(&mut b, 4096).await.unwrap(), Some(e.clone()));
    assert_eq!(read_frame(&mut b, 4096).await.unwrap(), Some(e));
    task.await.unwrap();
    assert!(read_frame(&mut b, 4096).await.unwrap().is_none());
    let (mut a, mut b) = tokio::io::duplex(16);
    a.write_all(&[0, 0, 0, 10, 1]).await.unwrap();
    drop(a);
    assert!(matches!(
        read_frame(&mut b, 4096).await,
        Err(ProtocolError::Truncated)
    ));
    let (mut a, mut b) = tokio::io::duplex(16);
    a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    assert!(matches!(
        read_frame(&mut b, 4096).await,
        Err(ProtocolError::Oversized)
    ));
}
#[test]
fn exact_decimal_and_optional_fields_roundtrip() {
    use marketforge_engine::stream_protocol::market::*;
    let d = Decimal {
        coefficient: "1234567890123456789012345678901234567890123456789012345678901234567890123456"
            .into(),
        scale: 28,
    };
    let trade:Trade=serde_json::from_value(serde_json::json!({"event_timestamp_ns":i64::MIN,"system_timestamp_ns":i64::MAX,"exchange":"bybit","instrument_id":42,"symbol":"BTCUSDT","stream_id":"source","trade_id":"18446744073709551615","sequence":u64::MAX,"side":"buy","price":{"coefficient":"12300","scale":7},"quantity_base":null,"quantity_quote":{"coefficient":"10000000000000000000000000000","scale":28},"quantity_contracts":{"coefficient":"500","scale":2},"is_rpi":true,"trade_iv":{"coefficient":"123","scale":4},"mark_iv":null,"index_price":null,"mark_price":null})).unwrap();
    let record = Record {
        ordinal: 0,
        native: NativeEvent::Trade(trade),
        origin: None,
        upstream: None,
        processing_input_position: None,
        processing_admission_ordinal: None,
        baseline_barrier: None,
        integrity: None,
        valuation: Some(Valuation {
            target_quote: "USD".into(),
            rate: d.clone(),
            numerator: "100000000000000000000000000000000000000000001".into(),
            denominator: "3".into(),
            references: vec![],
            reference_integrity: vec![],
            path: vec!["USD".into()],
            values: vec![ConvertedValues {
                price: d,
                quantity_quote: None,
            }],
        }),
    };
    let mut e = Envelope::control(
        0,
        Message::Stream(StreamMessage::Publication(StreamPublication {
            stream_id: "derived".into(),
            runtime_generation: u64::MAX,
            router_sequence: u64::MAX,
            recovered_records: false,
            records: vec![record],
        })),
    );
    e.subscription_id = 9;
    e.subscription_sequence = 1;
    assert_eq!(decode(&encode(&e, 16384).unwrap(), 16384).unwrap(), e);
    let mut invalid = e;
    if let Message::Stream(StreamMessage::Publication(p)) = &mut invalid.message {
        p.records[0].valuation.as_mut().unwrap().denominator = "-000".into();
    }
    let mut bytes = vec![];
    ciborium::ser::into_writer(&invalid, &mut bytes).unwrap();
    assert!(matches!(
        decode(&bytes, 16384),
        Err(ProtocolError::Malformed)
    ));
}
