use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};

use tokio::{net::TcpListener, sync::mpsc, time};

use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

// ============================================================
// TEST PROTOCOL
// ============================================================

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct ExpectedConnection {
    subscriptions: Vec<String>,
}

impl ExpectedConnection {
    fn new(subscriptions: &[&str]) -> Self {
        Self {
            subscriptions: subscriptions
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ServerEvent {
    Connected(usize),
    Subscribed { connection: usize, stream: String },
    Disconnected(usize),
}

// ============================================================
// SCRIPTED SERVER
// ============================================================

async fn run_server(
    listener: TcpListener,
    connections: Vec<ExpectedConnection>,
    events: mpsc::Sender<ServerEvent>,
) {
    for (index, expected) in connections.into_iter().enumerate() {
        let connection_number = index + 1;

        let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
            .await
            .expect("client did not connect")
            .expect("TCP accept failed");

        let mut socket = accept_async(tcp).await.expect("WebSocket handshake failed");

        events
            .send(ServerEvent::Connected(connection_number))
            .await
            .unwrap();

        for expected_stream in expected.subscriptions {
            let frame = time::timeout(TIMEOUT, socket.next())
                .await
                .expect("subscription not received")
                .expect("socket closed before subscription")
                .expect("invalid WebSocket frame");

            let Message::Text(text) = frame else {
                panic!("expected subscription text frame");
            };

            let request: Value =
                serde_json::from_str(text.as_str()).expect("invalid subscription JSON");

            assert_eq!(request["method"], "SUBSCRIBE",);

            assert_eq!(request["params"], json!([expected_stream]),);

            let request_id = request.get("id").expect("missing request ID").clone();

            events
                .send(ServerEvent::Subscribed {
                    connection: connection_number,
                    stream: expected_stream,
                })
                .await
                .unwrap();

            // Binance-style successful subscription ACK.
            //
            // This is a deterministic protocol fixture,
            // not a live exchange response capture.
            let acknowledgement = json!({
                "result": null,
                "id": request_id,
            });

            socket
                .send(Message::Text(acknowledgement.to_string().into()))
                .await
                .expect("could not send ACK");
        }

        // Deliberately end the physical generation.
        socket.close(None).await.unwrap();

        events
            .send(ServerEvent::Disconnected(connection_number))
            .await
            .unwrap();
    }
}

// ============================================================
// TEST CLIENT
// ============================================================

async fn test_client_generation(url: &str, streams: &[&str], request_id_base: u64) {
    let (mut socket, _) = connect_async(url).await.unwrap();

    for (index, stream) in streams.iter().enumerate() {
        let request_id = request_id_base + index as u64;

        let request = json!({
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": request_id,
        });

        socket
            .send(Message::Text(request.to_string().into()))
            .await
            .unwrap();

        let frame = time::timeout(TIMEOUT, socket.next())
            .await
            .expect("ACK timeout")
            .expect("socket closed before ACK")
            .expect("invalid ACK frame");

        let Message::Text(text) = frame else {
            panic!("expected ACK text frame");
        };

        let acknowledgement: Value = serde_json::from_str(text.as_str()).unwrap();

        assert_eq!(acknowledgement["id"], json!(request_id),);

        assert!(acknowledgement["result"].is_null());
    }

    // The scripted server closes the connection
    // after acknowledging the expected subscriptions.
    let close = time::timeout(TIMEOUT, socket.next())
        .await
        .expect("server did not close");

    assert!(
        matches!(close, Some(Ok(Message::Close(_))) | None),
        "unexpected frame after subscriptions: {close:?}"
    );
}

// ============================================================
// TWO PHYSICAL GENERATIONS
// ============================================================

#[tokio::test]
async fn scripted_server_supports_two_generations() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();
    let url = format!("ws://{address}");

    let (event_tx, mut event_rx) = mpsc::channel(16);

    let server = tokio::spawn(run_server(
        listener,
        vec![
            ExpectedConnection::new(&["btcusdt@trade", "ethusdt@trade"]),
            ExpectedConnection::new(&["btcusdt@trade", "xrpusdt@trade"]),
        ],
        event_tx,
    ));

    // First physical connection.
    test_client_generation(&url, &["btcusdt@trade", "ethusdt@trade"], 1).await;

    // Demand changes before the next connection.
    //
    // ETH is removed.
    // XRP is added.
    //
    // The second connection must not replay ETH.
    test_client_generation(&url, &["btcusdt@trade", "xrpusdt@trade"], 100).await;

    time::timeout(TIMEOUT, server)
        .await
        .expect("server task timeout")
        .expect("server task panicked");

    let mut received = Vec::new();

    while let Ok(event) = event_rx.try_recv() {
        received.push(event);
    }

    assert_eq!(
        received,
        vec![
            ServerEvent::Connected(1),
            ServerEvent::Subscribed {
                connection: 1,
                stream: "btcusdt@trade".into(),
            },
            ServerEvent::Subscribed {
                connection: 1,
                stream: "ethusdt@trade".into(),
            },
            ServerEvent::Disconnected(1),
            ServerEvent::Connected(2),
            ServerEvent::Subscribed {
                connection: 2,
                stream: "btcusdt@trade".into(),
            },
            ServerEvent::Subscribed {
                connection: 2,
                stream: "xrpusdt@trade".into(),
            },
            ServerEvent::Disconnected(2),
        ],
    );
}
