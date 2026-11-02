use marketforge_engine::{
    client::management::{ClientError, ClientOptions, MarketForgeManagementClient, Notification},
    management_protocol::{ManagementEvent, Overview, PoolView},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const TOKEN: &str = "client-only-token-never-in-diagnostics";
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
}
async fn request(socket: &mut tokio::net::TcpStream) {
    let mut bytes = vec![];
    let mut buffer = [0; 1024];
    while !bytes.windows(4).any(|b| b == b"\r\n\r\n") {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        assert!(bytes.len() < 16384);
    }
}
fn overview() -> Overview {
    Overview {
        daemon_instance: "daemon".into(),
        observation_unix_ms: u64::MAX.to_string(),
        graph_revision: u64::MAX.to_string(),
        config_revision: "1".into(),
        event_cursor: "1".into(),
        serving: true,
        shutting_down: false,
        streams: vec![],
        pool: PoolView {
            revision: "0".into(),
            requested_target: 1,
            live_workers: 1,
            starting_workers: 0,
            retiring_workers: 0,
            executing_turns: 0,
            resizing: false,
            resize_error: None,
            saved_startup_workers: Some(1),
            registered_processors: 0,
            ready_processors: None,
            retained_payload_events: None,
            retained_payload_bytes: None,
        },
        metrics: None,
        operations: vec![],
        consistency: "independent samples".into(),
    }
}
#[tokio::test]
async fn lost_mutation_response_is_unknown_and_never_retried_automatically() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{").await.unwrap();
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client =
        MarketForgeManagementClient::new(&endpoint, TOKEN.into(), Default::default()).unwrap();
    let error = client.apply("plan-1", "stable-key").await.unwrap_err();
    assert!(matches!(error,ClientError::UnknownMutation{ref key} if key=="stable-key"));
    assert!(!format!("{client:?} {error:?}").contains(TOKEN));
    bounded(peer).await.unwrap();
}
#[tokio::test]
async fn response_capacity_and_concurrent_request_capacity_are_enforced() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        entered.send(()).unwrap();
        gate.await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99999999\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let client = MarketForgeManagementClient::new(
        &endpoint,
        TOKEN.into(),
        ClientOptions {
            max_concurrent_requests: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let first = client.clone();
    let first = tokio::spawn(async move { first.overview().await });
    bounded(ready).await.unwrap();
    assert!(matches!(
        client.overview().await,
        Err(ClientError::Capacity)
    ));
    release.send(()).unwrap();
    assert!(matches!(
        bounded(first).await.unwrap(),
        Err(ClientError::Capacity)
    ));
    bounded(peer).await.unwrap();
    assert!(
        MarketForgeManagementClient::new("http://example.com", TOKEN.into(), Default::default())
            .is_err()
    );
}
#[tokio::test]
async fn fragmented_crlf_sse_and_cancelled_application_receive_keep_the_event() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (opened, ready) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        let body = serde_json::to_string(&overview()).unwrap();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        drop(socket);
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        opened.send(()).unwrap();
        gate.await.unwrap();
        let event = ManagementEvent {
            cursor: "2".into(),
            daemon_instance: "daemon".into(),
            kind: "operation_completed".into(),
            operation_id: Some("op-1".into()),
            detail: "owned result retained".into(),
        };
        let frame = format!(
            "id: daemon:2\r\nevent: notification\r\ndata: {}\r\n\r\n",
            serde_json::to_string(&event).unwrap()
        );
        for byte in frame.bytes() {
            socket.write_all(&[byte]).await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    let client =
        MarketForgeManagementClient::new(&endpoint, TOKEN.into(), Default::default()).unwrap();
    let mut feed = client.notifications();
    let snapshot = bounded(feed.recv()).await.unwrap().unwrap();
    assert!(
        matches!(snapshot,Notification::Snapshot(ref v) if v.graph_revision==u64::MAX.to_string())
    );
    bounded(ready).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), feed.recv())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert!(
        matches!(bounded(feed.recv()).await.unwrap().unwrap(),Notification::Event(e) if e.cursor=="2")
    );
    feed.close().await;
    client.close().await;
    bounded(peer).await.unwrap();
}
