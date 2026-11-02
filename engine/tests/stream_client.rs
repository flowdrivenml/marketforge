#![cfg(unix)]
use marketforge_engine::{
    client::streaming::*,
    stream_protocol::{codec::*, *},
};
use std::time::Duration;
#[tokio::test]
async fn malformed_server_and_unexpected_eof_are_errors() {
    for malformed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let peer = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let h = read_frame(&mut s, 4096).await.unwrap().unwrap();
            write_frame(
                &mut s,
                &Envelope::control(
                    h.request_id,
                    Message::Welcome {
                        version: 1,
                        schemas: vec![1],
                        capabilities: vec![],
                    },
                ),
                4096,
            )
            .await
            .unwrap();
            let request = read_frame(&mut s, 4096).await.unwrap().unwrap();
            let Message::Subscribe {
                subscription_id, ..
            } = request.message
            else {
                panic!()
            };
            write_frame(
                &mut s,
                &Envelope::control(request.request_id, Message::Subscribed { subscription_id }),
                4096,
            )
            .await
            .unwrap();
            if malformed {
                let mut e = Envelope::control(0, Message::Pong);
                e.subscription_id = subscription_id;
                e.subscription_sequence = 1;
                use tokio::io::AsyncWriteExt;
                let mut bytes = vec![];
                ciborium::ser::into_writer(&e, &mut bytes).unwrap();
                s.write_all(&(bytes.len() as u32).to_be_bytes())
                    .await
                    .unwrap();
                s.write_all(&bytes).await.unwrap();
            }
        });
        let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
            .await
            .unwrap();
        let mut sub = client.subscribe("a").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), sub.recv())
                .await
                .unwrap()
                .is_err()
        );
        client.close().await.unwrap();
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn cancelled_subscribe_sends_bounded_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let (seen, observed) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let h = read_frame(&mut s, 4096).await.unwrap().unwrap();
        write_frame(
            &mut s,
            &Envelope::control(
                h.request_id,
                Message::Welcome {
                    version: 1,
                    schemas: vec![1],
                    capabilities: vec![],
                },
            ),
            4096,
        )
        .await
        .unwrap();
        let request = read_frame(&mut s, 4096).await.unwrap().unwrap();
        let Message::Subscribe {
            subscription_id, ..
        } = request.message
        else {
            panic!()
        };
        seen.send(()).unwrap();
        released.await.unwrap();
        write_frame(
            &mut s,
            &Envelope::control(request.request_id, Message::Subscribed { subscription_id }),
            4096,
        )
        .await
        .unwrap();
        let cleanup = tokio::time::timeout(Duration::from_secs(3), read_frame(&mut s, 4096))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(cleanup.message,Message::Unsubscribe{subscription_id:s} if s==subscription_id)
        );
    });
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let clone = client.clone();
    let task = tokio::spawn(async move { clone.subscribe("a").await });
    observed.await.unwrap();
    task.abort();
    match task.await {
        Err(e) => assert!(e.is_cancelled()),
        _ => panic!("subscribe task was not cancelled"),
    };
    release.send(()).unwrap();
    peer.await.unwrap();
    client.close().await.unwrap();
}
