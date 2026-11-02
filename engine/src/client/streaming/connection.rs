use super::subscription::Subscription;
use crate::stream_protocol::{codec::*, *};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    sync::{Notify, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    #[error("connection lost: {0}")]
    ConnectionLost(String),
    #[error("streaming protocol: {0}")]
    Protocol(String),
    #[error("request rejected: {0}")]
    Rejected(String),
    #[error("bounded client capacity exceeded")]
    Capacity,
    #[error("request deadline exceeded")]
    Timeout,
    #[error("client closed")]
    Closed,
}
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub max_frame_bytes: usize,
    pub max_subscriptions: usize,
    pub subscription_queue_messages: usize,
    pub subscription_queue_bytes: usize,
    pub max_pending_requests: usize,
    pub request_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_subscriptions: 64,
            subscription_queue_messages: 64,
            subscription_queue_bytes: 1024 * 1024,
            max_pending_requests: 32,
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        }
    }
}
pub(super) struct LocalSubscription {
    stream_id: String,
    pub sender: mpsc::Sender<QueuedMessage>,
    pub terminal: Arc<Mutex<Option<ClientError>>>,
    pub bytes: Arc<std::sync::atomic::AtomicUsize>,
    sequence: u64,
    router_sequence: Option<u64>,
    runtime_generation: Option<u64>,
}
pub(super) enum Command {
    Request {
        message: Message,
        reply: oneshot::Sender<Result<Message, ClientError>>,
        subscription: Option<(u64, LocalSubscription)>,
    },
    Release(u64),
}
pub(super) struct Inner {
    pub commands: mpsc::Sender<Command>,
    pub stop: Arc<Notify>,
    pub options: ClientOptions,
    pub next_subscription: AtomicU64,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.notify_one();
    }
}
#[derive(Clone)]
pub struct MarketForgeStreamClient {
    pub(super) inner: Arc<Inner>,
}
impl MarketForgeStreamClient {
    pub async fn connect_unix(
        path: impl AsRef<std::path::Path>,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        if !(4096..=16 * 1024 * 1024).contains(&options.max_frame_bytes)
            || options.max_pending_requests == 0
            || options.max_pending_requests > 1024
            || options.max_subscriptions == 0
            || options.max_subscriptions > 1024
            || options.subscription_queue_messages == 0
            || options.subscription_queue_messages > 16384
            || options.subscription_queue_bytes < 4096
            || options.subscription_queue_bytes > 16 * 1024 * 1024
            || options.request_timeout.is_zero()
            || options.write_timeout.is_zero()
            || options.request_timeout > Duration::from_secs(3600)
            || options.write_timeout > Duration::from_secs(3600)
        {
            return Err(ClientError::Capacity);
        }
        let mut socket = tokio::time::timeout(options.request_timeout, UnixStream::connect(path))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|e| ClientError::ConnectionLost(e.to_string()))?;
        let hello = Envelope::control(
            1,
            Message::Hello {
                versions: vec![VERSION],
                schemas: vec![SCHEMA],
            },
        );
        tokio::time::timeout(
            options.write_timeout,
            write_frame(&mut socket, &hello, options.max_frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let response = tokio::time::timeout(
            options.request_timeout,
            read_frame(&mut socket, options.max_frame_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(|e| ClientError::Protocol(e.to_string()))?
        .ok_or_else(|| ClientError::ConnectionLost("handshake EOF".into()))?;
        if response.request_id != 1
            || response.subscription_id != 0
            || !matches!(response.message,Message::Welcome{version:VERSION,ref schemas,..} if schemas.contains(&SCHEMA))
        {
            return Err(ClientError::Protocol("invalid Welcome".into()));
        }
        let (commands, rx) = mpsc::channel(options.max_pending_requests);
        let stop = Arc::new(Notify::new());
        let (r, w) = socket.into_split();
        let task = tokio::spawn(run(r, w, rx, stop.clone(), options.clone()));
        Ok(Self {
            inner: Arc::new(Inner {
                commands,
                stop,
                options,
                next_subscription: AtomicU64::new(1),
                task: Mutex::new(Some(task)),
            }),
        })
    }
    pub(super) async fn request(
        &self,
        message: Message,
        subscription: Option<(u64, LocalSubscription)>,
    ) -> Result<Message, ClientError> {
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .try_send(Command::Request {
                message,
                reply,
                subscription,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => ClientError::Capacity,
                mpsc::error::TrySendError::Closed(_) => ClientError::Closed,
            })?;
        tokio::time::timeout(self.inner.options.request_timeout, response)
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|_| ClientError::Closed)?
    }
    pub async fn list_streams(&self, page: PageRequest) -> Result<StreamPage, ClientError> {
        if page.after.as_ref().is_some_and(|a| a.len() > 4096) {
            return Err(ClientError::Capacity);
        }
        match self.request(Message::ListStreams(page), None).await? {
            Message::Streams(p) => Ok(p),
            _ => Err(ClientError::Protocol("expected stream page".into())),
        }
    }
    pub async fn describe_stream(&self, id: &str) -> Result<StreamDescription, ClientError> {
        if id.len() > 4096 {
            return Err(ClientError::Capacity);
        }
        match self
            .request(
                Message::DescribeStream {
                    stream_id: id.into(),
                },
                None,
            )
            .await?
        {
            Message::Description(d) => Ok(d),
            _ => Err(ClientError::Protocol("expected stream description".into())),
        }
    }
    pub async fn ping(&self) -> Result<(), ClientError> {
        match self.request(Message::Ping, None).await? {
            Message::Pong => Ok(()),
            _ => Err(ClientError::Protocol("expected Pong".into())),
        }
    }
    pub async fn subscribe(&self, id: &str) -> Result<Subscription, ClientError> {
        if id.len() > 4096 {
            return Err(ClientError::Capacity);
        }
        let sid = self
            .inner
            .next_subscription
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| ClientError::Capacity)?;
        let (sender, receiver) = mpsc::channel(self.inner.options.subscription_queue_messages);
        let terminal = Arc::new(Mutex::new(None));
        let bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // This guard also cleans up a cancelled Subscribe future.
        let mut subscription = Subscription {
            inner: self.inner.clone(),
            id: sid,
            receiver,
            terminal: terminal.clone(),
            bytes: bytes.clone(),
            ended: false,
            closed: false,
        };
        let local = LocalSubscription {
            stream_id: id.into(),
            sender,
            terminal,
            bytes,
            sequence: 0,
            router_sequence: None,
            runtime_generation: None,
        };
        match self
            .request(
                Message::Subscribe {
                    stream_id: id.into(),
                    subscription_id: sid,
                },
                Some((sid, local)),
            )
            .await?
        {
            Message::Subscribed { subscription_id } if subscription_id == sid => Ok(subscription),
            _ => {
                subscription.closed = true;
                Err(ClientError::Protocol(
                    "expected subscription acceptance".into(),
                ))
            }
        }
    }
    pub async fn close(&self) -> Result<(), ClientError> {
        self.inner.stop.notify_one();
        let task = self.inner.task.lock().unwrap().take();
        if let Some(task) = task {
            task.await
                .map_err(|e| ClientError::ConnectionLost(e.to_string()))?;
        }
        Ok(())
    }
}
struct Pending {
    reply: Option<oneshot::Sender<Result<Message, ClientError>>>,
    expires: Instant,
    subscription: Option<u64>,
}
async fn run<R, W>(
    mut r: R,
    mut w: W,
    mut commands: mpsc::Receiver<Command>,
    stop: Arc<Notify>,
    options: ClientOptions,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (incoming, mut frames) = mpsc::channel(options.max_pending_requests);
    let mut tasks = JoinSet::new();
    let max = options.max_frame_bytes;
    tasks.spawn(async move {
        loop {
            let message = read_frame(&mut r, max).await;
            let terminal = !matches!(message, Ok(Some(_)));
            if incoming.send(message).await.is_err() || terminal {
                break;
            }
        }
    });
    let mut pending: BTreeMap<u64, Pending> = BTreeMap::new();
    let mut subscriptions: BTreeMap<u64, LocalSubscription> = BTreeMap::new();
    let mut next = 2u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let failure = loop {
        let outgoing = tokio::select! {
            _=stop.notified()=>break ClientError::Closed,
            _=ticker.tick()=>{
                let expired: Vec<_> = pending
                    .iter()
                    .filter(|(_, p)| p.expires <= Instant::now())
                    .map(|(id, _)| *id)
                    .collect();
                if !expired.is_empty() {
                    break ClientError::Timeout;
                }
                continue;
            },
            command=commands.recv()=>{
                match command {
                    None => break ClientError::Closed,
                    Some(Command::Release(sid)) => {
                        subscriptions.remove(&sid);
                        Some((
                            Message::Unsubscribe {
                                subscription_id: sid,
                            },
                            None,
                            None,
                        ))
                    }
                    Some(Command::Request {
                        message,
                        reply,
                        subscription,
                    }) => {
                        if reply.is_closed() {
                            continue;
                        }
                        if pending.len() >= options.max_pending_requests {
                            let _ = reply.send(Err(ClientError::Capacity));
                            continue;
                        }
                        let sid = subscription.as_ref().map(|(sid, _)| *sid);
                        if let Some((sid, local)) = subscription {
                            if subscriptions.len() >= options.max_subscriptions {
                                let _ = reply.send(Err(ClientError::Capacity));
                                continue;
                            }
                            subscriptions.insert(sid, local);
                        }
                        Some((message, Some(reply), sid))
                    }
                }
            },
            result=frames.recv()=>{
                let envelope = match result {
                    Some(Ok(Some(e))) => e,
                    Some(Err(e)) => break ClientError::Protocol(e.to_string()),
                    _ => break ClientError::ConnectionLost("unexpected socket EOF".into()),
                };
                if envelope.subscription_id != 0 {
                    if envelope.request_id != 0 {
                        break ClientError::Protocol("mixed request and subscription identity".into());
                    }
                    let sid = envelope.subscription_id;
                    let Message::Stream(message) = envelope.message else {
                        break ClientError::Protocol("expected stream message".into());
                    };
                    let Some(local) = subscriptions.get_mut(&sid) else {
                        continue;
                    };
                    if local.sequence.checked_add(1) != Some(envelope.subscription_sequence) {
                        break ClientError::Protocol("subscription sequence discontinuity".into());
                    }
                    local.sequence = envelope.subscription_sequence;
                    match &message {
                        StreamMessage::Publication(p) => {
                            if p.stream_id != local.stream_id
                                || local.sequence == 1
                                || local
                                    .router_sequence
                                    .is_some_and(|s| s.checked_add(1) != Some(p.router_sequence))
                                || local
                                    .runtime_generation
                                    .is_some_and(|g| g != p.runtime_generation)
                            {
                                break ClientError::Protocol("market publication continuity violation".into());
                            }
                            local.router_sequence = Some(p.router_sequence);
                            local.runtime_generation = Some(p.runtime_generation);
                        }
                        StreamMessage::IntegritySnapshot(s) => {
                            if s.stream_id != local.stream_id {
                                break ClientError::Protocol("snapshot stream identity mismatch".into());
                            }
                            if let Some(g) = s.runtime_generation {
                                if local.runtime_generation.is_some_and(|old| old != g) {
                                    break ClientError::Protocol(
                                        "generation changed without subscription boundary".into(),
                                    );
                                }
                                local.runtime_generation = Some(g);
                            }
                        }
                        StreamMessage::IntegrityUpdate(_) if local.sequence == 1 => {
                            break ClientError::Protocol("missing initial integrity snapshot".into());
                        }
                        _ => (),
                    }

                    let terminal = matches!(message, StreamMessage::End(_));
                    let queued = QueuedMessage::reserve(message, local.bytes.clone(), options.subscription_queue_bytes);
                    let sent = queued.is_ok_and(|q| local.sender.try_send(q).is_ok());
                    if !sent {
                        *local.terminal.lock().unwrap() = Some(ClientError::Capacity);
                        subscriptions.remove(&sid);
                        Some((
                            Message::Unsubscribe {
                                subscription_id: sid,
                            },
                            None,
                            None,
                        ))
                    } else {
                        if terminal {
                            subscriptions.remove(&sid);
                        }
                        None
                    }
                } else {
                    if envelope.subscription_sequence != 0 {
                        break ClientError::Protocol("unexpected subscription sequence".into());
                    }
                    if envelope.request_id == 0 {
                        if let Message::Error { code, detail } = &envelope.message {
                            break ClientError::Protocol(format!("server error {code}: {detail}"));
                        }
                    }
                    let Some(p) = pending.remove(&envelope.request_id) else {
                        break ClientError::Protocol("unknown response identity".into());
                    };
                    let reply = match envelope.message {
                        Message::Error { code, .. } => Err(ClientError::Rejected(code)),
                        message => Ok(message),
                    };
                    if reply.is_err() {
                        if let Some(sid) = p.subscription {
                            subscriptions.remove(&sid);
                        }
                    }
                    if let Some(tx) = p.reply {
                        if tx.send(reply).is_err() {
                            if let Some(sid) = p.subscription {
                                subscriptions.remove(&sid);
                                Some((
                                    Message::Unsubscribe {
                                        subscription_id: sid,
                                    },
                                    None,
                                    None,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        };
        if let Some((message, reply, subscription)) = outgoing {
            if let Message::Unsubscribe { subscription_id } = message {
                subscriptions.remove(&subscription_id);
            }
            if pending.len() >= options.max_pending_requests {
                break ClientError::Capacity;
            }
            let id = next;
            next = match next.checked_add(1) {
                Some(n) => n,
                None => break ClientError::Capacity,
            };
            let e = Envelope::control(id, message);
            let result =
                tokio::time::timeout(options.write_timeout, write_frame(&mut w, &e, max)).await;
            match result {
                Ok(Ok(())) => {
                    pending.insert(
                        id,
                        Pending {
                            reply,
                            expires: Instant::now() + options.request_timeout,
                            subscription,
                        },
                    );
                }
                _ => {
                    if let Some(reply) = reply {
                        let _ =
                            reply.send(Err(ClientError::ConnectionLost("write failure".into())));
                    }
                    break ClientError::ConnectionLost("write failure".into());
                }
            }
        }
    };
    for (_, p) in pending {
        if let Some(tx) = p.reply {
            let _ = tx.send(Err(failure.clone()));
        }
    }
    for (_, s) in subscriptions {
        *s.terminal.lock().unwrap() = Some(failure.clone());
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _ = w.shutdown().await;
}

/// The reservation travels with the queued payload. Every destruction path,
/// including failed insertion and receiver Drop, releases exactly once.
pub(super) struct Reservation {
    bytes: Arc<std::sync::atomic::AtomicUsize>,
    charge: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.bytes.fetch_sub(self.charge, Ordering::AcqRel);
    }
}
pub(super) struct QueuedMessage {
    pub message: StreamMessage,
    _reservation: Reservation,
}
impl QueuedMessage {
    fn reserve(
        message: StreamMessage,
        bytes: Arc<std::sync::atomic::AtomicUsize>,
        limit: usize,
    ) -> Result<Self, ClientError> {
        let charge = crate::stream_protocol::accounting::retained_charge(&message)
            .map_err(|_| ClientError::Capacity)?;
        bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(charge).filter(|n| *n <= limit)
            })
            .map_err(|_| ClientError::Capacity)?;
        Ok(Self {
            message,
            _reservation: Reservation { bytes, charge },
        })
    }
}

#[cfg(test)]
mod accounting_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    fn message() -> StreamMessage {
        StreamMessage::End(SubscriptionEnd {
            reason: "done".into(),
        })
    }
    #[tokio::test]
    async fn reservations_release_on_every_queue_destruction_path() {
        let bytes = Arc::new(AtomicUsize::new(0));
        let charge = crate::stream_protocol::accounting::retained_charge(&message()).unwrap();
        assert!(QueuedMessage::reserve(message(), bytes.clone(), charge - 1).is_err());
        assert_eq!(bytes.load(Ordering::Acquire), 0);
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(QueuedMessage::reserve(message(), bytes.clone(), charge).unwrap())
            .ok()
            .unwrap();
        assert_eq!(bytes.load(Ordering::Acquire), charge);
        assert!(QueuedMessage::reserve(message(), bytes.clone(), charge).is_err());
        let queued = rx.recv().await.unwrap();
        drop(queued);
        assert_eq!(bytes.load(Ordering::Acquire), 0);
        tx.try_send(QueuedMessage::reserve(message(), bytes.clone(), charge * 2).unwrap())
            .ok()
            .unwrap();
        assert!(
            tx.try_send(QueuedMessage::reserve(message(), bytes.clone(), charge * 2).unwrap())
                .is_err()
        );
        assert_eq!(bytes.load(Ordering::Acquire), charge);
        // Cancellation of a receive leaves the queued reservation untouched.
        let future = rx.recv();
        drop(future);
        assert_eq!(bytes.load(Ordering::Acquire), charge);
        drop(tx); // Connection failure preserves valid queued data.
        assert_eq!(bytes.load(Ordering::Acquire), charge);
        drop(rx); // Unread subscription Drop releases it.
        assert_eq!(bytes.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn full_cleanup_channel_closes_connection_without_spawning() {
        let (commands, _rx) = mpsc::channel(1);
        commands.try_send(Command::Release(99)).ok().unwrap();
        let stop = Arc::new(Notify::new());
        let inner = Arc::new(Inner {
            commands,
            stop: stop.clone(),
            options: ClientOptions::default(),
            next_subscription: AtomicU64::new(1),
            task: Mutex::new(None),
        });
        let bytes = Arc::new(AtomicUsize::new(0));
        let (tx, receiver) = mpsc::channel(1);
        tx.try_send(QueuedMessage::reserve(message(), bytes.clone(), 4096).unwrap())
            .ok()
            .unwrap();
        let sub = Subscription {
            inner,
            id: 1,
            receiver,
            terminal: Arc::new(Mutex::new(None)),
            bytes: bytes.clone(),
            ended: false,
            closed: false,
        };
        drop(sub);
        tokio::time::timeout(Duration::from_secs(1), stop.notified())
            .await
            .unwrap();
        assert_eq!(bytes.load(Ordering::Acquire), 0);
    }
}

#[cfg(test)]
mod lifecycle_accounting_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    fn setup() -> (
        Subscription,
        mpsc::Sender<QueuedMessage>,
        mpsc::Receiver<Command>,
    ) {
        let (commands, rx) = mpsc::channel(2);
        let inner = Arc::new(Inner {
            commands,
            stop: Arc::new(Notify::new()),
            options: ClientOptions::default(),
            next_subscription: AtomicU64::new(1),
            task: Mutex::new(None),
        });
        let (tx, receiver) = mpsc::channel(2);
        (
            Subscription {
                inner,
                id: 1,
                receiver,
                terminal: Arc::new(Mutex::new(Some(ClientError::Capacity))),
                bytes: Arc::new(AtomicUsize::new(0)),
                ended: false,
                closed: false,
            },
            tx,
            rx,
        )
    }
    fn update() -> StreamMessage {
        StreamMessage::IntegrityUpdate(DataIntegrityUpdate {
            revision: 1,
            evidence: DataIntegrityEvidence::unavailable("a".into(), Some(1)),
        })
    }
    #[tokio::test]
    async fn overflow_and_connection_failure_allow_valid_queue_to_drain_once() {
        let (mut sub, tx, _rx) = setup();
        for _ in 0..2 {
            tx.try_send(QueuedMessage::reserve(update(), sub.bytes.clone(), 4096).unwrap())
                .ok()
                .unwrap();
        }
        let initial = sub.queued_payload_bytes();
        assert!(initial > 0);
        drop(tx);
        sub.recv().await.unwrap().unwrap();
        assert_eq!(sub.queued_payload_bytes(), initial / 2);
        sub.recv().await.unwrap().unwrap();
        assert_eq!(sub.queued_payload_bytes(), 0);
        assert!(matches!(sub.recv().await, Err(ClientError::Capacity)));
        assert!(sub.recv().await.unwrap().is_none());
    }
    #[tokio::test]
    async fn explicit_close_drains_reservations_after_boundary() {
        let (mut sub, tx, mut commands) = setup();
        tx.try_send(QueuedMessage::reserve(update(), sub.bytes.clone(), 4096).unwrap())
            .ok()
            .unwrap();
        let actor = tokio::spawn(async move {
            let Some(Command::Request {
                message: Message::Unsubscribe { subscription_id },
                reply,
                ..
            }) = commands.recv().await
            else {
                panic!()
            };
            reply
                .send(Ok(Message::Unsubscribed { subscription_id }))
                .ok()
                .unwrap();
        });
        sub.close().await.unwrap();
        assert_eq!(sub.queued_payload_bytes(), 0);
        assert!(sub.recv().await.unwrap().is_none());
        actor.await.unwrap();
        assert!(
            tx.try_send(QueuedMessage::reserve(update(), sub.bytes.clone(), 4096).unwrap())
                .is_err()
        );
        assert_eq!(sub.queued_payload_bytes(), 0);
    }
}
