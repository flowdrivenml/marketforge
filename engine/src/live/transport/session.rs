use crate::{
    live::{
        config::general::PublicationConfig,
        stream::{registry::DemandOwner, router::*},
        transport::streaming::project,
    },
    stream_protocol::{codec::*, *},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};
struct Worker(JoinHandle<()>,Arc<crate::live::metrics::PublicationMetrics>);
struct ConnectionMetrics(Arc<crate::live::metrics::PublicationMetrics>);
impl Drop for ConnectionMetrics {fn drop(&mut self){self.0.connection_closed();}}
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
        self.1.subscription_closed();
    }
}
struct Frame {
    metrics: Arc<crate::live::metrics::PublicationMetrics>,
    id: u64,
    bytes: Box<[u8]>,
    terminal: bool,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Frame {fn drop(&mut self){self.metrics.queue_removed(self.bytes.len() as u64);}}
async fn send<W: tokio::io::AsyncWrite + Unpin>(w:&mut W,e:&Envelope,c:&PublicationConfig)->Result<usize,ProtocolError>{
    let bytes=encode(e,c.streaming.max_frame_bytes)?;let length=bytes.len()+4;
    tokio::time::timeout(Duration::from_millis(c.streaming.write_timeout_ms),async{w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;w.write_all(&bytes).await}).await.map_err(|_|std::io::Error::new(std::io::ErrorKind::TimedOut,"socket write deadline"))??;Ok(length)
}
fn error(id: u64, code: &str) -> Envelope {
    Envelope::control(
        id,
        Message::Error {
            code: code.into(),
            detail: code.into(),
        },
    )
}
pub(super) async fn serve<R, W>(
    mut r: R,
    mut w: W,
    router: Arc<StreamRouter>,
    config: PublicationConfig,
    connection: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let metrics=router.publication_metrics();metrics.connection_opened();let _connection_metrics=ConnectionMetrics(metrics.clone());
    macro_rules! send {($writer:expr,$envelope:expr,$cfg:expr $(,)?)=>{async {
        let result=send($writer,$envelope,$cfg).await;
        match &result {Ok(bytes)=>metrics.written(*bytes as u64),Err(ProtocolError::Oversized)=>metrics.frame_capacity_failed(),Err(_)=>metrics.write_failed()};result
    }}}
    let max = config.streaming.max_frame_bytes;
    let hello = tokio::select! {_=shutdown.changed()=>return,result=tokio::time::timeout(Duration::from_millis(config.streaming.handshake_timeout_ms),read_frame(&mut r,max))=>result};
    let hello = match hello {
        Ok(Ok(Some(e))) => e,
        Ok(Err(ProtocolError::UnsupportedVersion)) => {
            let _ = send!(&mut w, &error(0, "unsupported_version"), &config).await;
            return;
        }
        _ => return,
    };
    let Message::Hello { versions, schemas } = &hello.message else {
        let _ = send!(&mut w, &error(hello.request_id, "hello_required"), &config).await;
        return;
    };
    if !versions.contains(&VERSION) || !schemas.contains(&SCHEMA) {
        let _ = send!(
            &mut w,
            &error(hello.request_id, "unsupported_version"),
            &config,
        )
        .await;
        return;
    }
    if send!(
        &mut w,
        &Envelope::control(
            hello.request_id,
            Message::Welcome {
                version: VERSION,
                schemas: vec![SCHEMA],
                capabilities: vec![
                    "live_tail".into(),
                    "integrity".into(),
                    "depth_snapshot_omission".into(),
                ],
            },
        ),
        &config,
    )
    .await
    .is_err()
    {
        return;
    }
    let (commands, mut command_rx) =
        mpsc::channel::<Result<Envelope, ProtocolError>>(config.streaming.pending_commands);
    let (frames, mut frame_rx) = mpsc::channel::<Frame>(config.streaming.max_subscriptions);
    let budget = Arc::new(Semaphore::new(config.subscriber_queue_bytes));
    let mut tasks = JoinSet::new();
    let read_timeout = config.streaming.read_timeout_ms;
    tasks.spawn(async move {
        loop {
            match read_frame_with_deadline(&mut r, max, Duration::from_millis(read_timeout)).await {
                Ok(Some(e)) => {
                    if commands.send(Ok(e)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = commands.send(Err(e)).await;
                    break;
                }
                Ok(None) => break,
            }
        }
    });
    let mut subscriptions: BTreeMap<u64, Worker> = BTreeMap::new();
    let mut used_subscriptions = BTreeSet::new();
    loop {
        tokio::select! {
            _=shutdown.changed()=>break,
            command=command_rx.recv()=>{
                let Some(result) = command else {
                    break;
                };
                let e = match result {
                    Ok(e) => e,
                    Err(failure) => {
                        let code = match failure {
                            ProtocolError::UnsupportedVersion => "unsupported_version",
                            ProtocolError::Oversized => "frame_too_large",
                            ProtocolError::Truncated => "truncated_frame",
                            ProtocolError::Malformed => "malformed_frame",
                            ProtocolError::Io(_) => "read_failure",
                        };
                        let _ = send!(&mut w, &error(0, code), &config).await;
                        break;
                    }
                };
                if e.request_id == 0 || e.subscription_id != 0 || e.subscription_sequence != 0 {
                    if send!(&mut w, &error(e.request_id, "invalid_envelope"), &config)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    break;
                }
                let id = e.request_id;
                let response = match e.message {
                    Message::Ping => Envelope::control(id, Message::Pong),
                    Message::ListStreams(page) => match router.list_streams_bounded(&page, max) {
                        Ok(page) => Envelope::control(id, Message::Streams(page)),
                        Err(()) => error(id, "discovery_response_too_large"),
                    },
                    Message::DescribeStream { stream_id } => match router
                        .describe_stream_bounded(&stream_id, max.saturating_sub(512).min(1024 * 1024))
                    {
                        Ok(Some(d)) => Envelope::control(id, Message::Description(d)),
                        Ok(None) => error(id, "unknown_stream"),
                        Err(()) => error(id, "discovery_response_too_large"),
                    },
                    Message::Subscribe {
                        stream_id,
                        subscription_id: sid,
                    } => {
                        if sid == 0 || used_subscriptions.contains(&sid) {
                            error(id, "subscription_id_reused")
                        } else if subscriptions.len() >= config.streaming.max_subscriptions
                            || used_subscriptions.len() >= 65536
                        {
                            error(id, "subscription_capacity")
                        } else {
                            used_subscriptions.insert(sid);
                            match router
                                .subscribe_live(SubscribeRequest {
                                    stream_id: stream_id.clone(),
                                    owner: DemandOwner::Unix {
                                        connection_id: format!("stream-v1-{connection}"),
                                        subscription_id: sid.to_string(),
                                    },
                                })
                                .await
                            {
                                Err(_) => error(id, "subscription_rejected"),
                                Ok(mut subscription) => {
                                    // Response delivery precedes worker startup; failure drops Router ownership.
                                    if send!(
                                        &mut w,
                                        &Envelope::control(
                                            id,
                                            Message::Subscribed {
                                                subscription_id: sid,
                                            },
                                        ),
                                        &config,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        break;
                                    }
                                    let frames = frames.clone();
                                    let budget = budget.clone();
                                    let worker_metrics=metrics.clone();
                                    let task = tokio::spawn(async move {
                                        let mut sequence = 0u64;
                                        while let Some(m) = subscription.recv_live().await {
                                            let started=std::time::Instant::now();
                                            sequence = match sequence.checked_add(1) {
                                                Some(n) => n,
                                                None => break,
                                            };
                                            let message = project(&stream_id, m, (max / 512).min(16384))
                                                .unwrap_or_else(|reason| {
                                                    StreamMessage::End(SubscriptionEnd { reason })
                                                });
                                            let events=match &message {StreamMessage::Publication(p)=>p.records.iter().filter(|r|matches!(&r.native,crate::stream_protocol::market::NativeEvent::Trade(_))||matches!(&r.native,crate::stream_protocol::market::NativeEvent::Depth(d) if d.kind=="delta")).count() as u64,_=>0};
                                            let mut terminal = matches!(message, StreamMessage::End(_));
                                            let envelope = Envelope {
                                                protocol_version: VERSION,
                                                schema_id: SCHEMA,
                                                schema_version: 1,
                                                request_id: 0,
                                                subscription_id: sid,
                                                subscription_sequence: sequence,
                                                message: Message::Stream(message),
                                            };
                                            let bytes = match encode(&envelope, max) {
                                                Ok(b) => b,
                                                Err(_) => {
                                                    worker_metrics.frame_capacity_failed();
                                                    terminal = true;
                                                    let mut e = envelope;
                                                    e.message =
                                                        Message::Stream(StreamMessage::End(SubscriptionEnd {
                                                            reason: "publication_too_large".into(),
                                                        }));
                                                    match encode(&e, max) {
                                                        Ok(b) => b,
                                                        Err(_) => break,
                                                    }
                                                }
                                            };
                                            // Data overflow uses bounded reserved terminal space: one small terminal per worker.
                                            let (bytes, terminal, permit) =
                                                match budget.clone().try_acquire_many_owned(bytes.len() as u32)
                                                {
                                                    Ok(p) => (bytes, terminal, p),
                                                    Err(_) => {
                                                        worker_metrics.record_queue_overflow();
                                                        let e = Envelope {
                                                            protocol_version: VERSION,
                                                            schema_id: SCHEMA,
                                                            schema_version: 1,
                                                            request_id: 0,
                                                            subscription_id: sid,
                                                            subscription_sequence: sequence,
                                                            message: Message::Stream(StreamMessage::End(
                                                                SubscriptionEnd {
                                                                    reason: "connection_queue_capacity".into(),
                                                                },
                                                            )),
                                                        };
                                                        let Ok(b) = encode(&e, 4096) else {
                                                            break;
                                                        };
                                                        let Ok(p) = budget.clone().acquire_many_owned(0).await
                                                        else {
                                                            break;
                                                        };
                                                        (b, true, p)
                                                    }
                                                };
                                            worker_metrics.queue_added(bytes.len() as u64);
                                            if frames
                                                .send(Frame {
                                                    metrics: worker_metrics.clone(),
                                                    id: sid,
                                                    bytes: bytes.into_boxed_slice(),
                                                    terminal,
                                                    _permit: permit,
                                                })
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                            if !terminal {worker_metrics.record_events_queued(events);if events>0{worker_metrics.record_publication_latency(started.elapsed());}}
                                            if terminal {
                                                break;
                                            }
                                        }
                                    });
                                    metrics.subscription_opened();
                                    subscriptions.insert(sid, Worker(task,metrics.clone()));
                                    continue;
                                }
                            }
                        }
                    }
                    Message::Unsubscribe {
                        subscription_id: sid,
                    } => {
                        if let Some(mut task) = subscriptions.remove(&sid) {
                            task.0.abort();
                            let _ = (&mut task.0).await;
                        }
                        Envelope::control(
                            id,
                            Message::Unsubscribed {
                                subscription_id: sid,
                            },
                        )
                    }
                    _ => error(id, "invalid_command"),
                };
                if send!(&mut w, &response, &config).await.is_err() {
                    break;
                }
            },
            frame=frame_rx.recv()=>{
                let Some(frame) = frame else {
                    break;
                };
                if !subscriptions.contains_key(&frame.id) {
                    continue;
                }
                let write = async {
                    w.write_all(&(frame.bytes.len() as u32).to_be_bytes())
                        .await?;
                    w.write_all(&frame.bytes).await
                };
                if !matches!(
                    tokio::time::timeout(
                        Duration::from_millis(config.streaming.write_timeout_ms),
                        write
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    metrics.write_failed();
                    break;
                }
                metrics.written(frame.bytes.len() as u64 + 4);
                if frame.terminal {
                    subscriptions.remove(&frame.id);
                }
            },
            _=tasks.join_next(),if !tasks.is_empty()=>(),
        }
    }
    for (_, mut worker) in subscriptions {
        worker.0.abort();
        let _ = (&mut worker.0).await;
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _ = w.shutdown().await;
}
