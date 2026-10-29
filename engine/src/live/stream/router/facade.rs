use super::*;
use crate::live::stream::{
    persistence::StreamDefinition,
    registry::{DemandOwner, DemandRegistry, RuntimePublicationAuthority, StreamGraph},
    request::SourceChannel,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::Notify;

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    pub publications: usize,
    pub bytes: usize,
}
impl QueueLimits {
    fn admits(self, count: usize, bytes: usize, extra: usize) -> bool {
        count < self.publications && bytes.checked_add(extra).is_some_and(|n| n <= self.bytes)
    }
}
#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub ingress: QueueLimits,
    pub consumer: QueueLimits,
    pub max_subscriptions: usize,
    pub max_producers: usize,
    /// Source callbacks may wait this long in total to acquire admission locks.
    /// Does not wait for ingress capacity or consumer processing. Range: (0, 1s].
    pub source_lock_timeout: Duration,
}
impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            ingress: QueueLimits {
                publications: 1024,
                bytes: 64 * 1024 * 1024,
            },
            consumer: QueueLimits {
                publications: 1024,
                bytes: 64 * 1024 * 1024,
            },
            max_subscriptions: 1024,
            max_producers: 1024,
            source_lock_timeout: Duration::from_millis(50),
        }
    }
}
/// Reuses the application's restored durable graph and shared demand owner.
/// The Router never writes definitions or creates an alternative runtime registry.
#[derive(Clone)]
pub struct RouterContext {
    pub graph: Arc<StreamGraph>,
    pub demand: Arc<Mutex<DemandRegistry>>,
}

pub struct AttachProducerRequest {
    pub authority: RuntimePublicationAuthority,
}
pub struct SubscribeRequest {
    pub stream_id: String,
    /// Identity components must be nonempty and at most 4096 bytes each.
    pub owner: DemandOwner,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RouterError {
    #[error("invalid Router configuration")]
    InvalidConfig,
    #[error("Router requires an active Tokio runtime")]
    NoRuntime,
    #[error("Router is shut down")]
    Shutdown,
    #[error("unknown or unsupported stream")]
    UnknownStream,
    #[error("producer authority is revoked or already attached")]
    Unauthorized,
    #[error("Router binding/subscription limit reached")]
    Capacity,
    #[error("demand ownership: {0}")]
    Demand(String),
    #[error("consumer identity is empty or exceeds 4096 bytes")]
    InvalidIdentity,
    #[error("identity counter exhausted")]
    Exhausted,
    #[error("Router dispatch task failed: {0}")]
    Worker(String),
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublishError {
    #[error("Router admission is busy; retry without committing source state")]
    Busy,
    #[error("Router Source publication lock deadline exceeded")]
    ContentionTimeout,
    #[error("Router is shut down")]
    Shutdown,
    #[error("producer is detached or revoked")]
    Revoked,
    #[error("publication has incorrect identity, type, or depth authority")]
    InvalidPublication,
    #[error("bounded ingress capacity exhausted")]
    Capacity,
    #[error("publication sequence exhausted")]
    Exhausted,
}
#[derive(Debug, Clone, Default)]
pub struct RouterStatus {
    pub shutdown: bool,
    pub producers: usize,
    pub subscriptions: usize,
    pub ingress_publications: usize,
    pub ingress_bytes: usize,
    pub consumer_publications: usize,
    pub consumer_bytes: usize,
    pub accepted: u64,
    pub consumer_failures: u64,
    pub cleanup_error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionStatus {
    pub id: u64,
    pub stream_id: String,
    pub state: SubscriptionState,
    pub synchronized: bool,
    pub queued_publications: usize,
    pub queued_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    Active,
    Failed(ContinuityFailure),
    Closed,
}
struct ConsumerQueue {
    queue: VecDeque<(RouterDelivery, usize)>,
    bytes: usize,
    state: SubscriptionState,
    terminal_pending: bool,
    synchronized: bool,
}
struct Consumer {
    id: u64,
    stream: String,
    owner: DemandOwner,
    queue: Mutex<ConsumerQueue>,
    notify: Notify,
}
struct Binding {
    id: u64,
    authority: RuntimePublicationAuthority,
    sequence: u64,
    // Source's own stream generation is independent of registry runtime generation.
    source_generation: Option<u64>,
    book: Option<u64>,
    highest_book: Option<u64>,
}
struct Pending {
    stream: String,
    binding: u64,
    sequence: u64,
    bytes: usize,
    publication: Arc<RouterPublication>,
    // Subscribers joining after admission must not receive pre-subscription data.
    subscriber_cutoff: u64,
}
struct State {
    bindings: BTreeMap<String, Binding>,
    consumers: BTreeMap<u64, Arc<Consumer>>,
    ingress: VecDeque<Pending>,
    ingress_bytes: usize,
    next_id: u64,
    status: RouterStatus,
}
struct Shared {
    config: RouterConfig,
    context: RouterContext,
    state: parking_lot::Mutex<State>,
    #[cfg(test)]
    source_wait: Mutex<Option<std::sync::mpsc::SyncSender<&'static str>>>,
    wake: Notify,
}
/// Owns one dispatch task. Dropping the facade cancels all volatile work.
pub struct StreamRouter {
    shared: Arc<Shared>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
pub struct ProducerHandle {
    shared: Arc<Shared>,
    stream: String,
    binding: u64,
}
pub struct SubscriptionHandle {
    shared: Arc<Shared>,
    consumer: Arc<Consumer>,
}

impl StreamRouter {
    pub fn new(config: RouterConfig, context: RouterContext) -> Result<Self, RouterError> {
        if config.ingress.publications == 0
            || config.ingress.bytes == 0
            || config.consumer.publications == 0
            || config.consumer.bytes == 0
            || config.max_subscriptions == 0
            || config.max_producers == 0
            || config.source_lock_timeout.is_zero()
            || config.source_lock_timeout > Duration::from_secs(1)
            || config
                .consumer
                .bytes
                .checked_mul(config.max_subscriptions)
                .and_then(|n| n.checked_add(config.ingress.bytes))
                .is_none()
            || config
                .consumer
                .publications
                .checked_mul(config.max_subscriptions)
                .is_none()
        {
            return Err(RouterError::InvalidConfig);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| RouterError::NoRuntime)?;
        let shared = Arc::new(Shared {
            config,
            context,
            #[cfg(test)]
            source_wait: Mutex::new(None),
            state: parking_lot::Mutex::new(State {
                bindings: BTreeMap::new(),
                consumers: BTreeMap::new(),
                ingress: VecDeque::new(),
                ingress_bytes: 0,
                next_id: 1,
                status: RouterStatus::default(),
            }),
            wake: Notify::new(),
        });
        let worker_shared = shared.clone();
        let worker = runtime.spawn(async move { dispatch(worker_shared).await });
        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }
    pub fn attach_producer(
        &self,
        request: AttachProducerRequest,
    ) -> Result<ProducerHandle, RouterError> {
        let stream = request.authority.stream_id().to_owned();
        let mut state = self.shared.state.lock();
        if state.status.shutdown {
            return Err(RouterError::Shutdown);
        }
        self.shared.source(&stream)?;
        if self.shared.context.graph.get(&stream) != Some(request.authority.definition.as_ref()) {
            return Err(RouterError::Unauthorized);
        }
        let guard = request.authority.guard();
        if !*guard {
            return Err(RouterError::Unauthorized);
        }
        if let Some(old) = state.bindings.get(&stream) {
            if old.authority.generation().get() >= request.authority.generation().get() {
                return Err(RouterError::Unauthorized);
            }
        } else if state.bindings.len() >= self.shared.config.max_producers {
            return Err(RouterError::Capacity);
        }
        let id = next_id(&mut state)?;
        if !request.authority.claim() {
            return Err(RouterError::Unauthorized);
        }
        if state.bindings.contains_key(&stream) {
            self.shared.retire(&mut state, &stream);
        }
        drop(guard);
        state.bindings.insert(
            stream.clone(),
            Binding {
                id,
                authority: request.authority,
                sequence: 0,
                source_generation: None,
                book: None,
                highest_book: None,
            },
        );
        self.shared.wake.notify_one();
        Ok(ProducerHandle {
            shared: self.shared.clone(),
            stream,
            binding: id,
        })
    }
    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<SubscriptionHandle, RouterError> {
        let mut state = self.shared.state.lock();
        if state.status.shutdown {
            return Err(RouterError::Shutdown);
        }
        let channel = self.shared.source(&request.stream_id)?.channel;
        let components: Vec<&str> = match &request.owner {
            DemandOwner::Direct {
                consumer_id,
                subscription_id,
            } => vec![consumer_id, subscription_id],
            DemandOwner::Unix {
                connection_id,
                subscription_id,
            } => vec![connection_id, subscription_id],
            DemandOwner::Manual { session_id } => vec![session_id],
            DemandOwner::Recording { recorder_id } => vec![recorder_id],
        };
        if components
            .iter()
            .any(|s| s.trim().is_empty() || s.len() > 4096)
        {
            return Err(RouterError::InvalidIdentity);
        }
        if state.consumers.len() >= self.shared.config.max_subscriptions {
            return Err(RouterError::Capacity);
        }
        let id = next_id(&mut state)?;
        lock(&self.shared.context.demand)
            .acquire(
                &self.shared.context.graph,
                request.owner.clone(),
                &request.stream_id,
            )
            .map_err(|e| RouterError::Demand(e.to_string()))?;
        let consumer = Arc::new(Consumer {
            id,
            stream: request.stream_id,
            owner: request.owner,
            queue: Mutex::new(ConsumerQueue {
                queue: VecDeque::new(),
                bytes: 0,
                state: SubscriptionState::Active,
                terminal_pending: false,
                synchronized: channel == SourceChannel::Trades,
            }),
            notify: Notify::new(),
        });
        state.consumers.insert(id, consumer.clone());
        Ok(SubscriptionHandle {
            shared: self.shared.clone(),
            consumer,
        })
    }
    pub fn status(&self) -> RouterStatus {
        let state = self.shared.state.lock();
        let mut status = state.status.clone();
        status.producers = state.bindings.len();
        status.subscriptions = state.consumers.len();
        status.ingress_publications = state.ingress.len();
        status.ingress_bytes = state.ingress_bytes;
        for consumer in state.consumers.values() {
            let q = lock(&consumer.queue);
            status.consumer_publications += q.queue.len();
            status.consumer_bytes += q.bytes;
        }
        status
    }
    /// Cancels undelivered volatile work and explicitly fails remaining consumers.
    /// No promise of end-consumer processing or durable draining is made.
    pub async fn shutdown(&self) -> Result<(), RouterError> {
        self.shared.stop(ContinuityFailure::RouterShutdown);
        let worker = lock(&self.worker).take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|e| RouterError::Worker(e.to_string()))?;
        }
        let state = self.shared.state.lock();
        if let Some(error) = &state.status.cleanup_error {
            return Err(RouterError::Demand(error.clone()));
        }
        Ok(())
    }
}
impl Drop for StreamRouter {
    fn drop(&mut self) {
        self.shared.stop(ContinuityFailure::RouterDropped);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            worker.abort();
        }
    }
}
fn next_id(state: &mut State) -> Result<u64, RouterError> {
    let id = state.next_id;
    state.next_id = id.checked_add(1).ok_or(RouterError::Exhausted)?;
    Ok(id)
}
impl Shared {
    #[cfg(test)]
    fn note_source_wait(&self, lock_name: &'static str) {
        if let Some(observer) = lock(&self.source_wait).as_ref() {
            let _ = observer.try_send(lock_name);
        }
    }

    fn source(
        &self,
        stream: &str,
    ) -> Result<&crate::live::stream::request::CreateSourceStreamRequest, RouterError> {
        let definition = self
            .context
            .graph
            .get(stream)
            .ok_or(RouterError::UnknownStream)?;
        match &definition.definition {
            StreamDefinition::Source(request) if definition.desired.enabled => Ok(request),
            _ => Err(RouterError::UnknownStream),
        }
    }
    fn remove(&self, state: &mut State, id: u64, reason: Option<ContinuityFailure>) {
        if let Some(consumer) = state.consumers.remove(&id) {
            let mut queue = lock(&consumer.queue);
            queue.queue.clear();
            queue.bytes = 0;
            queue.synchronized = false;
            queue.state = reason.map_or(SubscriptionState::Closed, SubscriptionState::Failed);
            queue.terminal_pending = reason.is_some();
            if reason.is_some() {
                state.status.consumer_failures = state.status.consumer_failures.saturating_add(1);
            }
            #[cfg(test)]
            self.note_source_wait("demand-release");
            if let Err(error) = lock(&self.context.demand).release(
                &self.context.graph,
                &consumer.owner,
                &consumer.stream,
            ) {
                state.status.cleanup_error = Some(error.to_string());
            }
            consumer.notify.notify_one();
        }
    }
    fn retire(&self, state: &mut State, stream: &str) {
        state.bindings.remove(stream);
        state.ingress.retain(|p| p.stream != stream);
        state.ingress_bytes = state.ingress.iter().map(|p| p.bytes).sum();
        let ids: Vec<_> = state
            .consumers
            .values()
            .filter(|c| c.stream == stream)
            .map(|c| c.id)
            .collect();
        for id in ids {
            self.remove(state, id, Some(ContinuityFailure::ProducerRevoked));
        }
    }
    fn stop(&self, reason: ContinuityFailure) {
        let mut state = self.state.lock();
        if state.status.shutdown {
            return;
        }
        state.status.shutdown = true;
        state.bindings.clear();
        state.ingress.clear();
        state.ingress_bytes = 0;
        let ids: Vec<_> = state.consumers.keys().copied().collect();
        for id in ids {
            self.remove(&mut state, id, Some(reason));
        }
        self.wake.notify_one();
    }
}
impl ProducerHandle {
    /// Linearizes admission at the bounded ingress insertion while holding the
    /// Router state and runtime authority guards. Contention returns Busy.
    pub fn try_publish(
        &self,
        publication: RouterPublication,
    ) -> Result<RouterAcceptance, PublishError> {
        self.admit(publication, None)
    }

    /// Source-only bounded lock acquisition. Exactly one admission attempt follows
    /// successful acquisition; errors after that point are never retried.
    pub(super) fn publish_for_source(
        &self,
        publication: RouterPublication,
    ) -> Result<RouterAcceptance, PublishError> {
        let deadline = Instant::now() + self.shared.config.source_lock_timeout;
        let result = self.admit(publication, Some(deadline));
        #[cfg(test)]
        if matches!(result, Err(PublishError::ContentionTimeout)) {
            self.shared.note_source_wait("timeout");
        }
        result
    }

    fn admit(
        &self,
        publication: RouterPublication,
        deadline: Option<Instant>,
    ) -> Result<RouterAcceptance, PublishError> {
        let mut state = match self.shared.state.try_lock() {
            Some(state) => state,
            None => {
                let deadline = deadline.ok_or(PublishError::Busy)?;
                #[cfg(test)]
                self.shared.note_source_wait("state");
                self.shared
                    .state
                    .try_lock_until(deadline)
                    .ok_or(PublishError::ContentionTimeout)?
            }
        };
        if state.status.shutdown {
            return Err(PublishError::Shutdown);
        }
        let binding = state
            .bindings
            .get(&self.stream)
            .filter(|b| b.id == self.binding)
            .ok_or(PublishError::Revoked)?;
        let authority = binding.authority.clone();
        let guard = match authority.try_guard() {
            Some(guard) => guard,
            None => {
                let deadline = deadline.ok_or(PublishError::Busy)?;
                #[cfg(test)]
                self.shared.note_source_wait("authority");
                authority
                    .guard_until(deadline)
                    .ok_or(PublishError::ContentionTimeout)?
            }
        };
        if !*guard {
            self.shared.wake.notify_one();
            return Err(PublishError::Revoked);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(PublishError::ContentionTimeout);
        }
        let source = self
            .shared
            .source(&self.stream)
            .map_err(|_| PublishError::InvalidPublication)?;
        if publication.empty() {
            return Err(PublishError::InvalidPublication);
        }
        let bytes = publication
            .retained_bytes()
            .and_then(|n| n.checked_add(self.stream.len()))
            .ok_or(PublishError::Capacity)?;
        if !self
            .shared
            .config
            .ingress
            .admits(state.ingress.len(), state.ingress_bytes, bytes)
        {
            return Err(PublishError::Capacity);
        }
        let (source_generation, book, highest_book) =
            validate(&publication, source, &self.stream, binding)?;
        let sequence = binding
            .sequence
            .checked_add(1)
            .ok_or(PublishError::Exhausted)?;
        let binding = state.bindings.get_mut(&self.stream).unwrap();
        binding.sequence = sequence;
        binding.source_generation = source_generation;
        binding.book = book;
        binding.highest_book = highest_book;
        let subscriber_cutoff = state.next_id;
        state.ingress.push_back(Pending {
            stream: self.stream.clone(),
            binding: self.binding,
            sequence,
            bytes,
            publication: Arc::new(publication),
            subscriber_cutoff,
        });
        state.ingress_bytes += bytes;
        state.status.accepted = state.status.accepted.saturating_add(1);
        self.shared.wake.notify_one();
        Ok(RouterAcceptance {
            sequence,
            runtime_generation: authority.generation(),
        })
    }
}

impl Drop for ProducerHandle {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        if state
            .bindings
            .get(&self.stream)
            .is_some_and(|b| b.id == self.binding)
        {
            self.shared.retire(&mut state, &self.stream);
        }
    }
}
impl SubscriptionHandle {
    pub fn id(&self) -> u64 {
        self.consumer.id
    }
    pub fn status(&self) -> SubscriptionStatus {
        let queue = lock(&self.consumer.queue);
        SubscriptionStatus {
            id: self.consumer.id,
            stream_id: self.consumer.stream.clone(),
            state: queue.state,
            synchronized: queue.synchronized,
            queued_publications: queue.queue.len(),
            queued_bytes: queue.bytes,
        }
    }
    pub fn state(&self) -> SubscriptionState {
        lock(&self.consumer.queue).state
    }
    pub async fn recv(&mut self) -> Option<RouterDelivery> {
        loop {
            let notified = self.consumer.notify.notified();
            {
                // Serialize extraction with producer replacement and runtime revocation.
                let mut state = self.shared.state.lock();
                if let Some(binding) = state.bindings.get(&self.consumer.stream) {
                    let authority = binding.authority.clone();
                    let guard = authority.guard();
                    if !*guard {
                        self.shared.retire(&mut state, &self.consumer.stream);
                    } else if let Some(delivery) = take_delivery(&self.consumer) {
                        return delivery;
                    }
                }
                if let Some(delivery) = take_delivery(&self.consumer) {
                    return delivery;
                }
            }
            notified.await;
        }
    }
    pub async fn close(self) -> Result<(), RouterError> {
        let mut state = self.shared.state.lock();
        self.shared.remove(&mut state, self.consumer.id, None);
        if let Some(error) = &state.status.cleanup_error {
            return Err(RouterError::Demand(error.clone()));
        }
        Ok(())
    }
}
// Outer Some means an immediately available result, including terminal EOF.
fn take_delivery(consumer: &Consumer) -> Option<Option<RouterDelivery>> {
    let mut q = lock(&consumer.queue);
    if let Some((delivery, bytes)) = q.queue.pop_front() {
        q.bytes -= bytes;
        return Some(Some(delivery));
    }
    if q.terminal_pending {
        q.terminal_pending = false;
        if let SubscriptionState::Failed(reason) = q.state {
            return Some(Some(RouterDelivery::ContinuityLost(reason)));
        }
    }
    if q.state != SubscriptionState::Active {
        return Some(None);
    }
    None
}
impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        self.shared.remove(&mut state, self.consumer.id, None);
    }
}

fn validate(
    publication: &RouterPublication,
    source: &crate::live::stream::request::CreateSourceStreamRequest,
    stream: &str,
    binding: &Binding,
) -> Result<(Option<u64>, Option<u64>, Option<u64>), PublishError> {
    let invalid = || PublishError::InvalidPublication;
    let mut generation = binding.source_generation;
    let mut book = binding.book;
    let mut highest = binding.highest_book;
    match publication {
        RouterPublication::Trades { records, .. } if source.channel == SourceChannel::Trades => {
            for trade in records.iter() {
                if trade.envelope.stream_id != stream
                    || trade.envelope.instrument_id != source.instrument_id
                    || trade.envelope.symbol != source.symbol
                    || exchange_name(trade.envelope.exchange) != source.exchange.as_str()
                {
                    return Err(invalid());
                }
            }
        }
        RouterPublication::Depth(events) if source.channel == SourceChannel::Depth => {
            for event in events.iter() {
                let (g, b) = event.generations();
                if g == 0 || b == 0 || generation.is_some_and(|old| g < old) {
                    return Err(invalid());
                }
                if generation != Some(g) {
                    if !matches!(
                        event,
                        RouterDepthEvent::Snapshot { .. } | RouterDepthEvent::Invalidated { .. }
                    ) {
                        return Err(invalid());
                    }
                    generation = Some(g);
                    book = None;
                    highest = None;
                }
                match event {
                    RouterDepthEvent::Snapshot { baseline, .. } => {
                        if !baseline.synchronized || highest.is_some_and(|old| b < old) {
                            return Err(invalid());
                        }
                        book = Some(b);
                        highest = Some(b);
                    }
                    RouterDepthEvent::Delta { baseline, .. } => {
                        if book != Some(b) || !baseline.synchronized {
                            return Err(invalid());
                        }
                    }
                    RouterDepthEvent::Invalidated { .. } => {
                        if highest.is_some_and(|old| b < old) {
                            return Err(invalid());
                        }
                        book = None;
                        highest = Some(b);
                    }
                }
            }
        }
        _ => return Err(invalid()),
    }
    Ok((generation, book, highest))
}
fn exchange_name(exchange: crate::shared::domain::canonical::Exchange) -> &'static str {
    use crate::shared::domain::canonical::Exchange::*;
    match exchange {
        Bybit => "bybit",
        Binance => "binance",
        Okx => "okx",
        Bitget => "bitget",
        GateIo => "gateio",
    }
}
async fn dispatch(shared: Arc<Shared>) {
    loop {
        let notified = shared.wake.notified();
        let did_work = {
            let mut state = shared.state.lock();
            if state.status.shutdown {
                return;
            }
            if let Some(pending) = state.ingress.pop_front() {
                state.ingress_bytes -= pending.bytes;
                let authority = state
                    .bindings
                    .get(&pending.stream)
                    .filter(|b| b.id == pending.binding)
                    .map(|b| b.authority.clone());
                if let Some(authority) = authority {
                    let guard = authority.guard();
                    if !*guard {
                        shared.retire(&mut state, &pending.stream);
                    } else {
                        let consumers: Vec<_> = state
                            .consumers
                            .values()
                            .filter(|c| {
                                c.stream == pending.stream && c.id < pending.subscriber_cutoff
                            })
                            .cloned()
                            .collect();
                        for consumer in consumers {
                            let mut q = lock(&consumer.queue);
                            if !shared
                                .config
                                .consumer
                                .admits(q.queue.len(), q.bytes, pending.bytes)
                            {
                                drop(q);
                                shared.remove(
                                    &mut state,
                                    consumer.id,
                                    Some(ContinuityFailure::ConsumerCapacity),
                                );
                                continue;
                            }
                            if let RouterPublication::Depth(events) = pending.publication.as_ref() {
                                for event in events.iter() {
                                    match event {
                                        RouterDepthEvent::Snapshot { .. } => q.synchronized = true,
                                        RouterDepthEvent::Invalidated { .. } => {
                                            q.synchronized = false
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            let synchronized = q.synchronized;
                            q.queue.push_back((
                                RouterDelivery::Publication {
                                    sequence: pending.sequence,
                                    runtime_generation: authority.generation(),
                                    publication: pending.publication.clone(),
                                    synchronized,
                                },
                                pending.bytes,
                            ));
                            q.bytes += pending.bytes;
                            consumer.notify.notify_one();
                        }
                    }
                }
                true
            } else {
                false
            }
        };
        if did_work {
            tokio::task::yield_now().await;
        } else {
            use futures_util::StreamExt;
            let authorities: Vec<_> = shared
                .state
                .lock()
                .bindings
                .values()
                .map(|b| b.authority.clone())
                .collect();
            let mut revoked = futures_util::stream::FuturesUnordered::new();
            for authority in authorities {
                revoked.push(async move {
                    authority.revoked().await;
                });
            }
            tokio::select! {
                _ = notified => {},
                _ = revoked.next(), if !revoked.is_empty() => {
                    let mut state = shared.state.lock();
                    let streams: Vec<_> = state.bindings.iter().filter(|(_, b)| !*b.authority.guard()).map(|(id, _)| id.clone()).collect();
                    for stream in streams { shared.retire(&mut state, &stream); }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
mod contention_tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/live/integration/router/contention.rs"
    ));
}
