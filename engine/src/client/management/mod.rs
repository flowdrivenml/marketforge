//! Bounded HTTP administration client, separate from market-data delivery.
use crate::management_protocol::*;
use serde::de::DeserializeOwned;
type Result<T, E = ClientError> = std::result::Result<T, E>;
use futures_util::StreamExt;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, watch};
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    #[error("management API: {0}")]
    Api(ManagementError),
    #[error("management connection unavailable (credentials and URLs redacted)")]
    Connection,
    #[error("management request deadline exceeded")]
    Timeout,
    #[error("bounded client capacity exceeded")]
    Capacity,
    #[error("invalid management response")]
    Protocol,
    #[error("mutation outcome unknown; resolve using the same idempotency key: {key}")]
    UnknownMutation { key: String },
    #[error("management client closed")]
    Closed,
}
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub max_concurrent_requests: usize,
    pub max_response_bytes: usize,
    pub request_timeout: Duration,
}
impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 32,
            max_response_bytes: 4 * 1024 * 1024,
            request_timeout: Duration::from_secs(10),
        }
    }
}
struct Inner {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    token: String,
    options: ClientOptions,
    requests: Arc<Semaphore>,
    closed: watch::Sender<bool>,
}
#[derive(Clone)]
pub struct MarketForgeManagementClient {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for MarketForgeManagementClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MarketForgeManagementClient { credentials: [REDACTED] }")
    }
}
impl MarketForgeManagementClient {
    pub fn from_token_file(
        endpoint: &str,
        path: impl AsRef<Path>,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m =
                std::fs::symlink_metadata(path.as_ref()).map_err(|_| ClientError::Connection)?;
            if !m.is_file() || m.len() > 4096 || m.permissions().mode() & 0o077 != 0 {
                return Err(ClientError::Protocol);
            }
        }
        let token = std::fs::read_to_string(path).map_err(|_| ClientError::Connection)?;
        Self::new(endpoint, token.trim().into(), options)
    }
    pub fn from_environment(
        endpoint: &str,
        variable: &str,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        let token = std::env::var(variable).map_err(|_| ClientError::Connection)?;
        Self::new(endpoint, token, options)
    }
    /// Embedding boundary. Credentials are never included in Debug or errors.
    pub fn new(endpoint: &str, token: String, options: ClientOptions) -> Result<Self, ClientError> {
        let mut url = reqwest::Url::parse(endpoint).map_err(|_| ClientError::Protocol)?;
        if url.scheme() != "http"
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.host_str().is_some_and(|h| {
                h == "localhost"
                    || h.trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|i| i.is_loopback())
            })
            || token.len() < 24
            || token.len() > 256
            || token.chars().any(char::is_whitespace)
            || options.max_concurrent_requests == 0
            || options.max_concurrent_requests > 1024
            || !(4096..=16 * 1024 * 1024).contains(&options.max_response_bytes)
            || options.request_timeout.is_zero()
            || options.request_timeout > Duration::from_secs(60)
        {
            return Err(ClientError::Capacity);
        }
        url.set_path("/");
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(options.request_timeout)
            .build()
            .map_err(|_| ClientError::Connection)?;
        let (closed, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                endpoint: url,
                token,
                requests: Arc::new(Semaphore::new(options.max_concurrent_requests)),
                options,
                closed,
            }),
        })
    }
    fn url(&self, path: &str) -> Result<reqwest::Url, ClientError> {
        self.inner
            .endpoint
            .join(path)
            .map_err(|_| ClientError::Protocol)
    }
    async fn request<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
        key: Option<&str>,
    ) -> Result<T, ClientError> {
        if *self.inner.closed.borrow() {
            return Err(ClientError::Closed);
        }
        let _permit = self
            .inner
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| ClientError::Capacity)?;
        let task = async {
            let mut request = self
                .inner
                .http
                .request(method, self.url(path)?)
                .bearer_auth(&self.inner.token);
            if let Some(body) = body {
                let bytes = serde_json::to_vec(&body).map_err(|_| ClientError::Protocol)?;
                if bytes.len() > 1024 * 1024 {
                    return Err(ClientError::Capacity);
                }
                request = request
                    .header("content-type", "application/json")
                    .body(bytes);
            }
            let response = request.send().await.map_err(|_| ClientError::Connection)?;
            let status = response.status();
            let bytes = self.collect(response).await?;
            if !status.is_success() {
                return Err(ClientError::Api(
                    serde_json::from_slice(&bytes).map_err(|_| ClientError::Protocol)?,
                ));
            }
            serde_json::from_slice(&bytes).map_err(|_| ClientError::Protocol)
        };
        let result = tokio::time::timeout(self.inner.options.request_timeout, task)
            .await
            .unwrap_or(Err(ClientError::Timeout));
        match (result, key) {
            (
                Err(
                    ClientError::Connection
                    | ClientError::Timeout
                    | ClientError::Protocol
                    | ClientError::Capacity,
                ),
                Some(key),
            ) => Err(ClientError::UnknownMutation { key: key.into() }),
            (result, _) => result,
        }
    }
    async fn collect(&self, response: reqwest::Response) -> Result<Vec<u8>, ClientError> {
        if response
            .content_length()
            .is_some_and(|n| n > self.inner.options.max_response_bytes as u64)
        {
            return Err(ClientError::Capacity);
        }
        let mut stream = response.bytes_stream();
        let mut bytes = vec![];
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| ClientError::Connection)?;
            if chunk.len()
                > self
                    .inner
                    .options
                    .max_response_bytes
                    .saturating_sub(bytes.len())
            {
                return Err(ClientError::Capacity);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request(reqwest::Method::GET, path, None, None).await
    }
    fn segment(value: &str) -> Result<String, ClientError> {
        if value.is_empty() || value.len() > 4096 {
            return Err(ClientError::Capacity);
        }
        let mut url = reqwest::Url::parse("http://localhost/").unwrap();
        url.path_segments_mut().unwrap().push(value);
        Ok(url.path().trim_start_matches('/').into())
    }
    pub async fn overview(&self) -> Result<Overview, ClientError> {
        self.get("api/v1/overview").await
    }
    pub async fn runtime(&self) -> Result<PoolView, ClientError> {
        self.get("api/v1/runtime").await
    }
    pub async fn activity(&self, page: PageRequest) -> Result<Page<Operation>, ClientError> {
        self.paged("api/v1/activity", page).await
    }
    pub async fn source_templates(
        &self,
        filter: &str,
    ) -> Result<Vec<crate::management_protocol::definitions::CreateSourceStreamRequest>, ClientError>
    {
        self.paged(
            "api/v1/instruments",
            PageRequest {
                filter: Some(filter.into()),
                limit: Some(128),
                after: None,
            },
        )
        .await
    }
    pub async fn capabilities(&self) -> Result<Capabilities, ClientError> {
        self.get("api/v1/capabilities").await
    }
    pub async fn streams(&self, page: PageRequest) -> Result<Page<StreamView>, ClientError> {
        self.paged("api/v1/streams", page).await
    }
    pub async fn operations(&self, page: PageRequest) -> Result<Page<Operation>, ClientError> {
        self.paged("api/v1/operations", page).await
    }
    async fn paged<T: DeserializeOwned>(
        &self,
        path: &str,
        page: PageRequest,
    ) -> Result<T, ClientError> {
        let mut url = self.url(path)?;
        {
            let mut q = url.query_pairs_mut();
            if let Some(a) = page.after {
                q.append_pair("after", &a);
            }
            if let Some(f) = page.filter {
                q.append_pair("filter", &f);
            }
            if let Some(l) = page.limit {
                q.append_pair("limit", &l.to_string());
            }
        }
        self.get(url.as_str()).await
    }
    pub async fn stream(&self, id: &str) -> Result<StreamView, ClientError> {
        self.get(&format!("api/v1/streams/{}", Self::segment(id)?))
            .await
    }
    pub async fn integrity(&self, id: &str) -> Result<serde_json::Value, ClientError> {
        self.get(&format!("api/v1/streams/{}/integrity", Self::segment(id)?))
            .await
    }
    pub async fn clients(&self) -> Result<serde_json::Value, ClientError> {
        self.get("api/v1/clients").await
    }
    pub async fn config(&self) -> Result<Vec<SettingView>, ClientError> {
        self.get("api/v1/config").await
    }
    pub async fn preview(&self, action: Action) -> Result<ChangePlan, ClientError> {
        self.request(
            reqwest::Method::POST,
            "api/v1/change-plans",
            Some(
                serde_json::to_value(PreviewRequest { action })
                    .map_err(|_| ClientError::Protocol)?,
            ),
            None,
        )
        .await
    }
    pub async fn plan(&self, id: &str) -> Result<ChangePlan, ClientError> {
        self.get(&format!("api/v1/change-plans/{}", Self::segment(id)?))
            .await
    }
    pub async fn apply(&self, id: &str, key: &str) -> Result<Operation, ClientError> {
        self.request(
            reqwest::Method::POST,
            &format!("api/v1/change-plans/{}/apply", Self::segment(id)?),
            Some(serde_json::json!({"idempotency_key":key})),
            Some(key),
        )
        .await
    }
    pub async fn operation(&self, id: &str) -> Result<Operation, ClientError> {
        self.get(&format!("api/v1/operations/{}", Self::segment(id)?))
            .await
    }
    pub async fn wait_operation(
        &self,
        id: &str,
        deadline: Duration,
    ) -> Result<Operation, ClientError> {
        let end = tokio::time::Instant::now() + deadline;
        loop {
            let op = self.operation(id).await?;
            if op.cleanup_complete {
                return Ok(op);
            }
            if tokio::time::Instant::now() >= end {
                return Err(ClientError::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn acquire_lease(&self, request: LeaseRequest) -> Result<DemandLease, ClientError> {
        let key = request.idempotency_key.clone();
        self.request(
            reqwest::Method::POST,
            "api/v1/demand-leases",
            Some(serde_json::to_value(request).map_err(|_| ClientError::Protocol)?),
            Some(&key),
        )
        .await
    }
    pub async fn renew_lease(&self, id: &str, seconds: u64) -> Result<DemandLease, ClientError> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let counter = NEXT
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |n| n.checked_add(1),
            )
            .map_err(|_| ClientError::Capacity)?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        self.renew_lease_with_key(
            id,
            seconds,
            &format!("renew-{}-{timestamp}-{counter}", std::process::id()),
        )
        .await
    }
    pub async fn renew_lease_with_key(
        &self,
        id: &str,
        seconds: u64,
        key: &str,
    ) -> Result<DemandLease, ClientError> {
        self.request(
            reqwest::Method::POST,
            &format!("api/v1/demand-leases/{}/renew", Self::segment(id)?),
            Some(serde_json::json!({"seconds":seconds,"idempotency_key":key})),
            Some(key),
        )
        .await
    }
    pub async fn release_lease(&self, id: &str) -> Result<LeaseRelease, ClientError> {
        self.release_lease_with_override(id, false).await
    }
    /// Explicit administrator override; normal release always remains owner-scoped.
    pub async fn release_lease_with_override(
        &self,
        id: &str,
        force: bool,
    ) -> Result<LeaseRelease, ClientError> {
        self.request(
            reqwest::Method::DELETE,
            &format!("api/v1/demand-leases/{}?force={force}", Self::segment(id)?),
            None,
            Some(&format!("release:{id}")),
        )
        .await
    }
    pub async fn close(&self) {
        let _ = self.inner.closed.send(true);
    }
    pub fn notifications(&self) -> Notifications {
        let (tx, receiver) = mpsc::channel(16);
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let client = self.clone();
        let mut closed = self.inner.closed.subscribe();
        let task = tokio::spawn(async move {
            let mut delay = Duration::from_millis(100);
            loop {
                let observe = async {
                    let view = client.overview().await?;
                    let mut url = client.url("api/v1/events")?;
                    url.query_pairs_mut()
                        .append_pair("instance", &view.daemon_instance)
                        .append_pair("after", &view.event_cursor);
                    tx.send(Ok(Notification::Snapshot(view)))
                        .await
                        .map_err(|_| ClientError::Closed)?;
                    let response = client
                        .inner
                        .http
                        .get(url)
                        .bearer_auth(&client.inner.token)
                        .send()
                        .await
                        .map_err(|_| ClientError::Connection)?;
                    if !response.status().is_success() {
                        return Err(ClientError::Connection);
                    }
                    let mut chunks = response.bytes_stream();
                    let mut buffer = Vec::new();
                    while let Some(chunk) = chunks.next().await {
                        let chunk = chunk.map_err(|_| ClientError::Connection)?;
                        if chunk.len()
                            > client
                                .inner
                                .options
                                .max_response_bytes
                                .saturating_sub(buffer.len())
                        {
                            return Err(ClientError::Capacity);
                        }
                        buffer.extend_from_slice(&chunk);
                        while let Some((end, delimiter)) = buffer
                            .windows(2)
                            .position(|b| b == b"\n\n")
                            .map(|n| (n, 2))
                            .or_else(|| {
                                buffer
                                    .windows(4)
                                    .position(|b| b == b"\r\n\r\n")
                                    .map(|n| (n, 4))
                            })
                        {
                            let frame = std::str::from_utf8(&buffer[..end])
                                .map_err(|_| ClientError::Protocol)?;
                            let kind = frame
                                .lines()
                                .find_map(|l| l.strip_prefix("event:").map(str::trim));
                            let data = frame
                                .lines()
                                .filter_map(|l| l.strip_prefix("data:").map(str::trim))
                                .collect::<Vec<_>>()
                                .join("\n");
                            let event = match kind {
                                Some("notification") => Some(Notification::Event(
                                    serde_json::from_str(&data)
                                        .map_err(|_| ClientError::Protocol)?,
                                )),
                                Some("resync_required") => return Err(ClientError::Protocol),
                                _ => None,
                            };
                            if let Some(event) = event {
                                tx.try_send(Ok(event)).map_err(|_| ClientError::Capacity)?;
                            }
                            buffer.drain(..end + delimiter);
                        }
                    }
                    Err::<(), ClientError>(ClientError::Connection)
                };
                tokio::select! {_= &mut stopped=>break,_=closed.changed()=>break,result=observe=>{
                    if tx.try_send(Ok(Notification::Disconnected)).is_err(){break;}
                    if matches!(result,Err(ClientError::Closed|ClientError::Capacity)){let _=tx.try_send(Err(ClientError::Capacity));break;}
                }}
                tokio::select! {_= &mut stopped=>break,_=closed.changed()=>break,_=tx.closed()=>break,_=tokio::time::sleep(delay)=>()}
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        });
        Notifications {
            receiver,
            stop: Some(stop),
            task: Some(task),
        }
    }
}
#[derive(Debug)]
pub enum Notification {
    Snapshot(Overview),
    Event(ManagementEvent),
    Disconnected,
}
pub struct Notifications {
    receiver: mpsc::Receiver<Result<Notification, ClientError>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Notifications {
    pub async fn recv(&mut self) -> Option<Result<Notification, ClientError>> {
        self.receiver.recv().await
    }
    pub async fn close(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Notifications {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
