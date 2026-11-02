//! Thin authenticated, loopback-only Axum adapter.
mod connection;
use crate::{live::management::*, management_protocol::*};
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, watch};
#[derive(Clone)]
struct Api {
    management: ManagementHandle,
    credentials: Arc<Credentials>,
    connections: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    observers: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
}
struct ApiError(ManagementError);
impl From<ManagementError> for ApiError {
    fn from(e: ManagementError) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.code.as_str() {
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            "capacity" => StatusCode::TOO_MANY_REQUESTS,
            "too_large" => StatusCode::PAYLOAD_TOO_LARGE,
            "stale_plan" | "revision_conflict" | "idempotency_conflict" | "config_conflict" => {
                StatusCode::CONFLICT
            }
            "shutting_down" => StatusCode::SERVICE_UNAVAILABLE,
            "request_timeout" => StatusCode::REQUEST_TIMEOUT,
            _ => StatusCode::BAD_REQUEST,
        };
        let correlation = self.0.correlation_id.clone();
        let mut response = (status, Json(self.0)).into_response();
        if let Ok(value) = HeaderValue::from_str(&correlation) {
            response.headers_mut().insert("x-correlation-id", value);
        }
        response
    }
}
type ApiResult = std::result::Result<Response, ApiError>;
fn failure(code: &str, detail: &str) -> ApiError {
    ApiError(crate::live::management::error(code, detail))
}
fn json<T: Serialize>(api: &Api, value: T) -> ApiResult {
    struct Bounded {
        bytes: Vec<u8>,
        max: usize,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if b.len() > self.max.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("response capacity"));
            }
            self.bytes.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: vec![],
        max: api.management.limits().max_response_bytes,
    };
    serde_json::to_writer(&mut writer, &value).map_err(|_| {
        failure(
            "too_large",
            "response exceeds configured budget; use pagination",
        )
    })?;
    Ok(([("content-type", "application/json")], writer.bytes).into_response())
}
fn body<T>(
    value: std::result::Result<Json<T>, axum::extract::rejection::JsonRejection>,
) -> std::result::Result<T, ApiError> {
    value
        .map(|Json(v)| v)
        .map_err(|_| failure("invalid_json", "malformed or unsupported request body"))
}
async fn guard(State(api): State<Api>, request: Request, next: Next) -> Response {
    let _permit = match api.requests.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return failure("capacity", "concurrent request capacity").into_response(),
    };
    let token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let principal = match token.and_then(|t| api.credentials.authenticate(t).ok()) {
        Some(p) => p,
        None => {
            return failure("unauthorized", "valid management credentials required")
                .into_response();
        }
    };
    let (mut parts, body) = request.into_parts();
    parts.extensions.insert(principal);
    let deadline = Duration::from_millis(api.management.limits().request_timeout_ms);
    let future = async {
        let bytes = axum::body::to_bytes(body, api.management.limits().max_body_bytes)
            .await
            .map_err(|_| failure("too_large", "request body exceeds configured limit"))?;
        Ok::<_, ApiError>(
            next.run(Request::from_parts(parts, axum::body::Body::from(bytes)))
                .await,
        )
    };
    match tokio::time::timeout(deadline, future).await {
        Ok(Ok(mut r)) => {
            if r.status().is_client_error()
                && !r
                    .headers()
                    .get("content-type")
                    .is_some_and(|v| v.as_bytes().starts_with(b"application/json"))
            {
                let status = r.status();
                r = failure(
                    if status == StatusCode::NOT_FOUND {
                        "not_found"
                    } else {
                        "invalid_request"
                    },
                    "unsupported route, query, or extraction",
                )
                .into_response();
                *r.status_mut() = status;
            }
            r
        }
        Ok(Err(e)) => e.into_response(),
        Err(_) => failure(
            "request_timeout",
            "request deadline; accepted mutations may still be executing",
        )
        .into_response(),
    }
}
pub struct ManagementHttpServer {
    pub address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    deadline: Duration,
}
impl ManagementHttpServer {
    pub async fn bind(
        address: SocketAddr,
        management: ManagementHandle,
        credentials: Credentials,
    ) -> std::io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(std::io::Error::other("management v1 requires loopback"));
        }
        let listener = tokio::net::TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (stop, shutdown) = watch::channel(false);
        let limits = management.limits().clone();
        let connections = Arc::new(Semaphore::new(limits.max_clients));
        let api = Api {
            connections: connections.clone(),
            management,
            credentials: Arc::new(credentials),
            requests: Arc::new(Semaphore::new(limits.max_requests)),
            observers: Arc::new(Semaphore::new(limits.sse_clients)),
            shutdown,
        };
        let listener = connection::LimitedListener {
            listener,
            permits: connections,
            timeout: Duration::from_millis(limits.request_timeout_ms),
        };
        let routes = Router::new()
            .route("/api/v1/overview", get(overview))
            .route("/api/v1/capabilities", get(capabilities))
            .route("/api/v1/instruments", get(instruments))
            .route("/api/v1/streams", get(streams))
            .route("/api/v1/streams/{id}", get(stream))
            .route("/api/v1/streams/{id}/integrity", get(integrity))
            .route("/api/v1/runtime", get(runtime))
            .route("/api/v1/clients", get(clients))
            .route("/api/v1/activity", get(operations))
            .route("/api/v1/config", get(config))
            .route("/api/v1/change-plans", post(preview))
            .route("/api/v1/change-plans/{id}", get(plan))
            .route("/api/v1/change-plans/{id}/apply", post(apply))
            .route("/api/v1/demand-leases", post(lease))
            .route("/api/v1/demand-leases/{id}/renew", post(renew))
            .route("/api/v1/demand-leases/{id}", delete(release))
            .route("/api/v1/operations", get(operations))
            .route("/api/v1/operations/{id}", get(operation))
            .route("/api/v1/events", get(events))
            .layer(DefaultBodyLimit::max(limits.max_body_bytes))
            .layer(middleware::from_fn_with_state(api.clone(), guard))
            .with_state(api);
        let mut closing = stop.subscribe();
        let task = tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(async move {
                    let _ = closing.changed().await;
                })
                .await
        });
        Ok(Self {
            address,
            stop,
            task: Some(task),
            deadline: Duration::from_millis(limits.request_timeout_ms),
        })
    }
    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        let _ = self.stop.send(true);
        if let Some(task) = &mut self.task {
            tokio::time::timeout(self.deadline, task)
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "HTTP cleanup remains owned")
                })?
                .map_err(std::io::Error::other)??;
        }
        self.task.take();
        Ok(())
    }
}
impl Drop for ManagementHttpServer {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
async fn overview(State(a): State<Api>, Extension(p): Extension<Principal>) -> ApiResult {
    json(&a, a.management.overview(&p)?)
}
async fn capabilities(State(a): State<Api>, Extension(p): Extension<Principal>) -> ApiResult {
    json(&a, a.management.capabilities(&p)?)
}
async fn streams(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Query(q): Query<PageRequest>,
) -> ApiResult {
    json(&a, a.management.streams(&p, q)?)
}
async fn stream(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult {
    json(&a, a.management.stream(&p, &id)?)
}
async fn integrity(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult {
    json(&a, a.management.integrity(&p, &id)?)
}
async fn runtime(State(a): State<Api>, Extension(p): Extension<Principal>) -> ApiResult {
    json(&a, a.management.runtime(&p)?)
}
async fn clients(State(a): State<Api>, Extension(p): Extension<Principal>) -> ApiResult {
    let mut value = a.management.clients(&p)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "management_http_connections".into(),
            (a.management.limits().max_clients - a.connections.available_permits()).into(),
        );
        object.insert(
            "sse_observers".into(),
            (a.management.limits().sse_clients - a.observers.available_permits()).into(),
        );
    }
    json(&a, value)
}
async fn config(State(a): State<Api>, Extension(p): Extension<Principal>) -> ApiResult {
    json(&a, a.management.config(&p)?)
}
async fn operations(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Query(q): Query<PageRequest>,
) -> ApiResult {
    json(&a, a.management.operations(&p, q)?)
}
async fn operation(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult {
    json(&a, a.management.operation(&p, &id)?)
}
async fn preview(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    value: std::result::Result<Json<PreviewRequest>, axum::extract::rejection::JsonRejection>,
) -> ApiResult {
    json(&a, a.management.preview(&p, body(value)?)?)
}
async fn plan(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult {
    json(&a, a.management.plan(&p, &id)?)
}
async fn apply(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    value: std::result::Result<Json<ApplyRequest>, axum::extract::rejection::JsonRejection>,
) -> ApiResult {
    let mut r = json(&a, a.management.apply(&p, &id, body(value)?)?)?;
    *r.status_mut() = StatusCode::ACCEPTED;
    Ok(r)
}
async fn lease(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    value: std::result::Result<Json<LeaseRequest>, axum::extract::rejection::JsonRejection>,
) -> ApiResult {
    json(&a, a.management.acquire_lease(&p, body(value)?).await?)
}
async fn renew(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    value: std::result::Result<Json<RenewLease>, axum::extract::rejection::JsonRejection>,
) -> ApiResult {
    json(&a, a.management.renew_lease(&p, &id, body(value)?)?)
}
#[derive(Deserialize)]
struct ReleaseQuery {
    #[serde(default)]
    force: bool,
}
async fn release(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    Query(q): Query<ReleaseQuery>,
) -> ApiResult {
    json(&a, a.management.release_lease(&p, &id, q.force).await?)
}
#[derive(Deserialize)]
struct EventQuery {
    instance: Option<String>,
    after: Option<String>,
}
async fn events(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Query(q): Query<EventQuery>,
    headers: axum::http::HeaderMap,
) -> ApiResult {
    let permit = a
        .observers
        .clone()
        .try_acquire_owned()
        .map_err(|_| failure("capacity", "SSE connection capacity"))?;
    let cursor = headers
        .get("last-event-id")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.rsplit_once(':'))
        .map(|(i, c)| (i.to_owned(), c.to_owned()));
    let (instance, after) = cursor
        .map(|(i, c)| (Some(i), Some(c)))
        .unwrap_or((q.instance, q.after));
    let after = after
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| failure("invalid_cursor", "invalid notification cursor"))?;
    let (tx, rx) = tokio::sync::mpsc::channel(9);
    let mut closing = a.shutdown.clone();
    let initial = if after.is_none() {
        Some(a.management.overview(&p)?)
    } else {
        None
    };
    let mut after =
        after.unwrap_or_else(|| initial.as_ref().unwrap().event_cursor.parse().unwrap());
    let instance = instance.unwrap_or_else(|| a.management.instance().to_string());
    tokio::spawn(async move {
        let _permit = permit;
        if let Some(view) = initial {
            if let Ok(data) = serde_json::to_string(&view) {
                if data.len() > a.management.limits().max_response_bytes {
                    return;
                }
                if tx
                    .send(Ok::<_, Infallible>(
                        Event::default()
                            .event("snapshot")
                            .id(format!("{}:{after}", a.management.instance()))
                            .data(data),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            let page = match a.management.events(&p, Some(&instance), after) {
                Ok(p) => p,
                Err(_) => break,
            };
            if page.resync_required {
                let _ = tx.try_send(Ok(Event::default()
                    .event("resync_required")
                    .data("notification cursor expired; fetch overview")));
                break;
            }
            for event in page.events {
                if tx.capacity() <= 1 {
                    let _ = tx.try_send(Ok(Event::default()
                        .event("resync_required")
                        .data("slow observer; fetch overview")));
                    return;
                }
                after = event.cursor.parse().unwrap();
                let id = format!("{}:{after}", a.management.instance());
                let Ok(data) = serde_json::to_string(&event) else {
                    return;
                };
                if tx
                    .try_send(Ok(Event::default().event("notification").id(id).data(data)))
                    .is_err()
                {
                    return;
                }
            }
            tokio::select! {_=closing.changed()=>break,_=tx.closed()=>break,_=a.management.changed()=>(),_=tick.tick()=>()}
        }
    });
    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response())
}

async fn instruments(
    State(a): State<Api>,
    Extension(p): Extension<Principal>,
    Query(q): Query<PageRequest>,
) -> ApiResult {
    json(
        &a,
        a.management
            .source_templates(&p, q.filter.unwrap_or_default(), q.limit.unwrap_or(64))
            .await?,
    )
}
