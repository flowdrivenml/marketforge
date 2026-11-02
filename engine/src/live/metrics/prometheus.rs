//! Bounded read-only HTTP exposition of completed collector snapshots.
use super::*;
use crate::live::config::general::PrometheusConfig;
use http_body_util::Full;
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
#[derive(Default)]
pub struct SamplingState {
    pub failures: std::sync::atomic::AtomicU64,
    pub completed: Mutex<Option<(Instant, Duration)>>,
}
impl SamplingState {
    pub fn success(&self, duration: Duration) {
        *self.completed.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((Instant::now(), duration));
    }
    pub fn age(&self) -> Option<Duration> {
        self.completed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .map(|(at, _)| at.elapsed())
    }
}
struct Encoder {
    text: String,
    max: usize,
}
impl std::fmt::Write for Encoder {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if s.len() > self.max.saturating_sub(self.text.len()) {
            return Err(std::fmt::Error);
        }
        self.text.push_str(s);
        Ok(())
    }
}
fn label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
pub fn encode(
    snapshot: &MetricsSnapshot,
    sampling: &SamplingState,
    max: usize,
) -> Result<String, std::fmt::Error> {
    use std::fmt::Write;
    let mut out = Encoder {
        text: String::new(),
        max,
    };
    macro_rules! metric {
        ($name:literal,$kind:literal,$help:literal,$value:expr) => {
            writeln!(
                out,
                concat!(
                    "# HELP marketforge_",
                    $name,
                    " ",
                    $help,
                    "\n# TYPE marketforge_",
                    $name,
                    " ",
                    $kind,
                    "\nmarketforge_",
                    $name,
                    " {}"
                ),
                $value
            )?;
        };
    }
    metric!(
        "uptime_seconds",
        "gauge",
        "Monotonic application uptime seconds.",
        snapshot.runtime.uptime_seconds
    );
    metric!(
        "lifecycle_starts_total",
        "counter",
        "New lifecycle generations admitted.",
        snapshot.runtime.lifecycle_starts_total
    );
    metric!(
        "lifecycle_stops_total",
        "counter",
        "Lifecycle retirements begun.",
        snapshot.runtime.lifecycle_stops_total
    );
    metric!(
        "lifecycle_failures_total",
        "counter",
        "Current-generation transitions into Failed.",
        snapshot.runtime.lifecycle_failures_total
    );
    metric!(
        "reconciliations_total",
        "counter",
        "Completed reconciliation inspections.",
        snapshot.runtime.reconciliations_total
    );
    metric!(
        "incomplete_shutdown_total",
        "counter",
        "Shutdown waits that exceeded their deadline.",
        snapshot.runtime.incomplete_shutdown_total
    );
    metric!(
        "router_accepted_publications_total",
        "counter",
        "Publications accepted at Router, including controls.",
        snapshot.runtime.router_accepted_publications_total
    );
    metric!(
        "router_capacity_failures_total",
        "counter",
        "Ingress rejections and consumer terminations due to queue capacity.",
        snapshot.runtime.internal_queue_overflows_total
    );
    metric!(
        "router_ingress_publications",
        "gauge",
        "Current ingress publication occupancy.",
        snapshot.runtime.router_ingress_publications
    );
    metric!(
        "router_ingress_bytes",
        "gauge",
        "Current retained ingress bytes.",
        snapshot.runtime.router_ingress_bytes
    );
    metric!(
        "router_consumer_publications",
        "gauge",
        "Current retained consumer publication occupancy.",
        snapshot.runtime.router_consumer_publications
    );
    metric!(
        "router_consumer_bytes",
        "gauge",
        "Current retained consumer bytes.",
        snapshot.runtime.router_consumer_bytes
    );
    metric!(
        "limiter_pending",
        "gauge",
        "Requests currently awaiting shared limiter admission.",
        snapshot.runtime.limiter_pending
    );
    metric!(
        "limiter_budgets",
        "gauge",
        "Shared compiled limiter budgets.",
        snapshot.runtime.limiter_budgets
    );
    if let Some(at) = snapshot.collected_at_unix_ns {
        metric!(
            "snapshot_timestamp_seconds",
            "gauge",
            "Completed snapshot wall-clock timestamp.",
            at as f64 / 1e9
        );
    }
    metric!(
        "source_events_total",
        "counter",
        "Source market events accepted by Router since application owner start.",
        snapshot.runtime.source_events_total
    );
    metric!(
        "derived_events_total",
        "counter",
        "Derived market events accepted by Router since application owner start.",
        snapshot.runtime.derived_events_total
    );
    metric!(
        "source_streams_running",
        "gauge",
        "Source streams ready to publish.",
        snapshot.runtime.source_streams_running
    );
    metric!(
        "derived_streams_running",
        "gauge",
        "Derived streams ready to publish.",
        snapshot.runtime.derived_streams_running
    );
    metric!(
        "streams_failed",
        "gauge",
        "Streams retaining a failed lifecycle.",
        snapshot.runtime.streams_failed
    );
    metric!(
        "collection_duration_seconds",
        "gauge",
        "Latest collector assembly duration.",
        snapshot.collection_duration_ns as f64 / 1e9
    );
    metric!(
        "snapshot_revision",
        "gauge",
        "Completed collection revision.",
        snapshot.revision
    );
    metric!(
        "collection_failures_total",
        "counter",
        "Failed collection attempts.",
        sampling.failures.load(std::sync::atomic::Ordering::Relaxed)
    );
    if let Some(age) = sampling.age() {
        metric!(
            "snapshot_age_seconds",
            "gauge",
            "Age of latest successful collection.",
            age.as_secs_f64()
        );
    }
    if let Some((_, duration)) = *sampling.completed.lock().unwrap_or_else(|p| p.into_inner()) {
        metric!(
            "sampling_duration_seconds",
            "gauge",
            "Latest observation and collection duration.",
            duration.as_secs_f64()
        );
    }
    if let Some(rss) = snapshot.runtime.memory_rss_bytes {
        metric!(
            "process_resident_memory_bytes",
            "gauge",
            "Process resident memory; omitted if unavailable.",
            rss
        );
    }
    if let Some(cpu) = snapshot.runtime.cpu_usage_percent {
        metric!(
            "process_cpu_cores",
            "gauge",
            "CPU usage in logical cores; one fully used logical CPU is one.",
            cpu / 100.0
        );
    }
    metric!(
        "recording_available",
        "gauge",
        "Recorder measurement owner exists; disabled application recorder is unavailable.",
        u8::from(snapshot.recording.available)
    );
    if snapshot.recording.available {
        metric!(
            "recording_committed_records_total",
            "counter",
            "Records with a confirmed complete database commit.",
            snapshot.recording.records_written_total
        );
        metric!(
            "recording_failed_attempts_total",
            "counter",
            "Failed or ambiguous recording attempts.",
            snapshot.recording.write_failures_total
        );
        metric!(
            "recording_pending_records",
            "gauge",
            "Queued plus in-flight recording work.",
            snapshot.recording.pending_records
        );
    }
    metric!(
        "native_messages_total",
        "counter",
        "Native data messages observed by Source processing in this process.",
        snapshot.runtime.source_messages_total
    );
    metric!(
        "normalization_errors_total",
        "counter",
        "Source JSON and normalization failures in this process.",
        snapshot.runtime.source_normalization_errors_total
    );
    metric!(
        "source_reacquisitions_process_total",
        "counter",
        "Confirmed Source reacquisitions in this application owner.",
        snapshot.runtime.source_reacquisitions_total
    );
    metric!(
        "recovery_requests_total",
        "counter",
        "Recovery requests registered for Source owners in this process.",
        snapshot.runtime.source_recovery_requests_total
    );
    metric!(
        "websocket_observations_available",
        "gauge",
        "Whether bounded infrastructure observation completed.",
        u8::from(snapshot.runtime.websocket_slots.is_some())
    );
    if let Some(slots) = snapshot.runtime.websocket_slots {
        metric!(
            "websocket_slots",
            "gauge",
            "Existing WebSocket manager slots.",
            slots
        );
    }
    if let Some(streams) = snapshot.runtime.websocket_streams {
        metric!(
            "websocket_streams",
            "gauge",
            "Existing WebSocket manager logical attachments.",
            streams
        );
    }
    metric!(
        "publication_connections_active",
        "gauge",
        "Active Unix connections.",
        snapshot.publication.connections_active
    );
    metric!(
        "publication_subscriptions_active",
        "gauge",
        "Active Unix subscriptions.",
        snapshot.publication.subscriptions_active
    );
    metric!(
        "publication_queued_frames",
        "gauge",
        "Encoded frames queued, awaiting admission or partially written.",
        snapshot.publication.queued_frames
    );
    metric!(
        "publication_frame_capacity_failures_total",
        "counter",
        "Complete frames that exceeded framing or schema collection capacity.",
        snapshot.publication.frame_capacity_failures_total
    );
    metric!(
        "process_cpu_available",
        "gauge",
        "Whether process CPU is available in this snapshot.",
        u8::from(snapshot.runtime.cpu_usage_percent.is_some())
    );
    metric!(
        "process_rss_available",
        "gauge",
        "Whether process RSS is available in this snapshot.",
        u8::from(snapshot.runtime.memory_rss_bytes.is_some())
    );
    metric!(
        "publication_queued_bytes",
        "gauge",
        "Encoded payload bytes queued or partially written.",
        snapshot.publication.queued_bytes_total
    );
    metric!(
        "publication_events_queued_total",
        "counter",
        "Successful event admissions including fanout.",
        snapshot.publication.events_queued_total
    );
    metric!(
        "publication_frames_written_total",
        "counter",
        "Control and stream frames completely written to the socket.",
        snapshot.publication.frames_written_total
    );
    metric!(
        "publication_bytes_written_total",
        "counter",
        "Control and stream frame bytes including prefixes completely written.",
        snapshot.publication.bytes_written_total
    );
    metric!(
        "publication_write_failures_total",
        "counter",
        "Failed or timed out control or stream writes.",
        snapshot.publication.write_failures_total
    );
    metric!(
        "publication_queue_overflows_total",
        "counter",
        "Publication queue capacity failures.",
        snapshot.publication.queue_overflows_total
    );
    writeln!(
        out,
        "# HELP marketforge_stream_generation Current runtime generation.\n# TYPE marketforge_stream_generation gauge\n# HELP marketforge_source_messages_total Native data messages observed at Source processing entry.\n# TYPE marketforge_source_messages_total counter\n# HELP marketforge_source_normalization_errors_total Native JSON or normalization failures.\n# TYPE marketforge_source_normalization_errors_total counter\n# HELP marketforge_stream_accepted_events_total Market events accepted by Router in this generation.\n# TYPE marketforge_stream_accepted_events_total counter\n# HELP marketforge_source_reacquisitions_total Confirmed successful Source reacquisitions.\n# TYPE marketforge_source_reacquisitions_total counter\n# HELP marketforge_processing_interval_mean_seconds Mean observed processing duration in the completed interval.\n# TYPE marketforge_processing_interval_mean_seconds gauge\n# HELP marketforge_processing_interval_max_seconds Maximum processing duration in the completed interval.\n# TYPE marketforge_processing_interval_max_seconds gauge\n# HELP marketforge_processing_interval_samples Number of observations in the completed interval.\n# TYPE marketforge_processing_interval_samples gauge"
    )?;
    writeln!(
        out,
        "# HELP marketforge_source_delay_seconds Signed Source-processing receipt minus exchange event time; unavailable observations omitted.\n# TYPE marketforge_source_delay_seconds gauge\n# HELP marketforge_depth_events_total Accepted complete native depth operations, excluding handoff controls.\n# TYPE marketforge_depth_events_total counter\n# HELP marketforge_depth_levels_total Accepted native depth levels, excluding handoff controls.\n# TYPE marketforge_depth_levels_total counter\n# HELP marketforge_processing_last_seconds Latest observed processing duration, possibly from a previous interval.\n# TYPE marketforge_processing_last_seconds gauge\n# HELP marketforge_conversion_failures_total Failed valuation attempts.\n# TYPE marketforge_conversion_failures_total counter\n# HELP marketforge_ordering_failures_total Detected emission-frontier regressions.\n# TYPE marketforge_ordering_failures_total counter"
    )?;
    for (id, s) in &snapshot.sources {
        let id = label(id);
        if let Some(delay) = s.metrics.source_delay_ns {
            writeln!(
                out,
                "marketforge_source_delay_seconds{{stream=\"{id}\"}} {}",
                delay as f64 / 1e9
            )?;
        }
        writeln!(
            out,
            "marketforge_depth_events_total{{stream=\"{id}\",kind=\"source\"}} {}\nmarketforge_depth_levels_total{{stream=\"{id}\",kind=\"source\"}} {}",
            s.metrics.depth_events_accepted_total, s.metrics.depth_levels_accepted_total
        )?;
        writeln!(
            out,
            "marketforge_stream_generation{{stream=\"{id}\",kind=\"source\"}} {}\nmarketforge_source_messages_total{{stream=\"{id}\"}} {}\nmarketforge_source_normalization_errors_total{{stream=\"{id}\"}} {}\nmarketforge_stream_accepted_events_total{{stream=\"{id}\",kind=\"source\"}} {}\nmarketforge_source_reacquisitions_total{{stream=\"{id}\"}} {}",
            s.generation,
            s.metrics.messages_received_total,
            s.metrics.normalization_errors_total,
            s.metrics.events_produced_total,
            s.metrics.reconnections_total
        )?;
        latency(&mut out, &id, "source", s.metrics.processing_latency)?;
    }
    writeln!(
        out,
        "# HELP marketforge_derived_calculated_events_total Successfully calculated non-reference market output.\n# TYPE marketforge_derived_calculated_events_total counter\n# HELP marketforge_derived_mailbox_events_total Output events admitted to the processor mailbox.\n# TYPE marketforge_derived_mailbox_events_total counter\n# HELP marketforge_derived_input_events_total Input market events taken into CPU-owned processing, including references.\n# TYPE marketforge_derived_input_events_total counter\n# HELP marketforge_derived_retained_bytes Retained processor work bytes.\n# TYPE marketforge_derived_retained_bytes gauge"
    )?;
    for (id, s) in &snapshot.derived {
        let id = label(id);
        writeln!(
            out,
            "marketforge_depth_events_total{{stream=\"{id}\",kind=\"derived\"}} {}\nmarketforge_depth_levels_total{{stream=\"{id}\",kind=\"derived\"}} {}\nmarketforge_conversion_failures_total{{stream=\"{id}\"}} {}\nmarketforge_ordering_failures_total{{stream=\"{id}\"}} {}",
            s.metrics.depth_events_accepted_total,
            s.metrics.depth_levels_accepted_total,
            s.metrics.conversion_failures_total,
            s.metrics.late_events_total
        )?;
        writeln!(
            out,
            "marketforge_derived_calculated_events_total{{stream=\"{id}\"}} {}\nmarketforge_derived_mailbox_events_total{{stream=\"{id}\"}} {}\nmarketforge_derived_input_events_total{{stream=\"{id}\"}} {}\nmarketforge_derived_retained_bytes{{stream=\"{id}\"}} {}",
            s.metrics.events_calculated_total,
            s.metrics.output_events_admitted_total,
            s.metrics.events_received_total,
            s.metrics.retained_bytes
        )?;
        writeln!(
            out,
            "marketforge_stream_generation{{stream=\"{id}\",kind=\"derived\"}} {}\nmarketforge_stream_accepted_events_total{{stream=\"{id}\",kind=\"derived\"}} {}",
            s.generation, s.metrics.events_produced_total
        )?;
        latency(&mut out, &id, "derived", s.metrics.processing_latency)?;
    }
    Ok(out.text)
}
fn latency(out: &mut Encoder, id: &str, kind: &str, l: LatencySnapshot) -> std::fmt::Result {
    use std::fmt::Write;
    if let Some(last) = l.last_ns {
        writeln!(
            out,
            "marketforge_processing_last_seconds{{stream=\"{id}\",kind=\"{kind}\"}} {}",
            last as f64 / 1e9
        )?;
    }
    if let Some(mean) = l.mean_ns {
        writeln!(
            out,
            "marketforge_processing_interval_mean_seconds{{stream=\"{id}\",kind=\"{kind}\"}} {}",
            mean as f64 / 1e9
        )?;
    }
    if let Some(max) = l.max_ns {
        writeln!(
            out,
            "marketforge_processing_interval_max_seconds{{stream=\"{id}\",kind=\"{kind}\"}} {}",
            max as f64 / 1e9
        )?;
    }
    writeln!(
        out,
        "marketforge_processing_interval_samples{{stream=\"{id}\",kind=\"{kind}\"}} {}",
        l.samples
    )
}
pub struct PrometheusServer {
    pub address: std::net::SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl PrometheusServer {
    pub async fn bind(
        config: PrometheusConfig,
        collector: Arc<MetricsSnapshotCollector>,
        sampling: Arc<SamplingState>,
    ) -> Result<Self, String> {
        if config.max_concurrent_requests == 0
            || config.max_concurrent_requests > 1024
            || config.max_response_bytes > 16 << 20
        {
            return Err("Prometheus resource limits invalid".into());
        }
        let listener = TcpListener::bind(&config.listen_address)
            .await
            .map_err(|e| e.to_string())?;
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        let (stop, mut shutdown) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let cache = Arc::new(Mutex::new(None::<(Instant, String)>));
            loop {
                tokio::select! {
                    _=&mut shutdown=>break,
                    _=connections.join_next(),if !connections.is_empty()=>(),
                    result=listener.accept()=>{let Ok((socket,_))=result else{break};if connections.len()>=config.max_concurrent_requests{continue;}let c=config.clone();let collector=collector.clone();let sampling=sampling.clone();let cache=cache.clone();connections.spawn(async move{
                        let service=service_fn(move |request:Request<Incoming>| {let c=c.clone();let collector=collector.clone();let sampling=sampling.clone();let cache=cache.clone();async move{
                            let (code,mut body)=if request.method()!=hyper::Method::GET || request.uri().path()!=c.path || request.uri().query().is_some(){(StatusCode::NOT_FOUND,"not found\n".into())}else if request.headers().contains_key(hyper::header::CONTENT_LENGTH)||request.headers().contains_key(hyper::header::TRANSFER_ENCODING){(StatusCode::BAD_REQUEST,"request bodies unsupported\n".into())}else{
                                let mut cached=cache.lock().unwrap_or_else(|p|p.into_inner());
                                if cached.as_ref().is_some_and(|(at,_)|at.elapsed()<Duration::from_millis(c.cache_interval_ms)){(StatusCode::OK,cached.as_ref().unwrap().1.clone())}else{match collector.latest(){None=>(StatusCode::SERVICE_UNAVAILABLE,"no completed metrics snapshot\n".into()),Some(snapshot)=>match encode(&snapshot,&sampling,c.max_response_bytes){Ok(body)=>{*cached=Some((Instant::now(),body.clone()));(StatusCode::OK,body)},Err(_)=>(StatusCode::SERVICE_UNAVAILABLE,"metrics response exceeds configured bound\n".into())}}}
                            };
                            if body.len()>c.max_response_bytes {body.clear();}
                    Ok::<_,Infallible>(Response::builder().status(code).header(hyper::header::CONTENT_TYPE,"text/plain; version=0.0.4; charset=utf-8").header(hyper::header::CONNECTION,"close").body(Full::new(Bytes::from(body))).unwrap())
                        }});
                        let mut http=http1::Builder::new();http.keep_alive(false).max_headers(32).max_buf_size(8192).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(2));let _=tokio::time::timeout(Duration::from_secs(5),http.serve_connection(TokioIo::new(socket),service)).await;
                    });}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            address,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for PrometheusServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exposition_has_one_type_per_family_and_explicit_limits() {
        let collector = MetricsSnapshotCollector::new();
        let runtime = RuntimeMetrics::new();
        let snapshot = collector
            .collect(
                &runtime,
                &PublicationMetrics::new(),
                &RecordingMetrics::new(),
                &[],
                &[],
            )
            .unwrap();
        let state = SamplingState::default();
        let text = encode(&snapshot, &state, 65536).unwrap();
        let mut names = std::collections::BTreeSet::new();
        for line in text.lines().filter(|line| line.starts_with("# TYPE ")) {
            assert!(
                names.insert(line.split_whitespace().nth(2).unwrap()),
                "duplicate family {line}"
            );
        }
        assert!(text.contains("marketforge_recording_available 0"));
        assert!(
            !text
                .lines()
                .any(|l| l.starts_with("marketforge_process_cpu_cores "))
        );
        assert!(encode(&snapshot, &state, 16).is_err());
        assert_eq!(label("a\\\"\n"), "a\\\\\\\"\\n");
    }
}
