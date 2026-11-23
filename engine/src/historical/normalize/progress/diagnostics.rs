//! Coalesced live snapshots: the worker owns no diagnostic I/O or event queue.
use super::{ProgressReporter, ProgressTaskId};
use crate::shared::historical::config::{DiagnosticsMode, ProcessingConfig};
use serde_json::json;
use std::{
    fs,
    io::{BufWriter, Write},
    path::Path,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

pub(crate) enum Command {
    Changed,
    Finish(bool),
}
pub struct Diagnostics {
    stop: mpsc::SyncSender<Command>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Diagnostics {
    pub fn start(
        config: &ProcessingConfig,
        parent: &Path,
        reporter: &ProgressReporter,
    ) -> Option<Self> {
        if config.diagnostics.mode == DiagnosticsMode::Disabled || reporter.snapshots_enabled() {
            return None;
        }
        if let Err(error) = crate::shared::historical::config::validate_processing_config(config) {
            crate::historical::normalize::progress::log::diagnostic(format_args!(
                "normalization diagnostics unavailable for invalid runtime configuration: {error}"
            ));
            return None;
        }
        let result = (|| -> std::io::Result<_> {
            fs::create_dir_all(parent)?;
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let root = parent.join(format!("performance-{stamp}-{}", std::process::id()));
            fs::create_dir(&root)?;
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join("live.jsonl"))?;
            fs::write(
                root.join("effective-config.json"),
                serde_json::to_vec_pretty(config)?,
            )?;
            Ok((root, file))
        })();
        let (root, file) = match result {
            Ok(v) => v,
            Err(e) => {
                crate::historical::normalize::progress::log::diagnostic(format_args!(
                    "normalization diagnostics unavailable: {e}"
                ));
                return None;
            }
        };
        reporter.enable_snapshots();
        let state = reporter.state();
        let interval = Duration::from_millis(config.diagnostics.interval_ms);
        let detailed = config.diagnostics.mode == DiagnosticsMode::Detailed;
        let (stop, receive) = mpsc::sync_channel(1);
        *state.wake.lock().unwrap() = Some(stop.clone());
        crate::historical::normalize::progress::log::diagnostic(format_args!(
            "normalization diagnostics started: mode={:?}, interval_ms={}, workers={}, memory_limit={}, scratch_limit={}, requested_fan_in={}, scratch_format={:?}, scratch_codec={:?}, report={}",
            config.diagnostics.mode,
            config.diagnostics.interval_ms,
            config.resources.workers,
            config.resources.memory_budget_bytes,
            config.resources.scratch_budget_bytes,
            config.merge_fan_in,
            config.scratch_format,
            config.scratch_codec,
            root.display()
        ));
        let thread = thread::spawn(move || {
            let start = Instant::now();
            let before = crate::historical::normalize::benchmark::measure::snapshot();
            let executable = std::env::current_exe().ok();
            let executable_metadata = executable.as_ref().and_then(|p| fs::metadata(p).ok());
            let software = json!({"version":env!("CARGO_PKG_VERSION"),"debug_assertions":cfg!(debug_assertions),"executable":executable,"executable_bytes":executable_metadata.as_ref().map(|m|m.len()),"executable_modified_unix_ns":executable_metadata.as_ref().and_then(|m|m.modified().ok()).and_then(|t|t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d|d.as_nanos().to_string())});
            let mut output = Some(BufWriter::new(file));
            let mut warn = false;
            let mut first = true;
            loop {
                let terminal =
                    match receive.recv_timeout(if first { Duration::ZERO } else { interval }) {
                        Ok(Command::Finish(success)) => Some(success),
                        Ok(Command::Changed) => None,
                        Err(mpsc::RecvTimeoutError::Disconnected) => Some(false),
                        Err(mpsc::RecvTimeoutError::Timeout) => None,
                    };
                first = false;
                let tasks: Vec<_> = state.snapshots().into_iter().map(|(id,c)| json!({"job":id.job_id,"source_task":id.task_id,"last_progress_unix_ms":if c.last_progress_unix_ms == 0 { None } else { Some(c.last_progress_unix_ms) },"source_records":c.records_read,"normalized_records":c.records_processed,"normalized_events":c.events_normalized,"sink_rows":c.records_written,"spooled_records":c.spooled_records,"rejected_records":c.records_rejected})).collect();
                let phases = state.phase_snapshots();
                let after = crate::historical::normalize::benchmark::measure::snapshot();
                let current_rss = if detailed { current_rss_bytes() } else { None };
                let process =
                    crate::historical::normalize::benchmark::measure::difference(&before, &after);
                let snapshot = json!({"process_current_rss_bytes":current_rss,"phase_completed_inclusive_ms":*state.phase_totals.lock().unwrap(),"run_id":root.file_name().unwrap().to_string_lossy(),"resources":*state.resources.lock().unwrap(),"process_wide":process,"schema_version":1,"software":software,"elapsed_ms":start.elapsed().as_millis() as u64,"measurement":"inclusive wall time; accounted counters, not RSS","status":terminal.map(|s| if s {"committed"} else {"incomplete_or_failed"}).unwrap_or("running"),"tasks":tasks,"phases":phases});
                crate::historical::normalize::progress::log::diagnostic(format_args!(
                    "normalization heartbeat: elapsed={:.1}s status={} phases={}",
                    start.elapsed().as_secs_f64(),
                    snapshot["status"],
                    snapshot["phases"]
                ));
                if let Some(writer) = output.as_mut() {
                    let result = serde_json::to_writer(&mut *writer, &snapshot)
                        .map_err(std::io::Error::other)
                        .and_then(|_| writer.write_all(b"\n"))
                        .and_then(|_| writer.flush());
                    if let Err(e) = result {
                        if !warn {
                            crate::historical::normalize::progress::log::diagnostic(format_args!(
                                "normalization diagnostic output disabled: {e}"
                            ));
                            warn = true;
                        }
                        output = None;
                    }
                }
                if terminal.is_some() {
                    if let Err(e) = fs::write(
                        root.join("performance.json"),
                        serde_json::to_vec_pretty(&snapshot).unwrap(),
                    ) && !warn
                    {
                        crate::historical::normalize::progress::log::diagnostic(format_args!(
                            "normalization final diagnostics unavailable: {e}"
                        ));
                    }
                    state.end_snapshots();
                    break;
                }
            }
        });
        Some(Self {
            stop,
            thread: Some(thread),
        })
    }
    pub fn finish(mut self, success: bool) {
        let _ = self.stop.send(Command::Finish(success));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
impl Drop for Diagnostics {
    fn drop(&mut self) {
        if let Some(t) = self.thread.take() {
            let _ = self.stop.send(Command::Finish(false));
            let _ = t.join();
        }
    }
}

fn current_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()?
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()?
                    .checked_mul(1024)
            })
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

impl ProgressReporter {
    pub fn stream_phase(&self, job: u64, stream: &str, phase: &'static str) {
        if self.is_enabled() {
            self.state().set_phase(job, None, stream, phase);
        }
    }
    pub fn phase(&self, id: ProgressTaskId, stream: &str, phase: &'static str) {
        if self.is_enabled() {
            self.state()
                .set_phase(id.job_id, Some(id.task_id), stream, phase);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ProcessingConfig {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        crate::shared::historical::config::load_processing_config(
            "data/.config/engine/normalize.toml",
            root,
        )
        .unwrap()
    }
    #[test]
    fn slow_telemetry_consumer_coalesces_without_growing_a_queue() {
        let reporter = ProgressReporter::disabled();
        reporter.enable_snapshots();
        let (send, receive) = mpsc::sync_channel(1);
        *reporter.state().wake.lock().unwrap() = Some(send);
        for i in 0..10000 {
            reporter.stream_phase(
                1,
                "stream",
                if i % 2 == 0 {
                    "identity merging"
                } else {
                    "chronological merging"
                },
            );
        }
        assert!(receive.try_recv().is_ok());
        assert!(receive.try_recv().is_err());
        assert_eq!(reporter.state().phase_snapshots().len(), 1);
        assert_eq!(reporter.state().phase_totals.lock().unwrap().len(), 2);
    }
    #[test]
    fn disabled_has_no_output_and_long_phase_has_timer_heartbeats() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config();
        config.diagnostics.mode = DiagnosticsMode::Disabled;
        let reporter = ProgressReporter::disabled();
        assert!(Diagnostics::start(&config, temp.path(), &reporter).is_none());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
        config.diagnostics.mode = DiagnosticsMode::Basic;
        config.diagnostics.interval_ms = 100;
        let diagnostics = Diagnostics::start(&config, temp.path(), &reporter).unwrap();
        reporter.phase(ProgressTaskId::new(7, 9), "stream", "identity merging");
        // No records arrive during this phase.
        thread::sleep(Duration::from_millis(240));
        diagnostics.finish(false);
        assert!(!reporter.is_enabled());
        let root = fs::read_dir(temp.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let live = fs::read_to_string(root.join("live.jsonl")).unwrap();
        assert!(live.lines().count() >= 3);
        let final_report: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("performance.json")).unwrap()).unwrap();
        assert_eq!(final_report["status"], "incomplete_or_failed");
        assert_eq!(final_report["phases"][0]["phase"], "identity merging");
    }
    #[test]
    fn optional_output_failure_and_drop_keep_processing_independent() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config();
        config.diagnostics.mode = DiagnosticsMode::Detailed;
        let blocked = temp.path().join("file");
        fs::write(&blocked, b"occupied").unwrap();
        assert!(Diagnostics::start(&config, &blocked, &ProgressReporter::disabled()).is_none());
        let reporter = ProgressReporter::disabled();
        let guard = Diagnostics::start(&config, temp.path(), &reporter).unwrap();
        drop(guard);
        let root = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().is_dir())
            .unwrap()
            .path();
        assert!(root.join("performance.json").is_file());
    }
}
