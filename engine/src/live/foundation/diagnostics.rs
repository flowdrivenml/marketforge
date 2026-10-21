//! Structured local diagnostic records. Raw payloads, endpoint strings and
//! resource identity strings cannot enter persisted records through this API.
use crate::live::{model::error::ErrorCode, ports::observability::DiagnosticEvent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactedDiagnostic {
    pub owner_fingerprint: String,
    pub generation: u64,
    pub code: ErrorCode,
    pub observed_at_ns: i64,
}
impl RedactedDiagnostic {
    pub fn from_event(event: &DiagnosticEvent) -> Result<Self, ErrorCode> {
        // Only a one-way identity fingerprint is retained. No free text,
        // credentials, native frames or exchange errors are accepted.
        let identity =
            serde_json::to_vec(&event.owner.resource).map_err(|_| ErrorCode::InvalidInput)?;
        if identity.len() > 4096 {
            return Err(ErrorCode::Limit);
        }
        let digest = Sha256::digest(identity);
        Ok(Self {
            owner_fingerprint: digest.iter().map(|b| format!("{b:02x}")).collect(),
            generation: event.owner.generation,
            code: event.code,
            observed_at_ns: event.observed_at_ns,
        })
    }
}
/// Bounded local history, oldest diagnostic evicted explicitly. This policy is
/// for operational observations only and never owns market-data obligations.
#[derive(Clone)]
pub struct DiagnosticHistory {
    events: VecDeque<RedactedDiagnostic>,
    capacity: usize,
    evicted: u64,
}
impl DiagnosticHistory {
    pub fn new(capacity: usize) -> Result<Self, ErrorCode> {
        if capacity == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            events: VecDeque::new(),
            capacity,
            evicted: 0,
        })
    }
    pub fn push(&mut self, event: &DiagnosticEvent) -> Result<(), ErrorCode> {
        let record = RedactedDiagnostic::from_event(event)?;
        if self.events.len() == self.capacity {
            self.events.pop_front();
            self.evicted = self.evicted.saturating_add(1);
        }
        self.events.push_back(record);
        Ok(())
    }
    pub fn snapshot(&self) -> Vec<RedactedDiagnostic> {
        self.events.iter().cloned().collect()
    }
    pub fn evicted(&self) -> u64 {
        self.evicted
    }
}
/// Blocking writer for an owned diagnostic worker, never a market-data hot path.
/// A fresh file is required; existing user history is never overwritten.
pub struct LocalDiagnosticWriter {
    file: File,
    limit: u64,
    written: u64,
    fenced: bool,
}
impl LocalDiagnosticWriter {
    pub fn create(path: &Path, max_bytes: u64) -> Result<Self, ErrorCode> {
        if max_bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|_| ErrorCode::Persistence)?;
        Ok(Self {
            file,
            limit: max_bytes,
            written: 0,
            fenced: false,
        })
    }
    pub fn append(&mut self, event: &DiagnosticEvent) -> Result<(), ErrorCode> {
        if self.fenced {
            return Err(ErrorCode::Persistence);
        }
        let record = RedactedDiagnostic::from_event(event)?;
        self.append_record(&record)
    }
    fn append_record(&mut self, record: &RedactedDiagnostic) -> Result<(), ErrorCode> {
        if self.fenced {
            return Err(ErrorCode::Persistence);
        }
        let mut bytes = serde_json::to_vec(record).map_err(|_| ErrorCode::InvalidInput)?;
        bytes.push(b'\n');
        let next = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or(ErrorCode::Limit)?;
        if next > self.limit {
            return Err(ErrorCode::Capacity);
        }
        // Any partial write or uncertain sync permanently fences this writer.
        if self
            .file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_data())
            .is_err()
        {
            self.fenced = true;
            return Err(ErrorCode::Persistence);
        }
        self.written = next;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::model::{
        identity::StreamId,
        operation::{ResourceId, ResourceVersion},
    };
    #[test]
    fn redaction_bounded_history_and_local_file() {
        let event = DiagnosticEvent {
            owner: ResourceVersion {
                resource: ResourceId::Source(StreamId("token=secret-private-endpoint".into())),
                generation: 2,
            },
            code: ErrorCode::Unavailable,
            observed_at_ns: 1,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("diagnostic.jsonl");
        let mut writer = LocalDiagnosticWriter::create(&path, 1024).unwrap();
        writer.append(&event).unwrap();
        assert!(LocalDiagnosticWriter::create(&path, 1024).is_err());
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("secret"));
        assert!(!text.contains("endpoint"));
        let record: RedactedDiagnostic = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(record.code, ErrorCode::Unavailable);
        let mut history = DiagnosticHistory::new(1).unwrap();
        history.push(&event).unwrap();
        history.push(&event).unwrap();
        assert_eq!(history.snapshot().len(), 1);
        assert_eq!(history.evicted(), 1);
        let mut tiny = LocalDiagnosticWriter::create(&dir.path().join("tiny"), 1).unwrap();
        assert_eq!(tiny.append(&event), Err(ErrorCode::Capacity));
        assert_eq!(std::fs::metadata(dir.path().join("tiny")).unwrap().len(), 0);
    }
}

/// Bounded local logging worker. Enqueue is explicitly not durability; shutdown
/// reports whether the worker joined and whether any write was rejected.
pub struct LocalDiagnosticWorker {
    sender: std::sync::Mutex<Option<std::sync::mpsc::SyncSender<RedactedDiagnostic>>>,
    state: std::sync::Arc<(std::sync::Mutex<WorkerState>, std::sync::Condvar)>,
    worker: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    dropped: std::sync::atomic::AtomicU64,
}
#[derive(Default)]
struct WorkerState {
    stopped: bool,
    error: Option<ErrorCode>,
    rejected: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticShutdown {
    pub joined: bool,
    pub error: Option<ErrorCode>,
    pub dropped: u64,
}
impl LocalDiagnosticWorker {
    pub fn start(path: &Path, max_bytes: u64, queue_records: usize) -> Result<Self, ErrorCode> {
        if queue_records == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        let mut writer = LocalDiagnosticWriter::create(path, max_bytes)?;
        let (sender, receiver) = std::sync::mpsc::sync_channel(queue_records);
        let state = std::sync::Arc::new((
            std::sync::Mutex::new(WorkerState::default()),
            std::sync::Condvar::new(),
        ));
        let owned = state.clone();
        let worker = std::thread::Builder::new()
            .name("live-diagnostics".into())
            .spawn(move || {
                let mut failed = false;
                for record in receiver {
                    let result = if failed {
                        Err(ErrorCode::Persistence)
                    } else {
                        writer.append_record(&record)
                    };
                    if let Err(error) = result {
                        failed = true;
                        let mut state = owned.0.lock().unwrap_or_else(|e| e.into_inner());
                        if state.error.is_none() {
                            state.error = Some(error);
                        }
                        state.rejected = state.rejected.saturating_add(1);
                    }
                }
                let mut state = owned.0.lock().unwrap_or_else(|e| e.into_inner());
                state.stopped = true;
                owned.1.notify_all();
            })
            .map_err(|_| ErrorCode::Capacity)?;
        Ok(Self {
            sender: std::sync::Mutex::new(Some(sender)),
            state,
            worker: std::sync::Mutex::new(Some(worker)),
            dropped: std::sync::atomic::AtomicU64::new(0),
        })
    }
    pub fn last_error(&self) -> Option<ErrorCode> {
        self.state.0.lock().unwrap_or_else(|e| e.into_inner()).error
    }
    pub fn dropped_records(&self) -> u64 {
        self.dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_add(
                self.state
                    .0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .rejected,
            )
    }
    pub fn enqueue(
        &self,
        event: &DiagnosticEvent,
    ) -> Result<crate::live::ports::observability::ObservationAdmission, ErrorCode> {
        use crate::live::ports::observability::ObservationAdmission;
        let record = RedactedDiagnostic::from_event(event)?;
        let Ok(sender) = self.sender.try_lock() else {
            self.count_drop();
            return Ok(ObservationAdmission::DroppedCapacity);
        };
        let Some(sender) = sender.as_ref() else {
            return Err(ErrorCode::Unavailable);
        };
        match sender.try_send(record) {
            Ok(()) => Ok(ObservationAdmission::Accepted),
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                self.count_drop();
                Ok(ObservationAdmission::DroppedCapacity)
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.count_drop();
                Err(ErrorCode::Persistence)
            }
        }
    }
    fn count_drop(&self) {
        use std::sync::atomic::Ordering;
        let _ = self
            .dropped
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
    }
    pub fn shutdown(&self, deadline: std::time::Instant) -> DiagnosticShutdown {
        self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
        let mut state = self.state.0.lock().unwrap_or_else(|e| e.into_inner());
        while !state.stopped {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            state = self
                .state
                .1
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        let mut report = DiagnosticShutdown {
            joined: state.stopped,
            error: state.error,
            dropped: self
                .dropped
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_add(state.rejected),
        };
        drop(state);
        if report.joined {
            if let Some(worker) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
                if worker.join().is_err() {
                    report.joined = false;
                    report.error = Some(ErrorCode::Internal);
                }
            }
        }
        report
    }
}
impl Drop for LocalDiagnosticWorker {
    fn drop(&mut self) {
        self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}
#[cfg(test)]
mod worker_tests {
    use super::*;
    use crate::live::model::operation::{ResourceId, ResourceVersion};
    #[test]
    fn real_worker_drains_and_reports_capacity_failure() {
        let dir = tempfile::tempdir().unwrap();
        let event = DiagnosticEvent {
            owner: ResourceVersion {
                resource: ResourceId::Runtime,
                generation: 1,
            },
            code: ErrorCode::Unavailable,
            observed_at_ns: 0,
        };
        let worker = LocalDiagnosticWorker::start(&dir.path().join("good"), 1024, 2).unwrap();
        worker.enqueue(&event).unwrap();
        let report = worker.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(1));
        assert!(report.joined);
        assert_eq!(report.error, None);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("good"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let worker = LocalDiagnosticWorker::start(&dir.path().join("full"), 1, 2).unwrap();
        worker.enqueue(&event).unwrap();
        let report = worker.shutdown(std::time::Instant::now() + std::time::Duration::from_secs(1));
        assert!(report.joined);
        assert_eq!(report.error, Some(ErrorCode::Capacity));
    }
}
