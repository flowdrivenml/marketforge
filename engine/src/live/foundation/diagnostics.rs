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
        let mut bytes = serde_json::to_vec(&record).map_err(|_| ErrorCode::InvalidInput)?;
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
