//! Preparation runs on the same worker as execution, using bounded/coalesced
//! telemetry. No discovery I/O or decoding is performed by the rendering thread.
use super::ProgressReporter;
use std::{cell::RefCell, io::Read};
thread_local! {static REPORTER:RefCell<Option<ProgressReporter>>=const {RefCell::new(None)};}
pub(crate) struct Scope(Option<ProgressReporter>);
impl Scope {
    pub fn enter(reporter: &ProgressReporter) -> Self {
        Self(REPORTER.with(|r| r.replace(Some(reporter.clone()))))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        REPORTER.with(|r| r.replace(self.0.take()));
    }
}
pub(crate) fn reporter() -> ProgressReporter {
    REPORTER.with(|r| {
        r.borrow()
            .clone()
            .unwrap_or_else(ProgressReporter::disabled)
    })
}
pub(crate) struct Reader<'a> {
    pub input: &'a mut dyn Read,
    pub reporter: ProgressReporter,
    pub job: u64,
    pub stream: &'a str,
    pub bytes: &'a mut u64,
    pub next: &'a mut u64,
}
impl Read for Reader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let n = self.input.read(output)?;
        *self.bytes = self
            .bytes
            .checked_add(n as u64)
            .ok_or_else(|| std::io::Error::other("discovery byte count overflow"))?;
        if *self.bytes >= *self.next || n == 0 {
            self.reporter.stage_count(
                self.job,
                self.stream,
                *self.bytes,
                "discovery decoded bytes",
            );
            *self.next = self.bytes.saturating_add(1024 * 1024);
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::historical::normalize::progress::{ProgressEvent, ProgressTaskId};
    #[test]
    fn discovery_coalesces_bytes_and_retains_registered_metadata() {
        let reporter = ProgressReporter::coalesced();
        let state = reporter.state();
        let id = ProgressTaskId::new(1, 2);
        reporter.register_source(
            id,
            "okx".into(),
            "family".into(),
            "depth".into(),
            "archive".into(),
            "family.depth".into(),
        );
        reporter.task_started(
            id,
            "okx".into(),
            "family".into(),
            "depth".into(),
            "archive".into(),
        );
        reporter.phase(id, "family.depth", "discovering members");
        let mut input = std::io::repeat(0).take(3 * 1024 * 1024);
        let mut bytes = 0;
        let mut next = 0;
        let mut reader = Reader {
            input: &mut input,
            reporter: reporter.clone(),
            job: 1,
            stream: "family.depth",
            bytes: &mut bytes,
            next: &mut next,
        };
        std::io::copy(&mut reader, &mut std::io::sink()).unwrap();
        let events = state.take_events(32);
        assert!(events.iter().any(
            |e| matches!(e,ProgressEvent::TaskRegistered {id:registered,..} if *registered==id)
        ));
        assert!(events.len() < 10);
        assert_eq!(bytes, 3 * 1024 * 1024);
        reporter.finished(true);
        assert!(!state.take_events(32).is_empty());
    }
}
