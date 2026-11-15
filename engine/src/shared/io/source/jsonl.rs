use serde_json::Value;
use std::io::{self, BufRead, BufReader, Read};
/// A complete malformed record can advance; an underlying source failure cannot.
#[derive(Debug, thiserror::Error)]
pub enum JsonlError {
    #[error("source read failure: {0}")]
    Source(#[source] io::Error),
    #[error("malformed JSONL record: {0}")]
    Record(#[source] serde_json::Error),
}
pub struct JsonlDecoder<R: Read> {
    reader: BufReader<R>,
    buffer: Vec<u8>,
}
impl<R: Read> JsonlDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            buffer: Vec::new(),
        }
    }
    pub fn next_record(&mut self) -> io::Result<Option<&[u8]>> {
        loop {
            self.buffer.clear();
            if self.read_line_bounded()? == 0 {
                return Ok(None);
            }
            // Preserve whitespace-only line skipping; invalid UTF-8 is a record
            // error after the complete byte boundary has been consumed.
            if std::str::from_utf8(&self.buffer).is_ok_and(|s| s.trim().is_empty()) {
                continue;
            }
            let bytes = &self.buffer;
            return Ok(Some(match std::str::from_utf8(bytes) {
                Ok(text) => text.trim().as_bytes(),
                Err(_) => bytes.trim_ascii(),
            }));
        }
    }
    fn read_line_bounded(&mut self) -> io::Result<usize> {
        let Some(limit) = crate::shared::historical::limits::current().map(|l| l.record_bytes)
        else {
            return self.reader.read_until(b'\n', &mut self.buffer);
        };
        loop {
            let chunk = self.reader.fill_buf()?;
            if chunk.is_empty() {
                return Ok(self.buffer.len());
            }
            let count = chunk
                .iter()
                .position(|b| *b == b'\n')
                .map_or(chunk.len(), |i| i + 1);
            let requested = self
                .buffer
                .len()
                .checked_add(count)
                .ok_or_else(|| io::Error::other("JSONL size overflow"))?;
            if requested as u64 > limit {
                return Err(io::Error::other(crate::shared::historical::limits::error(
                    "record bytes",
                    limit,
                    requested as u64,
                    "JSONL decoding",
                )));
            }
            let finished = chunk[count - 1] == b'\n';
            self.buffer
                .try_reserve_exact(count)
                .map_err(io::Error::other)?;
            self.buffer.extend_from_slice(&chunk[..count]);
            self.reader.consume(count);
            if finished {
                return Ok(self.buffer.len());
            }
        }
    }
    pub fn next_json(&mut self) -> Result<Option<Value>, JsonlError> {
        match self.next_record().map_err(JsonlError::Source)? {
            Some(bytes) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(JsonlError::Record),
            None => Ok(None),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};
    #[test]
    fn record_errors_advance_and_blank_lines_are_skipped() {
        let mut d = JsonlDecoder::new(&b"\n{\"x\":1}\n{broken}\n{\"x\":2}\n"[..]);
        assert_eq!(d.next_json().unwrap().unwrap()["x"], 1);
        assert!(matches!(d.next_json(), Err(JsonlError::Record(_))));
        assert_eq!(d.next_json().unwrap().unwrap()["x"], 2);
        assert!(d.next_json().unwrap().is_none());
    }
    struct Broken {
        prefix: std::io::Cursor<Vec<u8>>,
        calls: Rc<Cell<usize>>,
    }
    impl Read for Broken {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if self.prefix.position() < (self.prefix.get_ref().len() as u64) {
                return self.prefix.read(bytes);
            }
            self.calls.set(self.calls.get() + 1);
            Err(io::Error::other("injected persistent failure"))
        }
    }
    #[test]
    fn persistent_io_error_is_fatal_after_one_failed_read() {
        let calls = Rc::new(Cell::new(0));
        let mut d = JsonlDecoder::new(Broken {
            prefix: std::io::Cursor::new(b"{\"x\":1}\n".to_vec()),
            calls: calls.clone(),
        });
        assert!(d.next_json().unwrap().is_some());
        match d.next_json() {
            Err(JsonlError::Source(e)) => assert_eq!(e.kind(), io::ErrorKind::Other),
            other => panic!("{other:?}"),
        };
        assert_eq!(calls.get(), 1);
    }
    #[test]
    fn invalid_utf8_complete_line_is_recoverable() {
        let mut d = JsonlDecoder::new(&b"\xff\n{}\n"[..]);
        assert!(matches!(d.next_json(), Err(JsonlError::Record(_))));
        assert!(d.next_json().unwrap().is_some());
    }
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use crate::shared::{historical::limits::*, io::bounded::resource_error};
    #[test]
    fn jsonl_limit_is_checked_before_buffer_growth() {
        let mut limits = NormalizationLimits::default();
        limits.record_bytes = 3;
        let _scope = LimitScope::enter(limits, "jsonl synthetic".into());
        let mut decoder = JsonlDecoder::new(&b"{}\n"[..]);
        assert!(decoder.next_json().unwrap().is_some());
        let mut decoder = JsonlDecoder::new(&b"{ }\n"[..]);
        match decoder.next_json() {
            Err(JsonlError::Source(e)) => {
                assert_eq!(resource_error(&e).unwrap().stage, "JSONL decoding")
            }
            _ => panic!("expected resource error"),
        }
        assert!(decoder.buffer.is_empty());
    }
}
