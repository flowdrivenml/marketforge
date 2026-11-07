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
            if self.reader.read_until(b'\n', &mut self.buffer)? == 0 {
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
