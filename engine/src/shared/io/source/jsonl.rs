use std::io::{BufRead, BufReader, Read};

use serde_json::Value;

pub struct JsonlDecoder<R: Read> {
    reader: BufReader<R>,
    buffer: String,
}

impl<R: Read> JsonlDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            buffer: String::new(),
        }
    }

    pub fn next_record(&mut self) -> std::io::Result<Option<&str>> {
        loop {
            self.buffer.clear();

            let bytes = self.reader.read_line(&mut self.buffer)?;

            if bytes == 0 {
                return Ok(None);
            }

            // Skip empty lines before returning a borrowed reference.
            if self.buffer.trim().is_empty() {
                continue;
            }

            return Ok(Some(self.buffer.trim()));
        }
    }

    pub fn next_json(&mut self) -> Result<Option<Value>, serde_json::Error> {
        match self.next_record() {
            Ok(Some(record)) => serde_json::from_str(record).map(Some),

            Ok(None) => Ok(None),

            Err(error) => Err(serde_json::Error::io(error)),
        }
    }
}
