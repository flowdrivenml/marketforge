use std::io::Read;

use csv::{ByteRecord, Reader, ReaderBuilder};

pub struct CsvDecoder<R: Read> {
    reader: Reader<RecordBoundedReader<R>>,
}

impl<R: Read> CsvDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self::with_headers(reader, true)
    }

    pub fn with_headers(reader: R, has_headers: bool) -> Self {
        Self {
            reader: ReaderBuilder::new()
                .has_headers(has_headers)
                .from_reader(RecordBoundedReader::new(reader)),
        }
    }

    pub fn headers(&mut self) -> Result<ByteRecord, csv::Error> {
        Ok(self.reader.byte_headers()?.clone())
    }

    pub fn records(&mut self) -> impl Iterator<Item = Result<ByteRecord, csv::Error>> + '_ {
        self.reader.byte_records()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn reads_headers_and_records() {
        let data = b"timestamp,price,size\n1,100.5,2\n2,101.0,3\n";

        let mut decoder = CsvDecoder::new(Cursor::new(data));

        let headers = decoder.headers().expect("read headers");

        assert_eq!(
            headers,
            ByteRecord::from(vec!["timestamp", "price", "size"]),
        );

        let records = decoder
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("read records");

        assert_eq!(records.len(), 2);
    }

    #[test]
    fn reads_headerless_csv_without_losing_first_record() {
        let data = b"1,100.5,2\n2,101.0,3\n";

        let mut decoder = CsvDecoder::with_headers(Cursor::new(data), false);

        let records = decoder
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("read records");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get(0), Some(b"1".as_slice()));
        assert_eq!(records[1].get(0), Some(b"2".as_slice()));
    }
}

/// Use the same parser as csv to recognize quoted/multiline record boundaries.
/// The scanner has fixed output/field-index storage; it never collects a record.
struct RecordBoundedReader<R> {
    reader: std::io::BufReader<R>,
    parser: csv_core::Reader,
    bytes: u64,
    limit: Option<u64>,
}
impl<R: Read> RecordBoundedReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: std::io::BufReader::new(reader),
            parser: csv_core::Reader::new(),
            bytes: 0,
            limit: crate::shared::historical::limits::current().map(|l| l.record_bytes),
        }
    }
}
impl<R: Read> Read for RecordBoundedReader<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        use std::io::BufRead;
        let Some(limit) = self.limit else {
            return self.reader.read(output);
        };
        if output.is_empty() {
            return Ok(0);
        }
        let input = self.reader.fill_buf()?;
        let count = input.len().min(output.len()).min(8192);
        if count == 0 {
            return Ok(0);
        }
        // Blank lines and CRLF continuation bytes do not belong to a payload.
        // Only skip at a parser-confirmed record boundary, never inside quotes.
        if self.bytes == 0 {
            let skipped = input[..count]
                .iter()
                .take_while(|b| matches!(b, b'\r' | b'\n'))
                .count();
            if skipped > 0 {
                output[..skipped].copy_from_slice(&input[..skipped]);
                self.reader.consume(skipped);
                return Ok(skipped);
            }
        }
        let mut decoded = [0u8; 8192];
        let mut ends = [0usize; 8192];
        let (result, consumed, _, _) =
            self.parser
                .read_record(&input[..count], &mut decoded, &mut ends);
        let requested = self
            .bytes
            .checked_add(consumed as u64)
            .ok_or_else(|| std::io::Error::other("CSV size overflow"))?;
        let requested = if result == csv_core::ReadRecordResult::Record
            && consumed > 0
            && matches!(input[consumed - 1], b'\r' | b'\n')
        {
            requested
                .checked_sub(1)
                .ok_or_else(|| std::io::Error::other("CSV boundary accounting underflow"))?
        } else {
            requested
        };
        if requested > limit {
            return Err(std::io::Error::other(
                crate::shared::historical::limits::error(
                    "record bytes",
                    limit,
                    requested,
                    "CSV decoding",
                ),
            ));
        }
        self.bytes = if result == csv_core::ReadRecordResult::Record {
            0
        } else {
            requested
        };
        output[..consumed].copy_from_slice(&input[..consumed]);
        self.reader.consume(consumed);
        Ok(consumed)
    }
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use crate::shared::{historical::limits::*, io::bounded::resource_error};
    #[test]
    fn csv_limit_preserves_quoted_multiline_and_exact_boundaries() {
        let mut limits = NormalizationLimits::default();
        limits.record_bytes = 7;
        let _scope = LimitScope::enter(limits, "csv synthetic".into());
        let mut decoder = CsvDecoder::with_headers(&b"\"a\nb\",x\nc,d\n"[..], false);
        let records = decoder.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get(0), Some(&b"a\nb"[..]));
        let mut decoder = CsvDecoder::with_headers(&b"\"a\nbb\",x\n"[..], false);
        let error = decoder.records().next().unwrap().unwrap_err();
        match error.into_kind() {
            csv::ErrorKind::Io(e) => assert_eq!(resource_error(&e).unwrap().stage, "CSV decoding"),
            other => panic!("{other:?}"),
        }
    }
    #[test]
    fn csv_scanner_handles_chunked_fields_and_escaped_quotes() {
        struct SingleByte<'a>(&'a [u8]);
        impl Read for SingleByte<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.0.is_empty() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }
        let mut limits = NormalizationLimits::default();
        limits.record_bytes = 32;
        let _scope = LimitScope::enter(limits, "csv synthetic".into());
        let mut decoder =
            CsvDecoder::with_headers(SingleByte(b"\"a\"\"b\nc\",d\r\nx,y\r\n"), false);
        let records = decoder.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get(0), Some(&b"a\"b\nc"[..]));
    }
}

#[cfg(test)]
mod csv_boundary_resource_tests {
    use super::*;
    #[test]
    fn csv_payload_limit_ignores_blank_lines_and_crlf() {
        use crate::shared::historical::limits::*;
        let mut limits = NormalizationLimits::default();
        limits.record_bytes = 3;
        let _scope = LimitScope::enter(limits, "csv boundary".into());
        let mut decoder = CsvDecoder::with_headers(&b"\r\n\n\r\na,b\r\nc,d\r\n"[..], false);
        assert_eq!(
            decoder
                .records()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            2
        );
    }
}
