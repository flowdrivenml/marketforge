use std::io::Read;

use csv::{ByteRecord, Reader, ReaderBuilder};

pub struct CsvDecoder<R: Read> {
    reader: Reader<R>,
}

impl<R: Read> CsvDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self::with_headers(reader, true)
    }

    pub fn with_headers(reader: R, has_headers: bool) -> Self {
        Self {
            reader: ReaderBuilder::new()
                .has_headers(has_headers)
                .from_reader(reader),
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
