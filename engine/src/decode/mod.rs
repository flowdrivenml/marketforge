mod csv;

#[cfg(feature = "process")]
mod xlsx;

pub use csv::CsvDecoder;

#[cfg(feature = "process")]
pub use xlsx::{XlsxDepthDecoder, XlsxDepthRecord};
