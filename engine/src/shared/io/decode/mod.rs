mod csv;

#[cfg(feature = "historical")]
mod xlsx;

pub use csv::CsvDecoder;

#[cfg(feature = "historical")]
pub use xlsx::{XlsxDepthDecoder, XlsxDepthRecord};
