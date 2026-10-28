mod gzip;
mod jsonl;
mod plain;
mod tar_gzip;
mod zip;

pub use jsonl::JsonlDecoder;

pub use gzip::{open_gzip, with_gzip_reader};

pub use plain::{open_plain, with_plain_reader};

pub use tar_gzip::{open_tar_gzip_buffered, with_tar_gzip_reader};

pub use zip::{open_zip_buffered, with_zip_reader};

mod reader;
pub use reader::with_source_reader;
