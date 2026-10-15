use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};

use arrow_array::{Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn independently_validates_converted_values() {
    let root = root();

    let script = root.join("engine/tests/convert/fixtures/validate_conversion.py");

    assert!(script.is_file());

    let status = Command::new("python")
        .arg(script)
        .status()
        .expect("failed to execute Python conversion validator");

    assert!(status.success());
}
