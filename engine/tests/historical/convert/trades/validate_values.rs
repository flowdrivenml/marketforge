use std::{path::PathBuf, process::Command};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn independently_validates_converted_values() {
    let root = root();

    let script = root.join("engine/tests/historical/convert/fixtures/validate_conversion.py");

    assert!(script.is_file());

    let status = Command::new("python")
        .arg(script)
        .status()
        .expect("failed to execute Python conversion validator");

    assert!(status.success());
}
