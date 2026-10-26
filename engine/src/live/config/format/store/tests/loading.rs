use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::live::config::format::FormatConfigStore;

// ============================================================
// TEST HELPERS
// ============================================================

fn format_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/formats"
    ))
}

/// Creates an isolated temporary directory for tests.
///
/// The directory is removed when the returned guard is dropped.
struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-format-test-{}-{timestamp}-{counter}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn create_format_dirs(&self) {
        fs::create_dir_all(self.path.join("trades")).unwrap();
        fs::create_dir_all(self.path.join("depth")).unwrap();
    }

    fn copy_trade(&self, profile: &str) {
        let filename = format!("{profile}.toml");

        fs::copy(
            format_directory().join("trades").join(&filename),
            self.path.join("trades").join(&filename),
        )
        .unwrap();
    }

    fn copy_depth(&self, profile: &str) {
        let filename = format!("{profile}.toml");

        fs::copy(
            format_directory().join("depth").join(&filename),
            self.path.join("depth").join(&filename),
        )
        .unwrap();
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// ============================================================
// CONSTRUCTION
// ============================================================

// ============================================================
// COMPLETE STORE LOADING
// ============================================================

// ============================================================
// LOOKUP
// ============================================================

// ============================================================
// NAMESPACE ISOLATION
// ============================================================

// ============================================================
// SHARED OWNERSHIP
// ============================================================

// ============================================================
// ITERATION
// ============================================================

// ============================================================
// ERROR HANDLING
// ============================================================

#[test]
fn reject_missing_root_directory() {
    let directory = TestDirectory::new();
    let missing = directory.path().join("nonexistent");

    assert!(FormatConfigStore::load_dir(&missing).is_err());
}

#[test]
fn reject_missing_trade_directory() {
    let directory = TestDirectory::new();

    fs::create_dir_all(directory.path().join("depth")).unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_missing_depth_directory() {
    let directory = TestDirectory::new();

    fs::create_dir_all(directory.path().join("trades")).unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_invalid_trade_toml() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    fs::write(
        directory.path().join("trades").join("invalid.toml"),
        "[format\ninvalid",
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_invalid_depth_toml() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    fs::write(
        directory.path().join("depth").join("invalid.toml"),
        "[format\ninvalid",
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_trade_filename_identity_mismatch() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    fs::copy(
        format_directory().join("trades/binance_spot.toml"),
        directory.path().join("trades/wrong_name.toml"),
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_depth_filename_identity_mismatch() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    fs::copy(
        format_directory().join("depth/binance_spot.toml"),
        directory.path().join("depth/wrong_name.toml"),
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_duplicate_trade_identity() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    directory.copy_trade("binance_spot");

    // Same declared format identity, different filename.
    fs::copy(
        format_directory().join("trades/binance_spot.toml"),
        directory.path().join("trades/duplicate.toml"),
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

#[test]
fn reject_duplicate_depth_identity() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    directory.copy_depth("binance_spot");

    fs::copy(
        format_directory().join("depth/binance_spot.toml"),
        directory.path().join("depth/duplicate.toml"),
    )
    .unwrap();

    assert!(FormatConfigStore::load_dir(directory.path()).is_err());
}

// ============================================================
// PARTIAL STORES
// ============================================================

#[test]
fn load_trade_only_store() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();
    directory.copy_trade("binance_spot");

    let store = FormatConfigStore::load_dir(directory.path()).unwrap();

    assert_eq!(store.trade_count(), 1);
    assert_eq!(store.depth_count(), 0);
    assert_eq!(store.len(), 1);
}

#[test]
fn load_depth_only_store() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();
    directory.copy_depth("binance_spot");

    let store = FormatConfigStore::load_dir(directory.path()).unwrap();

    assert_eq!(store.trade_count(), 0);
    assert_eq!(store.depth_count(), 1);
    assert_eq!(store.len(), 1);
}

#[test]
fn load_empty_format_directories() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();

    let store = FormatConfigStore::load_dir(directory.path()).unwrap();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
}
