use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::live::config::{
    exchange::{ExchangeId, MarketType},
    format::{FormatConfigStore, FormatKind},
};

// ============================================================
// TEST HELPERS
// ============================================================

const EXPECTED_PROFILES: &[&str] = &[
    "binance_spot",
    "binance_linear",
    "binance_inverse",
    "bybit_spot",
    "bybit_linear",
    "bybit_inverse",
    "okx_spot",
    "okx_linear",
    "okx_inverse",
    "bitget_spot",
    "bitget_linear",
    "bitget_inverse",
    "gateio_spot",
    "gateio_linear",
    "gateio_inverse",
];

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

#[test]
fn new_store_is_empty() {
    let store = FormatConfigStore::new();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
    assert_eq!(store.trade_count(), 0);
    assert_eq!(store.depth_count(), 0);
}

#[test]
fn default_store_is_empty() {
    let store = FormatConfigStore::default();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
}

// ============================================================
// COMPLETE STORE LOADING
// ============================================================

#[test]
fn load_all_format_configs() {
    let store = FormatConfigStore::load_dir(format_directory())
        .expect("Failed to load format configurations");

    assert_eq!(store.trade_count(), 15);
    assert_eq!(store.depth_count(), 15);
    assert_eq!(store.len(), 30);
    assert!(!store.is_empty());

    for profile in EXPECTED_PROFILES {
        assert!(store.contains(FormatKind::Trades, profile));
        assert!(store.contains(FormatKind::Depth, profile));
    }
}

#[test]
fn loaded_store_validates() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    store
        .validate_all()
        .expect("Format store validation failed");
}

// ============================================================
// LOOKUP
// ============================================================

#[test]
fn lookup_trade_format() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let config = store.trade("binance_spot").unwrap();

    assert_eq!(config.format.id, "binance_spot");
    assert_eq!(config.qualified_id(), "trades/binance_spot");
}

#[test]
fn lookup_depth_format() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let config = store.depth("bybit_linear").unwrap();

    assert_eq!(config.format.id, "bybit_linear");
    assert_eq!(config.qualified_id(), "depth/bybit_linear");
}

#[test]
fn lookup_missing_trade_returns_none() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    assert!(store.trade("nonexistent").is_none());
}

#[test]
fn lookup_missing_depth_returns_none() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    assert!(store.depth("nonexistent").is_none());
}

#[test]
fn lookup_trade_by_exchange_and_market() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let config = store
        .trade_for(ExchangeId::Binance, MarketType::Spot)
        .unwrap();

    assert_eq!(config.format.id, "binance_spot");
}

#[test]
fn lookup_depth_by_exchange_and_market() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let config = store
        .depth_for(ExchangeId::Okx, MarketType::Linear)
        .unwrap();

    assert_eq!(config.format.id, "okx_linear");
}

// ============================================================
// NAMESPACE ISOLATION
// ============================================================

#[test]
fn trade_and_depth_namespaces_are_independent() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let trade = store.trade("binance_spot").unwrap();
    let depth = store.depth("binance_spot").unwrap();

    assert_eq!(trade.format.id, depth.format.id);

    assert_eq!(trade.qualified_id(), "trades/binance_spot");
    assert_eq!(depth.qualified_id(), "depth/binance_spot");
}

#[test]
fn contains_checks_correct_namespace() {
    let directory = TestDirectory::new();
    directory.create_format_dirs();
    directory.copy_trade("binance_spot");

    let store = FormatConfigStore::load_dir(directory.path()).unwrap();

    assert!(store.contains(FormatKind::Trades, "binance_spot"));
    assert!(!store.contains(FormatKind::Depth, "binance_spot"));
}

// ============================================================
// SHARED OWNERSHIP
// ============================================================

#[test]
fn shared_trade_config_uses_same_allocation() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let first = store.trade_shared("binance_spot").unwrap();
    let second = store.trade_shared("binance_spot").unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn shared_depth_config_uses_same_allocation() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let first = store.depth_shared("binance_spot").unwrap();
    let second = store.depth_shared("binance_spot").unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn shared_config_survives_store_drop() {
    let shared = {
        let store = FormatConfigStore::load_dir(format_directory()).unwrap();

        store.trade_shared("binance_spot").unwrap()
    };

    assert_eq!(shared.format.id, "binance_spot");
}

#[test]
fn cloned_store_shares_config_allocations() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();
    let cloned = store.clone();

    let first = store.trade_shared("binance_spot").unwrap();
    let second = cloned.trade_shared("binance_spot").unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

// ============================================================
// ITERATION
// ============================================================

#[test]
fn iterate_all_trade_formats() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let profiles: Vec<_> = store.trades().map(|(id, _)| id.to_string()).collect();

    assert_eq!(profiles.len(), 15);

    for profile in EXPECTED_PROFILES {
        assert!(profiles.contains(&profile.to_string()));
    }
}

#[test]
fn iterate_all_depth_formats() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let profiles: Vec<_> = store.depths().map(|(id, _)| id.to_string()).collect();

    assert_eq!(profiles.len(), 15);

    for profile in EXPECTED_PROFILES {
        assert!(profiles.contains(&profile.to_string()));
    }
}

#[test]
fn iteration_is_deterministic() {
    let store = FormatConfigStore::load_dir(format_directory()).unwrap();

    let profiles: Vec<_> = store.trades().map(|(id, _)| id).collect();

    let mut sorted = profiles.clone();
    sorted.sort();

    assert_eq!(profiles, sorted);
}

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
