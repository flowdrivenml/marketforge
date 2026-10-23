use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use marketforge_engine::live::config::{ConfigError, ExchangeConfigStore, ExchangeId};

// ============================================================
// HELPERS
// ============================================================

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

fn load_store() -> ExchangeConfigStore {
    ExchangeConfigStore::load_dir(config_dir())
        .unwrap_or_else(|error| panic!("Failed to load exchange configuration store: {error}"))
}

fn expected_profiles() -> Vec<&'static str> {
    vec![
        "binance_inverse",
        "binance_linear",
        "binance_spot",
        "bitget_inverse",
        "bitget_linear",
        "bitget_spot",
        "bybit_inverse",
        "bybit_linear",
        "bybit_spot",
        "gateio_inverse",
        "gateio_linear",
        "gateio_spot",
        "okx_inverse",
        "okx_linear",
        "okx_spot",
    ]
}

/// Create a unique temporary directory.
///
/// Uses the process ID and a monotonically increasing counter
/// to avoid collisions between tests.
fn temporary_directory() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let path = std::env::temp_dir().join(format!(
        "marketforge_exchange_config_test_{}_{}",
        std::process::id(),
        id
    ));

    fs::create_dir_all(&path).expect("Failed to create temporary directory");

    path
}

/// Automatically remove a temporary directory when dropped.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        Self {
            path: temporary_directory(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn copy_profile(&self, source: &str, destination: &str) {
        fs::copy(config_dir().join(source), self.path.join(destination))
            .expect("Failed to copy exchange configuration");
    }

    fn write(&self, filename: &str, contents: &str) {
        fs::write(self.path.join(filename), contents).expect("Failed to write test configuration");
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("Failed to remove temporary directory");
    }
}

// ============================================================
// LOADING
// ============================================================

#[test]
fn load_all_exchange_profiles() {
    let store = load_store();

    assert_eq!(store.len(), 15);
    assert!(!store.is_empty());

    println!("PASS: loaded {} exchange profiles", store.len());
}

#[test]
fn load_expected_exchange_profiles() {
    let store = load_store();

    for profile in expected_profiles() {
        assert!(
            store.contains(profile),
            "Missing exchange profile: {profile}"
        );

        println!("PASS: {profile}");
    }
}

#[test]
fn load_single_exchange_profile() {
    let temp = TempDir::new();

    temp.copy_profile("binance_spot.toml", "binance_spot.toml");

    let store =
        ExchangeConfigStore::load_dir(temp.path()).expect("Failed to load single exchange profile");

    assert_eq!(store.len(), 1);
    assert!(store.contains("binance_spot"));
}

// ============================================================
// PROFILE LOOKUP
// ============================================================

#[test]
fn retrieve_profile_by_name() {
    let store = load_store();

    let config = store
        .get("binance_spot")
        .expect("Binance Spot profile not found");

    assert_eq!(config.exchange.profile, "binance_spot");
    assert_eq!(config.exchange.id, ExchangeId::Binance);
}

#[test]
fn retrieve_profile_by_exchange_and_market() {
    let store = load_store();

    let config = store
        .get_market("bybit", "linear")
        .expect("Bybit Linear profile not found");

    assert_eq!(config.exchange.profile, "bybit_linear");
    assert_eq!(config.exchange.id, ExchangeId::Bybit);
}

#[test]
fn retrieve_nonexistent_profile() {
    let store = load_store();

    assert!(store.get("unknown_spot").is_none());
    assert!(store.get_market("unknown", "spot").is_none());
    assert!(!store.contains("unknown_spot"));
}

// ============================================================
// EXCHANGE FILTERING
// ============================================================

#[test]
fn filter_profiles_by_exchange() {
    let store = load_store();

    let profiles: Vec<_> = store
        .by_exchange(ExchangeId::Binance)
        .map(|config| config.exchange.profile.as_str())
        .collect();

    assert_eq!(
        profiles,
        vec!["binance_inverse", "binance_linear", "binance_spot",]
    );
}

#[test]
fn filter_all_supported_exchanges() {
    let store = load_store();

    let exchanges = [
        ExchangeId::Binance,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Bitget,
        ExchangeId::Gateio,
    ];

    for exchange in exchanges {
        let profiles: Vec<_> = store.by_exchange(exchange).collect();

        assert_eq!(
            profiles.len(),
            3,
            "Expected three profiles for {}",
            exchange.as_str()
        );
    }
}

// ============================================================
// ITERATION
// ============================================================

#[test]
fn iterate_all_profiles() {
    let store = load_store();

    let profiles: Vec<_> = store.iter().map(|(name, _)| name).collect();

    assert_eq!(profiles, expected_profiles());
}

#[test]
fn iterate_profile_names() {
    let store = load_store();

    let names: Vec<_> = store.names().collect();

    assert_eq!(names, expected_profiles());
}

#[test]
fn verify_deterministic_ordering() {
    let first = load_store();
    let second = load_store();

    let first_names: Vec<_> = first.names().collect();
    let second_names: Vec<_> = second.names().collect();

    assert_eq!(first_names, second_names);

    let mut sorted = first_names.clone();
    sorted.sort();

    assert_eq!(first_names, sorted);
}

// ============================================================
// IMMUTABLE SHARING
// ============================================================

#[test]
fn retrieve_shared_configuration() {
    let store = load_store();

    let first = store
        .get_shared("binance_spot")
        .expect("Missing Binance Spot profile");

    let second = store
        .get_shared("binance_spot")
        .expect("Missing Binance Spot profile");

    assert!(
        Arc::ptr_eq(&first, &second),
        "Shared configurations should reference the same allocation"
    );
}

#[test]
fn shared_configuration_matches_store() {
    let store = load_store();

    let shared = store
        .get_shared("bybit_linear")
        .expect("Missing Bybit Linear profile");

    let borrowed = store
        .get("bybit_linear")
        .expect("Missing borrowed configuration");

    assert!(
        std::ptr::eq(shared.as_ref(), borrowed),
        "Shared and borrowed configurations should reference the same object"
    );
}

#[test]
fn cloned_store_shares_configurations() {
    let original = load_store();
    let cloned = original.clone();

    let first = original.get_shared("okx_spot").unwrap();
    let second = cloned.get_shared("okx_spot").unwrap();

    assert!(
        Arc::ptr_eq(&first, &second),
        "Cloning the store should not duplicate configurations"
    );
}

#[test]
fn shared_configuration_survives_store_drop() {
    let shared = {
        let store = load_store();

        store
            .get_shared("binance_spot")
            .expect("Missing Binance Spot profile")
    };

    assert_eq!(shared.exchange.profile, "binance_spot");
}

#[test]
fn share_configuration_across_threads() {
    let store = load_store();

    let config = store
        .get_shared("binance_spot")
        .expect("Missing Binance Spot profile");

    let handle = std::thread::spawn(move || {
        assert_eq!(config.exchange.profile, "binance_spot");
    });

    handle.join().expect("Worker thread panicked");
}

// ============================================================
// DUPLICATE DETECTION
// ============================================================

#[test]
fn reject_duplicate_profile_identity() {
    let temp = TempDir::new();

    temp.copy_profile("binance_spot.toml", "first.toml");
    temp.copy_profile("binance_spot.toml", "second.toml");

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::DuplicateProfile { .. })),
        "Duplicate profile identity was accepted: {result:?}"
    );
}

// ============================================================
// INVALID DIRECTORIES
// ============================================================

#[test]
fn reject_nonexistent_directory() {
    let temp = TempDir::new();

    let missing = temp.path().join("nonexistent");

    let result = ExchangeConfigStore::load_dir(&missing);

    assert!(
        matches!(result, Err(ConfigError::Directory { .. })),
        "Nonexistent directory was accepted: {result:?}"
    );
}

#[test]
fn reject_empty_directory() {
    let temp = TempDir::new();

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::Validation { .. })),
        "Empty configuration directory was accepted: {result:?}"
    );
}

#[test]
fn reject_directory_without_toml_files() {
    let temp = TempDir::new();

    temp.write("README.txt", "Not a configuration");

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::Validation { .. })),
        "Directory without TOML files was accepted: {result:?}"
    );
}

// ============================================================
// INVALID CONFIGURATIONS
// ============================================================

#[test]
fn reject_malformed_toml() {
    let temp = TempDir::new();

    temp.write("invalid.toml", "this is not valid TOML =");

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::Parse { .. })),
        "Malformed TOML was accepted: {result:?}"
    );
}

#[test]
fn reject_invalid_exchange_profile() {
    let temp = TempDir::new();

    let original = fs::read_to_string(config_dir().join("binance_spot.toml"))
        .expect("Cannot read Binance Spot fixture");

    let invalid = original.replace("schema_version = 1", "schema_version = 999");

    temp.write("invalid.toml", &invalid);

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::Validation { .. })),
        "Invalid exchange profile was accepted: {result:?}"
    );
}

#[test]
fn reject_unknown_configuration_field() {
    let temp = TempDir::new();

    let original = fs::read_to_string(config_dir().join("binance_spot.toml"))
        .expect("Cannot read Binance Spot fixture");

    let invalid = original.replace(
        "schema_version = 1",
        "schema_version = 1\nunknown_setting = true",
    );

    temp.write("invalid.toml", &invalid);

    let result = ExchangeConfigStore::load_dir(temp.path());

    assert!(
        matches!(result, Err(ConfigError::Parse { .. })),
        "Unknown configuration field was accepted: {result:?}"
    );
}

// ============================================================
// FILE FILTERING
// ============================================================

#[test]
fn ignore_non_toml_files() {
    let temp = TempDir::new();

    temp.copy_profile("binance_spot.toml", "binance_spot.toml");
    temp.write("README.md", "Ignored documentation");
    temp.write("notes.txt", "Ignored notes");

    let store = ExchangeConfigStore::load_dir(temp.path())
        .expect("Failed to load directory with unrelated files");

    assert_eq!(store.len(), 1);
    assert!(store.contains("binance_spot"));
}

#[test]
fn ignore_subdirectories() {
    let temp = TempDir::new();

    temp.copy_profile("binance_spot.toml", "binance_spot.toml");

    let nested = temp.path().join("nested");
    fs::create_dir(&nested).expect("Cannot create nested directory");

    fs::write(nested.join("invalid.toml"), "invalid TOML").expect("Cannot write nested file");

    let store = ExchangeConfigStore::load_dir(temp.path()).expect("Subdirectory should be ignored");

    assert_eq!(store.len(), 1);
}

// ============================================================
// STORE CONSISTENCY
// ============================================================

#[test]
fn store_keys_match_profile_identities() {
    let store = load_store();

    for (name, config) in store.iter() {
        assert_eq!(
            name, config.exchange.profile,
            "Store key differs from exchange profile identity"
        );
    }
}

#[test]
fn store_contains_only_valid_configurations() {
    let store = load_store();

    for (name, config) in store.iter() {
        config.validate().unwrap_or_else(|error| {
            panic!("Store contains invalid profile '{name}': {error}");
        });
    }
}

#[test]
fn empty_store_has_correct_state() {
    let store = ExchangeConfigStore::new();

    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
    assert!(store.names().next().is_none());
    assert!(store.get("binance_spot").is_none());
}
