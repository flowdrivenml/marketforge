use super::exchange::ExchangeId;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{error::ConfigError, exchange::ExchangeConfig};

/// Immutable collection of validated exchange configurations.
///
/// All profiles are loaded during initialization.
/// The store performs no filesystem operations afterward.
#[derive(Debug, Clone, Default)]
pub struct ExchangeConfigStore {
    profiles: BTreeMap<String, Arc<ExchangeConfig>>,
}

impl ExchangeConfigStore {
    /// Create an empty configuration store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load every TOML exchange profile from a directory.
    ///
    /// Only regular files with a `.toml` extension are loaded.
    /// Subdirectories and unrelated files are ignored.
    ///
    /// Loading fails if any profile is invalid or duplicated.
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let entries = fs::read_dir(path).map_err(|source| ConfigError::Directory {
            path: path.to_path_buf(),
            source,
        })?;

        let mut paths = Vec::<PathBuf>::new();

        for entry in entries {
            let entry = entry.map_err(|source| ConfigError::Directory {
                path: path.to_path_buf(),
                source,
            })?;

            let file_type = entry.file_type().map_err(|source| ConfigError::Directory {
                path: entry.path(),
                source,
            })?;

            if !file_type.is_file() {
                continue;
            }

            let file_path = entry.path();

            if file_path.extension().is_some_and(|ext| ext == "toml") {
                paths.push(file_path);
            }
        }

        // Deterministic loading and error reporting.
        paths.sort();

        if paths.is_empty() {
            return Err(ConfigError::Validation {
                path: path.to_path_buf(),
                reason: "no exchange TOML profiles found".into(),
            });
        }

        let mut store = Self::new();

        for file_path in paths {
            let config = ExchangeConfig::load(&file_path)?;

            let profile = config.exchange.profile.clone();

            if store.profiles.contains_key(&profile) {
                return Err(ConfigError::DuplicateProfile {
                    profile,
                    path: file_path,
                });
            }

            store.profiles.insert(profile, Arc::new(config));
        }

        Ok(store)
    }

    /// Retrieve a profile by its unique name.
    ///
    /// Example: "binance_spot".
    pub fn get(&self, profile: &str) -> Option<&ExchangeConfig> {
        self.profiles.get(profile).map(Arc::as_ref)
    }

    /// Retrieve an Arc for sharing with an asynchronous component.
    ///
    /// Cloning the Arc does not clone the configuration.
    pub fn get_shared(&self, profile: &str) -> Option<Arc<ExchangeConfig>> {
        self.profiles.get(profile).map(Arc::clone)
    }

    /// Retrieve a profile using exchange and market identifiers.
    ///
    /// Example: get_market("binance", "spot").
    pub fn get_market(&self, exchange: &str, market: &str) -> Option<&ExchangeConfig> {
        let profile = format!("{exchange}_{market}");
        self.get(&profile)
    }

    /// Return all configurations belonging to an exchange.

    pub fn by_exchange(&self, exchange: ExchangeId) -> impl Iterator<Item = &ExchangeConfig> {
        self.profiles
            .values()
            .map(Arc::as_ref)
            .filter(move |config| config.exchange.id == exchange)
    }

    /// Iterate over all loaded configurations.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ExchangeConfig)> {
        self.profiles
            .iter()
            .map(|(name, config)| (name.as_str(), config.as_ref()))
    }

    /// Iterate over available profile names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.profiles.keys().map(String::as_str)
    }

    /// Number of loaded exchange profiles.
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the store contains no profiles.
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Check whether a profile exists.
    pub fn contains(&self, profile: &str) -> bool {
        self.profiles.contains_key(profile)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use crate::live::config::{ConfigError, ExchangeConfigStore};

    // ============================================================
    // HELPERS
    // ============================================================

    fn config_dir() -> &'static Path {
        Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../data/.config/live/exchanges"
        ))
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
            fs::write(self.path.join(filename), contents)
                .expect("Failed to write test configuration");
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

    // ============================================================
    // PROFILE LOOKUP
    // ============================================================

    // ============================================================
    // EXCHANGE FILTERING
    // ============================================================

    // ============================================================
    // ITERATION
    // ============================================================

    // ============================================================
    // IMMUTABLE SHARING
    // ============================================================

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

        let store =
            ExchangeConfigStore::load_dir(temp.path()).expect("Subdirectory should be ignored");

        assert_eq!(store.len(), 1);
    }

    // ============================================================
    // STORE CONSISTENCY
    // ============================================================

    #[test]
    fn empty_store_has_correct_state() {
        let store = ExchangeConfigStore::new();

        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert!(store.names().next().is_none());
        assert!(store.get("binance_spot").is_none());
    }
}
