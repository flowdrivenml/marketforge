use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::live::config::{
    error::ConfigError,
    exchange::{ExchangeId, MarketType},
};

use super::{
    depth::DepthFormatConfig,
    identity::FormatKind,
    trade::TradeFormatConfig,
};

// ============================================================
// FORMAT CONFIGURATION STORE
// ============================================================

/// Immutable collection of trade and depth format configurations.
///
/// Formats are indexed by their exchange-market identity,
/// independently within the trades and depth namespaces.
///
/// Example keys:
///
/// trades/binance_spot
/// depth/binance_spot
#[derive(Debug, Clone, Default)]
pub struct FormatConfigStore {
    trades: BTreeMap<String, Arc<TradeFormatConfig>>,
    depth: BTreeMap<String, Arc<DepthFormatConfig>>,
}

impl FormatConfigStore {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    /// Creates an empty format configuration store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads all trade and depth format configurations.
    ///
    /// Expected directory structure:
    ///
    /// formats/
    /// ├── trades/
    /// │   ├── binance_spot.toml
    /// │   └── ...
    /// └── depth/
    ///     ├── binance_spot.toml
    ///     └── ...
    ///
    /// Loading is atomic from the caller's perspective:
    /// either the complete store is returned or an error.
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let root = path.as_ref();

        let trades = Self::load_trades_dir(&root.join("trades"))?;
        let depth = Self::load_depth_dir(&root.join("depth"))?;

        Ok(Self { trades, depth })
    }

    // ========================================================
    // TRADE LOADING
    // ========================================================

    fn load_trades_dir(
        directory: &Path,
    ) -> Result<BTreeMap<String, Arc<TradeFormatConfig>>, ConfigError> {
        let mut configs = BTreeMap::new();

        for path in Self::toml_files(directory)? {
            let config = TradeFormatConfig::load(&path)?;

            let id = config.format.id.clone();

            Self::validate_filename(&path, &id)?;

            if configs.contains_key(&id) {
                return Err(ConfigError::Validation {
                    path,
                    reason: format!(
                        "duplicate trade format identity '{id}'"
                    ),
                });
            }

            configs.insert(id, Arc::new(config));
        }

        Ok(configs)
    }

    // ========================================================
    // DEPTH LOADING
    // ========================================================

    fn load_depth_dir(
        directory: &Path,
    ) -> Result<BTreeMap<String, Arc<DepthFormatConfig>>, ConfigError> {
        let mut configs = BTreeMap::new();

        for path in Self::toml_files(directory)? {
            let config = DepthFormatConfig::load(&path)?;

            let id = config.format.id.clone();

            Self::validate_filename(&path, &id)?;

            if configs.contains_key(&id) {
                return Err(ConfigError::Validation {
                    path,
                    reason: format!(
                        "duplicate depth format identity '{id}'"
                    ),
                });
            }

            configs.insert(id, Arc::new(config));
        }

        Ok(configs)
    }

    // ========================================================
    // FILE DISCOVERY
    // ========================================================

    /// Returns all regular TOML files in a directory.
    ///
    /// Files are sorted to guarantee deterministic loading
    /// and error reporting.
    fn toml_files(directory: &Path) -> Result<Vec<PathBuf>, ConfigError> {
        let entries = fs::read_dir(directory).map_err(|source| {
            ConfigError::Read {
                path: directory.to_path_buf(),
                source,
            }
        })?;

        let mut paths = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|source| ConfigError::Read {
                path: directory.to_path_buf(),
                source,
            })?;

            let path = entry.path();

            let file_type = entry.file_type().map_err(|source| {
                ConfigError::Read {
                    path: path.clone(),
                    source,
                }
            })?;

            if !file_type.is_file() {
                continue;
            }

            if path.extension().is_some_and(|extension| extension == "toml") {
                paths.push(path);
            }
        }

        paths.sort();

        Ok(paths)
    }

    // ========================================================
    // FILE IDENTITY VALIDATION
    // ========================================================

    /// Ensures the filename matches the declared format ID.
    ///
    /// Example:
    ///
    /// binance_spot.toml -> format.id = "binance_spot"
    fn validate_filename(path: &Path, id: &str) -> Result<(), ConfigError> {
        let filename = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| ConfigError::Validation {
                path: path.to_path_buf(),
                reason: "invalid format filename".into(),
            })?;

        if filename != id {
            return Err(ConfigError::Validation {
                path: path.to_path_buf(),
                reason: format!(
                    "format filename '{filename}' does not match declared ID '{id}'"
                ),
            });
        }

        Ok(())
    }

    // ========================================================
    // LOOKUP
    // ========================================================

    /// Returns a trade format by its unqualified ID.
    ///
    /// Example: "binance_spot"
    pub fn trade(&self, id: &str) -> Option<&TradeFormatConfig> {
        self.trades.get(id).map(Arc::as_ref)
    }

    /// Returns a depth format by its unqualified ID.
    ///
    /// Example: "binance_spot"
    pub fn depth(&self, id: &str) -> Option<&DepthFormatConfig> {
        self.depth.get(id).map(Arc::as_ref)
    }

    /// Returns a shared trade format.
    ///
    /// Cloning the Arc does not clone the configuration.
    pub fn trade_shared(&self, id: &str) -> Option<Arc<TradeFormatConfig>> {
        self.trades.get(id).cloned()
    }

    /// Returns a shared depth format.
    pub fn depth_shared(&self, id: &str) -> Option<Arc<DepthFormatConfig>> {
        self.depth.get(id).cloned()
    }

    /// Returns a trade format by exchange and market.
    pub fn trade_for(
        &self,
        exchange: ExchangeId,
        market: MarketType,
    ) -> Option<&TradeFormatConfig> {
        let id = format!("{}_{}", exchange.as_str(), market.as_str());

        self.trade(&id)
    }

    /// Returns a depth format by exchange and market.
    pub fn depth_for(
        &self,
        exchange: ExchangeId,
        market: MarketType,
    ) -> Option<&DepthFormatConfig> {
        let id = format!("{}_{}", exchange.as_str(), market.as_str());

        self.depth(&id)
    }

    // ========================================================
    // EXISTENCE CHECKS
    // ========================================================

    pub fn contains(&self, kind: FormatKind, id: &str) -> bool {
        match kind {
            FormatKind::Trades => self.trades.contains_key(id),
            FormatKind::Depth => self.depth.contains_key(id),
        }
    }

    // ========================================================
    // ITERATION
    // ========================================================

    pub fn trades(&self) -> impl Iterator<Item = (&str, &TradeFormatConfig)> {
        self.trades
            .iter()
            .map(|(id, config)| (id.as_str(), config.as_ref()))
    }

    pub fn depths(&self) -> impl Iterator<Item = (&str, &DepthFormatConfig)> {
        self.depth
            .iter()
            .map(|(id, config)| (id.as_str(), config.as_ref()))
    }

    // ========================================================
    // COUNTS
    // ========================================================

    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }

    pub fn depth_count(&self) -> usize {
        self.depth.len()
    }

    pub fn len(&self) -> usize {
        self.trades.len() + self.depth.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty() && self.depth.is_empty()
    }

    // ========================================================
    // VALIDATION
    // ========================================================

    /// Revalidates all loaded configurations.
    ///
    /// Useful for tests and diagnostic interfaces.
    pub fn validate_all(&self) -> Result<(), String> {
        for (id, config) in self.trades() {
            config
                .validate()
                .map_err(|error| format!("trades/{id}: {error}"))?;
        }

        for (id, config) in self.depths() {
            config
                .validate()
                .map_err(|error| format!("depth/{id}: {error}"))?;
        }

        Ok(())
    }
}
