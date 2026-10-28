use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;

use crate::shared::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Cache identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    path: PathBuf,
    row_group: usize,
}

// -----------------------------------------------------------------------------
// Cached row group
// -----------------------------------------------------------------------------

#[derive(Debug)]
struct CacheEntry {
    batches: Vec<RecordBatch>,
    bytes: u64,
    last_access: u64,
}

// -----------------------------------------------------------------------------
// Metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub oversized_bypasses: u64,
    pub decoded_bytes_inserted: u64,
    pub peak_cached_bytes: u64,
}

// -----------------------------------------------------------------------------
// Row-group cache
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct CanonicalRowGroupCache {
    entries: HashMap<CacheKey, CacheEntry>,

    max_bytes: u64,
    used_bytes: u64,

    access_clock: u64,

    metrics: CanonicalCacheMetrics,
}

impl CanonicalRowGroupCache {
    pub fn new(max_bytes: u64) -> Result<Self> {
        if max_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "canonical cache budget must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            entries: HashMap::new(),
            max_bytes,
            used_bytes: 0,
            access_clock: 0,
            metrics: CanonicalCacheMetrics::default(),
        })
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn metrics(&self) -> &CanonicalCacheMetrics {
        &self.metrics
    }

    // -------------------------------------------------------------------------
    // Lookup
    // -------------------------------------------------------------------------

    /// Return cached Arrow batches for one decoded Parquet row group.
    ///
    /// RecordBatch cloning shares Arrow buffers; it does not copy
    /// the underlying canonical values.
    pub fn get(&mut self, path: &Path, row_group: usize) -> Option<Vec<RecordBatch>> {
        let key = CacheKey {
            path: path.to_path_buf(),
            row_group,
        };

        if !self.entries.contains_key(&key) {
            self.metrics.misses += 1;
            return None;
        }

        self.access_clock += 1;

        let entry = self.entries.get_mut(&key)?;

        entry.last_access = self.access_clock;

        self.metrics.hits += 1;

        Some(entry.batches.clone())
    }

    // -------------------------------------------------------------------------
    // Insert
    // -------------------------------------------------------------------------

    /// Insert a complete decoded row group.
    ///
    /// Returns false if the row group exceeds the cache budget.
    pub fn insert(
        &mut self,
        path: &Path,
        row_group: usize,
        batches: Vec<RecordBatch>,
    ) -> Result<bool> {
        let bytes = batches.iter().try_fold(0_u64, |total, batch| {
            let size = u64::try_from(batch.get_array_memory_size())
                .map_err(|_| invalid_error("Arrow batch memory size overflow"))?;

            total
                .checked_add(size)
                .ok_or_else(|| invalid_error("cache entry memory size overflow"))
        })?;

        if bytes > self.max_bytes {
            self.metrics.oversized_bypasses += 1;
            return Ok(false);
        }

        let key = CacheKey {
            path: path.to_path_buf(),
            row_group,
        };

        // Replace existing entry if present.
        if let Some(previous) = self.entries.remove(&key) {
            self.used_bytes -= previous.bytes;
        }

        // Evict least-recently-used entries until sufficient space exists.
        while self.used_bytes > self.max_bytes - bytes {
            self.evict_lru()?;
        }

        self.access_clock += 1;

        self.entries.insert(
            key,
            CacheEntry {
                batches,
                bytes,
                last_access: self.access_clock,
            },
        );

        self.used_bytes = self
            .used_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid_error("cache memory accounting overflow"))?;

        self.metrics.insertions += 1;

        self.metrics.decoded_bytes_inserted =
            self.metrics.decoded_bytes_inserted.saturating_add(bytes);

        self.metrics.peak_cached_bytes = self.metrics.peak_cached_bytes.max(self.used_bytes);

        Ok(true)
    }

    // -------------------------------------------------------------------------
    // LRU eviction
    // -------------------------------------------------------------------------

    fn evict_lru(&mut self) -> Result<()> {
        let key = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())
            .ok_or_else(|| invalid_error("cache eviction requested on empty cache"))?;

        let removed = self
            .entries
            .remove(&key)
            .ok_or_else(|| invalid_error("cache eviction failed"))?;

        self.used_bytes -= removed.bytes;

        self.metrics.evictions += 1;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Reset
    // -------------------------------------------------------------------------

    pub fn clear(&mut self) {
        self.entries.clear();
        self.used_bytes = 0;
        self.access_clock = 0;
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
