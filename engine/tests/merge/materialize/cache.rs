use std::{path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use marketforge_engine::process::merge::CanonicalRowGroupCache;

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

#[test]
fn repeated_lookup_hits_cache() {
    let mut cache = CanonicalRowGroupCache::new(1024 * 1024).unwrap();

    let path = Path::new("canonical.parquet");

    cache.insert(path, 0, vec![batch(vec![1, 2, 3])]).unwrap();

    assert!(cache.get(path, 0).is_some());
    assert!(cache.get(path, 0).is_some());

    assert_eq!(cache.metrics().hits, 2);
    assert_eq!(cache.metrics().misses, 0);
}

#[test]
fn missing_entry_counts_miss() {
    let mut cache = CanonicalRowGroupCache::new(1024).unwrap();

    assert!(cache.get(Path::new("missing.parquet"), 0).is_none());

    assert_eq!(cache.metrics().misses, 1);
}

#[test]
fn evicts_least_recently_used() {
    let sample = batch(vec![1, 2, 3, 4]);
    let size = sample.get_array_memory_size() as u64;

    let mut cache = CanonicalRowGroupCache::new(size * 2).unwrap();

    let path = Path::new("canonical.parquet");

    cache.insert(path, 0, vec![sample.clone()]).unwrap();
    cache.insert(path, 1, vec![sample.clone()]).unwrap();

    // Refresh row group 0.
    assert!(cache.get(path, 0).is_some());

    // Inserting group 2 should evict group 1.
    cache.insert(path, 2, vec![sample]).unwrap();

    assert!(cache.get(path, 0).is_some());
    assert!(cache.get(path, 1).is_none());
    assert!(cache.get(path, 2).is_some());

    assert_eq!(cache.metrics().evictions, 1);
}

#[test]
fn oversized_entry_bypasses_cache() {
    let mut cache = CanonicalRowGroupCache::new(64).unwrap();

    let inserted = cache
        .insert(Path::new("large.parquet"), 0, vec![batch(vec![0; 1000])])
        .unwrap();

    assert!(!inserted);
    assert!(cache.is_empty());

    assert_eq!(cache.metrics().oversized_bypasses, 1);
}

#[test]
fn memory_budget_is_enforced() {
    let mut cache = CanonicalRowGroupCache::new(1024).unwrap();

    for group in 0..100 {
        cache
            .insert(
                Path::new("canonical.parquet"),
                group,
                vec![batch(vec![group as i64; 8])],
            )
            .unwrap();

        assert!(cache.used_bytes() <= cache.max_bytes());
    }
}

#[test]
fn clear_releases_entries() {
    let mut cache = CanonicalRowGroupCache::new(1024 * 1024).unwrap();

    cache
        .insert(
            Path::new("canonical.parquet"),
            0,
            vec![batch(vec![1, 2, 3])],
        )
        .unwrap();

    cache.clear();

    assert!(cache.is_empty());
    assert_eq!(cache.used_bytes(), 0);
}
