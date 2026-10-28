use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::merge::{CanonicalParquetReader, CanonicalRowGroupCache};

fn write_fixture(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for chunk in (0..30_i64).collect::<Vec<_>>().chunks(10) {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(chunk.to_vec()))],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

fn values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn repeated_reads_hit_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let reader = CanonicalParquetReader::new(4).unwrap();
    let mut cache = CanonicalRowGroupCache::new(1024 * 1024).unwrap();

    let first = reader.read_range_cached(&path, 2, 3, &mut cache).unwrap();
    let second = reader.read_range_cached(&path, 5, 3, &mut cache).unwrap();

    assert_eq!(values(&first), vec![2, 3, 4]);
    assert_eq!(values(&second), vec![5, 6, 7]);

    assert_eq!(cache.metrics().misses, 1);
    assert_eq!(cache.metrics().hits, 1);
}

#[test]
fn reads_across_cached_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let reader = CanonicalParquetReader::new(4).unwrap();
    let mut cache = CanonicalRowGroupCache::new(1024 * 1024).unwrap();

    let first = reader.read_range_cached(&path, 8, 15, &mut cache).unwrap();

    assert_eq!(values(&first), (8..23).collect::<Vec<_>>());
    assert_eq!(cache.metrics().misses, 3);

    let second = reader.read_range_cached(&path, 10, 10, &mut cache).unwrap();

    assert_eq!(values(&second), (10..20).collect::<Vec<_>>());
    assert_eq!(cache.metrics().hits, 1);
}

#[test]
fn small_cache_evicts_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let reader = CanonicalParquetReader::new(4).unwrap();

    // Enough for one small row group, but not all three.
    let mut cache = CanonicalRowGroupCache::new(256).unwrap();

    reader.read_range_cached(&path, 0, 10, &mut cache).unwrap();
    reader.read_range_cached(&path, 10, 10, &mut cache).unwrap();
    reader.read_range_cached(&path, 20, 10, &mut cache).unwrap();

    assert!(cache.used_bytes() <= cache.max_bytes());
    assert!(cache.metrics().evictions > 0 || cache.metrics().oversized_bypasses > 0);
}

#[test]
fn zero_row_reference_does_not_touch_cache() {
    let reader = CanonicalParquetReader::default();
    let mut cache = CanonicalRowGroupCache::new(1024).unwrap();

    let batches = reader
        .read_range_cached(Path::new("unused.parquet"), 0, 0, &mut cache)
        .unwrap();

    assert!(batches.is_empty());
    assert_eq!(cache.metrics().hits, 0);
    assert_eq!(cache.metrics().misses, 0);
}
