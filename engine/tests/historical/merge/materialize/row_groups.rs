use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::merge::CanonicalRowGroupIndex;

// -----------------------------------------------------------------------------
// Fixture
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn indexes_parquet_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    assert_eq!(index.total_rows(), 30);
    assert_eq!(index.row_group_count(), 3);

    assert_eq!(index.groups()[0].row_start, 0);
    assert_eq!(index.groups()[0].row_end, 10);

    assert_eq!(index.groups()[1].row_start, 10);
    assert_eq!(index.groups()[1].row_end, 20);

    assert_eq!(index.groups()[2].row_start, 20);
    assert_eq!(index.groups()[2].row_end, 30);
}

#[test]
fn resolves_single_row_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    let slices = index.resolve(12, 5).unwrap();

    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0].group_index, 1);
    assert_eq!(slices[0].local_offset, 2);
    assert_eq!(slices[0].row_count, 5);
}

#[test]
fn resolves_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    let slices = index.resolve(8, 15).unwrap();

    assert_eq!(slices.len(), 3);

    assert_eq!(slices[0].group_index, 0);
    assert_eq!(slices[0].local_offset, 8);
    assert_eq!(slices[0].row_count, 2);

    assert_eq!(slices[1].group_index, 1);
    assert_eq!(slices[1].local_offset, 0);
    assert_eq!(slices[1].row_count, 10);

    assert_eq!(slices[2].group_index, 2);
    assert_eq!(slices[2].local_offset, 0);
    assert_eq!(slices[2].row_count, 3);
}

#[test]
fn accepts_zero_row_reference() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    assert!(index.resolve(30, 0).unwrap().is_empty());
}

#[test]
fn rejects_out_of_bounds_reference() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    assert!(index.resolve(25, 10).is_err());
}

#[test]
fn rejects_range_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("canonical.parquet");

    write_fixture(&path);

    let index = CanonicalRowGroupIndex::open(&path).unwrap();

    assert!(index.resolve(u64::MAX, 2).is_err());
}

#[test]
fn immutable_index_is_thread_shareable() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<CanonicalRowGroupIndex>();
}
