use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use marketforge_engine::process::merge::CanonicalFileSlice;
use marketforge_engine::process::merge::CanonicalForwardCursor;
use marketforge_engine::process::merge::CanonicalParquetReader;

fn write_fixture(path: &Path, values: &[i64], group_size: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for chunk in values.chunks(group_size) {
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

fn read_values(batches: &[RecordBatch]) -> Vec<i64> {
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
fn reads_exact_single_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("single.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let reader = CanonicalParquetReader::new(4).unwrap();
    let batches = reader.read_range(&path, 15, 1).unwrap();

    assert_eq!(read_values(&batches), vec![15]);
}

#[test]
fn reads_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("groups.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let reader = CanonicalParquetReader::new(4).unwrap();
    let batches = reader.read_range(&path, 8, 15).unwrap();

    assert_eq!(read_values(&batches), (8..23).collect::<Vec<_>>());
}

#[test]
fn reads_across_files() {
    let dir = tempfile::tempdir().unwrap();

    let first = dir.path().join("first.parquet");
    let second = dir.path().join("second.parquet");

    write_fixture(&first, &(0..10).collect::<Vec<_>>(), 5);
    write_fixture(&second, &(10..20).collect::<Vec<_>>(), 5);

    let slices = vec![
        CanonicalFileSlice {
            path: first,
            local_offset: 8,
            row_count: 2,
        },
        CanonicalFileSlice {
            path: second,
            local_offset: 0,
            row_count: 4,
        },
    ];

    let reader = CanonicalParquetReader::new(3).unwrap();

    assert_eq!(
        read_values(&reader.read_slices(&slices).unwrap()),
        vec![8, 9, 10, 11, 12, 13]
    );
}

#[test]
fn rejects_out_of_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bounds.parquet");

    write_fixture(&path, &(0..10).collect::<Vec<_>>(), 5);

    let reader = CanonicalParquetReader::default();

    assert!(reader.read_range(&path, 9, 2).is_err());
}

#[test]
fn preserves_zero_row_references() {
    let reader = CanonicalParquetReader::default();

    assert!(
        reader
            .read_range(Path::new("unused.parquet"), 0, 0)
            .unwrap()
            .is_empty()
    );
}

// -----------------------------------------------------------------------------
// Persistent forward canonical reader
// -----------------------------------------------------------------------------

#[test]
fn forward_cursor_reads_consecutive_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("forward.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 0, 4).unwrap();

    assert_eq!(
        read_values(&cursor.read_range(0, 5).unwrap()),
        vec![0, 1, 2, 3, 4]
    );

    assert_eq!(
        read_values(&cursor.read_range(5, 5).unwrap()),
        vec![5, 6, 7, 8, 9]
    );

    assert_eq!(cursor.position(), 10);
}

#[test]
fn forward_cursor_skips_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gaps.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 0, 4).unwrap();

    assert_eq!(
        read_values(&cursor.read_range(2, 3).unwrap()),
        vec![2, 3, 4]
    );

    assert_eq!(
        read_values(&cursor.read_range(15, 3).unwrap()),
        vec![15, 16, 17]
    );

    assert_eq!(cursor.position(), 18);
}

#[test]
fn forward_cursor_reads_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("groups.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 8, 4).unwrap();

    assert_eq!(
        read_values(&cursor.read_range(8, 15).unwrap()),
        (8..23).collect::<Vec<_>>()
    );

    assert_eq!(cursor.position(), 23);
}

#[test]
fn forward_cursor_rejects_backward_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("backward.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 0, 4).unwrap();

    cursor.read_range(10, 5).unwrap();

    assert!(cursor.read_range(8, 2).is_err());
    assert_eq!(cursor.position(), 15);
}

#[test]
fn forward_cursor_rejects_out_of_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bounds.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    assert!(CanonicalForwardCursor::new(&path, 31, 4).is_err());

    let mut cursor = CanonicalForwardCursor::new(&path, 0, 4).unwrap();

    assert!(cursor.read_range(29, 2).is_err());
}

#[test]
fn forward_cursor_handles_zero_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 0, 4).unwrap();

    assert!(cursor.read_range(0, 0).unwrap().is_empty());
    assert_eq!(cursor.position(), 0);
}

#[test]
fn forward_cursor_starts_inside_later_row_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("later.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let mut cursor = CanonicalForwardCursor::new(&path, 22, 3).unwrap();

    assert_eq!(cursor.position(), 22);

    assert_eq!(
        read_values(&cursor.read_range(22, 5).unwrap()),
        vec![22, 23, 24, 25, 26]
    );
}
