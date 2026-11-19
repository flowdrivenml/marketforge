use crate::publication::trade_publication;
use marketforge_engine::historical::merge::TradeCursor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

#[test]
fn reads_generated_trade_rows_with_stable_offsets() {
    let root = tempfile::tempdir().unwrap();
    let publication = trade_publication(root.path(), 11);
    for (path, member) in publication
        .canonical_files
        .iter()
        .zip(&publication.manifest.files)
    {
        let file = File::open(&path).expect("open Parquet metadata");

        let metadata =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet metadata");

        let expected_rows =
            u64::try_from(metadata.metadata().file_metadata().num_rows()).expect("valid row count");

        let mut cursor = TradeCursor::open(&path).expect("open trade cursor");

        let mut count = 0_u64;
        let mut previous_timestamp = None;

        while let Some(trade) = cursor.next_trade().expect("read trade") {
            assert_eq!(trade.file_row_index, count, "incorrect canonical row index");

            if let Some(previous) = previous_timestamp {
                assert!(
                    trade.event_timestamp_ns >= previous,
                    "trade timestamp regression"
                );
            }

            previous_timestamp = Some(trade.event_timestamp_ns);

            count += 1;
        }

        assert_eq!(count, expected_rows, "trade row count mismatch");

        assert_eq!(cursor.rows_read(), expected_rows);

        assert_eq!(expected_rows, member.rows);
    }
}
