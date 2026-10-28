#![cfg(feature = "historical")]

use std::path::PathBuf;

use marketforge_engine::historical::merge::DepthEventCursor;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn indexed_cursor_matches_sequential_cursor() {
    let root = project_root();

    let datasets = [105, 108, 112, 92];

    let start_ns = 1788220800000000000_i64;
    let intervals = [
        (start_ns, start_ns + 60_000_000_000),
        (start_ns + 1_200_000_000_000, start_ns + 1_260_000_000_000),
        (start_ns + 3_300_000_000_000, start_ns + 3_360_000_000_000),
    ];

    for dataset_id in datasets {
        let path = root.join(format!(
            "data/processed/{dataset_id}/tasks/task-000000/depth/events.parquet"
        ));

        for (start, end) in intervals {
            let mut sequential =
                DepthEventCursor::open_range(&path, start, end).expect("open sequential cursor");

            let mut indexed = DepthEventCursor::open_indexed_range(&path, start, end)
                .expect("open indexed cursor");

            let mut count = 0_u64;

            loop {
                let expected = sequential.next_event().expect("read sequential event");

                let actual = indexed.next_event().expect("read indexed event");

                match (expected, actual) {
                    (None, None) => break,

                    (Some(a), Some(b)) => {
                        assert_eq!(a.event_ordinal, b.event_ordinal);
                        assert_eq!(a.event_timestamp_ns, b.event_timestamp_ns);
                        assert_eq!(a.canonical_row_offset, b.canonical_row_offset);
                        assert_eq!(a.canonical_row_count, b.canonical_row_count);
                        assert_eq!(a.sequence_start, b.sequence_start);
                        assert_eq!(a.sequence_end, b.sequence_end);

                        count += 1;
                    }

                    _ => panic!("indexed and sequential cursors returned different event counts"),
                }
            }

            println!(
                "dataset={} | start={} | events={} | MATCH",
                dataset_id, start, count,
            );
        }
    }
}
