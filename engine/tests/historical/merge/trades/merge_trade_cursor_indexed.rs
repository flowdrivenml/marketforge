use crate::publication::trade_publication;
use marketforge_engine::historical::merge::TradeCursor;
use std::path::Path;

fn compare_range(path: &Path, start_ns: i64, end_ns: i64) -> u64 {
    let mut sequential = TradeCursor::open(path).expect("open sequential trade cursor");

    let mut indexed =
        TradeCursor::open_indexed_range(path, start_ns, end_ns).expect("open indexed trade cursor");

    let mut expected = Vec::new();

    while let Some(trade) = sequential.next_trade().expect("read sequential trade") {
        if trade.event_timestamp_ns >= start_ns && trade.event_timestamp_ns < end_ns {
            expected.push(trade);
        }
    }

    let mut count = 0_usize;

    while let Some(actual) = indexed.next_trade().expect("read indexed trade") {
        assert_eq!(
            actual, expected[count],
            "indexed trade mismatch at row {count}"
        );

        count += 1;
    }

    assert_eq!(
        count,
        expected.len(),
        "indexed cursor returned incorrect event count"
    );

    count as u64
}

#[test]
fn indexed_trade_cursor_matches_sequential() {
    let root = tempfile::tempdir().unwrap();
    let publication = trade_publication(root.path(), 11);
    for (path, member) in publication
        .canonical_files
        .iter()
        .zip(&publication.manifest.files)
    {
        let first = member.start_timestamp_ns.unwrap();
        let last = member.end_timestamp_ns.unwrap();
        assert!(member.rows <= 256 && first < last);
        assert!(compare_range(path, first, first + 1) > 0);
        assert!(compare_range(path, last, last + 1) > 0);
        compare_range(path, first + 1, last);
        assert_eq!(compare_range(path, first, last + 1), member.rows);
    }
}
