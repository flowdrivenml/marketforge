use crate::depth_index::write_index;
use marketforge_engine::historical::merge::DepthEventCursor;
use std::{path::Path, time::Instant};

fn measure(path: &Path, start: i64, end: i64, indexed: bool) -> (u64, std::time::Duration) {
    let started = Instant::now();
    let mut cursor = if indexed {
        DepthEventCursor::open_indexed_range(path, start, end)
    } else {
        DepthEventCursor::open_range(path, start, end)
    }
    .unwrap();
    let mut count = 0;
    while cursor.next_event().unwrap().is_some() {
        count += 1;
    }
    (count, started.elapsed())
}

#[test]
#[ignore = "cursor performance benchmark"]
fn compares_indexed_and_sequential_cursors() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.parquet");
    write_index(&path, &(0..1024).collect::<Vec<_>>());
    for start in [0, 500, 1000] {
        let (sequential, sequential_time) = measure(&path, start, start + 16, false);
        let (indexed, indexed_time) = measure(&path, start, start + 16, true);
        assert_eq!((sequential, indexed), (16, 16));
        println!("start={start}, sequential={sequential_time:?}, indexed={indexed_time:?}");
    }
}
