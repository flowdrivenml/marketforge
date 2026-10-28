use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs, path::PathBuf};
use {
    marketforge_engine::historical::convert::reference::PhysicalReferenceReader,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
};

fn fixture() -> (PathBuf, PhysicalMergeManifest) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    let directory = root.join("data/merged/conversion-multihop");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();

    (directory, manifest)
}

fn row_group_boundaries(directory: &std::path::Path, manifest: &PhysicalMergeManifest) -> Vec<u64> {
    let mut files = manifest.files.clone();

    files.sort_by(|a, b| (a.partition_id, &a.path).cmp(&(b.partition_id, &b.path)));

    let mut boundaries = Vec::new();
    let mut offset = 0_u64;

    for entry in files {
        let file = fs::File::open(directory.join(entry.path)).unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        for index in 0..reader.metadata().num_row_groups() {
            let rows = reader.metadata().row_group(index).num_rows() as u64;

            offset += rows;

            if offset < manifest.events_written {
                boundaries.push(offset);
            }
        }
    }

    assert_eq!(offset, manifest.events_written);

    boundaries
}

#[test]
fn seeking_to_zero_matches_sequential_reader() {
    let (directory, manifest) = fixture();

    let mut sequential = PhysicalReferenceReader::new(&directory, &manifest, 4096).unwrap();

    let mut seeked = PhysicalReferenceReader::new_at(&directory, &manifest, 4096, 0).unwrap();

    for _ in 0..10_000 {
        let expected = sequential.next_observation().unwrap();
        let actual = seeked.next_observation().unwrap();

        assert_eq!(
            expected.map(|value| (value.dataset_id, value.stream_id, value.key, value.price,)),
            actual.map(|value| (value.dataset_id, value.stream_id, value.key, value.price,)),
        );
    }
}

#[test]
fn seeking_to_dataset_end_returns_eof() {
    let (directory, manifest) = fixture();

    let mut reader =
        PhysicalReferenceReader::new_at(&directory, &manifest, 4096, manifest.events_written)
            .unwrap();

    assert!(reader.next_observation().unwrap().is_none());
}

#[test]
fn rejects_seek_beyond_dataset_end() {
    let (directory, manifest) = fixture();

    assert!(
        PhysicalReferenceReader::new_at(&directory, &manifest, 4096, manifest.events_written + 1,)
            .is_err()
    );
}

fn assert_observations_equal(
    expected: Option<marketforge_engine::historical::convert::reference::ReferenceObservation>,
    actual: Option<marketforge_engine::historical::convert::reference::ReferenceObservation>,
) {
    assert_eq!(
        expected.map(|value| (value.dataset_id, value.stream_id, value.key, value.price,)),
        actual.map(|value| (value.dataset_id, value.stream_id, value.key, value.price,)),
    );
}

#[test]
fn seeking_to_nonzero_position_matches_sequential() {
    let (directory, manifest) = fixture();

    const POSITION: u64 = 100_000;
    const COMPARE: usize = 10_000;

    let mut sequential = PhysicalReferenceReader::new(&directory, &manifest, 4096).unwrap();

    for _ in 0..POSITION {
        sequential.next_observation().unwrap().unwrap();
    }

    let mut seeked =
        PhysicalReferenceReader::new_at(&directory, &manifest, 4096, POSITION).unwrap();

    for _ in 0..COMPARE {
        assert_observations_equal(
            sequential.next_observation().unwrap(),
            seeked.next_observation().unwrap(),
        );
    }

    assert_eq!(seeked.records_read(), POSITION + COMPARE as u64);
}

#[test]
fn seeking_across_row_group_boundaries_matches_sequential() {
    let (directory, manifest) = fixture();

    let boundaries = row_group_boundaries(&directory, &manifest);

    assert!(!boundaries.is_empty());

    let boundary = boundaries[0];

    let start = boundary - 2;

    let mut sequential = PhysicalReferenceReader::new(&directory, &manifest, 4096).unwrap();

    for _ in 0..start {
        sequential.next_observation().unwrap().unwrap();
    }

    let mut seeked = PhysicalReferenceReader::new_at(&directory, &manifest, 4096, start).unwrap();

    for _ in 0..10 {
        assert_observations_equal(
            sequential.next_observation().unwrap(),
            seeked.next_observation().unwrap(),
        );
    }
}

#[test]
fn seeking_directly_to_row_group_boundary() {
    let (directory, manifest) = fixture();

    let boundaries = row_group_boundaries(&directory, &manifest);

    let boundary = boundaries[0];

    let mut sequential = PhysicalReferenceReader::new(&directory, &manifest, 4096).unwrap();

    for _ in 0..boundary {
        sequential.next_observation().unwrap().unwrap();
    }

    let mut seeked =
        PhysicalReferenceReader::new_at(&directory, &manifest, 4096, boundary).unwrap();

    for _ in 0..100 {
        assert_observations_equal(
            sequential.next_observation().unwrap(),
            seeked.next_observation().unwrap(),
        );
    }
}
