//! Bounded selections from canonical descriptors and verified publication metadata.
use super::merge_test_job::{recorded_job, select_content_type};
use marketforge_engine::historical::{
    job::{ContentType, MergeFileInput, MergeJobDocument},
    normalize::manifest::{DatasetFile, DatasetManifest, file_sha256},
};
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::OnceLock,
};

pub fn recorded_manifest(input: &MergeFileInput) -> &'static DatasetManifest {
    static MANIFESTS: OnceLock<HashMap<PathBuf, DatasetManifest>> = OnceLock::new();
    &MANIFESTS.get_or_init(|| {
        let mut manifests = HashMap::new();
        for input in recorded_job().inputs {
            let publication = input.publication;
            manifests.entry(publication.manifest_path.clone()).or_insert_with(|| {
                let path = &publication.manifest_path;
                let size = path.metadata().unwrap_or_else(|error| {
                    panic!("canonical manifest prerequisite missing at {}: {error}", path.display())
                }).len();
                assert!(size <= 16 * 1024 * 1024, "canonical manifest exceeds 16 MiB: {}", path.display());

                let sha = file_sha256(path).unwrap_or_else(|error| {
                    panic!("cannot verify canonical manifest {}: {error}; configure fixture relocation", path.display())
                });
                assert_eq!(sha, publication.manifest_sha256, "manifest checksum {}", path.display());
                let manifest: DatasetManifest = serde_json::from_reader(File::open(path).unwrap()).unwrap();
                assert_eq!(manifest.dataset_id, publication.engine_dataset_id);
                manifest.validate_streams().unwrap();
                manifest
            });
        }
        manifests
    })[&input.publication.manifest_path]
}

pub fn recorded_members(
    job: &MergeJobDocument,
    kind: ContentType,
) -> Vec<(MergeFileInput, DatasetFile)> {
    let mut matches = select_content_type(job, kind)
        .unwrap()
        .inputs
        .into_iter()
        .map(|input| {
            let manifest = recorded_manifest(&input);
            assert_eq!(manifest.content_type, kind);
            let relative = input
                .path
                .strip_prefix(input.publication.manifest_path.parent().unwrap())
                .unwrap();
            let member = manifest
                .files
                .iter()
                .find(|file| Path::new(&file.path) == relative)
                .expect("a canonical descriptor must select a primary file");
            (input, member.clone())
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(_, file)| (file.rows, file.path.clone()));
    matches
}

pub fn recorded_stream(
    input: &MergeFileInput,
) -> &'static marketforge_engine::historical::normalize::streams::StreamOutput {
    let directory = input.publication.manifest_path.parent().unwrap();
    let mut owners = recorded_manifest(input)
        .streams
        .iter()
        .filter(|stream| input.path.parent() == Some(directory.join(&stream.directory).as_path()));
    let owner = owners.next().expect("canonical part belongs to a stream");
    assert!(owners.next().is_none());
    owner
}

pub fn stream_parts(
    job: &MergeJobDocument,
    input: &MergeFileInput,
) -> Vec<(MergeFileInput, std::ops::Range<u64>)> {
    let directory = input.publication.manifest_path.parent().unwrap();
    let owner = recorded_stream(input);
    let mut files = recorded_manifest(input)
        .files
        .iter()
        .filter(|file| Path::new(&file.path).parent() == Some(Path::new(&owner.directory)))
        .collect::<Vec<_>>();
    files.sort_by_key(|file| &file.path);
    let mut offset = 0u64;
    files
        .into_iter()
        .filter_map(|file| {
            let start = offset;
            offset = offset.checked_add(file.rows).unwrap();
            job.inputs
                .iter()
                .find(|candidate| {
                    candidate.publication == input.publication
                        && candidate.path == directory.join(&file.path)
                })
                .map(|candidate| (candidate.clone(), start..offset))
        })
        .collect()
}
