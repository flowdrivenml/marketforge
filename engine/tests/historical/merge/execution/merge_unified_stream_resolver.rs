use crate::publication::{
    DEPTH_STREAM, TRADE_STREAM, make_job, resolve_fixture, trade_publication, write_job,
};
use marketforge_engine::historical::{
    job::load_committed_processing_job,
    merge::{UnifiedMergeStreamKind, resolve_unified_merge_streams},
};
use std::{fs, path::Path};

fn rewrite_provenance(path: &Path, job: &marketforge_engine::historical::job::ProcessingJob) {
    use marketforge_engine::historical::normalize::manifest::write_publication_checksums;
    let checksums: std::collections::BTreeMap<std::path::PathBuf, String> =
        serde_json::from_slice(&fs::read(path.join("publication-checksums.json")).unwrap())
            .unwrap();
    write_job(path, job);
    write_publication_checksums(path, checksums.into_keys()).unwrap();
}

#[test]
fn preserves_mixed_stream_order_and_groups_distinct_files_in_one_publication() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let mut job = make_job(root, &[11], &[12]);
    job.inputs.rotate_left(2);
    assert_eq!(job.inputs.len(), 4);
    assert!(
        job.inputs
            .iter()
            .all(|input| input.dataset_id != input.publication.engine_dataset_id)
    );
    let resolved = resolve_fixture(root, job);
    assert_eq!(resolved.len(), 2);
    assert!(
        resolved
            .iter()
            .all(|input| input.canonical_files.len() == 2)
    );
    let streams = resolve_unified_merge_streams(&resolved, root).unwrap();
    assert_eq!(streams.len(), 2);
    for (stream, (rank, id, kind, stream_id)) in streams.iter().zip([
        (0, 12, UnifiedMergeStreamKind::Depth, DEPTH_STREAM),
        (1, 11, UnifiedMergeStreamKind::Trade, TRADE_STREAM),
    ]) {
        assert_eq!(
            (stream.stream_rank, stream.dataset_id, stream.kind),
            (rank, id, kind)
        );
        assert_eq!(
            (
                stream.exchange.as_str(),
                stream.instrument_id,
                stream.symbol.as_str(),
                stream.stream_id.as_str()
            ),
            ("bybit", 688, "BTCUSDT", stream_id)
        );
    }
}

#[test]
fn rejects_duplicate_registered_file() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let mut job = make_job(root, &[11], &[]);
    resolve_fixture(root, job.clone());
    let mut duplicate = job.inputs[0].clone();
    duplicate.dataset_id += 50;
    job.inputs.push(duplicate);
    let error = job.validate().unwrap_err();
    assert!(
        error.to_string().contains("duplicate registered file"),
        "{error}"
    );
}

fn with_trade_fixture(change: impl FnOnce(&Path), diagnostic: &str) {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let inputs = resolve_fixture(root, make_job(root, &[11], &[]));
    resolve_unified_merge_streams(&inputs, root).unwrap();
    change(&inputs[0].dataset_path);
    let error = resolve_unified_merge_streams(&inputs, root).unwrap_err();
    assert!(error.to_string().contains(diagnostic), "{error}");
}

#[test]
fn rejects_missing_processing_job() {
    with_trade_fixture(
        |path| fs::remove_file(path.join("processing-job.json")).unwrap(),
        "committed job: No such file",
    );
}

#[test]
fn rejects_processing_job_checksum_mismatch() {
    with_trade_fixture(
        |path| fs::write(path.join("processing-job.sha256"), "stale").unwrap(),
        "processing job checksum mismatch",
    );
}

#[test]
fn rejects_processing_job_task_mismatch() {
    with_trade_fixture(
        |path| {
            let mut job = load_committed_processing_job(path).unwrap();
            job.tasks[1].task_id.0 += 5;
            rewrite_provenance(path, &job);
        },
        "processing job task mismatch",
    );
}

#[test]
fn rejects_inconsistent_instrument_metadata_within_stream() {
    with_trade_fixture(
        |path| {
            let mut job = load_committed_processing_job(path).unwrap();
            job.tasks[1].instrument_id += 1;
            rewrite_provenance(path, &job);
        },
        "incompatible selected task metadata",
    );
}

#[test]
fn rejects_processing_job_dataset_mismatch() {
    with_trade_fixture(
        |path| {
            let mut job = load_committed_processing_job(path).unwrap();
            job.dataset_id.0 += 1;
            rewrite_provenance(path, &job);
        },
        "processing job identity mismatch",
    );
}

#[test]
fn rejects_duplicate_resolved_publication() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let input = trade_publication(root, 11);
    resolve_unified_merge_streams(std::slice::from_ref(&input), root).unwrap();
    let error = resolve_unified_merge_streams(&[input.clone(), input], root).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate unified input dataset"),
        "{error}"
    );
}
