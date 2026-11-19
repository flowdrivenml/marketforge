use crate::{merge_test_job::bounded_runtime, publication::make_job};
use marketforge_engine::historical::{job::ContentType, merge::resolve_merge_inputs};

#[test]
fn resolves_generated_merge_inputs() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let job = make_job(root, &[11], &[12]);
    let resolved = resolve_merge_inputs(&bounded_runtime(job.clone()), root).unwrap();
    assert_eq!(resolved.len(), 2);
    for (input, (kind, members)) in resolved.iter().zip([
        (ContentType::Trades, &job.inputs[..2]),
        (ContentType::Depth, &job.inputs[2..]),
    ]) {
        assert_eq!(input.dataset_id, members[0].publication.engine_dataset_id);
        assert_eq!(input.manifest.content_type, kind);
        assert_eq!(
            input.canonical_files,
            members
                .iter()
                .map(|item| item.path.clone())
                .collect::<Vec<_>>()
        );
    }
    assert!(!job.output.dataset_path.exists());
}
