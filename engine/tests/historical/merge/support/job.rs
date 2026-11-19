//! Shared, working-directory-independent baseline for the public merge contract.
use marketforge_engine::historical::job::{MergeJob, MergeJobDocument, load_merge_job};
use std::path::{Path, PathBuf};

pub fn canonical_job() -> MergeJobDocument {
    load_merge_job(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/historical/fixtures/merge.json"),
    )
    .expect("parse the canonical Forge merge job")
}

pub fn small_runtime() -> marketforge_engine::shared::historical::config::MergeRuntimeSettings {
    use marketforge_engine::historical::job::{
        MergeParquetConfig, MergePartitioning, MergeResources,
    };
    use marketforge_engine::shared::historical::config::MergeRuntimeSettings;
    MergeRuntimeSettings {
        resources: MergeResources {
            workers: 1,
            memory_limit_mb: 256,
            max_open_files: 32,
        },
        partitioning: MergePartitioning {
            partitions_per_worker: 1,
        },
        parquet: MergeParquetConfig {
            row_group_target_mb: 1,
            file_target_mb: 4,
            compression_level: 3,
        },
    }
}

#[allow(dead_code)]
pub fn recorded_job() -> MergeJobDocument {
    let mut job = canonical_job();
    if let Some(from) = std::env::var_os("MARKETFORGE_MERGE_FIXTURE_FROM") {
        let from = PathBuf::from(from);
        let to = PathBuf::from(
            std::env::var_os("MARKETFORGE_MERGE_FIXTURE_TO")
                .expect("set the relocation destination"),
        );
        assert!(
            from.is_absolute() && to.is_absolute(),
            "use absolute relocation prefixes"
        );
        for input in &mut job.inputs {
            for path in [&mut input.path, &mut input.publication.manifest_path] {
                let suffix = path.strip_prefix(&from).unwrap_or_else(|_| {
                    panic!(
                        "{} is outside relocation prefix {}",
                        path.display(),
                        from.display()
                    )
                });
                *path = to.join(suffix);
            }
        }
    }
    job
}

pub fn bounded_runtime(document: MergeJobDocument) -> MergeJob {
    assert!(
        document.inputs.len() <= 4,
        "merge fixture exceeds four selected files"
    );
    let mut runtime = small_runtime();
    runtime.resources.memory_limit_mb = 2048;
    document.with_runtime(runtime).unwrap()
}
