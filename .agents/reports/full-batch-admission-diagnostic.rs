// Temporary unit-test probe, appended to resources/estimate.rs for diagnosis and then removed.

#[cfg(test)]
#[test]
fn diagnostic_full_batch_admission_breakdown() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let base = root.join("data/tests/historical/normalize/full-run/20261115-185513/staging");
    let runtime = crate::shared::historical::load_processing_config(root.join("data/.config/historical/general.toml"), root).unwrap();
    let catalog = crate::historical::config::FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let batch = crate::historical::config::read_prepared_batch(root.join("engine/tests/historical/fixtures/normalize.json"), root).unwrap();
    let resolved: Vec<_> = batch.jobs.iter().map(|j| batch.resolve_job(j, &catalog, &runtime, root).unwrap()).collect();
    let mut cumulative = 0;
    for mut job in resolved {
        let id = job.dataset_id.0.to_string();
        let original: crate::historical::job::ProcessingJob = serde_json::from_slice(&std::fs::read(base.join(format!("dataset-{id}/processing-job.json"))).unwrap()).unwrap();
        job.output = original.output;
        let l = job.resources.normalization_limits;
        let p = crate::shared::persistence::config::ParquetResourceConfig {row_group_target_bytes:268435456,file_target_bytes:2147483648};
        let r = retained_jobs_memory(std::slice::from_ref(&job), &l).unwrap();
        let scopes: u64 = job.tasks.iter().map(|t|(t.stream_id.0.len()+t.input_path.to_string_lossy().len()) as u64).sum();
        let prepared: u64 = job.tasks.iter().map(|t|prepared_task_memory(t).unwrap()).sum();
        println!("DIAG scopes={scopes} prepared={prepared}");
        cumulative += r;
        println!("DIAG dataset={id} tasks={} retained={r} cumulative={cumulative} finalization={}",job.tasks.len(),job_finalization_memory(&p,id=="7336746276859674154",&l).unwrap());
        for t in &job.tasks {
            if t.task_id.0 == 249 && id=="7336746276859674154" {
                println!("DIAG task249 {:?} retained={} prepared={} sizes span={} execution={} category={} metrics={} diagnostic={} segment={}",estimate_breakdown(t,&p,&l).unwrap(),retained_task_memory(t,&l).unwrap(),prepared_task_memory(t).unwrap(),std::mem::size_of::<crate::historical::normalize::streams::SourceSpan>(),std::mem::size_of::<(usize,crate::historical::normalize::task::TaskExecution)>(),std::mem::size_of::<crate::historical::normalize::metrics::CategoryStatistics>(),std::mem::size_of::<crate::historical::normalize::metrics::TaskMetrics>(),std::mem::size_of::<crate::historical::normalize::metrics::IntegrityDiagnostic>(),std::mem::size_of::<crate::shared::domain::book::BookSegment>());
            }
        }
    }
}
