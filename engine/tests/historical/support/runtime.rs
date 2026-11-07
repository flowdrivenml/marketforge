use super::{TestResult, root};
use marketforge_engine::{
    historical::{
        config::{FormatCatalog, PreparedBatch, ResolvedBatch, load_normalization_batch},
        normalize::{ProcessingResult, execute_processing_job},
    },
    shared::{
        historical::{ProcessingConfig, load_processing_config, validate_processing_config},
        io::container::SourceContainer,
        io::source::with_source_reader,
    },
};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Default)]
pub struct Overrides {
    pub workers: Option<usize>,
    pub memory: Option<u64>,
    pub scratch: Option<u64>,
    pub row_group: Option<u64>,
    pub file: Option<u64>,
}
pub struct TestRun {
    pub path: PathBuf,
    pub metadata: Value,
}
impl TestRun {
    pub fn new(name: &str) -> TestResult<Self> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("test name must be a safe nonempty component".into());
        }
        let base = root().join("data/tests/historical/normalize").join(name);
        let mut ancestor = PathBuf::new();
        for component in base.components() {
            ancestor.push(component);
            if fs::symlink_metadata(&ancestor).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(format!(
                    "test output ancestor is a symlink: {}",
                    ancestor.display()
                ));
            }
        }
        fs::create_dir_all(&base).map_err(|e| e.to_string())?;
        let path = loop {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos();
            let candidate = base.join(format!(
                "{stamp}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => break candidate.canonicalize().map_err(|e| e.to_string())?,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.to_string()),
            }
        };
        let run = Self {
            metadata: json!({"schema_version":1,"test_name":name,"run_id":path.file_name().unwrap().to_string_lossy(),"status":"running","started_at_unix_ns":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string(),"fixture":"engine/tests/historical/fixtures/normalize.json"}),
            path,
        };
        run.save()?;
        Ok(run)
    }
    pub fn owned(&self, path: &Path) -> TestResult<()> {
        if path == self.path
            || !path.starts_with(&self.path)
            || path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(format!("output escapes test run: {}", path.display()));
        }
        let mut current = Some(path);
        while let Some(p) = current {
            if p.exists() {
                if !p
                    .canonicalize()
                    .map_err(|e| e.to_string())?
                    .starts_with(&self.path)
                {
                    return Err("output resolves outside test run".into());
                }
                break;
            }
            current = p.parent();
        }
        Ok(())
    }
    pub fn save(&self) -> TestResult<()> {
        let next = self.path.join("test.json.next");
        let bytes = serde_json::to_vec_pretty(&self.metadata).map_err(|e| e.to_string())?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&next)
            .map_err(|e| e.to_string())?;
        file.write_all(&bytes).map_err(|e| e.to_string())?;
        fs::rename(next, self.path.join("test.json")).map_err(|e| e.to_string())
    }
    pub fn finish(&mut self, result: &TestResult<()>) -> TestResult<()> {
        self.metadata["completed_at_unix_ns"] = json!(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos()
                .to_string()
        );
        self.metadata["status"] = json!(if result.is_ok() { "passed" } else { "failed" });
        self.metadata["failure"] = json!(result.as_ref().err());
        self.save()
    }
    pub fn runtime(&self, overrides: Overrides) -> TestResult<ProcessingConfig> {
        let mut c = load_processing_config("data/.config/historical/general.toml", root())
            .map_err(|e| e.to_string())?;
        c.resources.workers = overrides.workers.unwrap_or(1);
        c.resources.memory_budget_bytes = overrides.memory.unwrap_or(1 << 30);
        c.resources.scratch_budget_bytes = overrides.scratch.unwrap_or(1 << 30);
        c.resources.parquet.row_group_target_bytes = overrides.row_group.unwrap_or(1 << 20);
        c.resources.parquet.file_target_bytes = overrides.file.unwrap_or(4 << 20);
        c.resources.scratch_path = self.path.join("scratch");
        self.owned(&c.resources.scratch_path)?;
        validate_processing_config(&c).map_err(|e| e.to_string())?;
        Ok(c)
    }
    pub fn prepare(
        &mut self,
        batch: &PreparedBatch,
        runtime: &ProcessingConfig,
    ) -> TestResult<ResolvedBatch> {
        let mut b = batch.clone();
        self.metadata["selection"] =
            json!(b.jobs.iter().flat_map(|j| &j.tasks).collect::<Vec<_>>());
        for job in &mut b.jobs {
            job.output.dataset_path = self
                .path
                .join("datasets")
                .join(job.dataset_id.0.to_string());
            job.output.staging_path = self.path.join("staging").join(job.dataset_id.0.to_string());
            self.owned(&job.output.dataset_path)?;
            self.owned(&job.output.staging_path)?;
        }
        b.run_directory = self.path.clone();
        b.batch_path = self.path.join("selected-batch.json");
        self.metadata["runtime"] =
            json!({"resources":runtime.resources,"integrity_policy":runtime.integrity_policy});
        self.save()?;
        fs::write(
            &b.batch_path,
            serde_json::to_vec_pretty(&b).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let resolved = load_normalization_batch(
            &b.batch_path,
            &FormatCatalog::load(root().join("data/.config/historical"))
                .map_err(|e| e.to_string())?,
            runtime,
            root(),
        )
        .map_err(|e| e.to_string())?;
        self.metadata["resolved_jobs"] = json!(resolved.jobs);
        self.save()?;
        Ok(resolved)
    }
    pub fn execute(
        &mut self,
        batch: &PreparedBatch,
        overrides: Overrides,
    ) -> TestResult<Vec<ProcessingResult>> {
        let c = self.runtime(overrides)?;
        let b = self.prepare(batch, &c)?;
        let mut results = vec![];
        for j in &b.jobs {
            self.owned(&j.output.dataset_path)?;
            self.owned(&j.output.staging_path)?;
            results.push(execute_processing_job(j, &c).map_err(|e| e.to_string())?);
        }
        self.metadata["results"] = json!(results);
        self.save()?;
        Ok(results)
    }
}
/// Read a bounded prefix through the production archive reader, then create a complete
/// small wrapper. Never truncate compressed bytes or modify the original source.
pub fn sample_prefix(run: &mut TestRun, batch: &mut PreparedBatch, lines: usize) -> TestResult<()> {
    if lines == 0 {
        return Err("sample must include initialization/header records".into());
    }
    let c = run.runtime(Overrides::default())?;
    let resolved = run.prepare(batch, &c)?;
    run.metadata["original_sources"] = json!(
        batch
            .jobs
            .iter()
            .flat_map(|j| j.tasks.iter().map(|t| &t.input_path))
            .collect::<Vec<_>>()
    );
    for (j, r) in batch.jobs.iter_mut().zip(resolved.jobs.iter()) {
        if j.tasks.len() != r.tasks.len() {
            return Err("sample family expansion requires explicit member handling".into());
        }
        for (t, task) in j.tasks.iter_mut().zip(&r.tasks) {
            let bytes = with_source_reader(
                &task.input_path,
                task.source_compression,
                task.archive_member.as_deref(),
                |reader| {
                    let mut reader = std::io::BufReader::new(reader);
                    let mut bytes = vec![];
                    for _ in 0..lines {
                        let n = std::io::BufRead::read_until(&mut reader, b'\n', &mut bytes)
                            .map_err(|source| {
                                marketforge_engine::shared::error::MarketForgeError::SourceRead {
                                    path: task.input_path.clone(),
                                    source,
                                }
                            })?;
                        if n == 0 {
                            break;
                        }
                    }
                    Ok(bytes)
                },
            )
            .map_err(|e| format!("required source {}: {e}", task.input_path.display()))?;
            let path = run
                .path
                .join(format!("source-{}-{}.archive", j.job_id.0, t.task_id.0));
            run.owned(&path)?;
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|e| e.to_string())?;
            match task.source_compression {
                SourceContainer::Plain => {
                    let mut file = file;
                    file.write_all(&bytes).map_err(|e| e.to_string())?;
                }
                SourceContainer::Gzip => {
                    let mut e = flate2::write::GzEncoder::new(file, flate2::Compression::default());
                    e.write_all(&bytes).map_err(|e| e.to_string())?;
                    e.finish().map_err(|e| e.to_string())?;
                }
                SourceContainer::Zip => {
                    let mut z = zip::ZipWriter::new(file);
                    z.start_file(
                        task.archive_member.as_deref().unwrap_or("sample.data"),
                        zip::write::SimpleFileOptions::default(),
                    )
                    .map_err(|e| e.to_string())?;
                    z.write_all(&bytes).map_err(|e| e.to_string())?;
                    z.finish().map_err(|e| e.to_string())?;
                }
                SourceContainer::TarGzip => {
                    let e = flate2::write::GzEncoder::new(file, flate2::Compression::default());
                    let mut tar = tar::Builder::new(e);
                    let mut header = tar::Header::new_gnu();
                    header.set_size(bytes.len() as u64);
                    header.set_mode(0o644);
                    header.set_cksum();
                    tar.append_data(
                        &mut header,
                        task.archive_member.as_deref().unwrap_or("sample.data"),
                        bytes.as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
                    tar.into_inner()
                        .map_err(|e| e.to_string())?
                        .finish()
                        .map_err(|e| e.to_string())?;
                }
            };
            t.input_path = path;
        }
    }
    run.save()
}
