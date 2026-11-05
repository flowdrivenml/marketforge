use super::{
    FormatCatalog,
    catalog::{default_catalog, fingerprint, invalid},
};
use crate::historical::normalize::config::{
    ProcessingConfig, project_root, resolve_path, validate_integrity_policy,
    validate_processing_config, validate_resources,
};
use crate::{
    historical::job::*,
    shared::{
        error::Result,
        formats::{depth::DepthSpec, trades::TradeSpec},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

pub const NORMALIZATION_PROTOCOL_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationOperation {
    Normalize,
}

/// Preferred wire contract. Runtime resources and policies are deliberately absent.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizationJobV3 {
    pub protocol_version: u32,
    pub job_id: JobId,
    pub dataset_id: DatasetId,
    pub operation: NormalizationOperation,
    #[serde(deserialize_with = "v3_tasks")]
    pub tasks: Vec<ProfileTask>,
    #[serde(deserialize_with = "v3_streams")]
    pub streams: Vec<StreamConfig>,
    #[serde(deserialize_with = "v3_ordering")]
    pub ordering: OrderingConfig,
    #[serde(deserialize_with = "v3_output")]
    pub output: OutputConfig,
}

impl NormalizationJobV3 {
    fn with_runtime(self, runtime: &ProcessingConfig) -> Result<ProfileJob> {
        if self.protocol_version != NORMALIZATION_PROTOCOL_VERSION {
            return Err(invalid(
                "invalid normalization protocol_version; expected 3",
            ));
        }
        Ok(ProfileJob {
            protocol_version: PROFILE_PROTOCOL_VERSION,
            job_id: self.job_id,
            dataset_id: self.dataset_id,
            operation: ProcessingOperation::Process,
            tasks: self.tasks,
            streams: self.streams,
            ordering: self.ordering,
            output: self.output,
            resources: runtime.resources.clone(),
            integrity_policy: runtime.integrity_policy.clone(),
        })
    }
}

/// The executable uses this entrypoint with one explicit catalog and runtime.
/// All document paths are rooted before final structure validation. No archives open.
pub fn load_normalization_job(
    path: impl AsRef<Path>,
    catalog: &FormatCatalog,
    runtime: &ProcessingConfig,
    root: impl AsRef<Path>,
) -> Result<ProcessingJob> {
    let root = project_root(root)?;
    validate_processing_config(runtime)?;
    let path = resolve_path(&root, path.as_ref());
    let mut job = load_document_inner(&path, catalog, Some(runtime))?;
    for task in &mut job.tasks {
        if task.input_path.as_os_str().is_empty() {
            return Err(invalid(format!(
                "processing job {}: tasks.input_path must not be empty",
                path.display()
            )));
        }
        task.input_path = resolve_path(&root, &task.input_path);
    }
    if job.output.staging_path.as_os_str().is_empty()
        || job.output.dataset_path.as_os_str().is_empty()
    {
        return Err(invalid(format!(
            "processing job {}: output paths must not be empty",
            path.display()
        )));
    }
    job.output.staging_path = resolve_path(&root, &job.output.staging_path);
    job.output.dataset_path = resolve_path(&root, &job.output.dataset_path);
    job.resources.scratch_path = resolve_path(&root, &runtime.resources.scratch_path);
    validate_processing_job_structure(&job)
        .map_err(|e| invalid(format!("processing job {}: {e}", path.display())))?;
    Ok(job)
}

pub const PROFILE_PROTOCOL_VERSION: u32 = 2;
/// The wire task references immutable normalization configuration instead of
/// embedding schemas/rules. The existing WorkTask is the resolved execution type.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileTask {
    pub task_id: TaskId,
    pub stream_id: StreamId,
    pub input_path: std::path::PathBuf,
    pub format_profile: String,
    pub profile_fingerprint: Option<String>,
    pub exchange: crate::shared::domain::canonical::Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub instrument: InstrumentSpec,
    pub archive_member: Option<String>,
    /// An archive wrapper may be chosen only from the profile's supported set.
    pub source_compression: Option<SourceContainer>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileJob {
    protocol_version: u32,
    job_id: JobId,
    dataset_id: DatasetId,
    operation: ProcessingOperation,
    tasks: Vec<ProfileTask>,
    streams: Vec<StreamConfig>,
    ordering: OrderingConfig,
    integrity_policy: IntegrityPolicy,
    resources: ResourceConfig,
    output: OutputConfig,
}

pub fn load_document(path: impl AsRef<Path>) -> Result<ProcessingJob> {
    load_document_with_catalog(path, default_catalog()?)
}
pub fn load_document_with_catalog(
    path: impl AsRef<Path>,
    catalog: &FormatCatalog,
) -> Result<ProcessingJob> {
    load_document_inner(path.as_ref(), catalog, None)
}

fn load_document_inner(
    path: &Path,
    catalog: &FormatCatalog,
    runtime: Option<&ProcessingConfig>,
) -> Result<ProcessingJob> {
    let result = (|| -> Result<ProcessingJob> {
        let data = std::fs::read(path).map_err(|e| invalid(e.to_string()))?;
        let value: Value = serde_json::from_slice(&data).map_err(|e| invalid(e.to_string()))?;
        let version = value
            .get("protocol_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("missing protocol_version"))?;
        let mut job = match version {
            1 => {
                let mut job: ProcessingJob =
                    serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
                for task in &mut job.tasks {
                    let provenance = task
                        .raw_schema
                        .as_object_mut()
                        .and_then(|object| object.remove("marketforge_profile"));
                    if let Some(profile) = catalog.for_task(task)? {
                        check_provenance(
                            provenance.as_ref(),
                            &profile.definition.id,
                            &profile.fingerprint,
                        )?;
                        catalog.check_task(task, profile)?;
                        // A provenance tag is descriptive, never executable configuration.
                        let mut raw = task.raw_schema.clone();
                        if let Some(object) = raw.as_object_mut() {
                            object.remove("marketforge_profile");
                        }
                        if schema_without_null_metadata(raw.clone()) != profile.physical.raw_schema
                            || task.normalizations != profile.normalizations
                            || Some(task.source_ordering) != profile.definition.source_ordering
                        {
                            return Err(invalid(format!(
                                "task {} conflicts with TOML profile {} ({}): raw_schema={}, normalizations={}, ordering={}",
                                task.task_id.0,
                                profile.definition.id,
                                profile.path.display(),
                                schema_without_null_metadata(raw) != profile.physical.raw_schema,
                                task.normalizations != profile.normalizations,
                                Some(task.source_ordering) != profile.definition.source_ordering
                            )));
                        }
                        task.raw_schema = profile.physical.raw_schema.clone();
                        task.normalizations = profile.normalizations.clone();
                        tag(task, &profile.definition.id, &profile.fingerprint)?;
                    } else {
                        // Transitional custom/synthetic v1 formats still use the same
                        // Shared compilers. Known format codes cannot evade compatibility.
                        if catalog
                            .profiles()
                            .any(|p| p.definition.format_code == task.format_code.0)
                        {
                            return Err(invalid(format!(
                                "incompatible legacy format {}",
                                task.format_code.0
                            )));
                        }
                        compile_inline(task)?;
                        let hash = fingerprint(&(
                            &task.raw_schema,
                            &task.normalizations,
                            task.source_ordering,
                            task.source_compression,
                        ))?;
                        check_provenance(provenance.as_ref(), "legacy-inline", &hash)?;
                        tag(task, "legacy-inline", &hash)?;
                    }
                }
                validate_processing_job_structure(&job)?;
                Ok(job)
            }
            2 | 3 => {
                let wire: ProfileJob = if version == 3 {
                    let wire: NormalizationJobV3 =
                        serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
                    wire.with_runtime(runtime.ok_or_else(|| invalid(
                        "protocol v3 requires explicit runtime configuration; use load_normalization_job"
                    ))?)?
                } else {
                    serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?
                };
                if wire.protocol_version != PROFILE_PROTOCOL_VERSION {
                    return Err(invalid("invalid profile protocol version"));
                }
                let mut tasks = vec![];
                for task in wire.tasks {
                    let profile = catalog.get(&task.format_profile)?;
                    if task
                        .profile_fingerprint
                        .as_ref()
                        .is_some_and(|f| f != &profile.fingerprint)
                    {
                        return Err(invalid(format!(
                            "profile {} fingerprint mismatch",
                            task.format_profile
                        )));
                    }
                    let mut resolved = WorkTask {
                        task_id: task.task_id,
                        stream_id: task.stream_id,
                        input_path: task.input_path,
                        format_code: FormatCode(profile.definition.format_code.clone()),
                        exchange: task.exchange,
                        instrument_id: task.instrument_id,
                        symbol: task.symbol,
                        instrument: task.instrument,
                        archive_member: task.archive_member,
                        raw_schema: profile.physical.raw_schema.clone(),
                        normalizations: profile.normalizations.clone(),
                        source_ordering: profile
                            .definition
                            .source_ordering
                            .ok_or_else(|| invalid("profile missing ordering"))?,
                        source_compression: match task.source_compression {
                            Some(container) => container,
                            None => match profile.physical.supported_containers.as_slice() {
                                [container] => *container,
                                _ => {
                                    return Err(invalid(format!(
                                        "profile {} ({}): source_compression is required when multiple archive containers are supported",
                                        profile.definition.id,
                                        profile.path.display()
                                    )));
                                }
                            },
                        },
                    };
                    catalog.check_task(&resolved, profile)?;
                    tag(&mut resolved, &profile.definition.id, &profile.fingerprint)?;
                    tasks.push(resolved);
                }
                let job = ProcessingJob {
                    // The wire v2 request resolves into the established v1
                    // execution/snapshot model, so serialized resolved jobs can
                    // be reloaded through the same legacy compatibility boundary.
                    protocol_version: PROCESSING_PROTOCOL_VERSION,
                    job_id: wire.job_id,
                    dataset_id: wire.dataset_id,
                    operation: wire.operation,
                    tasks,
                    streams: wire.streams,
                    ordering: wire.ordering,
                    integrity_policy: wire.integrity_policy,
                    resources: wire.resources,
                    output: wire.output,
                };
                validate_processing_job_structure(&job)?;
                Ok(job)
            }
            _ => Err(invalid(format!(
                "unsupported processing protocol_version {version}; expected legacy 1/2 or normalization 3"
            ))),
        }?;
        // Validate embedded legacy settings before replacing them. They are never
        // authoritative in the executable, but malformed legacy documents still fail.
        validate_resources(&job.resources)?;
        validate_integrity_policy(&job.integrity_policy)?;
        if let Some(runtime) = runtime {
            job.resources = runtime.resources.clone();
            job.integrity_policy = runtime.integrity_policy.clone();
        }
        Ok(job)
    })();
    result.map_err(|e| invalid(format!("processing job {}: {e}", path.display())))
}
// Apply strict nested validation to direct Rust deserialization as well as the
// file loader, without tightening shared Live/legacy data types.
fn strict_v3_field<'de, D, T>(deserializer: D, key: &str) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let value = Value::deserialize(deserializer)?;
    let mut envelope = serde_json::Map::new();
    envelope.insert(key.to_owned(), value.clone());
    validate_v3_fields(&Value::Object(envelope)).map_err(serde::de::Error::custom)?;
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}
fn v3_tasks<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<ProfileTask>, D::Error> {
    strict_v3_field(d, "tasks")
}
fn v3_streams<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<StreamConfig>, D::Error> {
    strict_v3_field(d, "streams")
}
fn v3_ordering<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<OrderingConfig, D::Error> {
    strict_v3_field(d, "ordering")
}
fn v3_output<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<OutputConfig, D::Error> {
    strict_v3_field(d, "output")
}

// Shared internal structs intentionally retain their legacy deserialization.
// Enforce the strict v3 nested contract at the wire boundary only.
fn validate_v3_fields(value: &Value) -> Result<()> {
    fn keys(value: &Value, allowed: &[&str], path: &str) -> Result<()> {
        if let Some(object) = value.as_object() {
            for key in object.keys() {
                if !allowed.contains(&key.as_str()) {
                    return Err(invalid(format!("unknown field {path}.{key}")));
                }
            }
        }
        Ok(())
    }
    keys(
        &value["output"],
        &["content_type", "staging_path", "dataset_path"],
        "output",
    )?;
    keys(&value["ordering"], &["primary", "tie_break"], "ordering")?;
    if let Some(streams) = value["streams"].as_array() {
        for stream in streams {
            keys(stream, &["stream_id", "stream_rank"], "streams[]")?;
        }
    }
    if let Some(tasks) = value["tasks"].as_array() {
        for task in tasks {
            keys(
                &task["instrument"],
                &[
                    "instrument_kind",
                    "contract_kind",
                    "tick_size",
                    "contract_value",
                    "contract_value_asset",
                ],
                "tasks[].instrument",
            )?;
        }
    }
    Ok(())
}

fn check_provenance(provenance: Option<&Value>, id: &str, hash: &str) -> Result<()> {
    if let Some(provenance) = provenance {
        if provenance["id"].as_str() != Some(id) || provenance["sha256"].as_str() != Some(hash) {
            return Err(invalid(format!(
                "resolved profile {id} provenance fingerprint or identity mismatch"
            )));
        }
    }
    Ok(())
}

fn compile_inline(task: &WorkTask) -> Result<()> {
    if task.normalizations.is_empty() {
        return Err(invalid("legacy task has no normalizations"));
    }
    for n in &task.normalizations {
        match n.target_schema {
            TargetSchema::Trade => {
                TradeSpec::from_normalization(n)?;
            }
            TargetSchema::Depth => {
                DepthSpec::from_normalization(n)?;
            }
        }
    }
    Ok(())
}
fn tag(task: &mut WorkTask, id: &str, hash: &str) -> Result<()> {
    task.raw_schema
        .as_object_mut()
        .ok_or_else(|| invalid("raw schema must be an object"))?
        .insert(
            "marketforge_profile".into(),
            serde_json::json!({"id":id,"sha256":hash}),
        );
    Ok(())
}

fn schema_without_null_metadata(mut value: Value) -> Value {
    match &mut value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                *v = schema_without_null_metadata(v.take());
            }
        }
        Value::Array(values) => {
            for v in values {
                *v = schema_without_null_metadata(v.take());
            }
        }
        _ => {}
    }
    value
}

#[cfg(test)]
mod catalog_fingerprint_tests {
    use super::*;

    #[test]
    fn resolved_profile_provenance_rejects_stale_id_or_fingerprint() {
        // Pure provenance validation: no job fixture, input archive, or execution.
        let id = "bybit.spot.spot.bybit-t1.v1";
        let hash = fingerprint(&(id, "bybit.trades.bybit-t1.v1")).unwrap();
        let valid = serde_json::json!({"id": id, "sha256": hash});
        check_provenance(Some(&valid), id, &hash).unwrap();
        for stale in [
            serde_json::json!({"id": "old.profile", "sha256": hash}),
            serde_json::json!({"id": id, "sha256": "stale"}),
        ] {
            let error = check_provenance(Some(&stale), id, &hash).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("fingerprint or identity mismatch")
            );
        }
    }
}

#[cfg(test)]
mod protocol_tests;
