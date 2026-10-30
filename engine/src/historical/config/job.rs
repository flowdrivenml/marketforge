use super::{
    FormatCatalog,
    catalog::{default_catalog, fingerprint, invalid},
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
    let path = path.as_ref();
    let result = (|| -> Result<ProcessingJob> {
        let data = std::fs::read(path).map_err(|e| invalid(e.to_string()))?;
        let value: Value = serde_json::from_slice(&data).map_err(|e| invalid(e.to_string()))?;
        let version = value
            .get("protocol_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("missing protocol_version"))?;
        match version {
            1 => {
                let mut job: ProcessingJob =
                    serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
                for task in &mut job.tasks {
                    if let Some(object) = task.raw_schema.as_object_mut() {
                        object.remove("marketforge_profile");
                    }
                    if let Some(profile) = catalog.for_task(task)? {
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
                        tag(task, "legacy-inline", &hash)?;
                    }
                }
                validate_processing_job_structure(&job)?;
                Ok(job)
            }
            2 => {
                let wire: ProfileJob =
                    serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
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
                        source_compression: task
                            .source_compression
                            .unwrap_or(profile.physical.supported_containers[0]),
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
                "unsupported processing protocol_version {version}; expected legacy 1 or profile 2"
            ))),
        }
    })();
    result.map_err(|e| invalid(format!("processing job {}: {e}", path.display())))
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
