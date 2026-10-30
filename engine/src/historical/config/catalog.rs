use crate::{
    historical::job::{SourceContainer, SourceOrdering, WorkTask},
    shared::{
        domain::{
            canonical::Exchange,
            instrument::{ContractKind, InstrumentKind},
        },
        error::{MarketForgeError, Result},
        formats::{depth::DepthSpec, trades::TradeSpec},
        normalization::{NormalizationConfig, QuantityEncoding, TargetSchema, TimestampEncoding},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalFormat {
    pub schema_version: u32,
    pub id: String,
    pub container_format: PhysicalRecord,
    pub record_format: RecordFormat,
    pub supported_containers: Vec<SourceContainer>,
    pub raw_schema: Value,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalRecord {
    Csv,
    Jsonl,
    Xlsx,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordFormat {
    Rows,
    Events,
    SnapshotRows,
    BookRows,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema_version: u32,
    pub id: String,
    pub physical: String,
    pub format_code: String,
    pub exchange: Exchange,
    pub instrument_type: InstrumentKind,
    pub market_category: MarketCategory,
    pub source_ordering: Option<SourceOrdering>,
    pub unsupported_reason: Option<String>,
    pub rules: Vec<Rule>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketCategory {
    Spot,
    Linear,
    Inverse,
    Option,
}
impl MarketCategory {
    fn of(task: &WorkTask) -> Self {
        match task.instrument.contract_kind {
            Some(ContractKind::Linear) => Self::Linear,
            Some(ContractKind::Inverse) => Self::Inverse,
            None if task.instrument.instrument_kind == InstrumentKind::Option => Self::Option,
            None => Self::Spot,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleTarget {
    Trade,
    L2Snapshot,
    L2Update,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub target: RuleTarget,
    pub timestamp_encoding: Option<TimestampEncoding>,
    pub quantity_encoding: Option<QuantityEncoding>,
    pub rules: Value,
}
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Document {
    Physical(PhysicalFormat),
    Profile(Profile),
}
#[derive(Debug, Clone)]
pub struct CompiledProfile {
    pub definition: Profile,
    pub physical: Arc<PhysicalFormat>,
    pub normalizations: Vec<NormalizationConfig>,
    pub fingerprint: String,
    pub path: PathBuf,
}
#[derive(Debug)]
pub struct FormatCatalog {
    profiles: BTreeMap<String, CompiledProfile>,
}
impl FormatCatalog {
    pub fn load(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let mut paths = vec![];
        collect(root, &mut paths)?;
        paths.sort();
        let mut physical = BTreeMap::new();
        let mut profiles = vec![];
        for path in paths {
            let parsed = (|| -> Result<Document> {
                let text = fs::read_to_string(&path).map_err(|e| invalid(e.to_string()))?;
                toml::from_str(&text).map_err(|e| invalid(e.to_string()))
            })()
            .map_err(|e| invalid(format!("{}: {e}", path.display())))?;
            match parsed {
                Document::Physical(p) => {
                    validate_physical(&p).map_err(|e| context(&path, &p.id, e))?;
                    if physical.insert(p.id.clone(), Arc::new(p)).is_some() {
                        return Err(invalid(format!(
                            "{}: duplicate physical definition",
                            path.display()
                        )));
                    }
                }
                Document::Profile(p) => profiles.push((path, p)),
            }
        }
        if profiles.is_empty() {
            return Err(invalid(format!(
                "{}: no Historical format profiles",
                root.display()
            )));
        }
        let mut result = BTreeMap::new();
        let mut identities = BTreeSet::new();
        for (path, p) in profiles {
            let build = (|| -> Result<CompiledProfile> {
                if p.schema_version != 1 {
                    return Err(invalid("unsupported profile schema_version"));
                }
                let expected = format!(
                    "{}.{}.{}.{}.v1",
                    name(&p.exchange)?,
                    name(&p.market_category)?,
                    name(&p.instrument_type)?,
                    p.format_code.to_lowercase()
                );
                if p.id != expected {
                    return Err(invalid(format!("profile identity must be {expected}")));
                }
                let compatible = matches!(
                    (p.instrument_type, p.market_category),
                    (InstrumentKind::Spot, MarketCategory::Spot)
                        | (InstrumentKind::Option, MarketCategory::Option)
                        | (
                            InstrumentKind::Perpetual | InstrumentKind::Future,
                            MarketCategory::Linear | MarketCategory::Inverse
                        )
                );
                if !compatible {
                    return Err(invalid(
                        "incompatible profile instrument type and market category",
                    ));
                }
                let key = (
                    name(&p.exchange)?,
                    p.format_code.clone(),
                    name(&p.instrument_type)?,
                    name(&p.market_category)?,
                );
                if !identities.insert(key) {
                    return Err(invalid("ambiguous profile resolution"));
                }
                let raw = physical
                    .get(&p.physical)
                    .ok_or_else(|| invalid(format!("missing physical format {}", p.physical)))?
                    .clone();
                if p.rules.is_empty() {
                    return Err(invalid("profile has no normalization rules"));
                }
                let mut normalizations = vec![];
                let mut kinds = BTreeSet::new();
                for rule in &p.rules {
                    if !kinds.insert(name(&rule.target)?) {
                        return Err(invalid("duplicate normalization target"));
                    }
                    if p.unsupported_reason.is_some() {
                        continue;
                    }
                    let target_schema = if rule.target == RuleTarget::Trade {
                        TargetSchema::Trade
                    } else {
                        TargetSchema::Depth
                    };
                    if (rule.target == RuleTarget::L2Snapshot)
                        != rule.rules.get("snapshot").is_some()
                        && target_schema == TargetSchema::Depth
                    {
                        return Err(invalid("depth normalization target/boundary mismatch"));
                    }
                    let n = NormalizationConfig {
                        timestamp_encoding: rule
                            .timestamp_encoding
                            .ok_or_else(|| invalid("missing timestamp_encoding"))?,
                        quantity_encoding: rule
                            .quantity_encoding
                            .ok_or_else(|| invalid("missing quantity_encoding"))?,
                        target_schema,
                        rules: rule.rules.clone(),
                    };
                    match target_schema {
                        TargetSchema::Trade => {
                            TradeSpec::from_normalization(&n)?;
                        }
                        TargetSchema::Depth => {
                            DepthSpec::from_normalization(&n)?;
                        }
                    }
                    validate_sources(&n.rules, &raw.raw_schema)?;
                    normalizations.push(n);
                }
                if kinds.contains("trade") && kinds.len() != 1 {
                    return Err(invalid("mixed trade and depth profile"));
                }
                if p.unsupported_reason
                    .as_ref()
                    .is_some_and(|s| s.trim().is_empty())
                {
                    return Err(invalid("empty unsupported_reason"));
                }
                if p.unsupported_reason.is_none() && p.source_ordering.is_none() {
                    return Err(invalid("missing source_ordering"));
                }
                let fingerprint = fingerprint(&(&p, raw.as_ref()))?;
                Ok(CompiledProfile {
                    definition: p.clone(),
                    physical: raw,
                    normalizations,
                    fingerprint,
                    path: path.clone(),
                })
            })()
            .map_err(|e| context(&path, &p.id, e))?;
            if result.insert(p.id.clone(), build).is_some() {
                return Err(context(&path, &p.id, invalid("duplicate profile")));
            }
        }
        Ok(Self { profiles: result })
    }
    pub fn profiles(&self) -> impl Iterator<Item = &CompiledProfile> {
        self.profiles.values()
    }
    pub fn get(&self, id: &str) -> Result<&CompiledProfile> {
        let p = self
            .profiles
            .get(id)
            .ok_or_else(|| invalid(format!("unknown Historical profile {id}")))?;
        if let Some(reason) = &p.definition.unsupported_reason {
            return Err(context(
                &p.path,
                id,
                invalid(format!("unsupported profile: {reason}")),
            ));
        }
        Ok(p)
    }
    pub fn for_task(&self, task: &WorkTask) -> Result<Option<&CompiledProfile>> {
        let mut matches = self.profiles.values().filter(|p| {
            p.definition.format_code == task.format_code.0
                && p.definition.exchange == task.exchange
                && p.definition.instrument_type == task.instrument.instrument_kind
                && p.definition.market_category == MarketCategory::of(task)
        });
        let first = matches.next();
        if matches.next().is_some() {
            return Err(invalid("ambiguous Historical profile"));
        }
        if let Some(p) = first {
            return self.get(&p.definition.id).map(Some);
        }
        Ok(None)
    }
    pub fn check_task(&self, task: &WorkTask, p: &CompiledProfile) -> Result<()> {
        if task.exchange != p.definition.exchange
            || task.instrument.instrument_kind != p.definition.instrument_type
            || MarketCategory::of(task) != p.definition.market_category
        {
            return Err(context(
                &p.path,
                &p.definition.id,
                invalid("incompatible exchange/instrument metadata"),
            ));
        }
        if task.symbol.trim().is_empty() || task.instrument_id <= 0 {
            return Err(context(
                &p.path,
                &p.definition.id,
                invalid("invalid instrument identity"),
            ));
        }
        if p.normalizations
            .iter()
            .any(|n| n.quantity_encoding == QuantityEncoding::Contracts)
            && (task
                .instrument
                .contract_value
                .is_none_or(|v| v <= rust_decimal::Decimal::ZERO)
                || task
                    .instrument
                    .contract_value_asset
                    .as_ref()
                    .is_none_or(|s| s.trim().is_empty()))
        {
            return Err(context(
                &p.path,
                &p.definition.id,
                invalid(
                    "contract normalization requires authoritative positive contract value and asset",
                ),
            ));
        }
        if !p
            .physical
            .supported_containers
            .contains(&task.source_compression)
        {
            return Err(context(
                &p.path,
                &p.definition.id,
                invalid("unsupported source container"),
            ));
        }
        Ok(())
    }
}
pub fn default_catalog() -> Result<&'static FormatCatalog> {
    static CATALOG: OnceLock<std::result::Result<FormatCatalog, String>> = OnceLock::new();
    CATALOG
        .get_or_init(|| {
            let root = std::env::var_os("MARKETFORGE_HISTORICAL_FORMATS")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../data/.config/historical/formats")
                });
            FormatCatalog::load(root).map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| invalid(e.clone()))
}
fn collect(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root).map_err(|e| invalid(format!("{}: {e}", root.display())))? {
        let path = entry.map_err(|e| invalid(e.to_string()))?.path();
        if path.is_dir() {
            collect(&path, paths)?;
        } else if path.extension().is_some_and(|e| e == "toml") {
            paths.push(path);
        }
    }
    Ok(())
}
fn validate_physical(p: &PhysicalFormat) -> Result<()> {
    if p.schema_version != 1 || p.id.trim().is_empty() || p.supported_containers.is_empty() {
        return Err(invalid(
            "invalid physical format version/identity/containers",
        ));
    }
    let fields = p
        .raw_schema
        .get("fields")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
        .ok_or_else(|| invalid("raw_schema requires nonempty fields"))?;
    let mut names = BTreeSet::new();
    let mut positions = BTreeSet::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("unnamed raw field"))?;
        if !names.insert(name) {
            return Err(invalid("duplicate raw field"));
        }
        if !matches!(
            f.get("type").and_then(Value::as_str),
            Some(
                "decimal"
                    | "integer"
                    | "string"
                    | "array"
                    | "boolean"
                    | "integer/string"
                    | "uuid/string"
                    | "integer/bool"
                    | "string/integer"
                    | "signed_integer"
                    | "signed_decimal"
            )
        ) {
            return Err(invalid(format!(
                "field {name} has missing or unsupported type"
            )));
        }
        if let Some(position) = f.get("position") {
            let n = position
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or_else(|| invalid("invalid field position"))?;
            if !positions.insert(n) {
                return Err(invalid("duplicate field position"));
            }
        }
    }
    if !positions.is_empty()
        && (positions.len() != fields.len()
            || positions.iter().copied().ne(1..=fields.len() as u64))
    {
        return Err(invalid("raw positions must be contiguous and 1-based"));
    }
    if matches!(p.container_format, PhysicalRecord::Csv)
        && p.raw_schema
            .get("header")
            .and_then(Value::as_bool)
            .is_none()
    {
        return Err(invalid("CSV requires boolean header behavior"));
    }
    Ok(())
}
pub(crate) fn fingerprint(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
fn name(value: &impl Serialize) -> Result<String> {
    serde_json::to_value(value)
        .map_err(|e| invalid(e.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("invalid identity"))
}
fn context(path: &Path, id: &str, e: MarketForgeError) -> MarketForgeError {
    invalid(format!("profile {id} ({}): {e}", path.display()))
}
pub(crate) fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn validate_sources(rules: &Value, schema: &Value) -> Result<()> {
    let names: BTreeSet<_> = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["name"].as_str())
        .collect();
    fn walk(value: &Value, names: &BTreeSet<&str>) -> Result<()> {
        match value {
            Value::Object(map) => {
                for (key, v) in map {
                    if matches!(
                        key.as_str(),
                        "source" | "action_source" | "group_by" | "sort_by"
                    ) {
                        let field = v
                            .as_str()
                            .ok_or_else(|| invalid("source must be a string"))?;
                        if !names.contains(field) {
                            return Err(invalid(format!(
                                "normalization source {field} absent from raw schema"
                            )));
                        }
                    } else if key != "values" {
                        walk(v, names)?;
                    }
                }
            }
            Value::Array(values) => {
                for v in values {
                    walk(v, names)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(rules, &names)
}
