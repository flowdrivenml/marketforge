//! Compatible schema-v4 extension. Wire family references are resolved before
//! legacy concrete/family validation; no numeric identity passes through f64.
use super::*;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineFamily {
    pub family_id: i64,
    pub exchange: Exchange,
    pub instrument_type: InstrumentKind,
    pub market_category: MarketCategory,
    pub identifier: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub instrument: InstrumentMetadata,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BatchDocument {
    schema_version: u32,
    kind: String,
    status: String,
    run_id: String,
    run_directory: PathBuf,
    batch_path: PathBuf,
    ranking_snapshot: Option<Value>,
    ranking_policy: String,
    requested_interval: RequestedInterval,
    instrument_namespace: String,
    instruments: Vec<OfflineInstrument>,
    #[serde(default)]
    families: Vec<OfflineFamily>,
    jobs: Vec<Value>,
}
impl TryFrom<BatchDocument> for PreparedBatch {
    type Error = MarketForgeError;
    fn try_from(document: BatchDocument) -> Result<Self> {
        let mut batch = Self {
            schema_version: document.schema_version,
            kind: document.kind,
            status: document.status,
            run_id: document.run_id,
            run_directory: document.run_directory,
            batch_path: document.batch_path,
            ranking_snapshot: document.ranking_snapshot,
            ranking_policy: document.ranking_policy,
            requested_interval: document.requested_interval,
            instrument_namespace: document.instrument_namespace,
            instruments: document.instruments,
            families: document.families,
            jobs: Vec::new(),
        };
        validate_families(&batch)?;
        for mut job in document.jobs {
            for (field, allowed) in [
                (
                    "output",
                    &["content_type", "staging_path", "dataset_path"][..],
                ),
                ("ordering", &["primary", "tie_break"][..]),
            ] {
                if job[field]
                    .as_object()
                    .is_some_and(|o| o.keys().any(|k| !allowed.contains(&k.as_str())))
                {
                    return Err(invalid(format!("unknown job {field} field")));
                }
            }
            let content: ContentType =
                serde_json::from_value(job["output"]["content_type"].clone())
                    .map_err(|e| invalid(e.to_string()))?;
            let tasks = job["tasks"]
                .as_array_mut()
                .ok_or_else(|| invalid("job tasks must be an array"))?;
            for task in tasks {
                let Some(reference) = task.get("family_id") else {
                    continue;
                };
                let id = reference
                    .as_i64()
                    .ok_or_else(|| invalid("family_id must be a positive signed 64-bit integer"))?;
                let family = batch
                    .families
                    .iter()
                    .find(|f| f.family_id == id)
                    .ok_or_else(|| invalid(format!("unknown family_id {id}")))?;
                let object = task
                    .as_object_mut()
                    .ok_or_else(|| invalid("task must be an object"))?;
                for field in [
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "base_asset",
                    "quote_asset",
                    "symbol",
                    "instrument",
                    "instrument_id",
                    "scope",
                    "stream_id",
                ] {
                    if object.contains_key(field) {
                        return Err(invalid(format!(
                            "family_id task must not also declare concrete identity field {field}"
                        )));
                    }
                }
                object.remove("family_id");
                for (key, value) in [
                    ("exchange", json!(family.exchange)),
                    ("instrument_type", json!(family.instrument_type)),
                    ("market_category", json!(family.market_category)),
                    ("base_asset", json!(family.base_asset)),
                    ("quote_asset", json!(family.quote_asset)),
                    (
                        "scope",
                        json!({"kind":"family", "identifier":family.identifier,"family_id":id}),
                    ),
                    (
                        "stream_id",
                        json!(format!(
                            "{}.{}.{}.family:{}.{}",
                            token(&family.exchange)?,
                            token(&family.market_category)?,
                            token(&family.instrument_type)?,
                            family.identifier,
                            token(&content)?
                        )),
                    ),
                ] {
                    object.insert(key.into(), value);
                }
            }
            batch
                .jobs
                .push(serde_json::from_value(job).map_err(|e| invalid(e.to_string()))?);
        }
        Ok(batch)
    }
}
use crate::shared::error::MarketForgeError;
pub(super) fn validate_families(batch: &PreparedBatch) -> Result<()> {
    let mut ids = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for family in &batch.families {
        if family.family_id <= 0
            || !ids.insert(family.family_id)
            || !identities.insert(format!(
                "{}.{}.{}.{}",
                token(&family.exchange)?,
                token(&family.instrument_type)?,
                token(&family.market_category)?,
                family.identifier
            ))
        {
            return Err(invalid("duplicate or invalid family identity/ID"));
        }
        if family.instrument_type != InstrumentKind::Future
            || !matches!(
                family.exchange,
                Exchange::Binance | Exchange::Bybit | Exchange::Okx
            )
            || !matches!(
                family.market_category,
                MarketCategory::Linear | MarketCategory::Inverse
            )
            || family.identifier.trim().is_empty()
            || family.base_asset.trim().is_empty()
            || family.quote_asset.trim().is_empty()
            || family.base_asset == family.quote_asset
            || family.instrument.tick_size <= Decimal::ZERO
            || family
                .instrument
                .contract_value
                .is_some_and(|v| v <= Decimal::ZERO)
            || family.instrument.contract_value.is_some()
                != family.instrument.contract_value_asset.is_some()
            || family
                .instrument
                .contract_value_asset
                .as_ref()
                .is_some_and(|v| v.trim().is_empty())
        {
            return Err(invalid(
                "unsupported family identity or invalid family normalization metadata",
            ));
        }
    }
    for task in batch.jobs.iter().flat_map(|j| &j.tasks) {
        if let Some(scope) = &task.scope
            && let Some(id) = scope.family_id
        {
            let family = batch
                .families
                .iter()
                .find(|f| f.family_id == id)
                .ok_or_else(|| invalid(format!("unknown family_id {id}")))?;
            if task.exchange != family.exchange
                || task.instrument_type != family.instrument_type
                || task.market_category != family.market_category
                || scope.identifier != family.identifier
                || task.base_asset.as_ref() != Some(&family.base_asset)
                || task.quote_asset.as_ref() != Some(&family.quote_asset)
            {
                return Err(invalid(
                    "resolved family reference conflicts with family authority",
                ));
            }
        }
    }
    Ok(())
}
