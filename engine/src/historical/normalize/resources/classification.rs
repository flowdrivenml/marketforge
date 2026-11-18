use crate::{
    historical::job::WorkTask,
    shared::error::{MarketForgeError, Result},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFormat {
    Jsonl,
    Csv,
    Xlsx,
}

pub fn source_format(task: &WorkTask) -> Result<SourceFormat> {
    if task
        .normalizations
        .iter()
        .all(|n| n.target_schema == crate::shared::normalization::TargetSchema::Trade)
    {
        return Ok(if task.raw_schema["marketforge_trade_jsonl"] == true {
            SourceFormat::Jsonl
        } else {
            SourceFormat::Csv
        });
    }
    let fields = task
        .raw_schema
        .get("fields")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth raw schema missing fields".to_owned())
        })?;

    // XLSX workbooks must be identified explicitly.
    //
    // Non-positional fields alone are insufficient because
    // JSONL sources also use non-positional schemas.
    let is_xlsx = match task.raw_schema.get("workbook_sheets") {
        Some(value) if value.as_u64().is_some_and(|n| n > 0) => true,
        Some(_) => {
            return Err(MarketForgeError::InvalidConfiguration(
                "workbook_sheets must be a positive integer".into(),
            ));
        }
        None => false,
    };

    if is_xlsx {
        return Ok(SourceFormat::Xlsx);
    }

    let positional = fields
        .iter()
        .filter(|field| field.get("position").is_some())
        .count();

    if positional == fields.len() && !fields.is_empty() {
        return Ok(SourceFormat::Csv);
    }

    if positional == 0 {
        return Ok(SourceFormat::Jsonl);
    }

    Err(MarketForgeError::InvalidConfiguration(
        "depth raw schema mixes positional and non-positional fields".to_owned(),
    ))
}
