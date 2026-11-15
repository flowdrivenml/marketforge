//! Limits apply only inside a normalization scope; unrelated shared callers retain behavior.
use crate::shared::error::{MarketForgeError, ResourceLimitError, Result};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NormalizationLimits {
    pub workbook_bytes: u64,
    pub workbook_xml_bytes: u64,
    pub workbook_rows: u64,
    pub record_bytes: u64,
    pub depth_levels: u64,
    pub metric_entries: u64,
    pub open_files: u64,
    pub reconciliation_bytes: u64,
}
impl Default for NormalizationLimits {
    fn default() -> Self {
        Self {
            workbook_bytes: 8 * 1024 * 1024,
            workbook_xml_bytes: 16 * 1024 * 1024,
            workbook_rows: 25_000,
            record_bytes: 1024 * 1024,
            depth_levels: 25_000,
            metric_entries: 4096,
            open_files: 64,
            reconciliation_bytes: 64 * 1024 * 1024,
        }
    }
}
impl NormalizationLimits {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("workbook_bytes", self.workbook_bytes),
            ("workbook_xml_bytes", self.workbook_xml_bytes),
            ("workbook_rows", self.workbook_rows),
            ("record_bytes", self.record_bytes),
            ("depth_levels", self.depth_levels),
            ("metric_entries", self.metric_entries),
            ("open_files", self.open_files),
            ("reconciliation_bytes", self.reconciliation_bytes),
        ] {
            if value == 0 || value > (usize::MAX as u64) / 128 {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "normalization_limits.{name}: must be positive and safely addressable"
                )));
            }
        }
        Ok(())
    }
}
thread_local! { static ACTIVE: RefCell<Option<(NormalizationLimits, String)>> = const { RefCell::new(None) }; }
pub struct LimitScope(
    Option<(NormalizationLimits, String)>,
    std::marker::PhantomData<std::rc::Rc<()>>,
);
impl LimitScope {
    pub fn enter(limits: NormalizationLimits, scope: String) -> Self {
        Self(
            ACTIVE.with(|slot| slot.replace(Some((limits, scope)))),
            std::marker::PhantomData,
        )
    }
}
impl Drop for LimitScope {
    fn drop(&mut self) {
        ACTIVE.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub fn current() -> Option<NormalizationLimits> {
    ACTIVE.with(|s| s.borrow().as_ref().map(|v| v.0))
}
pub fn error(
    resource: &'static str,
    permitted: u64,
    requested: u64,
    stage: &'static str,
) -> ResourceLimitError {
    ResourceLimitError {
        resource,
        permitted,
        requested,
        stage,
        scope: ACTIVE.with(|s| {
            s.borrow()
                .as_ref()
                .map(|v| v.1.clone())
                .unwrap_or_else(|| "unscoped".into())
        }),
    }
}
pub fn check(
    resource: &'static str,
    permitted: u64,
    requested: u64,
    stage: &'static str,
) -> Result<()> {
    if requested > permitted {
        Err(error(resource, permitted, requested, stage).into())
    } else {
        Ok(())
    }
}
pub fn depth(requested: usize, stage: &'static str) -> Result<()> {
    if let Some(limits) = current() {
        check("depth levels", limits.depth_levels, requested as u64, stage)?;
    }
    Ok(())
}
/// Recover a typed limit carried by an I/O-based decoder API.
pub fn source_error(path: std::path::PathBuf, source: std::io::Error) -> MarketForgeError {
    if let Some(limit) = source
        .get_ref()
        .and_then(|e| e.downcast_ref::<ResourceLimitError>())
    {
        MarketForgeError::ResourceLimit(limit.clone())
    } else {
        MarketForgeError::SourceRead { path, source }
    }
}

pub fn current_scope() -> Option<String> {
    ACTIVE.with(|slot| slot.borrow().as_ref().map(|(_, scope)| scope.clone()))
}
