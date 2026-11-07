//! Completed checks for one normalization unit, with no diagnostic allocations.
//!
//! ParseFailure: one physical decoder attempt (CSV row, JSONL message, XLSX row).
//! InvalidRecord: one applicable record/event validation unit; partial checks that
//! pass remain eligible even when a later, different category fails. A failing
//! category replaces its success within that unit, never counts twice.
//! TransformationFailure: one batch of attempted quantity transformations (one
//! trade, depth message or assembled snapshot), not one emitted canonical level.
//! MissingSnapshot: an incremental event reaching synchronization validation.
//! SequenceGap: an initialized, sequenced incremental event reaching continuity.
//! TimestampRegression: an applicable timestamp comparison (worker ordering).
//! InvalidBook: only explicit book-invariant checks; current level validation is
//! InvalidRecord and must not invent InvalidBook successes.
use super::IntegrityCategory;
#[derive(Debug, Clone, Default)]
pub struct IntegrityObservationTrace {
    passed: u8,
    pub event_timestamp_ns: Option<i64>,
}
pub const CATEGORIES: [IntegrityCategory; 7] = [
    IntegrityCategory::ParseFailure,
    IntegrityCategory::InvalidRecord,
    IntegrityCategory::SequenceGap,
    IntegrityCategory::TimestampRegression,
    IntegrityCategory::MissingSnapshot,
    IntegrityCategory::InvalidBook,
    IntegrityCategory::TransformationFailure,
];
impl IntegrityObservationTrace {
    pub fn passed(&mut self, category: IntegrityCategory) {
        self.passed |= 1 << CATEGORIES.iter().position(|c| *c == category).unwrap();
    }
    pub fn successful(
        &self,
        failed: Option<IntegrityCategory>,
    ) -> impl Iterator<Item = IntegrityCategory> + '_ {
        CATEGORIES
            .into_iter()
            .enumerate()
            .filter_map(move |(index, category)| {
                ((self.passed & (1 << index)) != 0 && Some(category) != failed).then_some(category)
            })
    }
}
