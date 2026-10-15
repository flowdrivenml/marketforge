use std::path::PathBuf;

use crate::error::{MarketForgeError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFileRange {
    pub path: PathBuf,
    pub row_start: u64,
    pub row_end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFileSlice {
    pub path: PathBuf,
    pub local_offset: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone)]
pub struct CanonicalRowIndex {
    files: Vec<CanonicalFileRange>,
    total_rows: u64,
}

impl CanonicalRowIndex {
    /// Build a task-relative index from ordered canonical files.
    ///
    /// Each input contains a file path and its canonical row count.
    pub fn new(files: Vec<(PathBuf, u64)>) -> Result<Self> {
        let mut indexed = Vec::with_capacity(files.len());
        let mut offset = 0_u64;

        for (path, rows) in files {
            if rows == 0 {
                return invalid(format!("canonical file has zero rows: {}", path.display()));
            }

            let end = offset
                .checked_add(rows)
                .ok_or_else(|| invalid_error("canonical row offset overflow"))?;

            indexed.push(CanonicalFileRange {
                path,
                row_start: offset,
                row_end: end,
            });

            offset = end;
        }

        Ok(Self {
            files: indexed,
            total_rows: offset,
        })
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn files(&self) -> &[CanonicalFileRange] {
        &self.files
    }

    /// Resolve a task-relative canonical row range.
    ///
    /// Returns ordered file-local slices.
    pub fn resolve(&self, row_offset: u64, row_count: u64) -> Result<Vec<CanonicalFileSlice>> {
        let end = row_offset
            .checked_add(row_count)
            .ok_or_else(|| invalid_error("canonical reference range overflow"))?;

        if end > self.total_rows {
            return invalid(format!(
                "canonical reference out of bounds: offset={}, count={}, total={}",
                row_offset, row_count, self.total_rows
            ));
        }

        if row_count == 0 {
            return Ok(Vec::new());
        }

        let mut slices = Vec::new();
        let mut resolved_rows = 0_u64;

        for file in &self.files {
            if file.row_end <= row_offset {
                continue;
            }

            if file.row_start >= end {
                break;
            }

            let overlap_start = row_offset.max(file.row_start);
            let overlap_end = end.min(file.row_end);

            if overlap_start >= overlap_end {
                continue;
            }

            let count = overlap_end - overlap_start;

            slices.push(CanonicalFileSlice {
                path: file.path.clone(),
                local_offset: overlap_start - file.row_start,
                row_count: count,
            });

            resolved_rows += count;
        }

        if resolved_rows != row_count {
            return invalid(format!(
                "incomplete canonical resolution: expected={}, resolved={}",
                row_count, resolved_rows
            ));
        }

        Ok(slices)
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> CanonicalRowIndex {
        CanonicalRowIndex::new(vec![
            (PathBuf::from("part-000000.parquet"), 100),
            (PathBuf::from("part-000001.parquet"), 100),
            (PathBuf::from("part-000002.parquet"), 50),
        ])
        .unwrap()
    }

    #[test]
    fn resolves_single_file() {
        let slices = index().resolve(20, 10).unwrap();

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].local_offset, 20);
        assert_eq!(slices[0].row_count, 10);
    }

    #[test]
    fn resolves_across_files() {
        let slices = index().resolve(90, 30).unwrap();

        assert_eq!(slices.len(), 2);

        assert_eq!(slices[0].local_offset, 90);
        assert_eq!(slices[0].row_count, 10);

        assert_eq!(slices[1].local_offset, 0);
        assert_eq!(slices[1].row_count, 20);
    }

    #[test]
    fn resolves_across_three_files() {
        let slices = index().resolve(90, 150).unwrap();

        assert_eq!(slices.len(), 3);

        assert_eq!(slices[0].row_count, 10);
        assert_eq!(slices[1].row_count, 100);
        assert_eq!(slices[2].row_count, 40);
    }

    #[test]
    fn accepts_zero_row_reference() {
        let slices = index().resolve(100, 0).unwrap();

        assert!(slices.is_empty());
    }

    #[test]
    fn rejects_out_of_bounds_reference() {
        assert!(index().resolve(240, 20).is_err());
    }

    #[test]
    fn rejects_offset_overflow() {
        assert!(index().resolve(u64::MAX, 2).is_err());
    }

    #[test]
    fn rejects_empty_canonical_file() {
        assert!(CanonicalRowIndex::new(vec![(PathBuf::from("empty.parquet"), 0)]).is_err());
    }

    #[test]
    fn validates_total_rows() {
        assert_eq!(index().total_rows(), 250);
    }
}
