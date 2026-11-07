use super::DatasetManifest;
use crate::{
    historical::job::ContentType,
    shared::error::{MarketForgeError, Result},
};
use std::collections::HashSet;
impl DatasetManifest {
    /// Validate authoritative source spans before readers join stream metadata paths.
    pub fn validate_streams(&self) -> Result<()> {
        let invalid = |s: &str| MarketForgeError::InvalidConfiguration(s.into());
        if self.streams.is_empty() {
            if self.files.iter().any(|f| f.path.starts_with("streams/")) {
                return Err(invalid("stream files require ownership metadata"));
            }
            return Ok(());
        }
        if self.protocol_version != 3 {
            return Err(invalid("stream ownership requires manifest3"));
        }
        let kind = match self.content_type {
            ContentType::Trades => "trades",
            ContentType::Depth => "depth",
            _ => return Err(invalid("unsupported stream content")),
        };
        let mut directories = HashSet::new();
        let mut tasks = HashSet::new();
        let mut owned = HashSet::new();
        for stream in &self.streams {
            let parts = stream.directory.split('/').collect::<Vec<_>>();
            if parts.len() != 3
                || parts[0] != "streams"
                || parts[2] != kind
                || parts[1].len() != 64
                || !parts[1]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !directories.insert(&stream.directory)
            {
                return Err(invalid("unsafe or duplicate stream directory"));
            }
            let mut offset = 0u64;
            for source in &stream.sources {
                let metrics = self
                    .processing_metrics
                    .tasks
                    .iter()
                    .find(|t| t.task_id == source.task_id)
                    .ok_or_else(|| invalid("missing source metrics"))?;
                if !tasks.insert(source.task_id)
                    || source.start_row != offset
                    || source.end_row.checked_sub(offset) != Some(metrics.counters.events_written)
                    || metrics.stream_id != stream.stream_id
                {
                    return Err(invalid("invalid stream source attribution"));
                }
                offset = source.end_row;
            }
            let prefix = format!("{}/", stream.directory);
            let mut rows = 0u64;
            for file in self.files.iter().filter(|f| f.path.starts_with(&prefix)) {
                let name = &file.path[prefix.len()..];
                if !name.starts_with("part-")
                    || !name.ends_with(".parquet")
                    || name.contains('/')
                    || !owned.insert(&file.path)
                {
                    return Err(invalid("invalid or duplicate stream part"));
                }
                rows = rows
                    .checked_add(file.rows)
                    .ok_or_else(|| invalid("stream row overflow"))?;
            }
            if rows != offset {
                return Err(invalid("stream source/physical row mismatch"));
            }
        }
        if tasks != self.source_tasks.iter().copied().collect()
            || tasks.len() != self.source_tasks.len()
            || owned.len() != self.files.len()
        {
            return Err(invalid("incomplete stream ownership"));
        }
        Ok(())
    }
}
