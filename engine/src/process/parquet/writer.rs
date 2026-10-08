use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::{
    canonical::Trade,
    error::{MarketForgeError, Result},
    job::ParquetResourceConfig,
    process::worker::TradeSink,
};

use super::{TradeBatchBuilder, trade_schema};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetWriterMetrics {
    pub trades_written: u64,
    pub files_written: u64,
    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

pub struct ParquetTradeWriter {
    output_dir: PathBuf,
    resources: ParquetResourceConfig,

    batch: TradeBatchBuilder,
    batch_bytes: u64,

    writer: Option<ArrowWriter<File>>,
    current_file_bytes: u64,
    next_file_index: u64,

    metrics: ParquetWriterMetrics,
    finished: bool,
    failed: bool,
}

impl ParquetTradeWriter {
    pub fn new(output_dir: impl AsRef<Path>, resources: ParquetResourceConfig) -> Result<Self> {
        if resources.row_group_target_bytes == 0 || resources.file_target_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet resource targets must be greater than zero".to_owned(),
            ));
        }

        if resources.file_target_bytes < resources.row_group_target_bytes {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet file target must be at least the row-group target".to_owned(),
            ));
        }

        let output_dir = output_dir.as_ref().to_path_buf();

        fs::create_dir_all(&output_dir).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create Parquet output directory {}: {error}",
                output_dir.display()
            ))
        })?;

        Ok(Self {
            output_dir,
            resources,
            batch: TradeBatchBuilder::new(),
            batch_bytes: 0,
            writer: None,
            current_file_bytes: 0,
            next_file_index: 0,
            metrics: ParquetWriterMetrics::default(),
            finished: false,
            failed: false,
        })
    }

    pub fn metrics(&self) -> &ParquetWriterMetrics {
        &self.metrics
    }

    fn open_file(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let path = self
            .output_dir
            .join(format!("part-{:06}.parquet", self.next_file_index));

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "Parquet output already exists: {}",
                path.display()
            )));
        }

        let file = File::create(&path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create Parquet file {}: {error}",
                path.display()
            ))
        })?;

        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).map_err(
                |error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "invalid ZSTD compression level: {error}"
                    ))
                },
            )?))
            .build();

        let writer =
            ArrowWriter::try_new(file, trade_schema(), Some(properties)).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to initialize Parquet writer: {error}"
                ))
            })?;

        self.writer = Some(writer);
        self.current_file_bytes = 0;
        self.next_file_index += 1;

        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        self.open_file()?;

        let batch = self.batch.finish()?.expect("nonempty batch");

        let writer = self.writer.as_mut().expect("writer initialized");

        writer.write(&batch).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to write Parquet batch: {error}"
            ))
        })?;

        writer.flush().map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to flush Parquet row group: {error}"
            ))
        })?;

        self.metrics.trades_written += batch.num_rows() as u64;

        self.batch_bytes = 0;

        // Approximate serialized size.
        self.current_file_bytes = writer.bytes_written() as u64;

        if self.current_file_bytes >= self.resources.file_target_bytes {
            self.close_file()?;
        }

        Ok(())
    }

    fn close_file(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        writer.close().map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to finalize Parquet file: {error}"
            ))
        })?;

        self.metrics.files_written += 1;

        self.current_file_bytes = 0;

        Ok(())
    }

    fn estimate_trade_bytes(trade: &Trade) -> u64 {
        let mut size = 256u64;

        size += trade.envelope.symbol.len() as u64;
        size += trade.envelope.stream_id.len() as u64;

        if let Some(trade_id) = &trade.trade_id {
            size += trade_id.len() as u64;
        }

        size
    }
    fn ensure_writable(&self) -> Result<()> {
        if self.failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet writer is in a failed state".to_owned(),
            ));
        }

        if self.finished {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet writer has already been finalized".to_owned(),
            ));
        }

        Ok(())
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl TradeSink for ParquetTradeWriter {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        self.ensure_writable()?;

        let timestamp = trade.envelope.event_timestamp_ns;

        self.batch_bytes += Self::estimate_trade_bytes(&trade);
        self.batch.push(trade);

        if self.batch_bytes >= self.resources.row_group_target_bytes {
            if let Err(error) = self.flush_batch() {
                self.failed = true;
                return Err(error);
            }
        }

        self.metrics.start_timestamp_ns = Some(
            self.metrics
                .start_timestamp_ns
                .map_or(timestamp, |current| current.min(timestamp)),
        );

        self.metrics.end_timestamp_ns = Some(
            self.metrics
                .end_timestamp_ns
                .map_or(timestamp, |current| current.max(timestamp)),
        );

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot finalize a failed Parquet writer".to_owned(),
            ));
        }

        if self.finished {
            return Ok(());
        }

        if let Err(error) = self.flush_batch() {
            self.failed = true;
            return Err(error);
        }

        if let Err(error) = self.close_file() {
            self.failed = true;
            return Err(error);
        }

        self.finished = true;

        Ok(())
    }
}
