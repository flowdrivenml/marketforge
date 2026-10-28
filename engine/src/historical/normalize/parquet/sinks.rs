use crate::historical::normalize::worker::{DepthSink, TradeSink};
use crate::shared::{
    domain::{book::boundary::DepthBoundaryManifest, canonical::Trade},
    error::Result,
    formats::depth::DepthProcessingOutcome,
    persistence::parquet::{ParquetDepthWriter, ParquetTradeWriter},
};
impl TradeSink for ParquetTradeWriter {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        ParquetTradeWriter::write_trade(self, trade)
    }
    fn finish(&mut self) -> Result<()> {
        ParquetTradeWriter::finish(self)
    }
}
impl DepthSink for ParquetDepthWriter {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        ParquetDepthWriter::write_outcome(self, outcome)
    }
    fn finish(&mut self) -> Result<()> {
        ParquetDepthWriter::finish(self)
    }
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        ParquetDepthWriter::write_boundary(self, boundary)
    }
}
