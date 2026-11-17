use {
    crate::historical::normalize::boundary::DepthBoundaryManifest,
    crate::shared::domain::canonical::Trade, crate::shared::error::Result,
    crate::shared::formats::depth::DepthProcessingOutcome,
};

pub trait TradeSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()>;
    fn write_trade_at(&mut self, trade: Trade, _source_ordinal: u64) -> Result<()> {
        self.write_trade(trade)
    }

    fn finish(&mut self) -> Result<()>;
}

pub trait DepthSink {
    /// Bounded evidence hook; output sinks need not retain decoded source data.
    fn observe_source_record(&mut self, _record: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    /// Accept one complete source-event outcome.
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()>;

    /// Accept completed boundary metadata.
    ///
    /// Every implementation must explicitly handle this operation.
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}
