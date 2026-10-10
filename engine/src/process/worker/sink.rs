use crate::{canonical::Trade, error::Result, formats::depth::DepthProcessingOutcome};

pub trait TradeSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}

pub trait DepthSink {
    /// Accept one complete source-event outcome.
    ///
    /// All levels belong to the same source message.
    /// The boundary identifies whether they initialize a new book segment.
    ///
    /// A successful return means the sink accepted the entire outcome.
    /// It does not necessarily mean the data has been persisted to disk.
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}
