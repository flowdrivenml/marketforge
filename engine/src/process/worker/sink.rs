use crate::{canonical::Trade, error::Result};

pub trait TradeSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}
