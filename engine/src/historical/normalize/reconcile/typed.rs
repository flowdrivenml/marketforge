//! Internal trade payload v1: explicit tags and little-endian integers. Decimal
//! transport is a signed 128-bit mantissa plus a u32 scale, never floating point.
//! Dynamic family payloads and complete depth events retain bounded JSON frames.
use super::TradeEntry;
use crate::shared::{
    domain::canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    error::{MarketForgeError, Result},
};
use rust_decimal::Decimal;
use std::io::{self, Write};
fn bad() -> MarketForgeError {
    MarketForgeError::InvalidConfiguration("corrupt typed scratch trade".into())
}
pub(super) fn string(w: &mut impl Write, value: &str) -> io::Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| io::Error::other("typed scratch string too long"))?;
    w.write_all(&length.to_le_bytes())?;
    w.write_all(value.as_bytes())
}
pub(super) fn optional<T>(
    w: &mut impl Write,
    value: Option<T>,
    encode: impl FnOnce(&mut dyn Write, T) -> io::Result<()>,
) -> io::Result<()> {
    w.write_all(&[u8::from(value.is_some())])?;
    if let Some(value) = value {
        encode(w, value)?;
    }
    Ok(())
}
pub(super) fn decimal(w: &mut dyn Write, value: Decimal) -> io::Result<()> {
    w.write_all(&value.mantissa().to_le_bytes())?;
    w.write_all(&value.scale().to_le_bytes())?;
    w.write_all(&[u8::from(value.is_sign_negative())])
}
pub(super) fn write_exchange(w: &mut impl Write, exchange: Exchange) -> io::Result<()> {
    w.write_all(&[match exchange {
        Exchange::Bybit => 1,
        Exchange::Binance => 2,
        Exchange::Okx => 3,
        Exchange::Bitget => 4,
        Exchange::GateIo => 5,
        Exchange::Manual(_) => 6,
    }])?;
    if matches!(exchange, Exchange::Manual(_)) {
        string(w, exchange.as_str())?;
    }
    Ok(())
}
pub(super) fn encode(w: &mut impl Write, entry: &TradeEntry) -> io::Result<()> {
    w.write_all(&[0, 1])?; // typed trade tag, payload version
    w.write_all(&(entry.source as u64).to_le_bytes())?;
    w.write_all(&entry.priority.to_le_bytes())?;
    w.write_all(&entry.ordinal.to_le_bytes())?;
    let t = &entry.trade;
    let e = &t.envelope;
    w.write_all(&e.event_timestamp_ns.to_le_bytes())?;
    optional(w, e.system_timestamp_ns, |w, v| {
        w.write_all(&v.to_le_bytes())
    })?;
    write_exchange(w, e.exchange)?;
    w.write_all(&e.instrument_id.to_le_bytes())?;
    string(w, &e.symbol)?;
    string(w, &e.stream_id)?;
    w.write_all(&[u8::from(t.trade_id.is_some())])?;
    if let Some(id) = t.trade_id.as_deref() {
        string(w, id)?;
    }
    optional(w, t.sequence, |w, v| w.write_all(&v.to_le_bytes()))?;
    w.write_all(&[match t.side {
        TradeSide::Buy => 1,
        TradeSide::Sell => 2,
        TradeSide::Unknown => 3,
    }])?;
    decimal(w, t.price)?;
    optional(w, t.quantity_base, decimal)?;
    optional(w, t.quantity_quote, decimal)?;
    optional(w, t.quantity_contracts, decimal)?;
    optional(w, t.is_rpi, |w, v| w.write_all(&[u8::from(v)]))?;
    optional(w, t.trade_iv, decimal)?;
    optional(w, t.mark_iv, decimal)?;
    optional(w, t.index_price, decimal)?;
    optional(w, t.mark_price, decimal)
}
pub(super) struct Input<'a>(pub(super) &'a [u8]);
impl<'a> Input<'a> {
    pub(super) fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let head = self.0.get(..N).ok_or_else(bad)?;
        let value = head.try_into().map_err(|_| bad())?;
        self.0 = &self.0[N..];
        Ok(value)
    }
    pub(super) fn byte(&mut self) -> Result<u8> {
        Ok(self.take::<1>()?[0])
    }
    pub(super) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    pub(super) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take()?))
    }
    pub(super) fn exchange(&mut self) -> Result<Exchange> {
        Ok(match self.byte()? {
            1 => Exchange::Bybit,
            2 => Exchange::Binance,
            3 => Exchange::Okx,
            4 => Exchange::Bitget,
            5 => Exchange::GateIo,
            6 => match Exchange::from_code(self.string_ref()?) {
                Some(venue @ Exchange::Manual(_)) => venue,
                _ => return Err(bad()),
            },
            _ => return Err(bad()),
        })
    }
    pub(super) fn string_ref(&mut self) -> Result<&'a str> {
        let length = u32::from_le_bytes(self.take()?) as usize;
        let bytes = self.0.get(..length).ok_or_else(bad)?;
        let value = std::str::from_utf8(bytes).map_err(|_| bad())?;
        self.0 = &self.0[length..];
        Ok(value)
    }
    pub(super) fn string(&mut self) -> Result<String> {
        Ok(self.string_ref()?.to_owned())
    }
    pub(super) fn optional<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<Option<T>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(decode(self)?)),
            _ => Err(bad()),
        }
    }
    pub(super) fn decimal(&mut self) -> Result<Decimal> {
        let mantissa = i128::from_le_bytes(self.take()?);
        let scale = u32::from_le_bytes(self.take()?);
        let negative = match self.byte()? {
            0 => false,
            1 => true,
            _ => return Err(bad()),
        };
        let mut value = Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|_| bad())?;
        value.set_sign_negative(negative);
        Ok(value)
    }
}
pub(super) fn decode(bytes: &[u8]) -> Result<TradeEntry> {
    if bytes.first() != Some(&0) {
        return serde_json::from_slice(bytes).map_err(|e| {
            MarketForgeError::InvalidConfiguration(format!("corrupt scratch trade: {e}"))
        });
    }
    let mut r = Input(bytes);
    if r.take::<2>()? != [0, 1] {
        return Err(bad());
    }
    let source = usize::try_from(r.u64()?).map_err(|_| bad())?;
    let priority = r.u64()?;
    let ordinal = r.u64()?;
    let event_timestamp_ns = r.i64()?;
    let system_timestamp_ns = r.optional(Input::i64)?;
    let exchange = r.exchange()?;
    let instrument_id = r.i64()?;
    let symbol = r.string()?;
    let stream_id = r.string()?;
    let trade_id = r.optional(Input::string)?;
    let sequence = r.optional(Input::u64)?;
    let side = match r.byte()? {
        1 => TradeSide::Buy,
        2 => TradeSide::Sell,
        3 => TradeSide::Unknown,
        _ => return Err(bad()),
    };
    let price = r.decimal()?;
    let quantity_base = r.optional(Input::decimal)?;
    let quantity_quote = r.optional(Input::decimal)?;
    let quantity_contracts = r.optional(Input::decimal)?;
    let is_rpi = r.optional(|r| match r.byte()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(bad()),
    })?;
    let trade_iv = r.optional(Input::decimal)?;
    let mark_iv = r.optional(Input::decimal)?;
    let index_price = r.optional(Input::decimal)?;
    let mark_price = r.optional(Input::decimal)?;
    if !r.0.is_empty() {
        return Err(bad());
    }
    Ok(TradeEntry {
        source,
        priority,
        ordinal,
        trade: Trade {
            envelope: EventEnvelope {
                event_timestamp_ns,
                system_timestamp_ns,
                exchange,
                instrument_id,
                symbol,
                stream_id,
            },
            trade_id,
            sequence,
            side,
            price,
            quantity_base,
            quantity_quote,
            quantity_contracts,
            is_rpi,
            trade_iv,
            mark_iv,
            index_price,
            mark_price,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn complete_fields_scale_extremes_and_malformed_lengths() {
        let mut entry = super::super::tests::event(2, 7, -99, Some("native"), 1);
        entry.ordinal = u64::MAX;
        let t = &mut entry.trade;
        t.envelope.system_timestamp_ns = Some(i64::MIN);
        t.envelope.instrument_id = i64::MIN;
        t.envelope.symbol = "BTC/€".into();
        t.sequence = Some(u64::MAX);
        t.side = TradeSide::Sell;
        let d: Decimal = "-123.45000000000000000000000000".parse().unwrap();
        t.price = d;
        t.quantity_base = Some(d);
        t.quantity_quote = Some(d);
        t.quantity_contracts = Some(d);
        t.is_rpi = Some(false);
        t.trade_iv = Some(d);
        t.mark_iv = Some(d);
        t.index_price = Some(d);
        t.mark_price = Some(d);
        let mut bytes = Vec::new();
        encode(&mut bytes, &entry).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            serde_json::to_value(decoded).unwrap()
        );
        assert!(bytes.len() < serde_json::to_vec(&entry).unwrap().len());
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        bytes[1] = 2;
        assert!(decode(&bytes).is_err());
        let mut r = Input(&[255, 255, 255, 255]);
        assert!(r.string().is_err());
    }
}
