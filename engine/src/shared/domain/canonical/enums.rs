use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Bybit,
    Binance,
    Okx,
    Bitget,
    GateIo,
    Manual(VenueCode),
}

/// A bounded canonical venue code. Keeping it inline preserves Copy semantics
/// for event envelopes and avoids allocating a venue string per market event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VenueCode {
    bytes: [u8; 32],
    len: u8,
}

impl Exchange {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "binance" => Some(Self::Binance),
            "bybit" => Some(Self::Bybit),
            "okx" => Some(Self::Okx),
            "bitget" => Some(Self::Bitget),
            "gateio" => Some(Self::GateIo),
            "external" => None,
            _ if code.len() <= 32
                && code.bytes().next()?.is_ascii_lowercase()
                && code
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') =>
            {
                let mut bytes = [0; 32];
                bytes[..code.len()].copy_from_slice(code.as_bytes());
                Some(Self::Manual(VenueCode {
                    bytes,
                    len: code.len() as u8,
                }))
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Bitget => "bitget",
            Self::GateIo => "gateio",
            Self::Manual(code) => std::str::from_utf8(&code.bytes[..code.len as usize])
                .expect("venue codes contain only ASCII"),
        }
    }
}

impl Serialize for Exchange {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Exchange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let code = String::deserialize(deserializer)?;
        Self::from_code(&code)
            .ok_or_else(|| serde::de::Error::custom("invalid execution venue code"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}
