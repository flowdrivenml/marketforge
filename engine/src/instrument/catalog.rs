use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};

use crate::error::{MarketForgeError, Result};

use super::{currency::Currency, identity::InstrumentIdentity};

pub const INSTRUMENT_CATALOG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentMetadata {
    #[serde(flatten)]
    pub identity: InstrumentIdentity,

    pub base_currency: Currency,
    pub quote_currency: Currency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentCatalogFile {
    pub version: u32,
    pub instruments: Vec<InstrumentMetadata>,
}

pub struct InstrumentCatalog {
    instruments: HashMap<InstrumentIdentity, InstrumentMetadata>,
}

impl InstrumentCatalog {
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to open instrument catalog {}: {error}",
                path.display()
            ))
        })?;

        let catalog: InstrumentCatalogFile = serde_json::from_reader(BufReader::new(file))
            .map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "invalid instrument catalog: {error}"
                ))
            })?;

        if catalog.version != INSTRUMENT_CATALOG_VERSION {
            return invalid("unsupported instrument catalog version");
        }

        let mut instruments = HashMap::new();

        for metadata in catalog.instruments {
            Currency::new(metadata.base_currency.as_str())?;
            Currency::new(metadata.quote_currency.as_str())?;

            if metadata.base_currency == metadata.quote_currency {
                return invalid("instrument base and quote currencies must differ");
            }

            if metadata.identity.exchange.trim().is_empty()
                || metadata.identity.symbol.trim().is_empty()
            {
                return invalid("instrument identity contains empty fields");
            }

            if instruments
                .insert(metadata.identity.clone(), metadata)
                .is_some()
            {
                return invalid("duplicate instrument identity");
            }
        }

        Ok(Self { instruments })
    }

    pub fn resolve(&self, identity: &InstrumentIdentity) -> Option<&InstrumentMetadata> {
        self.instruments.get(identity)
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
