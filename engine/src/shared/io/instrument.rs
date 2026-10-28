use crate::shared::{
    domain::instrument::{InstrumentCatalog, InstrumentCatalogFile},
    error::{MarketForgeError, Result},
};
use std::{fs::File, io::BufReader, path::Path};

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

        Self::from_file(catalog)
    }
}
