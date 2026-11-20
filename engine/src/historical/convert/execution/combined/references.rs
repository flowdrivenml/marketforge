use std::collections::HashSet;

use {
    crate::historical::convert::ConversionPlan, crate::historical::merge::PhysicalMergeManifest,
    crate::historical::merge::UnifiedMergeStreamKind, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

#[derive(Debug, Clone)]
pub struct CombinedReferenceSelection {
    pub internal_streams: HashSet<String>,
    pub external_streams: HashSet<String>,
}

impl CombinedReferenceSelection {
    pub fn resolve(
        plan: &ConversionPlan,
        source: &PhysicalMergeManifest,
        external: Option<&PhysicalMergeManifest>,
    ) -> Result<Self> {
        source.validate()?;

        if let Some(external) = external {
            external.validate()?;
        }

        let required = plan.references.iter().map(|reference| &reference.stream_id);
        let internal_available = source
            .streams
            .iter()
            .filter(|stream| stream.kind == UnifiedMergeStreamKind::Trade)
            .map(|stream| stream.stream_id.clone())
            .collect::<HashSet<_>>();

        let external_available = external
            .into_iter()
            .flat_map(|manifest| &manifest.streams)
            .filter(|stream| stream.kind == UnifiedMergeStreamKind::Trade)
            .map(|stream| stream.stream_id.clone())
            .collect::<HashSet<_>>();

        let mut internal_streams = HashSet::new();
        let mut external_streams = HashSet::new();

        for stream_id in required {
            if internal_available.contains(stream_id) {
                internal_streams.insert(stream_id.clone());
            } else if external_available.contains(stream_id) {
                external_streams.insert(stream_id.clone());
            } else {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "required reference stream unavailable: {stream_id}"
                )));
            }
        }

        Ok(Self {
            internal_streams,
            external_streams,
        })
    }
}
