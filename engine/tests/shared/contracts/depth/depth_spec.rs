use marketforge_engine::{
    historical::config::FormatCatalog,
    shared::formats::depth::{DepthOperation, DepthSpec},
};
use std::collections::BTreeSet;
#[test]
fn compiles_all_depth_normalization_specs() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/engine/normalize");
    let catalog = FormatCatalog::load(root).unwrap();
    let mut kinds = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for profile in catalog.profiles().filter(|p| {
        p.definition.id.ends_with(".depth") && p.definition.unsupported_reason.is_none()
    }) {
        assert!(identities.insert(&profile.definition.id));
        assert!(!profile.normalizations.is_empty());
        for normalization in &profile.normalizations {
            let spec = DepthSpec::from_normalization(normalization).unwrap();
            kinds.insert(match spec.operation {
                DepthOperation::Snapshot(_) => "snapshot",
                DepthOperation::AbsoluteUpdate => "absolute",
                DepthOperation::RelativeUpdate(_) => "relative",
            });
        }
    }
    assert!(!identities.is_empty());
    assert_eq!(kinds, BTreeSet::from(["snapshot", "absolute", "relative"]));
}
