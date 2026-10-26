use std::{fs, path::Path};

use marketforge_engine::live::config::format::DepthFormatConfig;

// ============================================================
// TEST HELPERS
// ============================================================

fn depth_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/formats/depth"
    ))
}

fn load_depth(profile: &str) -> DepthFormatConfig {
    let path = depth_directory().join(format!("{profile}.toml"));

    DepthFormatConfig::load(&path)
        .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()))
}

// ============================================================
// LEVEL EXTRACTION
// ============================================================

// ============================================================
// SEQUENCING
// ============================================================

// ============================================================
// SYNCHRONIZATION
// ============================================================

// ============================================================
// BOOK STATE
// ============================================================

// ============================================================
// CLASSIFICATION
// ============================================================

// ============================================================
// PROTOCOL
// ============================================================

// ============================================================
// INTEGRITY
// ============================================================

// ============================================================
// RECOVERY
// ============================================================

// ============================================================
// OUTPUT
// ============================================================

// ============================================================
// SYMBOL TYPE
// ============================================================

// ============================================================
// REAL DEPTH FORMAT CONFIGURATIONS
// ============================================================

#[test]
fn load_all_real_depth_formats() {
    let mut tested = 0;

    for entry in fs::read_dir(depth_directory()).expect("Cannot read depth format directory") {
        let entry = entry.expect("Cannot read directory entry");
        let path = entry.path();

        if !path.is_file() || path.extension().is_none_or(|extension| extension != "toml") {
            continue;
        }

        let config = DepthFormatConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()));

        assert_eq!(config.schema_version, 1);
        config.validate().unwrap();

        println!("PASS: {}", config.qualified_id());

        tested += 1;
    }

    assert_eq!(tested, 15, "Expected 15 depth formats");
}

#[test]
fn depth_format_qualified_identity() {
    let config = load_depth("binance_spot");

    assert_eq!(config.qualified_id(), "depth/binance_spot");
}

// ============================================================
// LEVEL VALIDATION
// ============================================================

// ============================================================
// BOOK VALIDATION
// ============================================================

// ============================================================
// OUTPUT VALIDATION
// ============================================================

// ============================================================
// STRICT DESERIALIZATION
// ============================================================
