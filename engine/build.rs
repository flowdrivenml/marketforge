use std::{env, process::Command};
fn main() {
    for name in ["PROFILE", "TARGET", "RUSTC"] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let compiler = env::var_os("RUSTC")
        .and_then(|c| Command::new(c).arg("--version").output().ok())
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".into());
    println!("cargo:rustc-env=MARKETFORGE_BUILD_RUSTC={compiler}");
    for name in ["PROFILE", "TARGET"] {
        println!(
            "cargo:rustc-env=MARKETFORGE_BUILD_{name}={}",
            env::var(name).unwrap_or_else(|_| "unavailable".into())
        );
    }
    let mut features: Vec<_> = env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
                .map(|s| s.to_ascii_lowercase().replace('_', "-"))
        })
        .collect();
    features.sort();
    println!(
        "cargo:rustc-env=MARKETFORGE_BUILD_FEATURES={}",
        features.join(",")
    );
}
