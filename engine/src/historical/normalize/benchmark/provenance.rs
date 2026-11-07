use super::{Result, model::Plan};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    time::UNIX_EPOCH,
};
pub fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
pub fn file_sha256(path: &Path) -> Result<String> {
    let mut f = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut h = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = f.read(&mut bytes).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        h.update(&bytes[..n]);
    }
    Ok(hex(h.finalize()))
}
fn output(program: &str, args: &[&str], root: &Path) -> Option<String> {
    Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceObservation {
    pub path: PathBuf,
    pub size_bytes: Option<u64>,
    pub modified_unix_ns: Option<String>,
    pub sha256: Option<String>,
    pub evidence: String,
}
pub fn sources(plan: &Plan) -> Vec<SourceObservation> {
    let mut sources = BTreeMap::new();
    for job in &plan.trials[0].jobs {
        for t in &job.tasks {
            sources.entry(t.input_path.clone()).or_insert_with(|| {
                let m = fs::metadata(&t.input_path).ok();
                SourceObservation {
                    path: t.input_path.clone(),
                    size_bytes: m.as_ref().filter(|m| m.is_file()).map(|m| m.len()),
                    modified_unix_ns: m
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_nanos().to_string()),
                    sha256: None,
                    evidence: "path/size/mtime only; full source checksum not collected".into(),
                }
            });
        }
    }
    sources.into_values().collect()
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Environment {
    pub schema_version: u32,
    pub git_commit: Option<String>,
    pub git_branch: Option<String>,
    pub git_dirty: Option<bool>,
    pub rust_compiler: String,
    pub build_profile: String,
    pub build_target: String,
    pub cargo_features: Vec<String>,
    pub benchmark_executable: PathBuf,
    pub executable_sha256: Option<String>,
    pub normalization_executable_identity: String,
    pub cargo_lock_sha256: Option<String>,
    pub architecture: String,
    pub operating_system: String,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub logical_cpu_count: Option<usize>,
    pub physical_cpu_count: Option<usize>,
    pub available_parallelism: Option<usize>,
    pub total_memory_bytes: Option<u64>,
    pub cpu_frequency: Option<String>,
    pub cpu_power_profile: Option<String>,
    pub filesystem_type: Option<String>,
    pub storage_device_type: Option<String>,
    pub limitations: Vec<String>,
}
#[allow(unused_assignments)] // Defaults are required on platforms without the host-specific probes below.
pub fn environment(root: &Path) -> Environment {
    let exe = std::env::current_exe().unwrap_or_default();
    let mut cpu_model = None;
    let mut physical = None;
    let mut total_memory = None;
    let mut logical = None;
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = fs::read_to_string("/proc/cpuinfo") {
            cpu_model = text.lines().find_map(|l| {
                l.strip_prefix("model name")
                    .and_then(|l| l.split_once(':'))
                    .map(|(_, v)| v.trim().into())
            });
            logical = Some(
                text.lines()
                    .filter(|l| l.starts_with("processor\t"))
                    .count(),
            );
            let cores: std::collections::BTreeSet<_> = text
                .split("\n\n")
                .filter_map(|block| {
                    let value = |name: &str| {
                        block.lines().find_map(|l| {
                            l.split_once(':')
                                .filter(|(k, _)| k.trim() == name)
                                .map(|(_, v)| v.trim())
                        })
                    };
                    Some((value("physical id")?, value("core id")?))
                })
                .collect();
            if !cores.is_empty() {
                physical = Some(cores.len());
            }
        }
        total_memory = fs::read_to_string("/proc/meminfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|s| s.parse::<u64>().ok())
                .and_then(|v| v.checked_mul(1024))
        });
    }
    #[cfg(target_os = "macos")]
    {
        cpu_model = output("sysctl", &["-n", "machdep.cpu.brand_string"], root);
        physical = output("sysctl", &["-n", "hw.physicalcpu"], root).and_then(|s| s.parse().ok());
        logical = output("sysctl", &["-n", "hw.logicalcpu"], root).and_then(|s| s.parse().ok());
        total_memory = output("sysctl", &["-n", "hw.memsize"], root).and_then(|s| s.parse().ok());
    }
    Environment {
        schema_version: 1,
        git_commit: output("git", &["rev-parse", "HEAD"], root),
        git_branch: output("git", &["branch", "--show-current"], root),
        git_dirty: output("git", &["status", "--porcelain"], root).map(|s| !s.is_empty()),
        rust_compiler: env!("MARKETFORGE_BUILD_RUSTC").into(),
        build_profile: env!("MARKETFORGE_BUILD_PROFILE").into(),
        build_target: env!("MARKETFORGE_BUILD_TARGET").into(),
        cargo_features: env!("MARKETFORGE_BUILD_FEATURES")
            .split(',')
            .map(str::to_owned)
            .collect(),
        benchmark_executable: exe.clone(),
        executable_sha256: file_sha256(&exe).ok(),
        normalization_executable_identity:
            "production normalization library linked into isolated benchmark trial process".into(),
        cargo_lock_sha256: file_sha256(&root.join("engine/Cargo.lock")).ok(),
        architecture: std::env::consts::ARCH.into(),
        operating_system: std::env::consts::OS.into(),
        kernel: output("uname", &["-sr"], root),
        cpu_model,
        logical_cpu_count: logical,
        physical_cpu_count: physical,
        available_parallelism: std::thread::available_parallelism().ok().map(usize::from),
        total_memory_bytes: total_memory,
        cpu_frequency: None,
        cpu_power_profile: None,
        filesystem_type: None,
        storage_device_type: None,
        limitations: vec![
            "optional frequency/power/storage metadata unavailable; no environment-variable dump"
                .into(),
            "dirty source changes are not captured by Git SHA alone".into(),
        ],
    }
}
