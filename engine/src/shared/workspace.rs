//! Workspace discovery for executable defaults.

use std::{
    env, io,
    path::{Path, PathBuf},
};

/// Resolve a caller-supplied root, the Forge environment setting, or the
/// enclosing checkout. Standalone invocations retain their working directory.
pub fn resolve(explicit: Option<&Path>, invocation: &Path) -> io::Result<PathBuf> {
    let root = if let Some(path) = explicit {
        invocation.join(path).canonicalize()?
    } else if let Some(path) = env::var_os("MARKETFORGE_PROJECT_ROOT") {
        if path.to_string_lossy().trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MARKETFORGE_PROJECT_ROOT must not be empty",
            ));
        }
        let root = invocation.join(path).canonicalize()?;
        if !is_workspace(&root) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid MarketForge workspace: {}", root.display()),
            ));
        }
        root
    } else {
        let current = invocation.canonicalize()?;
        current
            .ancestors()
            .find(|candidate| is_workspace(candidate))
            .unwrap_or(&current)
            .to_path_buf()
    };
    if !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("project root must be a directory: {}", root.display()),
        ));
    }
    Ok(root)
}

fn is_workspace(path: &Path) -> bool {
    path.join("forge/pyproject.toml").is_file() && path.join("engine").is_dir()
}
