use std::{fs, path::Path, process::Command};

pub(crate) const GENERAL: &str = include_str!("data/config/general.toml");

pub(crate) fn command(root: &Path) -> Command {
    let invocation = root.join("engine");
    fs::create_dir_all(&invocation).unwrap();
    fs::create_dir_all(root.join("forge")).unwrap();
    fs::write(root.join("forge/pyproject.toml"), "").unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_marketforge-live"));
    command
        .env_clear()
        .env("MARKETFORGE_PROJECT_ROOT", root)
        .current_dir(invocation);
    command
}
