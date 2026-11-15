//! Route normalization diagnostics away from the terminal while a dashboard owns it.
use std::{
    fmt,
    fs::File,
    io::{self, Write},
    path::PathBuf,
    sync::Mutex,
};
static DESTINATION: Mutex<Option<File>> = Mutex::new(None);
pub fn diagnostic(args: fmt::Arguments<'_>) {
    let mut destination = DESTINATION.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(file) = destination.as_mut() {
        let _ = writeln!(file, "{args}");
    } else {
        eprintln!("{args}");
    }
}
pub struct DashboardLog {
    pub path: PathBuf,
}
impl DashboardLog {
    pub fn enter() -> io::Result<Self> {
        let mut destination = DESTINATION.lock().unwrap_or_else(|p| p.into_inner());
        if destination.is_some() {
            return Err(io::Error::other(
                "a normalization dashboard is already active",
            ));
        }
        let path = std::env::temp_dir().join(format!(
            "marketforge-normalize-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        *destination = Some(file);
        Ok(Self { path })
    }
}
impl Drop for DashboardLog {
    fn drop(&mut self) {
        DESTINATION.lock().unwrap_or_else(|p| p.into_inner()).take();
    }
}
