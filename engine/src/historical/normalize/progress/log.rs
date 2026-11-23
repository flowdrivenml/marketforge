//! Route normalization diagnostics away from the terminal while a dashboard owns it.
use std::{
    fmt,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
static DESTINATION: Mutex<Option<(File, Option<u64>)>> = Mutex::new(None);
pub fn diagnostic(args: fmt::Arguments<'_>) {
    let mut destination = DESTINATION.lock().unwrap_or_else(|p| p.into_inner());
    if let Some((file, remaining)) = destination.as_mut() {
        if let Some(remaining) = remaining {
            let _ = writeln!(BoundedLog { file, remaining }, "{args}");
        } else {
            let _ = writeln!(file, "{args}");
        }
    } else {
        eprintln!("{args}");
    }
}
pub struct DashboardLog {
    pub path: PathBuf,
}
impl DashboardLog {
    pub fn enter() -> io::Result<Self> {
        Self::temporary(None)
    }

    fn temporary(limit: Option<u64>) -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "marketforge-normalize-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        Self::open(path, limit)
    }

    pub fn enter_managed(path: &Path) -> io::Result<Self> {
        let limit = Some(16 * 1024 * 1024);
        Self::open(path.to_owned(), limit).or_else(|_| Self::temporary(limit))
    }

    fn open(path: PathBuf, limit: Option<u64>) -> io::Result<Self> {
        let mut destination = DESTINATION.lock().unwrap_or_else(|p| p.into_inner());
        if destination.is_some() {
            return Err(io::Error::other(
                "a normalization dashboard is already active",
            ));
        }
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        *destination = Some((file, limit));
        Ok(Self { path })
    }
}
impl Drop for DashboardLog {
    fn drop(&mut self) {
        DESTINATION.lock().unwrap_or_else(|p| p.into_inner()).take();
    }
}

struct BoundedLog<'a> {
    file: &'a mut File,
    remaining: &'a mut u64,
}

impl Write for BoundedLog<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let len = bytes.len().min(*self.remaining as usize);
        self.file.write_all(&bytes[..len])?;
        *self.remaining -= len as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
