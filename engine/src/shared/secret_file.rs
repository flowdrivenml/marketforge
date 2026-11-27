//! Read a bounded credential file without following a final-component symlink.
use std::{
    fs::OpenOptions,
    io::{self, Read},
    path::Path,
};

pub(crate) fn read(path: &Path, max_bytes: usize) -> io::Result<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "credential file must be a bounded private regular file owned by the running user",
        )
    };
    let file = options.open(path).map_err(|error| {
        #[cfg(unix)]
        if error.raw_os_error() == Some(libc::ELOOP) {
            return invalid();
        }
        error
    })?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes as u64 {
        return Err(invalid());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // geteuid takes no arguments and has no side effects.
        if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(invalid());
        }
    }
    let mut bytes = Vec::new();
    file.take(max_bytes as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(invalid());
    }
    String::from_utf8(bytes).map_err(|_| invalid())
}
