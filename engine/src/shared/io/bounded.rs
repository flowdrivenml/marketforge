//! Incremental buffering with an explicit EOF probe at the exact boundary.
use crate::shared::{error::ResourceLimitError, historical::limits};
use std::io::{self, Read};
pub fn read_bounded(
    mut reader: impl Read,
    maximum: u64,
    stage: &'static str,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let remaining = maximum
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("bounded read arithmetic underflow"))?;
        let count = if remaining == 0 {
            // A boundary is not EOF. Never append this probe byte.
            match reader.read(&mut chunk[..1]) {
                Ok(0) => return Ok(bytes),
                Ok(_) => {
                    return Err(io::Error::other(limits::error(
                        "buffer bytes",
                        maximum,
                        maximum
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("bounded read overflow"))?,
                        stage,
                    )));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        } else {
            remaining.min(chunk.len() as u64) as usize
        };
        match reader.read(&mut chunk[..count]) {
            Ok(0) => return Ok(bytes),
            Ok(n) => {
                bytes.try_reserve_exact(n).map_err(io::Error::other)?;
                bytes.extend_from_slice(&chunk[..n]);
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}
pub fn resource_error(error: &io::Error) -> Option<ResourceLimitError> {
    error
        .get_ref()?
        .downcast_ref::<ResourceLimitError>()
        .cloned()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_reads_distinguish_eof_and_boundary() {
        assert_eq!(read_bounded(&b"ab"[..], 3, "test").unwrap(), b"ab");
        assert_eq!(read_bounded(&b"abc"[..], 3, "test").unwrap(), b"abc");
        let error = read_bounded(&b"abcd"[..], 3, "test").unwrap_err();
        let limit = resource_error(&error).unwrap();
        assert_eq!(
            (limit.permitted, limit.requested, limit.stage),
            (3, 4, "test")
        );
    }
    #[test]
    fn boundary_probe_preserves_io_errors() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"))
            }
        }
        assert_eq!(
            read_bounded(Broken, 0, "test").unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}
