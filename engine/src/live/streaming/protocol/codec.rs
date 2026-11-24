//! CBOR (RFC 8949), definite lengths only, maps with text field names.
use super::message::Envelope;
use std::io::{self, Write};
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("frame exceeds configured limit")]
    Oversized,
    #[error("invalid or unbounded CBOR payload")]
    Malformed,
    #[error("unsupported mandatory protocol or schema version")]
    UnsupportedVersion,
    #[error("EOF inside frame")]
    Truncated,
}
/// Preflight lengths and nesting before serde can allocate decoded collections.
fn item(bytes: &[u8], p: &mut usize, depth: usize, total: &mut usize) -> Result<(), ProtocolError> {
    if depth > 24 || *total >= 65536 {
        return Err(ProtocolError::Malformed);
    }
    *total += 1;
    let b = *bytes.get(*p).ok_or(ProtocolError::Malformed)?;
    *p += 1;
    let major = b >> 5;
    let ai = b & 31;
    let n = match ai {
        0..=23 => ai as u64,
        24..=27 => {
            let len = 1usize << (ai - 24);
            let end = p.checked_add(len).ok_or(ProtocolError::Malformed)?;
            let data = bytes.get(*p..end).ok_or(ProtocolError::Malformed)?;
            *p = end;
            data.iter().fold(0u64, |n, b| (n << 8) | *b as u64)
        }
        _ => return Err(ProtocolError::Malformed),
    };
    match major {
        0 | 1 => (),
        2 | 3 => {
            if n > 4096 {
                return Err(ProtocolError::Malformed);
            }
            let end = p.checked_add(n as usize).ok_or(ProtocolError::Malformed)?;
            let s = bytes.get(*p..end).ok_or(ProtocolError::Malformed)?;
            if major == 3 {
                std::str::from_utf8(s).map_err(|_| ProtocolError::Malformed)?;
            }
            *p = end;
        }
        4 | 5 => {
            if n > if major == 4 { 16384 } else { 64 } {
                return Err(ProtocolError::Malformed);
            }
            for _ in 0..n * (if major == 5 { 2 } else { 1 }) {
                item(bytes, p, depth + 1, total)?;
            }
        }
        7 if matches!(ai, 20..=22) => (),
        _ => return Err(ProtocolError::Malformed),
    }
    Ok(())
}
struct Limited(Vec<u8>, usize);
impl Write for Limited {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.len() > self.1.saturating_sub(self.0.len()) {
            return Err(io::Error::other("frame limit"));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn encode(envelope: &Envelope, max: usize) -> Result<Vec<u8>, ProtocolError> {
    envelope.validate()?;
    let mut out = Limited(Vec::new(), max.min(16 * 1024 * 1024));
    ciborium::ser::into_writer(envelope, &mut out).map_err(|_| ProtocolError::Oversized)?;
    preflight(&out.0)?;
    Ok(out.0)
}
fn preflight(bytes: &[u8]) -> Result<(), ProtocolError> {
    let mut p = 0;
    item(bytes, &mut p, 0, &mut 0)?;
    if p != bytes.len() {
        return Err(ProtocolError::Malformed);
    }
    Ok(())
}
pub fn decode(bytes: &[u8], max: usize) -> Result<Envelope, ProtocolError> {
    if bytes.len() > max.min(16 * 1024 * 1024) {
        return Err(ProtocolError::Oversized);
    }
    preflight(bytes)?;
    let result: Envelope =
        ciborium::de::from_reader(bytes).map_err(|_| ProtocolError::Malformed)?;
    result.validate()?;
    Ok(result)
}
#[cfg(any(feature = "live", feature = "stream-client"))]
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
) -> Result<Option<Envelope>, ProtocolError> {
    use tokio::io::AsyncReadExt;
    let mut prefix = [0; 4];
    if r.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    read_remainder(r, max, prefix).await
}
/// Idle sessions may wait indefinitely; the deadline starts with the first byte.
#[cfg(any(feature = "live", feature = "stream-client"))]
pub async fn read_frame_with_deadline<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
    deadline: std::time::Duration,
) -> Result<Option<Envelope>, ProtocolError> {
    use tokio::io::AsyncReadExt;
    let mut prefix = [0; 4];
    if r.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    tokio::time::timeout(deadline, read_remainder(r, max, prefix))
        .await
        .map_err(|_| {
            ProtocolError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "partial frame read deadline",
            ))
        })?
}
#[cfg(any(feature = "live", feature = "stream-client"))]
async fn read_remainder<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
    mut prefix: [u8; 4],
) -> Result<Option<Envelope>, ProtocolError> {
    use tokio::io::AsyncReadExt;
    r.read_exact(&mut prefix[1..]).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            ProtocolError::Truncated
        } else {
            e.into()
        }
    })?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 || len > max.min(16 * 1024 * 1024) {
        return Err(ProtocolError::Oversized);
    }
    let mut bytes = vec![0; len];
    r.read_exact(&mut bytes).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            ProtocolError::Truncated
        } else {
            e.into()
        }
    })?;
    decode(&bytes, max).map(Some)
}
#[cfg(any(feature = "live", feature = "stream-client"))]
pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    envelope: &Envelope,
    max: usize,
) -> Result<(), ProtocolError> {
    use tokio::io::AsyncWriteExt;
    let bytes = encode(envelope, max)?;
    let len = u32::try_from(bytes.len()).map_err(|_| ProtocolError::Oversized)?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    Ok(())
}
