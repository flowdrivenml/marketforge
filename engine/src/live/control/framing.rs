//! V1 transport framing: a four-byte big-endian JSON byte length. A partial
//! frame never becomes a request. Owners bound buffered bytes before reading.
use crate::live::model::error::ErrorCode;
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    pub max_bytes: u32,
    pub max_items: usize,
    pub max_string_bytes: usize,
}
impl FrameLimits {
    pub fn validate(self) -> Result<Self, ErrorCode> {
        if self.max_bytes == 0 || self.max_items == 0 || self.max_string_bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(self)
    }
}
pub fn frame(payload: &[u8], limits: FrameLimits) -> Result<Vec<u8>, ErrorCode> {
    limits.validate()?;
    let n = u32::try_from(payload.len()).map_err(|_| ErrorCode::Limit)?;
    if n == 0 || n > limits.max_bytes {
        return Err(ErrorCode::Limit);
    }
    let mut out = Vec::with_capacity(payload.len().checked_add(4).ok_or(ErrorCode::Limit)?);
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}
/// Returns one frame and the bytes consumed; trailing frames remain caller-owned.
pub fn unframe(input: &[u8], limits: FrameLimits) -> Result<Option<(&[u8], usize)>, ErrorCode> {
    limits.validate()?;
    let Some(header) = input.get(..4) else {
        return Ok(None);
    };
    let n = u32::from_be_bytes(header.try_into().map_err(|_| ErrorCode::Malformed)?);
    if n == 0 || n > limits.max_bytes {
        return Err(ErrorCode::Limit);
    }
    let end = usize::try_from(n)
        .map_err(|_| ErrorCode::Limit)?
        .checked_add(4)
        .ok_or(ErrorCode::Limit)?;
    Ok(input.get(4..end).map(|payload| (payload, end)))
}
