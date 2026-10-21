//! Bounded v1 control codec shared by server, client and fixture consumers.
use super::framing::{self, FrameLimits};
use crate::live::model::{error::ErrorCode, protocol::*};
use serde::{Serialize, de::DeserializeOwned};
#[derive(Debug, Clone, Copy)]
pub struct ControlCodec {
    limits: FrameLimits,
}
impl ControlCodec {
    pub fn new(limits: FrameLimits) -> Result<Self, ErrorCode> {
        Ok(Self {
            limits: limits.validate()?,
        })
    }
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ErrorCode> {
        // Bound output while serializing; do not allocate an unbounded temporary.
        struct Writer {
            bytes: Vec<u8>,
            limit: usize,
        }
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                    return Err(std::io::ErrorKind::FileTooLarge.into());
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Writer {
            bytes: Vec::new(),
            limit: self.limits.max_bytes as usize,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| ErrorCode::Limit)?;
        self.check_json(&writer.bytes)?;
        framing::frame(&writer.bytes, self.limits)
    }
    fn check_json(&self, bytes: &[u8]) -> Result<(), ErrorCode> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| ErrorCode::Malformed)?;
        fn check(
            v: &serde_json::Value,
            limits: FrameLimits,
            depth: usize,
        ) -> Result<(), ErrorCode> {
            if depth > 32 {
                return Err(ErrorCode::Limit);
            }
            match v {
                serde_json::Value::String(s) if s.len() > limits.max_string_bytes => {
                    return Err(ErrorCode::Limit);
                }
                serde_json::Value::Array(a) => {
                    if a.len() > limits.max_items {
                        return Err(ErrorCode::Limit);
                    }
                    for v in a {
                        check(v, limits, depth + 1)?;
                    }
                }
                serde_json::Value::Object(o) => {
                    if o.len() > limits.max_items {
                        return Err(ErrorCode::Limit);
                    }
                    for (k, v) in o {
                        if k.len() > limits.max_string_bytes {
                            return Err(ErrorCode::Limit);
                        }
                        check(v, limits, depth + 1)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        check(&value, self.limits, 0)
    }
    fn decode<T: DeserializeOwned>(&self, input: &[u8]) -> Result<Option<(T, usize)>, ErrorCode> {
        let Some((payload, n)) = framing::unframe(input, self.limits)? else {
            return Ok(None);
        };
        self.check_json(payload)?;
        #[derive(serde::Deserialize)]
        struct Version {
            protocol_version: u16,
        }
        let version: Version = serde_json::from_slice(payload).map_err(|_| ErrorCode::Malformed)?;
        if version.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ErrorCode::UnsupportedVersion);
        }
        // Decode original bytes, preserving duplicate-field detection by serde.
        Ok(Some((
            serde_json::from_slice(payload).map_err(|_| ErrorCode::Malformed)?,
            n,
        )))
    }
    fn id(&self, id: &RequestId) -> Result<(), ErrorCode> {
        if id.client_instance.trim().is_empty() || id.client_instance.len() > 128 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(())
    }
    pub fn encode_request(&self, request: &ControlRequest) -> Result<Vec<u8>, ErrorCode> {
        self.validate_request(request)?;
        self.encode(request)
    }
    fn validate_request(&self, r: &ControlRequest) -> Result<(), ErrorCode> {
        if r.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ErrorCode::UnsupportedVersion);
        }
        self.id(&r.request_id)?;
        r.command.validate(self.limits.max_items)
    }
    pub fn decode_request(
        &self,
        bytes: &[u8],
    ) -> Result<Option<(ControlRequest, usize)>, ErrorCode> {
        let Some((r, n)) = self.decode(bytes)? else {
            return Ok(None);
        };
        self.validate_request(&r)?;
        Ok(Some((r, n)))
    }
    pub fn encode_response(&self, r: &ControlResponse) -> Result<Vec<u8>, ErrorCode> {
        if r.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ErrorCode::UnsupportedVersion);
        }
        self.id(&r.request_id)?;
        r.result.validate()?;
        self.encode(r)
    }
    pub fn decode_response(
        &self,
        bytes: &[u8],
        expected: &RequestId,
    ) -> Result<Option<(ControlResponse, usize)>, ErrorCode> {
        let Some((r, n)): Option<(ControlResponse, usize)> = self.decode(bytes)? else {
            return Ok(None);
        };
        if r.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ErrorCode::UnsupportedVersion);
        }
        self.id(&r.request_id)?;
        r.result.validate()?;
        if &r.request_id != expected {
            return Err(ErrorCode::IdempotencyConflict);
        }
        Ok(Some((r, n)))
    }
}
