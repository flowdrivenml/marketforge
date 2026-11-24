use super::{DataIntegritySnapshot, DataIntegrityUpdate, market::StreamPublication};
use serde::{Deserialize, Serialize};
pub const VERSION: u16 = 1;
pub const SCHEMA: u16 = 1;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescription {
    pub stream_id: String,
    pub semantics: String,
    pub dependencies: Vec<String>,
    pub schemas: Vec<u16>,
    pub source: Option<SourceDescription>,
    pub operation: Option<String>,
    pub reference_inputs: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDescription {
    pub exchange: String,
    pub instrument_id: i64,
    pub symbol: String,
    pub market_category: String,
    pub instrument_type: String,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    pub after: Option<String>,
    pub limit: u16,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPage {
    pub streams: Vec<StreamDescription>,
    pub next: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionEnd {
    pub reason: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum StreamMessage {
    Publication(StreamPublication),
    IntegritySnapshot(DataIntegritySnapshot),
    IntegrityUpdate(DataIntegrityUpdate),
    End(SubscriptionEnd),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum Message {
    Hello {
        versions: Vec<u16>,
        schemas: Vec<u16>,
    },
    Welcome {
        version: u16,
        schemas: Vec<u16>,
        capabilities: Vec<String>,
    },
    ListStreams(PageRequest),
    Streams(StreamPage),
    DescribeStream {
        stream_id: String,
    },
    Description(StreamDescription),
    Subscribe {
        stream_id: String,
        subscription_id: u64,
    },
    Subscribed {
        subscription_id: u64,
    },
    Unsubscribe {
        subscription_id: u64,
    },
    Unsubscribed {
        subscription_id: u64,
    },
    Ping,
    Pong,
    Stream(StreamMessage),
    Error {
        code: String,
        detail: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub protocol_version: u16,
    pub schema_id: u16,
    pub schema_version: u16,
    pub request_id: u64,
    pub subscription_id: u64,
    pub subscription_sequence: u64,
    pub message: Message,
}
impl Envelope {
    pub fn control(id: u64, message: Message) -> Self {
        Self {
            protocol_version: VERSION,
            schema_id: SCHEMA,
            schema_version: 1,
            request_id: id,
            subscription_id: 0,
            subscription_sequence: 0,
            message,
        }
    }
    pub fn validate(&self) -> Result<(), super::codec::ProtocolError> {
        if self.protocol_version != VERSION || self.schema_id != SCHEMA || self.schema_version != 1
        {
            return Err(super::codec::ProtocolError::UnsupportedVersion);
        }
        let addressing = match &self.message {
            Message::Stream(_) => {
                self.request_id == 0 && self.subscription_id > 0 && self.subscription_sequence > 0
            }
            Message::Error { .. } => self.subscription_id == 0 && self.subscription_sequence == 0,
            _ => {
                self.request_id > 0 && self.subscription_id == 0 && self.subscription_sequence == 0
            }
        };
        if !addressing {
            return Err(super::codec::ProtocolError::Malformed);
        }
        super::validation::validate(&self.message)
    }
}
