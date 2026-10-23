use serde::{Deserialize, Serialize};

// ============================================================
// TUI SCREENS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TuiScreen {
    Dashboard,
    SourceStreams,
    DerivedStreams,
    Integrity,
    Publication,
    Recording,
    Settings,
}

impl TuiScreen {
    pub const ALL: [Self; 7] = [
        Self::Dashboard,
        Self::SourceStreams,
        Self::DerivedStreams,
        Self::Integrity,
        Self::Publication,
        Self::Recording,
        Self::Settings,
    ];

    pub const fn title(self) -> &'static str {
        match self {
            Self::Dashboard => "Dashboard",
            Self::SourceStreams => "Source Streams",
            Self::DerivedStreams => "Derived Streams",
            Self::Integrity => "Integrity",
            Self::Publication => "Publication",
            Self::Recording => "Recording",
            Self::Settings => "Settings",
        }
    }
}

// ============================================================
// TUI VIEWS
// ============================================================

/// Detailed views displayed within the main screens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiView {
    Overview,

    SourceDetails { stream_id: String },

    DerivedDetails { stream_id: String },

    DerivedInputs { stream_id: String },

    GapHistory { stream_id: String },

    ConnectionDetails { connection_id: String },
}

// ============================================================
// READ OPERATIONS
// ============================================================

/// Information the TUI can request from the REST API.
///
/// These are logical operations, not HTTP endpoints.
/// The future REST client maps them to actual requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiQuery {
    RuntimeStatus,
    MetricsSnapshot,

    ListSourceStreams,
    ListDerivedStreams,

    StreamDetails {
        stream_id: String,
    },

    DerivedInputs {
        stream_id: String,
    },

    StreamIntegrity {
        stream_id: String,
    },

    GapHistory {
        stream_id: String,
        window: crate::live::integrity::IntegrityWindow,
    },

    PublicationStatus,
    PublicationConnections,

    ConnectionDetails {
        connection_id: String,
    },

    RecordingStatus,
    RuntimeConfiguration,
}

// ============================================================
// MANAGEMENT ACTIONS
// ============================================================

/// Actions accessible through the TUI.
///
/// Stream creation uses the existing request models.
/// Runtime configuration uses the existing control models.
///
/// No duplicate management schemas are introduced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiAction {
    CreateSourceStream(crate::live::stream::request::CreateSourceStreamRequest),

    CreateDerivedStream(crate::live::stream::request::CreateDerivedStreamRequest),

    RemoveStream { stream_id: String },

    ManageStream(crate::live::control::StreamActionRequest),

    UpdatePublication(crate::live::control::UpdatePublicationRequest),

    UpdateRecording(crate::live::control::UpdateRecordingRequest),

    UpdateLogging(crate::live::control::UpdateLoggingRequest),

    Shutdown,
}

// ============================================================
// REFRESH POLICY
// ============================================================

/// Which information should be refreshed.
///
/// Metrics refresh periodically.
/// Detailed records can be loaded on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshTarget {
    Dashboard,
    Streams,
    Integrity,
    Publication,
    Recording,
    Settings,
}

// ============================================================
// USER CONFIRMATION
// ============================================================

/// Operations requiring explicit user confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmationAction {
    RemoveStream { stream_id: String },

    Shutdown,
}

// ============================================================
// CONNECTION STATE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Error,
}
