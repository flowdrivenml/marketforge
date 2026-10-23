use super::model::{ConfirmationAction, TuiConnectionState, TuiScreen, TuiView};

// ============================================================
// TUI APPLICATION STATE
// ============================================================

#[derive(Debug)]
pub struct TuiApp {
    // --------------------------------------------------------
    // Navigation
    // --------------------------------------------------------
    pub screen: TuiScreen,
    pub view: TuiView,

    // --------------------------------------------------------
    // REST connectivity
    // --------------------------------------------------------
    pub connection_state: TuiConnectionState,

    // --------------------------------------------------------
    // User interaction
    // --------------------------------------------------------
    pub pending_confirmation: Option<ConfirmationAction>,

    // --------------------------------------------------------
    // Application lifecycle
    // --------------------------------------------------------
    pub should_quit: bool,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl Default for TuiApp {
    fn default() -> Self {
        Self::new()
    }
}

impl TuiApp {
    pub fn new() -> Self {
        Self {
            screen: TuiScreen::Dashboard,
            view: TuiView::Overview,

            connection_state: TuiConnectionState::Disconnected,

            pending_confirmation: None,

            should_quit: false,
        }
    }

    // ========================================================
    // NAVIGATION
    // ========================================================

    pub fn navigate(&mut self, screen: TuiScreen) {
        self.screen = screen;
        self.view = TuiView::Overview;
    }

    pub fn open_view(&mut self, view: TuiView) {
        self.view = view;
    }

    pub fn go_back(&mut self) {
        self.view = TuiView::Overview;
    }

    // ========================================================
    // CONFIRMATION
    // ========================================================

    pub fn request_confirmation(&mut self, action: ConfirmationAction) {
        self.pending_confirmation = Some(action);
    }

    pub fn cancel_confirmation(&mut self) {
        self.pending_confirmation = None;
    }

    pub fn take_confirmation(&mut self) -> Option<ConfirmationAction> {
        self.pending_confirmation.take()
    }

    // ========================================================
    // CONNECTION STATE
    // ========================================================

    pub fn set_connection_state(&mut self, state: TuiConnectionState) {
        self.connection_state = state;
    }

    // ========================================================
    // QUIT
    // ========================================================

    /// Quit the TUI application.
    ///
    /// This does NOT shut down MarketForge Live.
    pub fn quit(&mut self) {
        self.should_quit = true;
    }
}
