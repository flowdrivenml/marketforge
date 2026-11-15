use crossterm::{
    cursor::Show,
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use std::{
    io,
    sync::{
        Once,
        atomic::{AtomicBool, Ordering},
    },
};
static INSTALL: Once = Once::new();
static ACTIVE: AtomicBool = AtomicBool::new(false);
static PANICKED: AtomicBool = AtomicBool::new(false);
fn restore() {
    if ACTIVE.swap(false, Ordering::SeqCst) {
        let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}
/// Restores the terminal on normal exits, errors, and unwinding, before panic diagnostics.
pub struct TerminalGuard;
impl TerminalGuard {
    pub fn enter() -> io::Result<Self> {
        INSTALL.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                if ACTIVE.load(Ordering::SeqCst) {
                    PANICKED.store(true, Ordering::SeqCst);
                    restore();
                }
                previous(info);
            }));
        });
        if ACTIVE.swap(true, Ordering::SeqCst) {
            return Err(io::Error::other("terminal already owned by a dashboard"));
        }
        PANICKED.store(false, Ordering::SeqCst);
        if let Err(error) =
            enable_raw_mode().and_then(|()| execute!(io::stdout(), EnterAlternateScreen))
        {
            restore();
            return Err(error);
        }
        Ok(Self)
    }
    pub fn interrupted_by_panic() -> bool {
        PANICKED.load(Ordering::SeqCst)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore();
    }
}
