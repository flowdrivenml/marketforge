use std::io;

// A raw terminal delivers Ctrl+C as a key. External signals also need to give
// the foreground loop a chance to restore the terminal before the process exits.
static TERMINAL_INTERRUPTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub(crate) struct InterruptSignals(Vec<(libc::c_int, libc::sighandler_t)>);

extern "C" fn request_interrupt(_: libc::c_int) {
    TERMINAL_INTERRUPTED.store(true, std::sync::atomic::Ordering::Relaxed);
}

impl InterruptSignals {
    pub(crate) fn requested() -> bool {
        TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn install() -> io::Result<Self> {
        TERMINAL_INTERRUPTED.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut installed = Self(Vec::new());
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            // The handler performs only a lock-free atomic store.
            let previous = unsafe {
                libc::signal(signal, request_interrupt as *const () as libc::sighandler_t)
            };
            if previous == libc::SIG_ERR {
                return Err(io::Error::last_os_error());
            }
            installed.0.push((signal, previous));
        }
        Ok(installed)
    }
}

impl Drop for InterruptSignals {
    fn drop(&mut self) {
        for (signal, handler) in self.0.drain(..) {
            // Restore the disposition captured when this scope entered.
            unsafe { libc::signal(signal, handler) };
        }
    }
}
