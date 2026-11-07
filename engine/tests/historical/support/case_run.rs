//! Persistent output guard for direct execution tests. Records outcomes, never cleans up.
pub struct CaseRun(super::support::runtime::TestRun);
impl CaseRun {
    pub fn new(name: &str) -> Result<Self, String> {
        super::support::runtime::TestRun::new(name).map(Self)
    }
}
impl std::ops::Deref for CaseRun {
    type Target = super::support::runtime::TestRun;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for CaseRun {
    fn drop(&mut self) {
        let result = if std::thread::panicking() {
            Err("test panicked; inspect test diagnostics and retained artifacts".into())
        } else {
            Ok(())
        };
        if let Err(error) = self.0.finish(&result) {
            if std::thread::panicking() {
                eprintln!("failed to record test outcome: {error}");
            } else {
                panic!("failed to record test outcome: {error}");
            }
        }
    }
}
