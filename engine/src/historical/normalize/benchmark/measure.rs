use super::model::ProcessMetrics;
#[derive(Debug, Default)]
pub struct Snapshot {
    pub user: Option<f64>,
    pub system: Option<f64>,
    pub peak_rss_bytes: Option<u64>,
    pub logical_read: Option<u64>,
    pub logical_write: Option<u64>,
    pub physical_read: Option<u64>,
    pub physical_write: Option<u64>,
}
pub fn rss_bytes(raw: i64) -> Option<u64> {
    let value = u64::try_from(raw).ok()?;
    #[cfg(target_os = "macos")]
    return Some(value);
    #[cfg(not(target_os = "macos"))]
    value.checked_mul(1024)
}
pub fn parse_linux_io(text: &str) -> Snapshot {
    let get = |key: &str| {
        text.lines().find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| *k == key)
                .and_then(|(_, v)| v.trim().parse().ok())
        })
    };
    Snapshot {
        logical_read: get("rchar"),
        logical_write: get("wchar"),
        physical_read: get("read_bytes"),
        physical_write: get("write_bytes"),
        ..Default::default()
    }
}
pub fn snapshot() -> Snapshot {
    #[cfg(target_os = "linux")]
    let mut s = std::fs::read_to_string("/proc/self/io")
        .ok()
        .map(|t| parse_linux_io(&t))
        .unwrap_or_default();
    #[cfg(not(target_os = "linux"))]
    let mut s = Snapshot::default();
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // SAFETY: getrusage writes a correctly sized initialized rusage object. No
        // pointers escape, and the return status is checked before values are used.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0 {
            s.user = Some(usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6);
            s.system = Some(usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6);
            s.peak_rss_bytes = rss_bytes(usage.ru_maxrss);
        }
    }
    s
}
pub fn difference(before: &Snapshot, after: &Snapshot) -> ProcessMetrics {
    let delta = |a: Option<u64>, b: Option<u64>| b.zip(a).and_then(|(b, a)| b.checked_sub(a));
    let user = after
        .user
        .zip(before.user)
        .and_then(|(b, a)| (b >= a).then_some(b - a));
    let system = after
        .system
        .zip(before.system)
        .and_then(|(b, a)| (b >= a).then_some(b - a));
    ProcessMetrics {
        process_cpu_user_seconds: user,
        process_cpu_system_seconds: system,
        process_cpu_total_seconds: user.zip(system).map(|(u, s)| u + s),
        peak_rss_bytes: after.peak_rss_bytes,
        logical_read_bytes: delta(before.logical_read, after.logical_read),
        logical_written_bytes: delta(before.logical_write, after.logical_write),
        physical_read_bytes: delta(before.physical_read, after.physical_read),
        physical_written_bytes: delta(before.physical_write, after.physical_write),
        peak_accounted_memory_bytes: None,
    }
}
pub fn rate(work: u64, seconds: f64, success: bool) -> Option<f64> {
    if success && seconds.is_finite() && seconds > 0.0 {
        Some(work as f64 / seconds)
    } else {
        None
    }
}
