use super::RuntimeMetrics;
use std::time::Instant;
#[derive(Default)]
pub(crate) struct ProcessSampler {
    previous: Option<(Instant, u64)>,
}
impl ProcessSampler {
    pub fn sample(&mut self, metrics: &RuntimeMetrics) {
        metrics.clear_process_observations();
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                if let Some(bytes) = status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|s| s.split_whitespace().next())
                        .and_then(|s| s.parse::<u64>().ok())
                        .and_then(|kb| kb.checked_mul(1024))
                }) {
                    metrics.set_memory_rss_bytes(bytes);
                }
            }
            if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
                if let Some((_, tail)) = stat.rsplit_once(") ") {
                    let fields: Vec<_> = tail.split_whitespace().collect();
                    let ticks = fields
                        .get(11)
                        .and_then(|s| s.parse::<u64>().ok())
                        .zip(fields.get(12).and_then(|s| s.parse::<u64>().ok()))
                        .and_then(|(a, b)| a.checked_add(b));
                    if let Some(ticks) = ticks {
                        let now = Instant::now();
                        // POSIX process constant, queried only at the periodic sample boundary.
                        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
                        if let Some((at, old)) = self.previous {
                            let elapsed = now.duration_since(at).as_secs_f64();
                            if hz > 0 && elapsed > 0.0 {
                                if let Some(delta) = ticks.checked_sub(old) {
                                    let _ = metrics.set_cpu_usage_percent(
                                        delta as f64 / hz as f64 / elapsed * 100.0,
                                    );
                                }
                            }
                        }
                        self.previous = Some((now, ticks));
                    }
                }
            }
        }
    }
}
