#![cfg(feature = "historical")]

use std::path::{Path, PathBuf};

use crate::support::migration::configuration as load_processing_config;
use marketforge_engine::historical::normalize::metrics::ScopedIntegrityMetrics;
use {
    marketforge_engine::historical::job::ProcessingJob,
    marketforge_engine::historical::normalize::metrics::TaskMetrics,
    marketforge_engine::historical::normalize::worker::TradeSink,
    marketforge_engine::historical::normalize::worker::process_trade_task_with_metrics,
    marketforge_engine::shared::domain::canonical::Trade,
    marketforge_engine::shared::error::Result,
};

fn load_policy() -> marketforge_engine::historical::job::IntegrityPolicy {
    let root = project_root();

    load_processing_config(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/shared/historical/config/fixtures/runtime.toml"),
        &root,
    )
    .expect("load processing configuration")
    .integrity_policy
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load_job() -> ProcessingJob {
    let mut job = crate::support::migration::template("bybit.spot.spot.trades:BTCUSDT").unwrap();
    let dir = crate::support::migration::persistent_path("trades_sink_failure".into());
    let path = dir.join("input.csv");
    let mut text = String::from("id,timestamp,price,volume,side,rpi\n");
    for id in 1..=10 {
        text.push_str(&format!("{id},1250,200,3,buy,0\n"));
    }
    std::fs::write(&path, text).unwrap();
    job.tasks.truncate(1);
    job.tasks[0].input_path = path;
    job.tasks[0].source_compression =
        marketforge_engine::shared::io::container::SourceContainer::Plain;
    job
}

#[test]
fn retains_metrics_after_sink_failure() {
    struct FailingSink {
        writes: u64,
    }

    impl TradeSink for FailingSink {
        fn write_trade(&mut self, _trade: Trade) -> Result<()> {
            self.writes += 1;

            if self.writes == 10 {
                return Err(
                    marketforge_engine::shared::error::MarketForgeError::InvalidConfiguration(
                        "intentional sink failure".to_owned(),
                    ),
                );
            }

            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    let job = load_job();
    let task = &job.tasks[0];

    let mut sink = FailingSink { writes: 0 };
    let mut metrics = TaskMetrics::default();

    let result = process_trade_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &load_policy(),
        &ScopedIntegrityMetrics::default(),
    );

    assert!(result.is_err());

    assert_eq!(metrics.counters.tasks_failed, 1);
    assert_eq!(metrics.counters.tasks_completed, 0);

    assert!(metrics.counters.records_read >= 10);
    assert!(metrics.counters.events_written > 0);
    assert_eq!(metrics.counters.events_written, 9);
}
