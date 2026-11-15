// #![cfg(feature = "historical")]

use std::collections::BTreeMap;

use {
    marketforge_engine::historical::job::WorkTask,
    marketforge_engine::historical::normalize::worker::TradeSink,
    marketforge_engine::historical::normalize::worker::process_trade_task,
    marketforge_engine::shared::domain::canonical::Trade,
    marketforge_engine::shared::error::Result,
};

// -----------------------------------------------------------------------------
// Counting sink
// -----------------------------------------------------------------------------

#[derive(Default)]
struct CountingTradeSink {
    count: u64,
    first_timestamp_ns: Option<i64>,
    last_timestamp_ns: Option<i64>,
}

impl TradeSink for CountingTradeSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        self.count += 1;

        let timestamp = trade.envelope.event_timestamp_ns;

        if self.first_timestamp_ns.is_none() {
            self.first_timestamp_ns = Some(timestamp);
        }

        self.last_timestamp_ns = Some(timestamp);

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Job discovery
// -----------------------------------------------------------------------------

fn discover_trade_tasks() -> BTreeMap<String, WorkTask> {
    super::trade_real_archives::discover_trade_tasks()
}

// -----------------------------------------------------------------------------
// Complete archive integration test
// -----------------------------------------------------------------------------

#[test]
fn processes_complete_trade_archives() {
    let tasks = discover_trade_tasks();
    let expected_configurations = tasks.len();

    println!("\n{}", "=".repeat(110));
    println!("MARKETFORGE — COMPLETE TRADE ARCHIVE PROCESSING");
    println!("{}", "=".repeat(110));

    println!("Configurations discovered : {}", tasks.len());
    println!("Expected configurations   : {expected_configurations}");

    assert_eq!(
        tasks.len(),
        expected_configurations,
        "unexpected number of trade configurations"
    );

    let mut successful = 0usize;
    let mut failures = Vec::new();
    let mut total_trades = 0u64;

    for (key, task) in tasks {
        println!("\n{}", "-".repeat(110));
        println!("CONFIGURATION: {key}");
        println!("SOURCE: {}", task.input_path.display());

        let mut sink = CountingTradeSink::default();

        match process_trade_task(&task, &mut sink) {
            Ok(metrics) => {
                if let Err(error) = sink.finish() {
                    println!("RESULT: FAIL — sink error: {error}");

                    failures.push((key, error.to_string()));

                    continue;
                }

                // -----------------------------------------------------
                // Verify worker metrics
                // -----------------------------------------------------

                assert_eq!(
                    metrics.trades_written, sink.count,
                    "worker and sink counts differ"
                );

                assert_eq!(
                    metrics.trades_normalized, sink.count,
                    "normalized trade count differs"
                );

                assert_eq!(
                    metrics.records_matched, sink.count,
                    "matched record count differs"
                );

                assert_eq!(
                    metrics.records_read,
                    metrics.records_matched + metrics.records_skipped_instrument,
                    "identity metrics are inconsistent"
                );

                // -----------------------------------------------------
                // Verify timestamp boundaries
                // -----------------------------------------------------

                if sink.count > 0 {
                    assert!(
                        metrics.start_timestamp_ns.is_some(),
                        "missing minimum timestamp"
                    );

                    assert!(
                        metrics.end_timestamp_ns.is_some(),
                        "missing maximum timestamp"
                    );

                    assert!(
                        metrics.start_timestamp_ns <= metrics.end_timestamp_ns,
                        "invalid timestamp boundaries"
                    );

                    let first = sink.first_timestamp_ns.expect("first timestamp");

                    let last = sink.last_timestamp_ns.expect("last timestamp");

                    let minimum = metrics.start_timestamp_ns.expect("minimum timestamp");

                    let maximum = metrics.end_timestamp_ns.expect("maximum timestamp");

                    assert!(
                        minimum <= first && first <= maximum,
                        "first trade outside timestamp boundaries"
                    );

                    assert!(
                        minimum <= last && last <= maximum,
                        "last trade outside timestamp boundaries"
                    );
                } else {
                    assert!(
                        metrics.start_timestamp_ns.is_none(),
                        "empty output has minimum timestamp"
                    );

                    assert!(
                        metrics.end_timestamp_ns.is_none(),
                        "empty output has maximum timestamp"
                    );
                }

                // -----------------------------------------------------
                // Print processing statistics
                // -----------------------------------------------------

                println!("\nRESULT: PASS");

                println!("  Records read      : {}", metrics.records_read);

                println!("  Records matched   : {}", metrics.records_matched);

                println!(
                    "  Records skipped   : {}",
                    metrics.records_skipped_instrument
                );

                println!("  Trades normalized : {}", metrics.trades_normalized);

                println!("  Trades written    : {}", metrics.trades_written);

                println!("  First timestamp   : {:?}", sink.first_timestamp_ns);

                println!("  Last timestamp    : {:?}", sink.last_timestamp_ns);

                println!("  Minimum timestamp : {:?}", metrics.start_timestamp_ns);

                println!("  Maximum timestamp : {:?}", metrics.end_timestamp_ns);

                total_trades += sink.count;
                successful += 1;
            }

            Err(error) => {
                println!("\nRESULT: FAIL — {error}");

                failures.push((key, error.to_string()));
            }
        }
    }

    // -----------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------

    println!("\n{}", "=".repeat(110));
    println!("SUMMARY");
    println!("{}", "=".repeat(110));

    println!("Successful configurations : {successful}");

    println!("Failed configurations     : {}", failures.len());

    println!(
        "Total configurations      : {}",
        successful + failures.len()
    );

    println!("Total trades processed    : {total_trades}");

    println!("{}", "=".repeat(110));

    if !failures.is_empty() {
        println!("\nFAILED CONFIGURATIONS");
        println!("{}", "-".repeat(110));

        for (key, error) in &failures {
            println!("{key}");
            println!("  {error}");
        }
    }

    // -----------------------------------------------------------------
    // Final assertions
    // -----------------------------------------------------------------

    assert!(
        failures.is_empty(),
        "{} trade configurations failed",
        failures.len()
    );

    assert_eq!(
        successful, expected_configurations,
        "not all trade configurations passed"
    );

    assert!(total_trades > 0, "no trades were processed");
}
