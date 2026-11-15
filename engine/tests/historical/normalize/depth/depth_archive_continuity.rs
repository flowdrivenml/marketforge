#![cfg(feature = "historical")]

use std::{fs::File, path::Path};

use {
    marketforge_engine::historical::normalize::boundary::BoundaryContinuity,
    marketforge_engine::historical::normalize::boundary::DepthBoundaryManifest,
    marketforge_engine::historical::normalize::boundary::fingerprint_book,
    marketforge_engine::historical::normalize::metrics::ScopedIntegrityMetrics,
    marketforge_engine::historical::normalize::metrics::TaskMetrics,
    marketforge_engine::historical::normalize::worker::process_depth_task_with_metrics,
    marketforge_engine::shared::domain::book::SequencePolicy,
    marketforge_engine::shared::persistence::parquet::ParquetDepthWriter,
};

// -----------------------------------------------------------------------------
// Process one archive independently
// -----------------------------------------------------------------------------

fn process_archive(
    task: &marketforge_engine::historical::job::WorkTask,
    output_path: &Path,
    config: &marketforge_engine::historical::normalize::config::ProcessingConfig,
) -> DepthBoundaryManifest {
    let mut writer = ParquetDepthWriter::new(output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    let _limits = marketforge_engine::shared::historical::limits::LimitScope::enter(
        config.resources.normalization_limits,
        format!("Gate.io continuity test: {}", task.input_path.display()),
    );

    let sequence_policy = marketforge_engine::historical::normalize::resolve_sequence_policy(task)
        .expect("resolve Gate.io sequence policy");

    println!("Sequence policy: {sequence_policy:?}");

    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        sequence_policy,
    )
    .expect("process Gate.io archive");

    writer.finish().expect("finalize Parquet writer");

    assert_eq!(
        metrics.counters.records_read,
        metrics.counters.records_processed,
    );

    assert_eq!(metrics.counters.records_rejected, 0);

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(output_path.join("boundary.json")).expect("open boundary manifest"),
    )
    .expect("read boundary manifest");

    let sequence_policy = marketforge_engine::historical::normalize::resolve_sequence_policy(task)
        .expect("resolve sequence policy for continuity classification");

    match sequence_policy {
        SequencePolicy::Unsequenced => {
            assert_eq!(boundary.continuity, BoundaryContinuity::Unverifiable);
        }
        SequencePolicy::Consecutive | SequencePolicy::Ranged => {
            assert_eq!(boundary.continuity, BoundaryContinuity::Verified);
        }
    }

    boundary
}

// -----------------------------------------------------------------------------
// Compare two independently reconstructed archives
// -----------------------------------------------------------------------------

fn verify_archive_boundary(previous: &DepthBoundaryManifest, next: &DepthBoundaryManifest) {
    let previous_final = previous
        .final_state
        .as_ref()
        .expect("previous archive missing final book");

    let next_initial = next
        .initial
        .as_ref()
        .expect("next archive missing initial book");

    // -------------------------------------------------------------------------
    // Sequence continuity
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.sequence, next_initial.sequence,
        "cross-archive sequence mismatch",
    );

    // -------------------------------------------------------------------------
    // Timestamp continuity
    // -------------------------------------------------------------------------

    // Matching sequenced books must describe the shared temporal boundary.
    assert!(
        previous_final.timestamp_ns >= next_initial.timestamp_ns,
        "sequenced archives have a gap at their shared boundary"
    );

    // -------------------------------------------------------------------------
    // Exact reconstructed book equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.state, next_initial.state,
        "cross-archive order-book mismatch",
    );

    // -------------------------------------------------------------------------
    // SHA-256 equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.fingerprint, next_initial.fingerprint,
        "cross-archive fingerprint mismatch",
    );

    assert_eq!(
        previous_final.fingerprint,
        fingerprint_book(&previous_final.state),
    );

    assert_eq!(
        next_initial.fingerprint,
        fingerprint_book(&next_initial.state),
    );
}

fn validate_gateio_hourly_continuity(
    kind: marketforge_engine::historical::job::InstrumentKind,
    category: &str,
) {
    use crate::support::{fixtures::workload, runtime::TestRun};
    use marketforge_engine::{historical::job::ContentType, shared::domain::canonical::Exchange};

    let mut batch = workload().expect("load authoritative prepared fixture");
    batch.jobs.retain_mut(|job| {
        if job.output.content_type != ContentType::Depth {
            return false;
        }
        job.tasks.retain(|task| {
            task.exchange == Exchange::GateIo
                && task.instrument_type == kind
                && serde_json::to_value(task.market_category).unwrap().as_str() == Some(category)
                && task.scope.is_none()
        });
        !job.tasks.is_empty()
    });
    // Pick a concrete stream with multiple archives, including a single feed.
    let mut groups = std::collections::BTreeMap::new();
    for job in &batch.jobs {
        for task in &job.tasks {
            *groups
                .entry((job.job_id.0, task.stream_id.0.clone()))
                .or_insert(0usize) += 1;
        }
    }
    let ((job_id, stream_id), _) = groups
        .into_iter()
        .find(|(_, n)| *n >= 2)
        .expect("continuity coverage requires multiple archives of one Gate.io stream");
    batch.jobs.retain_mut(|job| {
        if job.job_id.0 != job_id {
            return false;
        }
        job.tasks.retain(|task| task.stream_id.0 == stream_id);
        // Preserve the original bounded continuity sample (at most three
        // sources), now selected from prepared metadata without date/path keys.
        job.tasks.truncate(3);
        true
    });

    let mut run =
        TestRun::new("execution/gateio_hourly_continuity").expect("create isolated test run");

    let config = run.runtime_from_general().expect("load general.toml");

    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve selected Gate.io archives");

    let job = &resolved.jobs[0];

    let mut boundaries = Vec::new();

    for (index, task) in job.tasks.iter().enumerate() {
        assert!(
            task.input_path.is_file(),
            "missing archive: {}",
            task.input_path.display()
        );

        let output_path = run.path.join(format!("archive-{index}"));

        let boundary = process_archive(task, &output_path, &config);

        println!("Archive {index}: {}", task.input_path.display());

        boundaries.push(boundary);
    }

    boundaries.sort_by_key(|boundary| {
        boundary
            .initial
            .as_ref()
            .expect("initial book")
            .timestamp_ns
    });

    for (index, boundary) in boundaries.iter().enumerate() {
        println!("=== ARCHIVE {index} BOUNDARY ===");
        println!("Continuity: {:?}", boundary.continuity);

        if let Some(initial) = &boundary.initial {
            println!(
                "Initial: sequence={:?}, timestamp={}, fingerprint={}",
                initial.sequence, initial.timestamp_ns, initial.fingerprint
            );
        }

        if let Some(final_state) = &boundary.final_state {
            println!(
                "Final: sequence={:?}, timestamp={}, fingerprint={}",
                final_state.sequence, final_state.timestamp_ns, final_state.fingerprint
            );
        }
    }

    for index in 0..boundaries.len() - 1 {
        let previous = &boundaries[index];
        let next = &boundaries[index + 1];

        let previous_final = previous
            .final_state
            .as_ref()
            .expect("previous archive missing final book");

        let next_initial = next
            .initial
            .as_ref()
            .expect("next archive missing initial book");

        if kind == marketforge_engine::historical::job::InstrumentKind::Spot {
            // Verify the shared sequenced boundary, including the complete book.
            verify_archive_boundary(previous, next);

            println!(
                "Archive {} -> {}: VERIFIED sequence/book/fingerprint continuity",
                index,
                index + 1,
            );
        } else {
            // These Gate.io perpetual feeds have no sequence IDs.
            // Their independent archive boundaries must not be
            // represented as verified continuous books.
            assert_eq!(previous.continuity, BoundaryContinuity::Unverifiable);
            assert_eq!(next.continuity, BoundaryContinuity::Unverifiable);

            assert_eq!(previous_final.sequence, None);
            assert_eq!(next_initial.sequence, None);

            assert!(
                previous_final.timestamp_ns < next_initial.timestamp_ns,
                "unsequenced archives must remain chronologically ordered"
            );

            assert_eq!(
                previous_final.fingerprint,
                fingerprint_book(&previous_final.state)
            );
            assert_eq!(
                next_initial.fingerprint,
                fingerprint_book(&next_initial.state)
            );

            println!(
                "Archive {} -> {}: UNVERIFIABLE continuity, gap={} ns",
                index,
                index + 1,
                next_initial.timestamp_ns - previous_final.timestamp_ns,
            );
        }
    }

    println!("CROSS-ARCHIVE BOUNDARY VALIDATION: PASS");
}
// -----------------------------------------------------------------------------
// Gate.io spot continuity
// -----------------------------------------------------------------------------

#[test]
fn validates_gateio_spot_hourly_continuity() {
    validate_gateio_hourly_continuity(
        marketforge_engine::historical::job::InstrumentKind::Spot,
        "spot",
    );
}

#[test]
fn validates_gateio_linear_hourly_continuity() {
    validate_gateio_hourly_continuity(
        marketforge_engine::historical::job::InstrumentKind::Perpetual,
        "linear",
    );
}

#[test]
fn validates_gateio_inverse_hourly_continuity() {
    validate_gateio_hourly_continuity(
        marketforge_engine::historical::job::InstrumentKind::Perpetual,
        "inverse",
    );
}
