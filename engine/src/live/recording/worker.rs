use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{Duration, Instant, sleep_until},
};

use sqlx::PgPool;

use crate::live::config::general::RecordingConfig;

use super::{
    control::{RecordingCommand, RecordingControl, RecordingError},
    writer::{PostgresBatchWriter, RecordingEvent},
};

pub struct PostgresRecorder;

impl PostgresRecorder {
    pub fn start(
        pool: PgPool,
        config: RecordingConfig,
        queue_capacity: usize,
    ) -> Result<(RecordingControl, JoinHandle<Result<(), RecordingError>>), RecordingError> {
        config
            .validate()
            .map_err(RecordingError::InvalidConfiguration)?;

        if queue_capacity == 0 {
            return Err(RecordingError::InvalidQueueCapacity);
        }

        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (config_sender, config_receiver) = watch::channel(config);

        let control = RecordingControl::new(sender, config_sender);

        let worker = tokio::spawn(async move {
            run_worker(PostgresBatchWriter::new(pool), receiver, config_receiver).await
        });

        Ok((control, worker))
    }
}

async fn run_worker(
    writer: PostgresBatchWriter,
    mut receiver: mpsc::Receiver<RecordingCommand>,
    mut config_receiver: watch::Receiver<RecordingConfig>,
) -> Result<(), RecordingError> {
    let mut pending: Vec<RecordingEvent> = Vec::new();
    let mut batch_started_at: Option<Instant> = None;
    let mut config_channel_open = true;

    loop {
        let config = config_receiver.borrow().clone();

        // A configuration update can reduce the batch size,
        // shorten the delay, or disable recording.
        if !pending.is_empty() {
            let deadline = batch_started_at
                .expect("nonempty recording batch must have a start time")
                + Duration::from_millis(config.batch_max_delay_ms);

            if pending.len() >= config.batch_max_records
                || Instant::now() >= deadline
                || !config.enabled
            {
                flush(&writer, &mut pending).await?;
                batch_started_at = None;
                continue;
            }
        }

        let deadline =
            batch_started_at.map(|start| start + Duration::from_millis(config.batch_max_delay_ms));

        tokio::select! {
            biased;

            changed = config_receiver.changed(),
                if config_channel_open =>
            {
                if changed.is_err() {
                    config_channel_open = false;
                }
            }

            _ = async {
                if let Some(deadline) = deadline {
                    sleep_until(deadline).await;
                }
            }, if deadline.is_some() => {
                // The next iteration checks whether the batch
                // deadline has elapsed.
            }

            command = receiver.recv() => {
                match command {
                    Some(RecordingCommand::Event(event)) => {
                        if pending.is_empty() {
                            batch_started_at = Some(Instant::now());
                        }

                        pending.push(event);
                    }

                    Some(RecordingCommand::Shutdown(ack)) => {
                        // Reject new sends and drain everything already
                        // accepted into the channel.
                        receiver.close();

                        let _ = ack.send(());
                    }

                    None => {
                        flush(&writer, &mut pending).await?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn flush(
    writer: &PostgresBatchWriter,
    pending: &mut Vec<RecordingEvent>,
) -> Result<(), RecordingError> {
    if pending.is_empty() {
        return Ok(());
    }

    writer.write_batch(pending).await?;

    pending.clear();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_batch_limits_are_positive() {
        let config = RecordingConfig {
            enabled: false,
            batch_max_records: 10_000,
            batch_max_delay_ms: 100,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn smaller_runtime_batches_are_valid() {
        let config = RecordingConfig {
            enabled: true,
            batch_max_records: 100,
            batch_max_delay_ms: 10,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn batch_delay_preserves_millisecond_units() {
        let config = RecordingConfig {
            enabled: true,
            batch_max_records: 100,
            batch_max_delay_ms: 250,
        };

        assert_eq!(
            Duration::from_millis(config.batch_max_delay_ms),
            Duration::from_millis(250)
        );
    }
}
