use crate::{
    error::{MarketForgeError, Result},
    job::MergeJob,
};

// -----------------------------------------------------------------------------
// Merge partition
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergePartition {
    pub partition_id: u32,

    pub start_ns: i64,
    pub end_ns: i64,
}

impl MergePartition {
    pub fn contains(&self, timestamp_ns: i64) -> bool {
        timestamp_ns >= self.start_ns && timestamp_ns < self.end_ns
    }

    pub fn duration_ns(&self) -> i128 {
        i128::from(self.end_ns) - i128::from(self.start_ns)
    }
}

// -----------------------------------------------------------------------------
// Partition plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergePartitionPlan {
    pub job_id: u64,

    pub partitions: Vec<MergePartition>,

    pub requested_workers: usize,
    pub partitions_per_worker: usize,
}

impl MergePartitionPlan {
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn validate(&self, start_ns: i64, end_ns: i64) -> Result<()> {
        if self.partitions.is_empty() {
            return invalid("merge partition plan contains no partitions");
        }

        if self.partitions[0].start_ns != start_ns {
            return invalid("merge partition plan does not begin at requested start");
        }

        if self.partitions.last().unwrap().end_ns != end_ns {
            return invalid("merge partition plan does not end at requested end");
        }

        for (index, partition) in self.partitions.iter().enumerate() {
            if partition.start_ns >= partition.end_ns {
                return invalid(format!("merge partition {index} has an invalid interval"));
            }

            if index > 0 {
                let previous = &self.partitions[index - 1];

                if previous.end_ns != partition.start_ns {
                    return invalid(format!(
                        "merge partitions {} and {} are not contiguous",
                        index - 1,
                        index,
                    ));
                }
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Build partition plan
// -----------------------------------------------------------------------------

pub fn plan_merge_partitions(job: &MergeJob) -> Result<MergePartitionPlan> {
    job.validate()?;

    let requested_partitions = job
        .resources
        .workers
        .checked_mul(job.partitioning.partitions_per_worker)
        .ok_or_else(|| invalid_error("merge partition count overflow"))?;

    if requested_partitions == 0 {
        return invalid("merge partition count must be greater than zero");
    }

    let start = i128::from(job.time_range.start_ns);
    let end = i128::from(job.time_range.end_ns);

    let duration = end - start;

    if duration <= 0 {
        return invalid("merge time interval must be positive");
    }

    // A partition must contain at least one nanosecond.
    let duration_usize = usize::try_from(duration).unwrap_or(usize::MAX);

    let partition_count = requested_partitions.min(duration_usize);

    let partition_count_i128 = i128::try_from(partition_count)
        .map_err(|_| invalid_error("partition count conversion overflow"))?;

    let base_duration = duration / partition_count_i128;
    let remainder = duration % partition_count_i128;

    let mut partitions = Vec::with_capacity(partition_count);

    let mut cursor = start;

    for index in 0..partition_count {
        let extra = if (index as i128) < remainder { 1 } else { 0 };

        let partition_duration = base_duration + extra;

        let next = cursor + partition_duration;

        let start_ns =
            i64::try_from(cursor).map_err(|_| invalid_error("partition start overflow"))?;

        let end_ns = i64::try_from(next).map_err(|_| invalid_error("partition end overflow"))?;

        partitions.push(MergePartition {
            partition_id: u32::try_from(index)
                .map_err(|_| invalid_error("partition ID overflow"))?,

            start_ns,
            end_ns,
        });

        cursor = next;
    }

    let plan = MergePartitionPlan {
        job_id: job.job_id,
        partitions,

        requested_workers: job.resources.workers,
        partitions_per_worker: job.partitioning.partitions_per_worker,
    };

    plan.validate(job.time_range.start_ns, job.time_range.end_ns)?;

    Ok(plan)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::job::MergeJob;

    fn test_job() -> MergeJob {
        serde_json::from_value(serde_json::json!({
            "job_id": 200,
            "operation": "merge",
            "inputs": [105, 108, 112, 92],

            "time_range": {
                "start_ns": 1788220800000000000_i64,
                "end_ns": 1788307200000000000_i64
            },

            "resources": {
                "workers": 16,
                "memory_limit_mb": 65536,
                "max_open_files": 1024
            },

            "partitioning": {
                "partitions_per_worker": 4
            },

            "parquet": {
                "row_group_target_mb": 128,
                "file_target_mb": 1024,
                "compression_level": 3
            },

            "output": {
                "dataset_path": "/tmp/marketforge/merged/200"
            }
        }))
        .unwrap()
    }

    #[test]
    fn creates_64_partitions() {
        let job = test_job();

        let plan = plan_merge_partitions(&job).unwrap();

        assert_eq!(plan.partition_count(), 64);

        plan.validate(job.time_range.start_ns, job.time_range.end_ns)
            .unwrap();
    }

    #[test]
    fn partitions_cover_exact_interval() {
        let job = test_job();

        let plan = plan_merge_partitions(&job).unwrap();

        assert_eq!(
            plan.partitions.first().unwrap().start_ns,
            job.time_range.start_ns,
        );

        assert_eq!(
            plan.partitions.last().unwrap().end_ns,
            job.time_range.end_ns,
        );

        let total_duration: i128 = plan
            .partitions
            .iter()
            .map(MergePartition::duration_ns)
            .sum();

        assert_eq!(
            total_duration,
            i128::from(job.time_range.end_ns) - i128::from(job.time_range.start_ns)
        );
    }

    #[test]
    fn partitions_do_not_overlap() {
        let job = test_job();

        let plan = plan_merge_partitions(&job).unwrap();

        for pair in plan.partitions.windows(2) {
            assert_eq!(pair[0].end_ns, pair[1].start_ns);
        }
    }

    #[test]
    fn partitions_are_deterministic() {
        let job = test_job();

        let first = plan_merge_partitions(&job).unwrap();
        let second = plan_merge_partitions(&job).unwrap();

        assert_eq!(first.partitions, second.partitions);
    }

    #[test]
    fn partition_contains_uses_half_open_intervals() {
        let partition = MergePartition {
            partition_id: 0,
            start_ns: 100,
            end_ns: 200,
        };

        assert!(!partition.contains(99));
        assert!(partition.contains(100));
        assert!(partition.contains(199));
        assert!(!partition.contains(200));
    }

    #[test]
    fn distributes_remainder_without_gaps() {
        let mut job = test_job();

        job.time_range.start_ns = 0;
        job.time_range.end_ns = 10;

        job.resources.workers = 2;
        job.partitioning.partitions_per_worker = 2;

        let plan = plan_merge_partitions(&job).unwrap();

        assert_eq!(plan.partition_count(), 4);

        let durations = plan
            .partitions
            .iter()
            .map(MergePartition::duration_ns)
            .collect::<Vec<_>>();

        assert_eq!(durations, vec![3, 3, 2, 2]);
    }

    #[test]
    fn limits_partitions_to_available_nanoseconds() {
        let mut job = test_job();

        job.time_range.start_ns = 0;
        job.time_range.end_ns = 3;

        let plan = plan_merge_partitions(&job).unwrap();

        assert_eq!(plan.partition_count(), 3);

        assert!(plan.partitions.iter().all(|p| p.duration_ns() == 1));
    }
}
