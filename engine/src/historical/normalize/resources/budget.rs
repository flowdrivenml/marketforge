use std::sync::{Condvar, Mutex};

use crate::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Resource requirements
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceRequirements {
    pub memory_bytes: u64,
    pub scratch_bytes: u64,
    pub open_files: u64,
}

// -----------------------------------------------------------------------------
// Resource utilization
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub scratch_bytes: u64,
    pub open_files: u64,
}

// -----------------------------------------------------------------------------
// Resource budget
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ResourceBudget {
    capacity: ResourceRequirements,
    usage: Mutex<ResourceUsage>,
    available: Condvar,
}

impl ResourceBudget {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /// Construct a budget for memory and scratch resources.
    ///
    /// File reservations are disabled for this legacy constructor.
    /// Existing processing code can continue using this interface.
    pub fn new(memory_budget_bytes: u64, scratch_budget_bytes: u64) -> Result<Self> {
        Self::with_file_limit(memory_budget_bytes, scratch_budget_bytes, u64::MAX)
    }

    /// Construct a budget that also limits open-file reservations.
    pub fn with_file_limit(
        memory_budget_bytes: u64,
        scratch_budget_bytes: u64,
        max_open_files: u64,
    ) -> Result<Self> {
        if memory_budget_bytes == 0 {
            return invalid("memory budget must be greater than zero");
        }

        if scratch_budget_bytes == 0 {
            return invalid("scratch budget must be greater than zero");
        }

        if max_open_files == 0 {
            return invalid("open-file budget must be greater than zero");
        }

        Ok(Self {
            capacity: ResourceRequirements {
                memory_bytes: memory_budget_bytes,
                scratch_bytes: scratch_budget_bytes,
                open_files: max_open_files,
            },

            usage: Mutex::new(ResourceUsage::default()),
            available: Condvar::new(),
        })
    }

    // -------------------------------------------------------------------------
    // Resource admission
    // -------------------------------------------------------------------------

    /// Reserve resources before executing a task.
    ///
    /// Blocks until the requested resources are available.
    ///
    /// Reservations are released automatically when the returned
    /// ResourceReservation is dropped.
    pub fn acquire(&self, requirements: ResourceRequirements) -> Result<ResourceReservation<'_>> {
        self.validate_requirements(requirements)?;

        let mut usage = self
            .usage
            .lock()
            .map_err(|_| invalid_error("resource budget mutex poisoned"))?;

        while !self.fits(*usage, requirements) {
            usage = self
                .available
                .wait(usage)
                .map_err(|_| invalid_error("resource budget mutex poisoned while waiting"))?;
        }

        usage.memory_bytes = usage
            .memory_bytes
            .checked_add(requirements.memory_bytes)
            .ok_or_else(|| invalid_error("memory reservation overflow"))?;

        usage.scratch_bytes = usage
            .scratch_bytes
            .checked_add(requirements.scratch_bytes)
            .ok_or_else(|| invalid_error("scratch reservation overflow"))?;

        usage.open_files = usage
            .open_files
            .checked_add(requirements.open_files)
            .ok_or_else(|| invalid_error("open-file reservation overflow"))?;

        Ok(ResourceReservation {
            budget: self,
            requirements,
        })
    }

    // -------------------------------------------------------------------------
    // Resource inspection
    // -------------------------------------------------------------------------

    pub fn capacity(&self) -> ResourceRequirements {
        self.capacity
    }

    pub fn usage(&self) -> Result<ResourceUsage> {
        self.usage
            .lock()
            .map(|usage| *usage)
            .map_err(|_| invalid_error("resource budget mutex poisoned"))
    }

    // -------------------------------------------------------------------------
    // Internal validation
    // -------------------------------------------------------------------------

    fn validate_requirements(&self, requirements: ResourceRequirements) -> Result<()> {
        if requirements.memory_bytes > self.capacity.memory_bytes {
            return invalid(format!(
                "task memory requirement {} exceeds budget {}",
                requirements.memory_bytes, self.capacity.memory_bytes,
            ));
        }

        if requirements.scratch_bytes > self.capacity.scratch_bytes {
            return invalid(format!(
                "task scratch requirement {} exceeds budget {}",
                requirements.scratch_bytes, self.capacity.scratch_bytes,
            ));
        }

        if requirements.open_files > self.capacity.open_files {
            return invalid(format!(
                "task open-file requirement {} exceeds budget {}",
                requirements.open_files, self.capacity.open_files,
            ));
        }

        Ok(())
    }

    fn fits(&self, usage: ResourceUsage, requirements: ResourceRequirements) -> bool {
        requirements.memory_bytes <= self.capacity.memory_bytes - usage.memory_bytes
            && requirements.scratch_bytes <= self.capacity.scratch_bytes - usage.scratch_bytes
            && requirements.open_files <= self.capacity.open_files - usage.open_files
    }

    // -------------------------------------------------------------------------
    // Resource release
    // -------------------------------------------------------------------------

    fn release(&self, requirements: ResourceRequirements) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        usage.memory_bytes -= requirements.memory_bytes;
        usage.scratch_bytes -= requirements.scratch_bytes;
        usage.open_files -= requirements.open_files;

        drop(usage);

        self.available.notify_all();
    }
}

// -----------------------------------------------------------------------------
// RAII resource reservation
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ResourceReservation<'a> {
    budget: &'a ResourceBudget,
    requirements: ResourceRequirements,
}

impl Drop for ResourceReservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.requirements);
    }
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
