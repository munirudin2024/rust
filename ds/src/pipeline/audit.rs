use anyhow::Result;
use polars::prelude::DataFrame;

pub use crate::audit::AuditReport;

/// Step 1 pipeline: audit & profiling data mentah.
pub fn run(file_path: &str) -> Result<(DataFrame, AuditReport)> {
	crate::audit::run(file_path)
}
