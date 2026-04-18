use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::DataFrame;
use std::path::{Path, PathBuf};

/// Step 3 pipeline: feature engineering ringan pasca-cleaning.
pub fn apply_post_clean_features(df: &mut DataFrame) -> Result<()> {
	crate::clean::add_retention_count(df)
}

/// Step 3 pipeline: hitung KPI kualitas data.
pub fn write_quality_kpi(
	df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
	max_business_date: NaiveDate,
) -> Result<PathBuf> {
	crate::output::csv_writer::write_quality_kpi_csv(
		df,
		source_csv_path,
		output_root,
		max_business_date,
	)
}
