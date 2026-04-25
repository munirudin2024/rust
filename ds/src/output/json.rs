use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::output::html_writer::ensure_report_html_exists;

/// Ringkasan per dataset — dibaca oleh report.html via fetch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSummary {
	pub station:          String,
	pub file_path:        String,
	pub total_rows:       usize,
	pub total_cols:       usize,
	pub duplicate_rows:   usize,
	pub null_cells:       usize,
	pub null_pct:         f64,
	pub nulls_filled:     usize,
	pub outliers_capped:  usize,
	pub new_columns:      usize,
	pub metrics:          Vec<MetricStat>,
	pub payment_stats:    Vec<PaymentStat>,
	pub city_revenue_stats: Vec<CityRevenueStat>,
	pub observations:     Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStat {
	pub name: String,
	pub mean: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentStat {
	pub method: String,
	pub count:  usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CityRevenueStat {
	pub city:          String,
	pub total_revenue: f64,
}

/// Tulis report_data.json — tidak menyentuh report.html sama sekali
pub fn write_report_json(
	summaries:   &[DatasetSummary],
	output_root: &Path,
) -> Result<std::path::PathBuf> {
	let _ = crate::output::pathing::ensure_dir_structure(output_root)?;

	let html_dir = output_root.join("html");
	let legacy_html_dir = output_root.join("legacy").join("html");
	std::fs::create_dir_all(&html_dir).context("failed to create output/html")?;
	std::fs::create_dir_all(&legacy_html_dir).context("failed to create output/legacy/html")?;

	let json = serde_json::to_string_pretty(summaries)
		.context("failed to serialize summaries")?;

	let json_path = html_dir.join("report_data.json");
	std::fs::write(&json_path, &json)
		.context("failed to write report_data.json")?;

	let legacy_json_path = legacy_html_dir.join("report_data.json");
	std::fs::write(&legacy_json_path, &json)
		.context("failed to write legacy report_data.json")?;

	ensure_report_html_exists(&html_dir)?;
	ensure_report_html_exists(&legacy_html_dir)?;

	Ok(json_path)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_dataset_summary_serialize() {
		let s = DatasetSummary {
			station:            "test".into(),
			file_path:          "data/test.csv".into(),
			total_rows:         100,
			total_cols:         10,
			duplicate_rows:     2,
			null_cells:         5,
			null_pct:           0.5,
			nulls_filled:       5,
			outliers_capped:    0,
			new_columns:        3,
			metrics:            vec![],
			payment_stats:      vec![],
			city_revenue_stats: vec![],
			observations:       vec![],
		};
		let json = serde_json::to_string(&s).unwrap();
		assert!(json.contains("\"station\":\"test\""));
		assert!(json.contains("\"total_rows\":100"));
	}
}
