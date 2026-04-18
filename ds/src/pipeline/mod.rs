pub mod audit;
pub mod clean;
pub mod feature;
pub mod report;
pub mod score;

use anyhow::Result;
use polars::prelude::DataFrame;
use std::path::{Path, PathBuf};

use crate::config::Config;

pub struct ProcessedDataset {
	pub source_file: PathBuf,
	pub summary: report::DatasetSummary,
	pub artifacts: report::DatasetReportArtifacts,
}

pub struct PipelineRunResult {
	pub datasets: Vec<ProcessedDataset>,
	pub report_json: PathBuf,
}

pub struct RunSummary {
	pub dataset_count: usize,
	pub total_rows: usize,
	pub total_dropped_duplicates: usize,
	pub total_quarantine_rows: usize,
}

pub fn run_all(config: &Config) -> Result<PipelineRunResult> {
	let mut datasets = Vec::with_capacity(config.input_files.len());

	for source_file in &config.input_files {
		let processed = run_one_dataset(
			source_file,
			&config.output_root,
			config.max_date,
			config.hard_reject,
		)?;
		datasets.push(processed);
	}

	let summaries: Vec<report::DatasetSummary> =
		datasets.iter().map(|d| d.summary.clone()).collect();
	let report_json = report::write_summary_json(&summaries, &config.output_root)?;

	Ok(PipelineRunResult {
		datasets,
		report_json,
	})
}

pub fn summarize_run(datasets: &[ProcessedDataset]) -> RunSummary {
	RunSummary {
		dataset_count: datasets.len(),
		total_rows: datasets.iter().map(|d| d.summary.total_rows).sum(),
		total_dropped_duplicates: datasets
			.iter()
			.map(|d| d.artifacts.dropped_duplicates_count)
			.sum(),
		total_quarantine_rows: datasets
			.iter()
			.map(|d| d.artifacts.quarantine_rows)
			.sum(),
	}
}

pub fn run_one_dataset(
	source_file: &Path,
	output_root: &Path,
	max_business_date: chrono::NaiveDate,
	hard_reject: bool,
) -> Result<ProcessedDataset> {
	let source = source_file.to_string_lossy().to_string();

	let (raw_df, audit_report) = audit::run(&source)?;
	let (clean_df, mut clean_report) =
		crate::clean::run(raw_df.clone(), &audit_report, max_business_date)?;

	let dedup_id = clean::deduplicate_transaction_ids(clean_df)?;
	let dedup_cross = clean::deduplicate_cross_id(dedup_id.df)?;
	let dropped_dups = clean::merge_dropped(dedup_id.dropped, dedup_cross.dropped)?;
	let dropped_dups_count = dedup_id.dropped_count + dedup_cross.dropped_count;

	let mut dedup_df = dedup_cross.df;
	clean::refresh_post_dedup_flags(&mut dedup_df)?;
	let analysis_df = dedup_df.clone();

	let (mut final_df, quarantine_df) = if hard_reject {
		clean::apply_hard_reject(dedup_df)?
	} else {
		(dedup_df, None)
	};

	feature::apply_post_clean_features(&mut final_df)?;
	clean_report.new_columns.push("retention_count".to_string());

	let artifacts = report::write_dataset_outputs(
		source_file,
		&analysis_df,
		&final_df,
		quarantine_df.as_ref(),
		dropped_dups.as_ref(),
		dropped_dups_count,
		max_business_date,
		output_root,
		hard_reject,
	)?;

	let summary = build_dataset_summary(
		source_file,
		&final_df,
		&audit_report,
		&clean_report,
		dropped_dups_count,
	);

	Ok(ProcessedDataset {
		source_file: source_file.to_path_buf(),
		summary,
		artifacts,
	})
}

fn build_dataset_summary(
	source_file: &Path,
	final_df: &DataFrame,
	audit_report: &crate::audit::AuditReport,
	clean_report: &crate::clean::CleanReport,
	dropped_dups_count: usize,
) -> report::DatasetSummary {
	let total_rows = final_df.height();
	let total_cols = final_df.width();

	let null_cells: usize = final_df
		.get_column_names()
		.iter()
		.filter_map(|c| final_df.column(c).ok())
		.map(|s| s.null_count())
		.sum();
	let total_cells = total_rows.saturating_mul(total_cols).max(1);
	let null_pct = (null_cells as f64 * 100.0) / total_cells as f64;

	let nulls_filled: usize = clean_report.nulls_filled.iter().map(|(_, n)| *n).sum();
	let outliers_capped: usize = clean_report.outliers_capped.iter().map(|(_, n)| *n).sum();

	let metrics = audit_report
		.profiles
		.iter()
		.map(|p| report::MetricStat {
			name: p.name.clone(),
			mean: p.mean,
		})
		.collect();

	report::DatasetSummary {
		station: source_file
			.file_stem()
			.and_then(|s| s.to_str())
			.unwrap_or("dataset")
			.to_string(),
		file_path: source_file.to_string_lossy().to_string(),
		total_rows,
		total_cols,
		duplicate_rows: audit_report.duplicate_rows.saturating_add(dropped_dups_count),
		null_cells,
		null_pct,
		nulls_filled,
		outliers_capped,
		new_columns: clean_report.new_columns.len(),
		metrics,
		payment_stats: Vec::new(),
		city_revenue_stats: Vec::new(),
		observations: Vec::new(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::audit::{AuditReport, ColumnProfile};
	use crate::clean::CleanReport;
	use polars::prelude::{DataFrame, NamedFrom, Series};
	use std::fs;
	use std::path::PathBuf;
	use std::time::{SystemTime, UNIX_EPOCH};

	#[test]
	fn test_build_dataset_summary_basic_fields() {
		let df = DataFrame::new(vec![
			Series::new("nilai".into(), vec![Some(1_i64), None]),
		])
		.unwrap();

		let audit = AuditReport {
			total_rows: 2,
			total_cols: 1,
			duplicate_rows: 2,
			numeric_cols: vec!["nilai".to_string()],
			string_cols: vec![],
			profiles: vec![ColumnProfile {
				name: "nilai".to_string(),
				dtype: "Int64".to_string(),
				null_count: 1,
				null_pct: 50.0,
				unique_count: 1,
				mean: Some(1.0),
				median: Some(1.0),
				std_dev: Some(0.0),
				skewness: Some(0.0),
				kurtosis: Some(0.0),
				outlier_count: Some(0),
				lower_bound: Some(1.0),
				upper_bound: Some(1.0),
				top_value: None,
			}],
			generated_at: "2026-01-01T00:00:00Z".to_string(),
		};

		let clean = CleanReport {
			nulls_filled: vec![("nilai".to_string(), 1)],
			outliers_capped: vec![("nilai".to_string(), 0)],
			new_columns: vec!["retention_count".to_string()],
			rows_before: 2,
			rows_after: 2,
		};

		let summary = build_dataset_summary(
			Path::new("data/contoh.csv"),
			&df,
			&audit,
			&clean,
			3,
		);

		assert_eq!(summary.station, "contoh");
		assert_eq!(summary.total_rows, 2);
		assert_eq!(summary.total_cols, 1);
		assert_eq!(summary.duplicate_rows, 5);
		assert_eq!(summary.null_cells, 1);
		assert_eq!(summary.null_pct, 50.0);
		assert_eq!(summary.new_columns, 1);
		assert_eq!(summary.metrics.len(), 1);
	}

	#[test]
	fn test_run_one_dataset_creates_outputs() {
		let stamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		let base = std::env::temp_dir().join(format!("data_cleaner_it_{}_{}", std::process::id(), stamp));
		let input_dir = base.join("input");
		let output_dir = base.join("output");
		fs::create_dir_all(&input_dir).unwrap();

		let csv_path: PathBuf = input_dir.join("sample.csv");
		let csv = concat!(
			"id_transaksi,tanggal,nama_konsumen,kota,kategori,barang,harga,jumlah,pembayaran,diskon\n",
			"trx-1,2023-01-01,Budi Santoso,Jakarta,Elektronik,Headset,100000,1,cod,0\n",
			"trx-2,2023-01-02,Ani Wijaya,Bandung,Fashion,Tas,250000,2,transfer bank,5000\n"
		);
		fs::write(&csv_path, csv).unwrap();

		let result = run_one_dataset(
			&csv_path,
			&output_dir,
			chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
			false,
		)
		.unwrap();

		assert_eq!(result.summary.total_rows, 2);
		assert!(result.artifacts.cleaned_csv.exists());
		assert!(result.artifacts.payload_csv.exists());
		assert!(result.artifacts.audit_log_csv.exists());
		assert!(result.artifacts.kpi_csv.exists());
		assert!(result.artifacts.presentasi_txt.exists());
		assert!(result.artifacts.presentasi_html.exists());

		let _ = fs::remove_dir_all(&base);
	}

	#[test]
	fn test_summarize_run_aggregation() {
		let ds = vec![
			ProcessedDataset {
				source_file: PathBuf::from("a.csv"),
				summary: report::DatasetSummary {
					station: "a".into(),
					file_path: "a.csv".into(),
					total_rows: 10,
					total_cols: 3,
					duplicate_rows: 1,
					null_cells: 0,
					null_pct: 0.0,
					nulls_filled: 0,
					outliers_capped: 0,
					new_columns: 0,
					metrics: vec![],
					payment_stats: vec![],
					city_revenue_stats: vec![],
					observations: vec![],
				},
				artifacts: report::DatasetReportArtifacts {
					cleaned_csv: PathBuf::from("clean.csv"),
					payload_csv: PathBuf::from("payload.csv"),
					audit_log_csv: PathBuf::from("audit.csv"),
					kpi_csv: PathBuf::from("kpi.csv"),
					yearly_csvs: vec![],
					quarantine_csv: None,
					dropped_duplicates_csv: None,
					gap_report_md: None,
					quarantine_summary_md: None,
					hidden_insights_md: None,
					budi_investigation_md: PathBuf::from("budi.md"),
					presentasi_txt: PathBuf::from("p.txt"),
					presentasi_html: PathBuf::from("p.html"),
					dropped_duplicates_count: 2,
					quarantine_rows: 1,
				},
			},
			ProcessedDataset {
				source_file: PathBuf::from("b.csv"),
				summary: report::DatasetSummary {
					station: "b".into(),
					file_path: "b.csv".into(),
					total_rows: 20,
					total_cols: 4,
					duplicate_rows: 0,
					null_cells: 0,
					null_pct: 0.0,
					nulls_filled: 0,
					outliers_capped: 0,
					new_columns: 0,
					metrics: vec![],
					payment_stats: vec![],
					city_revenue_stats: vec![],
					observations: vec![],
				},
				artifacts: report::DatasetReportArtifacts {
					cleaned_csv: PathBuf::from("clean2.csv"),
					payload_csv: PathBuf::from("payload2.csv"),
					audit_log_csv: PathBuf::from("audit2.csv"),
					kpi_csv: PathBuf::from("kpi2.csv"),
					yearly_csvs: vec![],
					quarantine_csv: None,
					dropped_duplicates_csv: None,
					gap_report_md: None,
					quarantine_summary_md: None,
					hidden_insights_md: None,
					budi_investigation_md: PathBuf::from("budi2.md"),
					presentasi_txt: PathBuf::from("p2.txt"),
					presentasi_html: PathBuf::from("p2.html"),
					dropped_duplicates_count: 3,
					quarantine_rows: 4,
				},
			},
		];

		let s = summarize_run(&ds);
		assert_eq!(s.dataset_count, 2);
		assert_eq!(s.total_rows, 30);
		assert_eq!(s.total_dropped_duplicates, 5);
		assert_eq!(s.total_quarantine_rows, 5);
	}
}
