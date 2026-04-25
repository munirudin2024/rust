pub mod audit;
pub mod clean;
pub mod feature;
pub mod report;
pub mod score;

use anyhow::Result;
use polars::prelude::DataFrame;
use polars::prelude::{CsvWriter, NamedFrom, SerWriter, Series};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::terminal_ui::TerminalStyle;

const SECTION_WIDTH: usize = 46;
const SECTION_WIDTH_WITH_CLAUSE: usize = 74;

pub fn section_header(step: &str, title: &str) -> String {
	let prefix = format!("── {} {} ", step, title);
	let prefix_len = prefix.chars().count();
	if prefix_len >= SECTION_WIDTH {
		return prefix;
	}

	format!("{}{}", prefix, "─".repeat(SECTION_WIDTH - prefix_len))
}

pub fn section_header_with_clause(step: &str, title: &str, clause: &str) -> String {
	let prefix = format!("── {} {} ", step, title);
	let suffix = format!(" [{}]", clause);
	let total_len = prefix.chars().count() + suffix.chars().count();

	if total_len >= SECTION_WIDTH_WITH_CLAUSE {
		return format!("{}{}", prefix, suffix);
	}

	let fill = "─".repeat(SECTION_WIDTH_WITH_CLAUSE - total_len);
	format!("{}{}{}", prefix, fill, suffix)
}

pub struct ProcessedDataset {
	pub source_file: PathBuf,
	pub summary: report::DatasetSummary,
	pub artifacts: report::DatasetReportArtifacts,
	pub validation_report: Option<crate::output::iso_writer::ValidationReport>,
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
	let mut validation_reports = Vec::new();

	for source_file in &config.input_files {
		let processed = run_one_dataset(
			source_file,
			&config.output_root,
			config.max_date,
			config.hard_reject,
			config.validate_iso,
			config.generate_sample,
			&config.cleaning_version,
			&config.imputation_policy,
		)?;

		if let Some(report) = processed.validation_report.clone() {
			validation_reports.push(report);
		}
		datasets.push(processed);
	}

	let summaries: Vec<report::DatasetSummary> =
		datasets.iter().map(|d| d.summary.clone()).collect();
	let report_json = report::write_summary_json(&summaries, &config.output_root)?;

	if config.quality_dashboard && !validation_reports.is_empty() {
		let mut metrics = HashMap::new();
		for report in &validation_reports {
			metrics.insert(
				report.station.clone(),
				crate::output::iso_writer::estimate_quality_metrics(report),
			);
		}
		let _ = crate::output::iso_writer::write_quality_dashboard(
			&config.output_root,
			&validation_reports,
			&metrics,
			true,
		)?;
	}

	for report in &validation_reports {
		if report.syntactic_validity_rate < 95.0 {
			eprintln!(
				"ALERT [{}] syntactic validity {:.2}% < 95%",
				report.station,
				report.syntactic_validity_rate
			);
		}
		if report.semantic_validity_rate < 90.0 {
			eprintln!(
				"ALERT [{}] semantic validity {:.2}% < 90%",
				report.station,
				report.semantic_validity_rate
			);
		}
	}

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
	validate_iso: bool,
	generate_sample: bool,
	cleaning_version: &str,
	imputation_policy: &crate::config::ImputationPolicy,
) -> Result<ProcessedDataset> {
	let source = source_file.to_string_lossy().to_string();
	let station = source_file
		.file_stem()
		.and_then(|s| s.to_str())
		.unwrap_or("dataset")
		.to_string();

	let (raw_df, audit_report) = audit::run(&source)?;
	let mut validation_report = None;
	let mut input_df = apply_upstream_prevention(&raw_df, output_root, source_file, &station)?;
	emit_preclean_semantic_profile(&input_df);

	if validate_iso {
		let ui = TerminalStyle::detect();
		println!(
			"\n{}",
			ui.stage_iso_gate(&section_header("[1.5/4]", "VALIDASI ISO"))
		);
		let spec = crate::iso_standards::iso8000::load_spec(Path::new("config/data_requirement_spec.json"))?;
		let validation_run = crate::validators::iso_compliance_validator::validate_dataframe_iso(&input_df, &spec);

		input_df.with_column(Series::new(
			"quality_flag".into(),
			validation_run.quality_flags.clone(),
		))?;

		let invalid_syntax_df = crate::validators::iso_compliance_validator::filter_rows_by_indices(
			&input_df,
			&validation_run.invalid_syntax_indices,
		);
		let invalid_semantic_df = crate::validators::iso_compliance_validator::filter_rows_by_indices(
			&input_df,
			&validation_run.invalid_semantic_indices,
		);

		let (_syntax_csv, _semantic_csv) = crate::output::iso_writer::write_invalid_validation_csv(
			output_root,
			&station,
			&invalid_syntax_df,
			&invalid_semantic_df,
		)?;

		let (validation_report_json, report_obj) = crate::output::iso_writer::write_validation_report(
			output_root,
			&station,
			input_df.height(),
			&validation_run,
		)?;

		let quality_score = (report_obj.syntactic_validity_rate + report_obj.semantic_validity_rate) / 2.0;
		let _provenance = crate::output::iso_writer::write_provenance_json(
			output_root,
			&station,
			source_file,
			cleaning_version,
			quality_score,
		)?;

		let _sample = crate::output::iso_writer::write_manual_review_sample(
			output_root,
			&station,
			&input_df,
			generate_sample,
		)?;

		let _feedback = crate::output::iso_writer::write_feedback_template(output_root, &station)?;
		let _outlier_log = crate::output::iso_writer::write_outlier_justification_log(output_root, &station)?;

		let _ = validation_report_json;
		validation_report = Some(report_obj);
	}
	let (clean_df, mut clean_report) =
		crate::clean::run(input_df.clone(), &audit_report, max_business_date, imputation_policy)?;

	let dedup_id = clean::deduplicate_transaction_ids(clean_df)?;
	let dedup_cross = clean::deduplicate_cross_id(dedup_id.df)?;
	let dropped_dups = clean::merge_dropped(dedup_id.dropped, dedup_cross.dropped)?;
	let dropped_dups_count = dedup_id.dropped_count + dedup_cross.dropped_count;

	let mut dedup_df = dedup_cross.df;
	clean::refresh_post_dedup_flags(&mut dedup_df)?;
	let mut analysis_df = dedup_df.clone();

	let (mut final_df, quarantine_df) = if hard_reject {
		clean::apply_hard_reject(dedup_df)?
	} else {
		(dedup_df, None)
	};

	let quality_score = validation_report
		.as_ref()
		.map(|r| (r.syntactic_validity_rate + r.semantic_validity_rate) / 2.0)
		.unwrap_or(100.0);
	let collection_date = crate::iso_standards::iso8000::infer_collection_date_from_filename(source_file);

	for df in [&mut analysis_df, &mut final_df] {
		add_currentness_and_credibility_columns(df)?;
		df.with_column(Series::new(
			"data_source".into(),
			vec!["PRSA_Beijing_AirQuality"; df.height()],
		))?;
		df.with_column(Series::new(
			"measurement_method".into(),
			vec!["Continuous_Ambient_Air_Monitoring"; df.height()],
		))?;
		df.with_column(Series::new(
			"collection_date".into(),
			vec![collection_date.clone(); df.height()],
		))?;
		df.with_column(Series::new(
			"cleaning_version".into(),
			vec![cleaning_version.to_string(); df.height()],
		))?;
		df.with_column(Series::new(
			"quality_score".into(),
			vec![quality_score; df.height()],
		))?;
		df.with_column(Series::new(
			"sensor_calibration_date".into(),
			vec![""; df.height()],
		))?;
		df.with_column(Series::new(
			"data_collection_agency".into(),
			vec!["Beijing_Municipal_Environmental_Protection_Bureau"; df.height()],
		))?;
		df.with_column(Series::new(
			"qa_qc_procedure".into(),
			vec!["ISO_9001_certified"; df.height()],
		))?;
	}

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
		validation_report,
	})
}

fn apply_upstream_prevention(
	raw_df: &DataFrame,
	output_root: &Path,
	source_file: &Path,
	station: &str,
) -> Result<DataFrame> {
	let mut df = raw_df.clone();

	let env_col = df
		.get_column_names()
		.iter()
		.find(|name| name.eq_ignore_ascii_case("env"))
		.map(|value| value.to_string())
		.unwrap_or_else(|| "env".to_string());

	if !df.get_column_names().iter().any(|name| name.eq_ignore_ascii_case("env")) {
		df.with_column(Series::new(env_col.as_str().into(), vec!["production"; df.height()]))?;
	}

	let id_col = df
		.get_column_names()
		.iter()
		.find(|name| {
			let lower = name.to_ascii_lowercase();
			lower == "transaction_id" || lower == "id_transaksi" || lower.contains("trx")
		})
		.map(|value| value.to_string());

	let env_series = df.column(&env_col)?;
	let id_series = id_col.as_ref().and_then(|col| df.column(col).ok());

	let mut keep_mask = Vec::with_capacity(df.height());
	let mut reject_reason = Vec::with_capacity(df.height());
	let mut seen_keys = HashSet::new();

	for row_idx in 0..df.height() {
		let env_value = env_series
			.get(row_idx)
			.ok()
			.map(|value| value.to_string().trim_matches('"').to_ascii_lowercase())
			.unwrap_or_else(|| "production".to_string());

		let is_production = env_value == "production";
		let id_key = id_series
			.as_ref()
			.and_then(|series| series.get(row_idx).ok())
			.map(|value| value.to_string().trim_matches('"').to_string())
			.filter(|value| !value.trim().is_empty())
			.unwrap_or_else(|| format!("{}_row_{}", station, row_idx));

		let mut reasons = Vec::new();
		if !is_production {
			reasons.push(format!("NON_PRODUCTION_ENV:{}", env_value));
		}

		if is_production && !seen_keys.insert(id_key.clone()) {
			reasons.push(format!("DUPLICATE_IDEMPOTENCY_KEY:{}", id_key));
		}

		if reasons.is_empty() {
			keep_mask.push(true);
			reject_reason.push(None::<String>);
		} else {
			keep_mask.push(false);
			reject_reason.push(Some(reasons.join("|")));
		}
	}

	if keep_mask.iter().all(|keep| *keep) {
		return Ok(df);
	}

	df.with_column(Series::new(
		"upstream_reject_reason".into(),
		reject_reason,
	))?;

	let keep_series = Series::new("keep_mask".into(), keep_mask.clone());
	let keep = keep_series.bool()?;
	let reject_vec: Vec<bool> = keep_mask.into_iter().map(|value| !value).collect();
	let reject_series = Series::new("reject_mask".into(), reject_vec);
	let reject = reject_series.bool()?;

	let filtered = df.filter(&keep)?;
	let mut rejected = df.filter(&reject)?;

	let quarantine_dir = output_root.join("quarantine");
	std::fs::create_dir_all(&quarantine_dir)?;
	let path = quarantine_dir.join(format!("{}_upstream_rejected.csv", station));
	let mut file = File::create(&path)?;
	CsvWriter::new(&mut file).finish(&mut rejected)?;

	eprintln!(
		"[UPSTREAM-GUARD] {} baris ditolak pra-pipeline (detail: {})",
		rejected.height(),
		path.display()
	);

	let _ = source_file;
	Ok(filtered)
}

fn emit_preclean_semantic_profile(df: &DataFrame) {
	let q_col = df.column("Jumlah_Beli").ok().or_else(|| df.column("quantity").ok());
	let p_col = df.column("Harga_Satuan").ok().or_else(|| df.column("price").ok());
	let r_col = df
		.column("revenue_per_transaction")
		.ok()
		.or_else(|| df.column("revenue").ok());
	if let (Some(q_col), Some(p_col), Some(r_col)) = (q_col, p_col, r_col) {
		let d_col = df
			.column("Diskon_Rupiah")
			.ok()
			.or_else(|| df.column("discount").ok());
		let s_col = df
			.column("shipping_fee")
			.ok()
			.or_else(|| df.column("biaya_kirim").ok());

		let mut mismatch = 0usize;
		for i in 0..df.height() {
			let qty = q_col.get(i).ok().and_then(|v| v.to_string().trim_matches('"').replace("Rp.", "").parse::<f64>().ok());
			let price = p_col.get(i).ok().and_then(|v| v.to_string().trim_matches('"').replace("Rp.", "").parse::<f64>().ok());
			let rev = r_col.get(i).ok().and_then(|v| v.to_string().trim_matches('"').replace("Rp.", "").parse::<f64>().ok());
			let disc = d_col
				.as_ref()
				.and_then(|c| c.get(i).ok())
				.and_then(|v| v.to_string().trim_matches('"').parse::<f64>().ok())
				.unwrap_or(0.0);
			let ship = s_col
				.as_ref()
				.and_then(|c| c.get(i).ok())
				.and_then(|v| v.to_string().trim_matches('"').parse::<f64>().ok())
				.unwrap_or(0.0);

			if let (Some(q), Some(p), Some(r)) = (qty, price, rev) {
				let expected = (q * p) - disc + ship;
				let tolerance = expected.abs().max(1.0) * 0.01;
				if (r - expected).abs() > tolerance {
					mismatch += 1;
				}
			}
		}
		eprintln!(
			"[PRE-PROFILE] semantic revenue mismatch (pre-clean): {} baris",
			mismatch
		);
	}
}

fn add_currentness_and_credibility_columns(df: &mut DataFrame) -> Result<()> {
	let processing = chrono::Utc::now();
	let mut freshness_hours = Vec::with_capacity(df.height());
	let mut staleness_flags = Vec::with_capacity(df.height());
	let mut mqi = Vec::with_capacity(df.height());

	let year = df.column("year").ok();
	let month = df.column("month").ok();
	let day = df.column("day").ok();
	let hour = df.column("hour").ok();

	for i in 0..df.height() {
		let ts = match (&year, &month, &day, &hour) {
			(Some(y), Some(m), Some(d), Some(h)) => {
				let yy = y.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()).and_then(|v| v.parse::<i32>().ok());
				let mm = m.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()).and_then(|v| v.parse::<u32>().ok());
				let dd = d.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()).and_then(|v| v.parse::<u32>().ok());
				let hh = h.get(i).ok().map(|v| v.to_string().trim_matches('"').to_string()).and_then(|v| v.parse::<u32>().ok());
				if let (Some(yy), Some(mm), Some(dd), Some(hh)) = (yy, mm, dd, hh) {
					chrono::NaiveDate::from_ymd_opt(yy, mm, dd)
						.and_then(|d| d.and_hms_opt(hh, 0, 0))
						.map(|x| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(x, chrono::Utc))
				} else {
					None
				}
			}
			_ => None,
		};

		if let Some(record_ts) = ts {
			let hours = (processing - record_ts).num_hours().max(0) as f64;
			let staleness = crate::iso_standards::iso25012::calculate_staleness(record_ts, processing);
			freshness_hours.push(Some(hours));
			staleness_flags.push(Some(staleness.as_str().to_string()));
			let idx = match staleness {
				crate::iso_standards::iso25012::StalenessCategory::Fresh => 5,
				crate::iso_standards::iso25012::StalenessCategory::Stale => 3,
				crate::iso_standards::iso25012::StalenessCategory::Archive => 1,
			};
			mqi.push(Some(idx));
		} else {
			freshness_hours.push(None);
			staleness_flags.push(Some("archive".to_string()));
			mqi.push(Some(1));
		}
	}

	df.with_column(Series::new("data_freshness_hours".into(), freshness_hours))?;
	df.with_column(Series::new("staleness_flag".into(), staleness_flags))?;
	df.with_column(Series::new("measurement_quality_index".into(), mqi))?;
	Ok(())
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
			false,
			false,
			"0.1.0",
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
				validation_report: None,
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
				validation_report: None,
			},
		];

		let s = summarize_run(&ds);
		assert_eq!(s.dataset_count, 2);
		assert_eq!(s.total_rows, 30);
		assert_eq!(s.total_dropped_duplicates, 5);
		assert_eq!(s.total_quarantine_rows, 5);
	}
}
