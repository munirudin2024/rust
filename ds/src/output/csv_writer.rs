use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::{AnyValue, CsvWriter, DataFrame, SerWriter};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::domain::utils::{
	anyvalue_to_plain_string, csv_escape, find_col_by_keywords,
};
use crate::domain::validation::{
count_completeness_passed, count_uniqueness_passed, passes_timeliness,
	passes_validity,
};
use crate::pipeline::score::{
	evaluate_accuracy, evaluate_consistency, evaluate_integrity,
};

pub struct QualityKpiRow {
	pub dimension:     String,
	pub score:         i64,
	pub total_checked: usize,
	pub passed:        usize,
	pub failed:        usize,
	pub notes:         String,
}

pub fn build_kpi_row(
	dimension: &str,
	total_checked: usize,
	passed: usize,
	notes: String,
) -> QualityKpiRow {
	let failed = total_checked.saturating_sub(passed);
	let score = if total_checked == 0 {
		100
	} else {
		((passed as f64 * 100.0) / total_checked as f64).round() as i64
	};
	QualityKpiRow {
		dimension: dimension.to_string(),
		score,
		total_checked,
		passed,
		failed,
		notes,
	}
}

pub fn write_cleaned_csv(
	clean_df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<PathBuf> {
	let csv_output_dir = output_root.join("csv");
	std::fs::create_dir_all(&csv_output_dir)
		.context("failed to create output/csv directory")?;

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("cleaned.csv");
	let output_csv_path = csv_output_dir.join(file_name);

	let mut output_file = File::create(&output_csv_path)
		.with_context(|| format!("failed to create {}", output_csv_path.display()))?;
	let mut df_to_write = clean_df.clone();
	CsvWriter::new(&mut output_file)
		.finish(&mut df_to_write)
		.with_context(|| format!("failed to write {}", output_csv_path.display()))?;

	Ok(output_csv_path)
}

pub fn split_payload_and_audit_tables(
	df: &DataFrame,
) -> Result<(DataFrame, DataFrame)> {
	let id_col = find_col_by_keywords(df, &["id_transaksi", "transaction_id", "trx"]);

	let audit_exact = [
		"status_transaksi", "qty_nol", "qty_negatif_awal", "qty_ekstrem",
		"rating_tidak_valid", "revenue_anomali", "duplikat_id_transaksi",
		"duplikat_id_berbeda", "tanggal_diluar_range", "perlu_review_manual",
		"price_outlier_iqr", "retention_count", "cleaned_at", "alasan_karantina",
		"jumlah_alasan_karantina", "severity_karantina", "skor_severity_karantina",
	];

	let mut audit_cols: Vec<String> = df
		.get_column_names()
		.iter()
		.filter_map(|c| {
			let low = c.to_ascii_lowercase();
			if low.starts_with("is_outlier_") || audit_exact.contains(&low.as_str()) {
				Some((*c).to_string())
			} else {
				None
			}
		})
		.collect();

	if let Some(id_name) = &id_col {
		if !audit_cols.iter().any(|c| c == id_name) {
			audit_cols.insert(0, id_name.clone());
		}
	}

	let payload_cols: Vec<String> = df
		.get_column_names()
		.iter()
		.filter_map(|c| {
			let is_id = id_col.as_ref().map(|idn| idn == *c).unwrap_or(false);
			if audit_cols.iter().any(|ac| ac == *c) && !is_id {
				None
			} else {
				Some((*c).to_string())
			}
		})
		.collect();

	let payload_df = if payload_cols.is_empty() {
		df.clone()
	} else {
		let sel: Vec<&str> = payload_cols.iter().map(|s| s.as_str()).collect();
		df.select(sel)?
	};

	let audit_df = if audit_cols.is_empty() {
		DataFrame::default()
	} else {
		let sel: Vec<&str> = audit_cols.iter().map(|s| s.as_str()).collect();
		df.select(sel)?
	};

	Ok((payload_df, audit_df))
}

pub fn write_payload_audit_csvs(
	final_df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<(PathBuf, PathBuf)> {
	let (payload_df, audit_df) = split_payload_and_audit_tables(final_df)?;

	let clean_dir = output_root.join("csv").join("transaksi_clean");
	let audit_dir = output_root.join("csv").join("transaksi_audit_log");
	std::fs::create_dir_all(&clean_dir)?;
	std::fs::create_dir_all(&audit_dir)?;

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("cleaned.csv");

	let payload_path = clean_dir.join(file_name);
	let audit_path   = audit_dir.join(file_name);

	let mut pf = File::create(&payload_path)?;
	let mut pdf = payload_df.clone();
	CsvWriter::new(&mut pf).finish(&mut pdf)?;

	let mut af = File::create(&audit_path)?;
	let mut adf = audit_df.clone();
	CsvWriter::new(&mut af).finish(&mut adf)?;

	Ok((payload_path, audit_path))
}

pub fn write_yearly_splits(
	df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<Vec<PathBuf>> {
	use polars::prelude::{BooleanChunked, NewChunkedArray};

	let date_col = find_col_by_keywords(df, &["tanggal", "date", "tgl"]);
	let Some(date_name) = date_col else { return Ok(Vec::new()); };

	let ds = df.column(&date_name)?;
	let mut year_rows: HashMap<i32, Vec<usize>> = HashMap::new();

	for i in 0..df.height() {
		let year_opt = ds.get(i).ok()
			.filter(|v| !matches!(v, AnyValue::Null))
			.map(anyvalue_to_plain_string)
			.and_then(|s| s.get(0..4).map(|x| x.to_string()))
			.and_then(|y| y.parse::<i32>().ok());
		if let Some(year) = year_opt {
			year_rows.entry(year).or_default().push(i);
		}
	}

	let mut years: Vec<i32> = year_rows.keys().copied().collect();
	years.sort();

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("cleaned.csv");
	let mut outputs = Vec::new();

	for year in years {
		let mut mask_vals = vec![false; df.height()];
		if let Some(rows) = year_rows.get(&year) {
			for idx in rows { mask_vals[*idx] = true; }
		}
		let mask = BooleanChunked::from_iter_values("year_mask".into(), mask_vals.into_iter());
		let mut year_df = df.filter(&mask)?;
		if year_df.height() == 0 { continue; }

		let out_dir = output_root.join("csv").join("per_tahun").join(year.to_string());
		std::fs::create_dir_all(&out_dir)?;
		let out_path = out_dir.join(file_name);

		let mut file = File::create(&out_path)?;
		CsvWriter::new(&mut file).finish(&mut year_df)?;
		outputs.push(out_path);
	}

	Ok(outputs)
}

pub fn write_quality_kpi_csv(
	df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
	max_business_date: NaiveDate,
) -> Result<PathBuf> {
	let row_count     = df.height();
	let total_checked = row_count;

	let required_cols: Vec<String> = [
		find_col_by_keywords(df, &["id_transaksi", "transaction_id", "trx"]),
		find_col_by_keywords(df, &["tanggal", "date", "tgl"]),
		find_col_by_keywords(df, &["nama_konsumen", "customer", "konsumen", "pelanggan"]),
		find_col_by_keywords(df, &["kota", "city"]),
		find_col_by_keywords(df, &["kategori", "category", "produk"]),
		find_col_by_keywords(df, &["barang", "item", "product"]),
		find_col_by_keywords(df, &["harga", "price"]),
		find_col_by_keywords(df, &["jumlah", "qty", "quantity"]),
		find_col_by_keywords(df, &["pembayaran", "payment", "metode"]),
	]
	.into_iter()
	.flatten()
	.collect();

	let completeness_passed = count_completeness_passed(df, &required_cols);

	let uniqueness_passed = count_uniqueness_passed(df);

	let validity_passed = (0..row_count)
		.filter(|i| passes_validity(df, *i))
		.count();

	let consistency = evaluate_consistency(df, row_count)?;
	let accuracy = evaluate_accuracy(df, row_count)?;

	let timeliness_passed = (0..row_count)
		.filter(|i| passes_timeliness(df, *i))
		.count();

	let integrity = evaluate_integrity(df, row_count)?;

	let rows = vec![
		build_kpi_row("Completeness", total_checked, completeness_passed, String::new()),
		build_kpi_row("Uniqueness",   total_checked, uniqueness_passed,   String::new()),
		build_kpi_row("Validity",     total_checked, validity_passed,     String::new()),
		build_kpi_row("Consistency",  total_checked, consistency.passed,  consistency.notes),
		build_kpi_row("Accuracy",     total_checked, accuracy.passed,     accuracy.notes),
		build_kpi_row("Timeliness",   total_checked, timeliness_passed,
			format!("cutoff: {}", max_business_date.format("%Y-%m-%d"))),
		build_kpi_row("Integrity",    total_checked, integrity.passed,    integrity.notes),
	];

	let out_dir = output_root.join("csv").join("kpi_kualitas");
	std::fs::create_dir_all(&out_dir)?;

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("kpi_kualitas.csv");
	let out_path = out_dir.join(file_name);

	let mut out = File::create(&out_path)?;
	writeln!(out, "dimension,score,total_checked,passed,failed,notes")?;
	for row in &rows {
		writeln!(out, "{},{},{},{},{},{}",
			csv_escape(&row.dimension), row.score,
			row.total_checked, row.passed, row.failed,
			csv_escape(&row.notes))?;
	}

	Ok(out_path)
}
