use anyhow::{Context, Result};
use chrono::Local;
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::domain::utils::{
	anyvalue_to_f64, anyvalue_to_plain_string, find_col_by_keywords,
};
use crate::output::html_writer::build_report_html;

pub fn write_presentation_report(
	source_csv_path:  &Path,
	final_df:         &DataFrame,
	quarantine_df:    Option<&DataFrame>,
	dropped_dups_count: usize,
	yearly_file_count:  usize,
	output_root:      &Path,
	hard_reject:      bool,
) -> Result<(PathBuf, PathBuf)> {
	let dir = output_root.join("laporan");
	std::fs::create_dir_all(&dir).context("failed to create output/laporan")?;
	let txt_path  = dir.join("presentasi.txt");
	let html_path = dir.join("presentasi.html");

	let total_rows = final_df.height();
	let total_cols = final_df.width();
	let total_quar = quarantine_df.map(|q| q.height()).unwrap_or(0);

	let date_col = find_col_by_keywords(final_df, &["tanggal", "date", "tgl"]);
	let mut min_year: Option<i32> = None;
	let mut max_year: Option<i32> = None;
	if let Some(dc) = date_col {
		let ds = final_df.column(&dc)?;
		for i in 0..final_df.height() {
			let year = ds.get(i).ok()
				.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
				.map(anyvalue_to_plain_string)
				.and_then(|s| s.get(0..4).map(|x| x.to_string()))
				.and_then(|y| y.parse::<i32>().ok());
			if let Some(y) = year {
				min_year = Some(min_year.map(|m: i32| m.min(y)).unwrap_or(y));
				max_year = Some(max_year.map(|m: i32| m.max(y)).unwrap_or(y));
			}
		}
	}

	let mut sev_high   = 0usize;
	let mut sev_medium = 0usize;
	let mut sev_low    = 0usize;
	let mut reason_count: HashMap<String, usize> = HashMap::new();

	if let Some(qdf) = quarantine_df {
		let sev_col = qdf.column("Severity_Karantina").ok();
		let rsn_col = qdf.column("Alasan_Karantina").ok();
		for i in 0..qdf.height() {
			if let Some(sc) = &sev_col {
				let sev = sc.get(i).ok().map(anyvalue_to_plain_string).unwrap_or_default();
				match sev.as_str() {
					"HIGH"   => sev_high   += 1,
					"MEDIUM" => sev_medium += 1,
					"LOW"    => sev_low    += 1,
					_ => {}
				}
			}
			if let Some(rc) = &rsn_col {
				let txt = rc.get(i).ok().map(anyvalue_to_plain_string).unwrap_or_default();
				for part in txt.split('|') {
					let k = part.trim();
					if !k.is_empty() { *reason_count.entry(k.to_string()).or_insert(0) += 1; }
				}
			}
		}
	}

	let mut top_reasons: Vec<(String, usize)> = reason_count.into_iter().collect();
	top_reasons.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

	let cat_col = find_col_by_keywords(final_df, &["kategori", "category", "produk"]);
	let rev_col = find_col_by_keywords(final_df, &["revenue_per_transaction", "revenue"]);
	let mut cat_revenue: HashMap<String, f64> = HashMap::new();
	if let (Some(cc), Some(rc)) = (&cat_col, &rev_col) {
		let cs = final_df.column(cc)?;
		let rs = final_df.column(rc)?;
		for i in 0..final_df.height() {
			let cat = cs.get(i).ok()
				.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
				.map(anyvalue_to_plain_string).unwrap_or_default();
			if cat.trim().is_empty() { continue; }
			if let Some(rv) = rs.get(i).ok().and_then(anyvalue_to_f64) {
				*cat_revenue.entry(cat).or_insert(0.0) += rv;
			}
		}
	}
	let mut top_cats: Vec<(String, f64)> = cat_revenue.into_iter().collect();
	top_cats.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

	let mut content = String::new();
	content.push_str("LAPORAN PRESENTASI AUDIT DATA E-COMMERCE\n");
	content.push_str("============================================================\n\n");
	content.push_str(&format!("Waktu Generate : {}\n", Local::now().format("%Y-%m-%d %H:%M:%S")));
	content.push_str(&format!("Sumber Dataset : {}\n", source_csv_path.display()));
	content.push_str(&format!("Mode Pipeline  : {}\n\n",
		if hard_reject { "KETAT (hard reject + karantina)" } else { "NORMAL" }));

	content.push_str("1. Ringkasan Dataset\n");
	content.push_str(&format!("- Output utama setelah cleaning: {} baris, {} kolom\n", total_rows, total_cols));
	content.push_str(&format!("- Baris karantina: {}\n", total_quar));
	content.push_str(&format!("- Duplikat ID yang dibuang (post-impute): {}\n", dropped_dups_count));
	if let (Some(miny), Some(maxy)) = (min_year, max_year) {
		content.push_str(&format!("- Rentang tahun analisis: {} - {}\n", miny, maxy));
	}
	content.push_str(&format!("- Split data per tahun: {} file\n\n", yearly_file_count));

	content.push_str("2. Kualitas Data (Karantina)\n");
	content.push_str(&format!("- Severity HIGH  : {}\n", sev_high));
	content.push_str(&format!("- Severity MEDIUM: {}\n", sev_medium));
	content.push_str(&format!("- Severity LOW   : {}\n", sev_low));
	if top_reasons.is_empty() {
		content.push_str("- Top alasan anomali: (tidak tersedia)\n\n");
	} else {
		content.push_str("- Top alasan anomali:\n");
		for (r, n) in top_reasons.iter().take(5) {
			content.push_str(&format!("  * {}: {}\n", r, n));
		}
		content.push('\n');
	}

	content.push_str("3. Temuan Utama\n");
	content.push_str("- Pipeline mengoreksi format angka (scientific notation) agar revenue tidak terdistorsi.\n");
	content.push_str("- Data anomali tidak dibuang diam-diam: dipisah ke karantina untuk audit trail transparan.\n");
	content.push_str("- Deduplikasi post-impute aktif sehingga transaksi ganda tidak dobel di hasil akhir.\n");
	if !top_cats.is_empty() {
		content.push_str("- Top kategori berdasarkan total revenue:\n");
		for (cat, rev) in top_cats.iter().take(3) {
			content.push_str(&format!("  * {}: {:.2}\n", cat, rev));
		}
	}
	content.push('\n');

	content.push_str("4. Dampak Bisnis\n");
	content.push_str("- Keputusan bisnis berbasis dashboard lebih aman karena data outlier/invalid terkontrol.\n");
	content.push_str("- Tim audit mendapat prioritas investigasi yang jelas (HIGH/MEDIUM/LOW).\n");
	content.push_str("- Analisis tren tahun-ke-tahun lebih rapi lewat output per tahun.\n\n");

	content.push_str("5. Artefak untuk Presentasi\n");
	content.push_str("- Output bersih   : output/csv/transaksi_clean/\n");
	content.push_str("- Audit log       : output/csv/transaksi_audit_log/\n");
	content.push_str("- KPI kualitas    : output/csv/kpi_kualitas/\n");
	content.push_str("- Karantina       : output/quarantine/\n");
	content.push_str("- Split per tahun : output/csv/per_tahun/\n");
	content.push_str("- Dashboard       : output/html/report.html\n");

	let mut out = File::create(&txt_path)
		.with_context(|| format!("failed to create {}", txt_path.display()))?;
	out.write_all(content.as_bytes())?;

	let html = build_report_html(&content);
	let mut html_out = File::create(&html_path)
		.with_context(|| format!("failed to create {}", html_path.display()))?;
	html_out.write_all(html.as_bytes())?;

	Ok((txt_path, html_path))
}
