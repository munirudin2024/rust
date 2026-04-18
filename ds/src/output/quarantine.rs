use anyhow::{Context, Result};
use chrono::Local;
use polars::prelude::{CsvWriter, DataFrame, SerWriter};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::domain::utils::{
	anyvalue_to_bool, anyvalue_to_f64, anyvalue_to_plain_string,
	find_col_by_keywords, median, parse_trx_suffix,
};

pub fn write_quarantine_csv(
	quarantine_df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<PathBuf> {
	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir).context("failed to create output/quarantine")?;

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("karantina.csv");
	let out_path = dir.join(file_name);

	let mut f = File::create(&out_path)?;
	let mut df = quarantine_df.clone();
	CsvWriter::new(&mut f).finish(&mut df)?;
	Ok(out_path)
}

pub fn write_duplicate_drops_csv(
	dropped_df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<PathBuf> {
	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir).context("failed to create output/quarantine")?;

	let file_name = source_csv_path
		.file_name()
		.and_then(|n| n.to_str())
		.unwrap_or("duplikat_dihapus.csv");
	let out_path = dir.join(format!("duplikat_dihapus_{}", file_name));

	let mut f = File::create(&out_path)?;
	let mut df = dropped_df.clone();
	CsvWriter::new(&mut f).finish(&mut df)?;
	Ok(out_path)
}

pub fn write_id_gap_report(
	df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<Option<PathBuf>> {
	let id_col = find_col_by_keywords(df, &["id_transaksi", "transaction_id", "trx"]);
	let Some(id_name) = id_col else { return Ok(None); };

	let ids = df.column(&id_name)?;
	let mut numbers: Vec<i64> = (0..ids.len())
		.filter_map(|i| {
			ids.get(i).ok()
				.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
				.map(anyvalue_to_plain_string)
				.and_then(|s| parse_trx_suffix(&s))
		})
		.collect();

	if numbers.is_empty() { return Ok(None); }
	numbers.sort_unstable();
	numbers.dedup();

	let min_id = *numbers.first().unwrap_or(&0);
	let max_id = *numbers.last().unwrap_or(&0);
	let id_set: HashSet<i64> = numbers.iter().copied().collect();
	let missing: Vec<i64> = (min_id..=max_id).filter(|n| !id_set.contains(n)).collect();

	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir)?;

	let stem = source_csv_path
		.file_stem()
		.and_then(|s| s.to_str())
		.unwrap_or("dataset");
	let out_path = dir.join(format!("gap_id_transaksi_{}.md", stem));

	let mut content = String::new();
	content.push_str("# Laporan Gap ID Transaksi\n\n");
	content.push_str(&format!("Sumber: {}\n\n", source_csv_path.display()));
	content.push_str(&format!("Rentang numerik ID: {} - {}\n", min_id, max_id));
	content.push_str(&format!("Total ID unik: {}\n", id_set.len()));
	content.push_str(&format!("Total gap ID: {}\n\n", missing.len()));
	content.push_str("## Catatan Auditability\n");
	content.push_str("Setiap gap harus punya alasan resmi di log penghapusan/arsip data.\n\n");
	content.push_str("## Daftar Gap\n");
	if missing.is_empty() {
		content.push_str("Tidak ada gap ID pada rentang ini.\n");
	} else {
		let sample = missing.iter().take(30)
			.map(|n| format!("trx-{}", n))
			.collect::<Vec<_>>()
			.join(", ");
		content.push_str(&format!("{}\n", sample));
	}

	let mut out = File::create(&out_path)?;
	out.write_all(content.as_bytes())?;
	Ok(Some(out_path))
}

pub fn write_quarantine_summary(
	quarantine_df: &DataFrame,
	source_csv_path: &Path,
	output_root: &Path,
) -> Result<PathBuf> {
	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir)?;
	let out_path = dir.join("ringkasan_audit_ketat_senior.md");

	let mut severity_count: HashMap<String, usize> = HashMap::new();
	let mut reason_count:   HashMap<String, usize> = HashMap::new();
	let mut high_priority:  Vec<String> = Vec::new();

	let id_series  = quarantine_df.column("ID_Transaksi").ok();
	let sev_series = quarantine_df.column("Severity_Karantina").ok();
	let rsn_series = quarantine_df.column("Alasan_Karantina").ok();

	for i in 0..quarantine_df.height() {
		let id_val = id_series.as_ref()
			.and_then(|s| s.get(i).ok())
			.map(anyvalue_to_plain_string)
			.unwrap_or_else(|| "(tanpa-id)".to_string());

		let sev = sev_series.as_ref()
			.and_then(|s| s.get(i).ok())
			.map(anyvalue_to_plain_string)
			.unwrap_or_else(|| "UNKNOWN".to_string());
		*severity_count.entry(sev.clone()).or_insert(0) += 1;

		let reason = rsn_series.as_ref()
			.and_then(|s| s.get(i).ok())
			.map(anyvalue_to_plain_string)
			.unwrap_or_else(|| "PERLU_REVIEW_MANUAL".to_string());
		for part in reason.split('|') {
			let k = part.trim();
			if !k.is_empty() { *reason_count.entry(k.to_string()).or_insert(0) += 1; }
		}

		if sev == "HIGH" { high_priority.push(format!("- {}: {}", id_val, reason)); }
	}

	let mut reasons: Vec<(String, usize)> = reason_count.into_iter().collect();
	reasons.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

	let mut c = String::new();
	c.push_str("# Ringkasan Audit Data Ketat\n\n");
	c.push_str(&format!("Tanggal: {}  \n", Local::now().format("%Y-%m-%d %H:%M:%S")));
	c.push_str(&format!("Sumber: {}\n\n", source_csv_path.display()));
	c.push_str("## 1) Ringkasan Eksekutif\n");
	c.push_str(&format!("- Total baris karantina: {}\n", quarantine_df.height()));
	c.push_str(&format!("- Severity HIGH: {}\n",   severity_count.get("HIGH").copied().unwrap_or(0)));
	c.push_str(&format!("- Severity MEDIUM: {}\n", severity_count.get("MEDIUM").copied().unwrap_or(0)));
	c.push_str(&format!("- Severity LOW: {}\n\n",  severity_count.get("LOW").copied().unwrap_or(0)));
	c.push_str("## 2) Top Alasan Karantina\n");
	for (r, n) in reasons.iter().take(10) { c.push_str(&format!("- {}: {}\n", r, n)); }
	c.push('\n');
	c.push_str("## 3) Daftar Prioritas HIGH\n");
	if high_priority.is_empty() {
		c.push_str("- Tidak ada baris severity HIGH pada run ini.\n\n");
	} else {
		for item in &high_priority { c.push_str(item); c.push('\n'); }
		c.push('\n');
	}
	c.push_str("## 4) Rekomendasi\n");
	c.push_str("- Verifikasi manual seluruh severity HIGH lebih dulu.\n");
	c.push_str("- Perbaiki validasi input sumber data untuk alasan dominan.\n");
	c.push_str("- Pantau tren jumlah karantina per run sebagai KPI data quality.\n");

	let mut out = File::create(&out_path)?;
	out.write_all(c.as_bytes())?;
	Ok(out_path)
}

pub fn write_budi_santoso_investigation(
	df: &DataFrame,
	output_root: &Path,
) -> Result<PathBuf> {
	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir)?;
	let out_path = dir.join("investigasi_budi_santoso.md");

	let customer_col = find_col_by_keywords(df, &["nama_konsumen", "customer", "konsumen", "nama"]);
	let Some(cn) = customer_col else {
		let mut f = File::create(&out_path)?;
		f.write_all(b"# Investigasi Budi Santoso\n\nKolom customer tidak ditemukan.\n")?;
		return Ok(out_path);
	};

	let cs          = df.column(&cn)?;
	let revenue_col = find_col_by_keywords(df, &["revenue_per_transaction", "revenue"]);
	let city_col    = find_col_by_keywords(df, &["kota", "city"]);
	let date_col    = find_col_by_keywords(df, &["tanggal", "date", "tgl"]);
	let qty_ext     = df.column("Qty_Ekstrem").ok();

	let mut total_rows = 0usize;
	let mut total_rev  = 0.0f64;
	let mut extreme    = 0usize;
	let mut cities: HashMap<String, usize> = HashMap::new();
	let mut years:  HashMap<String, usize> = HashMap::new();

	for i in 0..df.height() {
		let name = cs.get(i).ok()
			.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
			.map(anyvalue_to_plain_string)
			.unwrap_or_default();
		if !name.eq_ignore_ascii_case("Budi Santoso") { continue; }
		total_rows += 1;

		if let Some(rc) = &revenue_col {
			if let Some(rv) = df.column(rc).ok().and_then(|s| s.get(i).ok()).and_then(anyvalue_to_f64) {
				total_rev += rv;
			}
		}
		if let Some(cc) = &city_col {
			let city = df.column(cc).ok().and_then(|s| s.get(i).ok())
				.map(anyvalue_to_plain_string).unwrap_or_default();
			if !city.trim().is_empty() { *cities.entry(city).or_insert(0) += 1; }
		}
		if let Some(dc) = &date_col {
			let raw = df.column(dc).ok().and_then(|s| s.get(i).ok())
				.map(anyvalue_to_plain_string).unwrap_or_default();
			if let Some(y) = raw.get(0..4) { *years.entry(y.to_string()).or_insert(0) += 1; }
		}
		if let Some(qe) = &qty_ext {
			if qe.get(i).ok().map(anyvalue_to_bool).unwrap_or(false) { extreme += 1; }
		}
	}

	let vip  = total_rows >= 3 || total_rev >= 5_000_000.0;
	let test = total_rows == 0 || (total_rows <= 1 && total_rev == 0.0);
	let mut city_vec: Vec<(String, usize)> = cities.into_iter().collect();
	city_vec.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
	let mut year_vec: Vec<(String, usize)> = years.into_iter().collect();
	year_vec.sort_by_key(|(y, _)| y.clone());

	let mut c = String::new();
	c.push_str("# Investigasi Budi Santoso\n\n");
	c.push_str(&format!("Tanggal: {}\n\n", Local::now().format("%Y-%m-%d %H:%M:%S")));
	c.push_str("## Ringkasan\n");
	c.push_str(&format!("- Jumlah transaksi: {}\n", total_rows));
	c.push_str(&format!("- Total revenue: {:.2}\n", total_rev));
	c.push_str(&format!("- Qty ekstrem: {}\n", extreme));
	c.push_str(&format!("- Indikasi VIP: {}\n",       if vip  { "YA" } else { "TIDAK" }));
	c.push_str(&format!("- Indikasi data test: {}\n\n", if test { "YA" } else { "TIDAK" }));
	c.push_str("## Sebaran Kota\n");
	if city_vec.is_empty() { c.push_str("- (tidak ada data)\n"); }
	else { for (city, n) in &city_vec { c.push_str(&format!("- {}: {}\n", city, n)); } }
	c.push_str("\n## Sebaran Tahun\n");
	if year_vec.is_empty() { c.push_str("- (tidak ada data)\n"); }
	else { for (year, n) in &year_vec { c.push_str(&format!("- {}: {}\n", year, n)); } }

	let mut out = File::create(&out_path)?;
	out.write_all(c.as_bytes())?;
	Ok(out_path)
}

pub fn write_hidden_insights(
	analysis_df:    &DataFrame,
	final_df:       &DataFrame,
	quarantine_df:  &DataFrame,
	dropped_dups:   Option<&DataFrame>,
	output_root:    &Path,
) -> Result<PathBuf> {
	let dir = output_root.join("quarantine");
	std::fs::create_dir_all(&dir)?;
	let out_path = dir.join("data_tersembunyi.md");

	let customer_col  = find_col_by_keywords(analysis_df, &["nama_konsumen", "customer", "konsumen", "nama"]);
	let revenue_col   = find_col_by_keywords(analysis_df, &["revenue_per_transaction", "revenue"]);
	let city_col      = find_col_by_keywords(analysis_df, &["kota", "city"]);
	let date_col      = find_col_by_keywords(analysis_df, &["tanggal", "date", "tgl"]);
	let category_col  = find_col_by_keywords(analysis_df, &["kategori", "category", "produk"]);
	let item_col      = find_col_by_keywords(analysis_df, &["barang", "item", "product"]);
	let id_col        = find_col_by_keywords(analysis_df, &["id_transaksi", "transaction_id", "trx"]);

	let mut budi_count = 0usize;
	let mut budi_rev   = 0.0f64;
	let mut budi_cities: HashSet<String> = HashSet::new();
	let mut budi_years:  HashSet<String> = HashSet::new();

	if let Some(cn) = &customer_col {
		let cs = analysis_df.column(cn)?;
		for i in 0..analysis_df.height() {
			let name = cs.get(i).ok()
				.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
				.map(anyvalue_to_plain_string).unwrap_or_default();
			if !name.eq_ignore_ascii_case("Budi Santoso") { continue; }
			budi_count += 1;
			if let Some(rc) = &revenue_col {
				if let Some(rv) = analysis_df.column(rc).ok()
					.and_then(|s| s.get(i).ok()).and_then(anyvalue_to_f64) { budi_rev += rv; }
			}
			if let Some(cc) = &city_col {
				let city = analysis_df.column(cc).ok()
					.and_then(|s| s.get(i).ok()).map(anyvalue_to_plain_string).unwrap_or_default();
				if !city.trim().is_empty() { budi_cities.insert(city); }
			}
			if let Some(dc) = &date_col {
				let raw = analysis_df.column(dc).ok()
					.and_then(|s| s.get(i).ok()).map(anyvalue_to_plain_string).unwrap_or_default();
				if let Some(y) = raw.get(0..4) { budi_years.insert(y.to_string()); }
			}
		}
	}

	let mut qty_extreme_count = 0usize;
	let mut qty_extreme_items: Vec<String> = Vec::new();
	if let Ok(qe) = quarantine_df.column("Qty_Ekstrem") {
		for i in 0..quarantine_df.height() {
			if qe.get(i).ok().map(anyvalue_to_bool).unwrap_or(false) {
				qty_extreme_count += 1;
				if let Some(ic) = &item_col {
					let item = quarantine_df.column(ic).ok()
						.and_then(|s| s.get(i).ok()).map(anyvalue_to_plain_string).unwrap_or_default();
					if !item.trim().is_empty() { qty_extreme_items.push(item); }
				}
			}
		}
	}
	qty_extreme_items.sort();
	qty_extreme_items.dedup();

	let mut skew_rows: Vec<(String, f64, f64, usize)> = Vec::new();
	if let (Some(cc), Some(rc)) = (&category_col, &revenue_col) {
		let cseries = analysis_df.column(cc)?;
		let rseries = analysis_df.column(rc)?;
		let mut groups: HashMap<String, Vec<f64>> = HashMap::new();
		for i in 0..analysis_df.height() {
			let cat = cseries.get(i).ok()
				.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
				.map(anyvalue_to_plain_string).unwrap_or_default();
			if cat.trim().is_empty() { continue; }
			if let Some(rv) = rseries.get(i).ok().and_then(anyvalue_to_f64) {
				groups.entry(cat).or_default().push(rv);
			}
		}
		for (cat, mut vals) in groups {
			if vals.is_empty() { continue; }
			let mean = vals.iter().sum::<f64>() / vals.len() as f64;
			let med  = median(&mut vals).unwrap_or(0.0);
			skew_rows.push((cat, mean, med, vals.len()));
		}
		skew_rows.sort_by(|a, b| (b.1 - b.2).abs().partial_cmp(&(a.1 - a.2).abs())
			.unwrap_or(std::cmp::Ordering::Equal));
	}

	let count_trx = |df: &DataFrame, target: &str| -> usize {
		find_col_by_keywords(df, &["id_transaksi", "transaction_id", "trx"])
			.and_then(|col| df.column(&col).ok())
			.map(|s| (0..s.len()).filter(|i| {
				s.get(*i).ok().map(anyvalue_to_plain_string).as_deref() == Some(target)
			}).count())
			.unwrap_or(0)
	};

	let trx_final     = count_trx(final_df, "trx-1045");
	let trx_quarantine= count_trx(quarantine_df, "trx-1045");
	let trx_dropped   = dropped_dups.map(|d| count_trx(d, "trx-1045")).unwrap_or(0);

	let _ = id_col;

	let mut c = String::new();
	c.push_str("# Data Tersembunyi yang Bisa Digali\n\n");
	c.push_str(&format!("Tanggal: {}\n\n", Local::now().format("%Y-%m-%d %H:%M:%S")));
	c.push_str("## 1) Budi Santoso - Loyal atau Anomali?\n");
	c.push_str(&format!("- Muncul: {} transaksi\n", budi_count));
	c.push_str(&format!("- Total revenue: {:.2}\n", budi_rev));
	c.push_str(&format!("- Sebaran kota unik: {}\n", budi_cities.len()));
	c.push_str(&format!("- Sebaran tahun unik: {}\n", budi_years.len()));
	c.push_str("- Catatan: cross-check ke master customer untuk konfirmasi VIP vs data test.\n\n");
	c.push_str("## 2) Pola Qty Ekstrem\n");
	c.push_str(&format!("- Baris Qty_Ekstrem di karantina: {}\n", qty_extreme_count));
	if !qty_extreme_items.is_empty() {
		c.push_str(&format!("- Contoh item: {}\n",
			qty_extreme_items.iter().take(8).cloned().collect::<Vec<_>>().join(", ")));
	}
	c.push('\n');
	c.push_str("## 3) Sinyal Median vs Mean per Kategori\n");
	if skew_rows.is_empty() {
		c.push_str("- Data tidak cukup untuk analisis skew.\n\n");
	} else {
		for (cat, mean, med, n) in skew_rows.iter().take(3) {
			c.push_str(&format!("- {}: mean {:.2}, median {:.2}, n={}\n", cat, mean, med, n));
		}
		c.push('\n');
	}
	c.push_str("## 4) Status Duplikat trx-1045\n");
	c.push_str(&format!("- Tersisa di output utama: {}\n", trx_final));
	c.push_str(&format!("- Tersisa di karantina: {}\n",   trx_quarantine));
	c.push_str(&format!("- Dibuang saat dedup: {}\n",     trx_dropped));

	let mut out = File::create(&out_path)?;
	out.write_all(c.as_bytes())?;
	Ok(out_path)
}
