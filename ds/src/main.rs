//! # Rust CLI Orchestrator
//!
//! Entry point for audit -> clean -> visualization pipeline.

use anyhow::{Context, Result};
use colored::*;
use polars::prelude::{BooleanChunked, CsvWriter, NamedFrom, NewChunkedArray, SerWriter};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

mod audit;
mod clean;
mod google_sheets;
mod looker_auto;
mod viz;

struct RunConfig {
    input_path: String,
    hard_reject: bool,
}

/// Validate CLI input and ensure path exists.
fn validate_args() -> Result<RunConfig> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        anyhow::bail!("Usage: cargo run -- <path_to_csv_or_folder> [--hard-reject]");
    }

    let mut input_path: Option<String> = None;
    let mut hard_reject = false;

    for arg in args.iter().skip(1) {
        if arg == "--hard-reject" {
            hard_reject = true;
        } else if input_path.is_none() {
            input_path = Some(arg.clone());
        } else {
            anyhow::bail!("Argumen tidak dikenal: {arg}");
        }
    }

    let path = input_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Path input belum diberikan"))?;
    let p = Path::new(path);

    if !p.exists() {
        anyhow::bail!("File not found: {}", path);
    }

    if p.is_file() {
        let ext = p.extension().and_then(|e| e.to_str()).unwrap_or_default();
        if !ext.eq_ignore_ascii_case("csv") {
            anyhow::bail!(
                "Input harus file CSV atau folder berisi CSV. Jika dataset masih ZIP, ekstrak dulu"
            );
        }
    } else if !p.is_dir() {
        anyhow::bail!("Path harus berupa file CSV atau folder yang valid");
    }

    Ok(RunConfig {
        input_path: path.to_string(),
        hard_reject,
    })
}

fn gather_csv_paths(input_path: &str) -> Result<Vec<PathBuf>> {
    let p = Path::new(input_path);
    if p.is_file() {
        return Ok(vec![p.to_path_buf()]);
    }

    let mut csvs = Vec::<PathBuf>::new();
    for entry in std::fs::read_dir(p).context("failed to read input directory")? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if ext == "csv" {
            csvs.push(path);
        }
    }

    csvs.sort();
    if csvs.is_empty() {
        anyhow::bail!("Tidak ada file CSV ditemukan di folder input");
    }
    Ok(csvs)
}

fn print_banner() {
    println!();
    println!(
        "{}",
        "╔══════════════════════════════════════════════╗".bold()
    );
    println!(
        "{}",
        "║           Pipeline Pembersihan Data          ║".bold()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════╝".bold()
    );
    println!();
}

fn print_final_summary(
    output_path: &str,
    json_path: &str,
    looker_path: &str,
    google_sheets_url: Option<&str>,
    station_count: usize,
) {
    println!();
    println!(
        "{}",
        "── [4/4] MEMBUAT LAPORAN ─────────────────────".bold()
    );
    println!("  {} Dataset  : {}", "".cyan(), station_count);
    println!("  {} HTML     : {}", "".cyan(), output_path);
    println!("  {} JSON     : {}", "".cyan(), json_path);
    println!("  {} Looker   : {}", "".cyan(), looker_path);
    println!(
        "  {} Sheets   : {}",
        "".cyan(),
        google_sheets_url.unwrap_or("belum dikonfigurasi")
    );
    println!();
    println!(
        "{}",
        "╔══════════════════════════════════════════════╗".bold()
    );
    println!(
        "{}",
        "║      PIPELINE SELESAI — Buka index.html      ║".bold()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════╝".bold()
    );
    println!();
}

fn write_cleaned_csv(
    clean_df: &polars::prelude::DataFrame,
    source_csv_path: &Path,
    output_root: &Path,
) -> Result<PathBuf> {
    let csv_output_dir = output_root.join("csv");
    std::fs::create_dir_all(&csv_output_dir).context("failed to create output/csv directory")?;

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

fn find_col_by_keywords(df: &polars::prelude::DataFrame, words: &[&str]) -> Option<String> {
    df.get_column_names().iter().find_map(|n| {
        let low = n.to_ascii_lowercase();
        if words.iter().any(|w| low.contains(&w.to_ascii_lowercase())) {
            Some((*n).to_string())
        } else {
            None
        }
    })
}

fn deduplicate_transaction_ids(
    df: polars::prelude::DataFrame,
) -> Result<(
    polars::prelude::DataFrame,
    Option<polars::prelude::DataFrame>,
    usize,
)> {
    let id_col = find_col_by_keywords(&df, &["id_transaksi", "transaction_id", "trx"]);
    let Some(id_name) = id_col else {
        return Ok((df, None, 0));
    };

    let id_series = df.column(&id_name)?;
    let col_names = df.get_column_names();

    let mut best_by_id: HashMap<String, (usize, i64)> = HashMap::new();
    let mut rows_by_id: HashMap<String, Vec<usize>> = HashMap::new();

    for i in 0..df.height() {
        let id_val = id_series
            .get(i)
            .ok()
            .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
            .map(anyvalue_to_plain_string)
            .map(|s| s.trim().to_string());

        let Some(id) = id_val else {
            continue;
        };
        if id.is_empty() {
            continue;
        }

        let mut score: i64 = 0;
        for col in &col_names {
            if let Ok(series) = df.column(col) {
                if let Ok(v) = series.get(i) {
                    if !matches!(v, polars::prelude::AnyValue::Null) {
                        let txt = anyvalue_to_plain_string(v);
                        if !txt.trim().is_empty() && txt.trim() != "unknown" {
                            score += 1;
                        }
                    }
                }
            }
        }

        rows_by_id.entry(id.clone()).or_default().push(i);
        let entry = best_by_id.entry(id).or_insert((i, score));
        if score > entry.1 {
            *entry = (i, score);
        }
    }

    let mut keep = vec![true; df.height()];
    let mut dropped_count = 0_usize;

    for (id, rows) in &rows_by_id {
        if rows.len() <= 1 {
            continue;
        }
        if let Some((best_idx, _)) = best_by_id.get(id) {
            for idx in rows {
                if idx != best_idx {
                    keep[*idx] = false;
                    dropped_count += 1;
                }
            }
        }
    }

    if dropped_count == 0 {
        return Ok((df, None, 0));
    }

    let drop_vals: Vec<bool> = keep.iter().map(|v| !*v).collect();
    let keep_mask = BooleanChunked::from_iter_values("keep_dedup".into(), keep.iter().copied());
    let drop_mask = BooleanChunked::from_iter_values("drop_dedup".into(), drop_vals.into_iter());

    let filtered = df.filter(&keep_mask)?;
    let dropped = df.filter(&drop_mask)?;
    Ok((filtered, Some(dropped), dropped_count))
}

fn write_quarantine_csv(
    quarantine_df: &polars::prelude::DataFrame,
    source_csv_path: &Path,
    output_root: &Path,
) -> Result<PathBuf> {
    let quarantine_dir = output_root.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)
        .context("failed to create output/quarantine directory")?;

    let file_name = source_csv_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("karantina.csv");
    let quarantine_path = quarantine_dir.join(file_name);

    let mut output_file = File::create(&quarantine_path)
        .with_context(|| format!("failed to create {}", quarantine_path.display()))?;
    let mut df_to_write = quarantine_df.clone();
    CsvWriter::new(&mut output_file)
        .finish(&mut df_to_write)
        .with_context(|| format!("failed to write {}", quarantine_path.display()))?;

    Ok(quarantine_path)
}

fn write_duplicate_drops_csv(
    dropped_df: &polars::prelude::DataFrame,
    source_csv_path: &Path,
    output_root: &Path,
) -> Result<PathBuf> {
    let quarantine_dir = output_root.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)
        .context("failed to create output/quarantine directory")?;

    let file_name = source_csv_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("duplikat_dihapus.csv");
    let drops_path = quarantine_dir.join(format!("duplikat_dihapus_{}", file_name));

    let mut output_file = File::create(&drops_path)
        .with_context(|| format!("failed to create {}", drops_path.display()))?;
    let mut df_to_write = dropped_df.clone();
    CsvWriter::new(&mut output_file)
        .finish(&mut df_to_write)
        .with_context(|| format!("failed to write {}", drops_path.display()))?;

    Ok(drops_path)
}

fn write_yearly_splits(
    df: &polars::prelude::DataFrame,
    source_csv_path: &Path,
    output_root: &Path,
) -> Result<Vec<PathBuf>> {
    let date_col = find_col_by_keywords(df, &["tanggal", "date", "tgl"]);
    let Some(date_name) = date_col else {
        return Ok(Vec::new());
    };

    let ds = df.column(&date_name)?;
    let mut year_rows: HashMap<i32, Vec<usize>> = HashMap::new();

    for i in 0..df.height() {
        let year_opt = ds
            .get(i)
            .ok()
            .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
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
            for idx in rows {
                mask_vals[*idx] = true;
            }
        }
        let mask = BooleanChunked::from_iter_values("year_mask".into(), mask_vals.into_iter());
        let mut year_df = df.filter(&mask)?;
        if year_df.height() == 0 {
            continue;
        }

        let out_dir = output_root.join("csv").join("per_tahun").join(year.to_string());
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("failed to create {}", out_dir.display()))?;
        let out_path = out_dir.join(file_name);

        let mut file = File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        CsvWriter::new(&mut file)
            .finish(&mut year_df)
            .with_context(|| format!("failed to write {}", out_path.display()))?;
        outputs.push(out_path);
    }

    Ok(outputs)
}

fn write_budi_santoso_investigation(
    df: &polars::prelude::DataFrame,
    output_root: &Path,
) -> Result<PathBuf> {
    let out_dir = output_root.join("quarantine");
    std::fs::create_dir_all(&out_dir).context("failed to create output/quarantine directory")?;
    let out_path = out_dir.join("investigasi_budi_santoso.md");

    let customer_col = find_col_by_keywords(df, &["nama_konsumen", "customer", "konsumen", "nama"]);
    let Some(customer_name_col) = customer_col else {
        let mut file = File::create(&out_path)?;
        file.write_all(b"# Investigasi Budi Santoso\n\nKolom customer tidak ditemukan.\n")?;
        return Ok(out_path);
    };

    let cs = df.column(&customer_name_col)?;
    let revenue_col = find_col_by_keywords(df, &["revenue_per_transaction", "revenue"]);
    let city_col = find_col_by_keywords(df, &["kota", "city"]);
    let date_col = find_col_by_keywords(df, &["tanggal", "date", "tgl"]);
    let qty_extreme_col = df.column("Qty_Ekstrem").ok();

    let mut total_rows = 0_usize;
    let mut total_revenue = 0.0_f64;
    let mut extreme_count = 0_usize;
    let mut cities: HashMap<String, usize> = HashMap::new();
    let mut years: HashMap<String, usize> = HashMap::new();

    for i in 0..df.height() {
        let name = cs
            .get(i)
            .ok()
            .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
            .map(anyvalue_to_plain_string)
            .unwrap_or_default();

        if !name.eq_ignore_ascii_case("Budi Santoso") {
            continue;
        }

        total_rows += 1;

        if let Some(rc) = &revenue_col {
            if let Ok(rs) = df.column(rc) {
                if let Ok(v) = rs.get(i) {
                    if let Ok(val) = anyvalue_to_plain_string(v).parse::<f64>() {
                        total_revenue += val;
                    }
                }
            }
        }

        if let Some(cc) = &city_col {
            if let Ok(series) = df.column(cc) {
                if let Ok(v) = series.get(i) {
                    let key = anyvalue_to_plain_string(v);
                    if !key.trim().is_empty() {
                        *cities.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }

        if let Some(dc) = &date_col {
            if let Ok(series) = df.column(dc) {
                if let Ok(v) = series.get(i) {
                    let raw = anyvalue_to_plain_string(v);
                    if let Some(y) = raw.get(0..4) {
                        *years.entry(y.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }

        if let Some(qe) = &qty_extreme_col {
            if let Ok(v) = qe.get(i) {
                if anyvalue_to_bool(v) {
                    extreme_count += 1;
                }
            }
        }
    }

    let vip_indicator = total_rows >= 3 || total_revenue >= 5_000_000.0;
    let test_indicator = total_rows == 0 || (total_rows <= 1 && total_revenue == 0.0);

    let mut city_vec: Vec<(String, usize)> = cities.into_iter().collect();
    city_vec.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut year_vec: Vec<(String, usize)> = years.into_iter().collect();
    year_vec.sort_by(|a, b| a.0.cmp(&b.0));

    let mut content = String::new();
    content.push_str("# Investigasi Budi Santoso\n\n");
    content.push_str(&format!(
        "Tanggal: {}\n\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    content.push_str("## Ringkasan\n");
    content.push_str(&format!("- Jumlah transaksi: {}\n", total_rows));
    content.push_str(&format!("- Total revenue: {:.2}\n", total_revenue));
    content.push_str(&format!("- Qty ekstrem: {}\n", extreme_count));
    content.push_str(&format!("- Indikasi VIP: {}\n", if vip_indicator { "YA" } else { "TIDAK" }));
    content.push_str(&format!(
        "- Indikasi data test: {}\n\n",
        if test_indicator { "YA" } else { "TIDAK" }
    ));

    content.push_str("## Sebaran Kota\n");
    if city_vec.is_empty() {
        content.push_str("- (tidak ada data)\n");
    } else {
        for (city, cnt) in city_vec {
            content.push_str(&format!("- {}: {}\n", city, cnt));
        }
    }
    content.push_str("\n## Sebaran Tahun\n");
    if year_vec.is_empty() {
        content.push_str("- (tidak ada data)\n");
    } else {
        for (year, cnt) in year_vec {
            content.push_str(&format!("- {}: {}\n", year, cnt));
        }
    }

    let mut out = File::create(&out_path)
        .with_context(|| format!("failed to create {}", out_path.display()))?;
    out.write_all(content.as_bytes())
        .with_context(|| format!("failed to write {}", out_path.display()))?;

    Ok(out_path)
}

fn write_quarantine_summary(
    quarantine_df: &polars::prelude::DataFrame,
    source_csv_path: &Path,
    output_root: &Path,
) -> Result<PathBuf> {
    let quarantine_dir = output_root.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)
        .context("failed to create output/quarantine directory")?;

    let summary_path = quarantine_dir.join("ringkasan_audit_ketat_senior.md");

    let mut severity_count: HashMap<String, usize> = HashMap::new();
    let mut reason_count: HashMap<String, usize> = HashMap::new();
    let mut high_priority: Vec<String> = Vec::new();

    let id_series = quarantine_df.column("ID_Transaksi").ok();
    let severity_series = quarantine_df.column("Severity_Karantina").ok();
    let reason_series = quarantine_df.column("Alasan_Karantina").ok();

    for i in 0..quarantine_df.height() {
        let id_val = id_series
            .as_ref()
            .and_then(|s| s.get(i).ok())
            .map(anyvalue_to_plain_string)
            .unwrap_or_else(|| "(tanpa-id)".to_string());

        let sev_val = severity_series
            .as_ref()
            .and_then(|s| s.get(i).ok())
            .map(anyvalue_to_plain_string)
            .unwrap_or_else(|| "UNKNOWN".to_string());
        *severity_count.entry(sev_val.clone()).or_insert(0) += 1;

        let reason_val = reason_series
            .as_ref()
            .and_then(|s| s.get(i).ok())
            .map(anyvalue_to_plain_string)
            .unwrap_or_else(|| "PERLU_REVIEW_MANUAL".to_string());

        for reason in reason_val.split('|') {
            let key = reason.trim();
            if !key.is_empty() {
                *reason_count.entry(key.to_string()).or_insert(0) += 1;
            }
        }

        if sev_val == "HIGH" {
            high_priority.push(format!("- {}: {}", id_val, reason_val));
        }
    }

    let mut reason_sorted: Vec<(String, usize)> = reason_count.into_iter().collect();
    reason_sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut content = String::new();
    content.push_str("# Ringkasan Audit Data Ketat\n\n");
    content.push_str(&format!(
        "Tanggal: {}  \n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    content.push_str(&format!("Sumber: {}\n\n", source_csv_path.display()));

    content.push_str("## 1) Ringkasan Eksekutif\n");
    content.push_str(&format!("- Total baris karantina: {}\n", quarantine_df.height()));
    content.push_str(&format!(
        "- Severity HIGH: {} baris\n",
        severity_count.get("HIGH").copied().unwrap_or(0)
    ));
    content.push_str(&format!(
        "- Severity MEDIUM: {} baris\n",
        severity_count.get("MEDIUM").copied().unwrap_or(0)
    ));
    content.push_str(&format!(
        "- Severity LOW: {} baris\n\n",
        severity_count.get("LOW").copied().unwrap_or(0)
    ));

    content.push_str("## 2) Top Alasan Karantina\n");
    for (reason, count) in reason_sorted.iter().take(10) {
        content.push_str(&format!("- {}: {}\n", reason, count));
    }
    content.push_str("\n");

    content.push_str("## 3) Daftar Prioritas HIGH\n");
    if high_priority.is_empty() {
        content.push_str("- Tidak ada baris severity HIGH pada run ini.\n\n");
    } else {
        for item in &high_priority {
            content.push_str(item);
            content.push('\n');
        }
        content.push('\n');
    }

    content.push_str("## 4) Dampak ke Analisis\n");
    content.push_str("- Mode hard reject mencegah baris berisiko masuk dataset utama.\n");
    content.push_str("- Dataset utama dipakai untuk analisis operasional.\n");
    content.push_str("- File karantina dipakai untuk investigasi data quality.\n\n");

    content.push_str("## 5) Rekomendasi Tindak Lanjut\n");
    content.push_str("- Verifikasi manual seluruh severity HIGH lebih dulu.\n");
    content.push_str("- Perbaiki validasi input sumber data untuk alasan dominan.\n");
    content.push_str("- Pantau tren jumlah karantina per run sebagai KPI data quality.\n");

    let mut out = File::create(&summary_path)
        .with_context(|| format!("failed to create {}", summary_path.display()))?;
    out.write_all(content.as_bytes())
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    Ok(summary_path)
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    if n % 2 == 0 {
        Some((values[n / 2 - 1] + values[n / 2]) / 2.0)
    } else {
        Some(values[n / 2])
    }
}

fn anyvalue_to_f64(v: polars::prelude::AnyValue<'_>) -> Option<f64> {
    match v {
        polars::prelude::AnyValue::Float64(x) => Some(x),
        polars::prelude::AnyValue::Float32(x) => Some(x as f64),
        polars::prelude::AnyValue::Int64(x) => Some(x as f64),
        polars::prelude::AnyValue::Int32(x) => Some(x as f64),
        polars::prelude::AnyValue::Int16(x) => Some(x as f64),
        polars::prelude::AnyValue::Int8(x) => Some(x as f64),
        polars::prelude::AnyValue::UInt64(x) => Some(x as f64),
        polars::prelude::AnyValue::UInt32(x) => Some(x as f64),
        polars::prelude::AnyValue::UInt16(x) => Some(x as f64),
        polars::prelude::AnyValue::UInt8(x) => Some(x as f64),
        polars::prelude::AnyValue::String(s) => s.parse::<f64>().ok(),
        _ => anyvalue_to_plain_string(v).parse::<f64>().ok(),
    }
}

fn write_hidden_insights(
    analysis_df: &polars::prelude::DataFrame,
    final_df: &polars::prelude::DataFrame,
    quarantine_df: &polars::prelude::DataFrame,
    dropped_dups_df: Option<&polars::prelude::DataFrame>,
    output_root: &Path,
) -> Result<PathBuf> {
    let out_dir = output_root.join("quarantine");
    std::fs::create_dir_all(&out_dir).context("failed to create output/quarantine directory")?;
    let out_path = out_dir.join("data_tersembunyi.md");

    let customer_col =
        find_col_by_keywords(analysis_df, &["nama_konsumen", "customer", "konsumen", "nama"]);
    let revenue_col = find_col_by_keywords(analysis_df, &["revenue_per_transaction", "revenue"]);
    let city_col = find_col_by_keywords(analysis_df, &["kota", "city"]);
    let date_col = find_col_by_keywords(analysis_df, &["tanggal", "date", "tgl"]);
    let category_col = find_col_by_keywords(analysis_df, &["kategori", "category", "produk"]);
    let item_col = find_col_by_keywords(analysis_df, &["barang", "item", "product"]);
    let id_col = find_col_by_keywords(analysis_df, &["id_transaksi", "transaction_id", "trx"]);

    let mut budi_count = 0_usize;
    let mut budi_rev = 0.0_f64;
    let mut budi_cities: HashSet<String> = HashSet::new();
    let mut budi_years: HashSet<String> = HashSet::new();

    if let Some(cn) = &customer_col {
        let cs = analysis_df.column(cn)?;
        for i in 0..analysis_df.height() {
            let name = cs
                .get(i)
                .ok()
                .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .unwrap_or_default();

            if !name.eq_ignore_ascii_case("Budi Santoso") {
                continue;
            }

            budi_count += 1;
            if let Some(rc) = &revenue_col {
                if let Ok(s) = analysis_df.column(rc) {
                    if let Ok(v) = s.get(i) {
                        if let Some(val) = anyvalue_to_f64(v) {
                            budi_rev += val;
                        }
                    }
                }
            }

            if let Some(cc) = &city_col {
                if let Ok(s) = analysis_df.column(cc) {
                    if let Ok(v) = s.get(i) {
                        let city = anyvalue_to_plain_string(v);
                        if !city.trim().is_empty() {
                            budi_cities.insert(city);
                        }
                    }
                }
            }

            if let Some(dc) = &date_col {
                if let Ok(s) = analysis_df.column(dc) {
                    if let Ok(v) = s.get(i) {
                        let raw = anyvalue_to_plain_string(v);
                        if let Some(y) = raw.get(0..4) {
                            budi_years.insert(y.to_string());
                        }
                    }
                }
            }
        }
    }

    let mut qty_extreme_count = 0_usize;
    let mut qty_extreme_items: Vec<String> = Vec::new();
    if let Ok(qe) = quarantine_df.column("Qty_Ekstrem") {
        for i in 0..quarantine_df.height() {
            let is_extreme = qe.get(i).ok().map(anyvalue_to_bool).unwrap_or(false);
            if is_extreme {
                qty_extreme_count += 1;
                if let Some(ic) = &item_col {
                    if let Ok(s) = quarantine_df.column(ic) {
                        if let Ok(v) = s.get(i) {
                            let item = anyvalue_to_plain_string(v);
                            if !item.trim().is_empty() {
                                qty_extreme_items.push(item);
                            }
                        }
                    }
                }
            }
        }
    }
    qty_extreme_items.sort();
    qty_extreme_items.dedup();

    let mut skew_rows: Vec<(String, f64, f64, usize)> = Vec::new();
    if let (Some(cat_col), Some(rev_col)) = (&category_col, &revenue_col) {
        let cseries = analysis_df.column(cat_col)?;
        let rseries = analysis_df.column(rev_col)?;
        let mut groups: HashMap<String, Vec<f64>> = HashMap::new();

        for i in 0..analysis_df.height() {
            let cat = cseries
                .get(i)
                .ok()
                .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .unwrap_or_default();
            if cat.trim().is_empty() {
                continue;
            }

            if let Ok(v) = rseries.get(i) {
                if let Some(rv) = anyvalue_to_f64(v) {
                    groups.entry(cat).or_default().push(rv);
                }
            }
        }

        for (cat, mut vals) in groups {
            if vals.is_empty() {
                continue;
            }
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let med = median(&mut vals).unwrap_or(0.0);
            skew_rows.push((cat, mean, med, vals.len()));
        }
        skew_rows.sort_by(|a, b| {
            (b.1 - b.2)
                .abs()
                .partial_cmp(&(a.1 - a.2).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    let mut trx_1045_final = 0_usize;
    let mut trx_1045_quarantine = 0_usize;
    let mut trx_1045_dropped = 0_usize;
    if let Some(id_name) = id_col {
        if let Ok(s) = final_df.column(&id_name) {
            for i in 0..final_df.height() {
                if let Ok(v) = s.get(i) {
                    if anyvalue_to_plain_string(v) == "trx-1045" {
                        trx_1045_final += 1;
                    }
                }
            }
        }
        if let Ok(s) = quarantine_df.column(&id_name) {
            for i in 0..quarantine_df.height() {
                if let Ok(v) = s.get(i) {
                    if anyvalue_to_plain_string(v) == "trx-1045" {
                        trx_1045_quarantine += 1;
                    }
                }
            }
        }
        if let Some(dd) = dropped_dups_df {
            if let Ok(s) = dd.column(&id_name) {
                for i in 0..dd.height() {
                    if let Ok(v) = s.get(i) {
                        if anyvalue_to_plain_string(v) == "trx-1045" {
                            trx_1045_dropped += 1;
                        }
                    }
                }
            }
        }
    }

    let mut content = String::new();
    content.push_str("# Data Tersembunyi yang Bisa Digali\n\n");
    content.push_str(&format!(
        "Tanggal: {}\n\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));

    content.push_str("## 1) Budi Santoso — Loyal atau Anomali?\n");
    content.push_str(&format!("- Muncul: {} transaksi\n", budi_count));
    content.push_str(&format!("- Total revenue (pasca-cleaning): {:.2}\n", budi_rev));
    content.push_str(&format!("- Sebaran kota unik: {}\n", budi_cities.len()));
    content.push_str(&format!("- Sebaran tahun unik: {}\n", budi_years.len()));
    content.push_str("- Catatan: perlu cross-check ke master customer untuk konfirmasi VIP vs data test.\n\n");

    content.push_str("## 2) Pola Qty Ekstrem\n");
    content.push_str(&format!(
        "- Baris Qty_Ekstrem di karantina: {}\n",
        qty_extreme_count
    ));
    if !qty_extreme_items.is_empty() {
        content.push_str("- Contoh item terdampak: ");
        content.push_str(&qty_extreme_items.into_iter().take(8).collect::<Vec<_>>().join(", "));
        content.push_str("\n");
    }
    content.push_str("\n");

    content.push_str("## 3) Sinyal Median vs Mean per Kategori\n");
    if skew_rows.is_empty() {
        content.push_str("- Data kategori/revenue tidak cukup untuk analisis skew.\n\n");
    } else {
        for (cat, mean, med, n) in skew_rows.iter().take(3) {
            content.push_str(&format!(
                "- {}: mean {:.2}, median {:.2}, n={}\n",
                cat, mean, med, n
            ));
        }
        content.push_str("\n");
    }

    content.push_str("## 4) Status Duplikat trx-1045\n");
    content.push_str(&format!("- Tersisa di output utama: {}\n", trx_1045_final));
    content.push_str(&format!("- Tersisa di karantina: {}\n", trx_1045_quarantine));
    content.push_str(&format!("- Dibuang saat dedup: {}\n", trx_1045_dropped));

    let mut out = File::create(&out_path)
        .with_context(|| format!("failed to create {}", out_path.display()))?;
    out.write_all(content.as_bytes())
        .with_context(|| format!("failed to write {}", out_path.display()))?;

    Ok(out_path)
}

fn write_presentation_report(
    source_csv_path: &Path,
    final_df: &polars::prelude::DataFrame,
    quarantine_df: Option<&polars::prelude::DataFrame>,
    dropped_dups_count: usize,
    yearly_file_count: usize,
    output_root: &Path,
    hard_reject: bool,
) -> Result<(PathBuf, PathBuf)> {
    let out_dir = output_root.join("laporan");
    std::fs::create_dir_all(&out_dir).context("failed to create output/laporan directory")?;
    let out_path = out_dir.join("presentasi.txt");
    let html_path = out_dir.join("presentasi.html");

    let total_final_rows = final_df.height();
    let total_final_cols = final_df.width();
    let total_quarantine_rows = quarantine_df.map(|q| q.height()).unwrap_or(0);

    let date_col = find_col_by_keywords(final_df, &["tanggal", "date", "tgl"]);
    let mut min_year: Option<i32> = None;
    let mut max_year: Option<i32> = None;
    if let Some(dc) = date_col {
        let ds = final_df.column(&dc)?;
        for i in 0..final_df.height() {
            let year = ds
                .get(i)
                .ok()
                .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .and_then(|s| s.get(0..4).map(|x| x.to_string()))
                .and_then(|y| y.parse::<i32>().ok());
            if let Some(y) = year {
                min_year = Some(min_year.map(|m| m.min(y)).unwrap_or(y));
                max_year = Some(max_year.map(|m| m.max(y)).unwrap_or(y));
            }
        }
    }

    let mut sev_high = 0_usize;
    let mut sev_medium = 0_usize;
    let mut sev_low = 0_usize;
    let mut reason_count: HashMap<String, usize> = HashMap::new();

    if let Some(qdf) = quarantine_df {
        let sev_col = qdf.column("Severity_Karantina").ok();
        let reason_col = qdf.column("Alasan_Karantina").ok();
        for i in 0..qdf.height() {
            if let Some(sc) = &sev_col {
                if let Ok(v) = sc.get(i) {
                    let sev = anyvalue_to_plain_string(v);
                    match sev.as_str() {
                        "HIGH" => sev_high += 1,
                        "MEDIUM" => sev_medium += 1,
                        "LOW" => sev_low += 1,
                        _ => {}
                    }
                }
            }

            if let Some(rc) = &reason_col {
                if let Ok(v) = rc.get(i) {
                    let txt = anyvalue_to_plain_string(v);
                    for part in txt.split('|') {
                        let key = part.trim();
                        if !key.is_empty() {
                            *reason_count.entry(key.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }

    let mut top_reasons: Vec<(String, usize)> = reason_count.into_iter().collect();
    top_reasons.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let category_col = find_col_by_keywords(final_df, &["kategori", "category", "produk"]);
    let revenue_col = find_col_by_keywords(final_df, &["revenue_per_transaction", "revenue"]);
    let mut category_revenue: HashMap<String, f64> = HashMap::new();
    if let (Some(cc), Some(rc)) = (&category_col, &revenue_col) {
        let cseries = final_df.column(cc)?;
        let rseries = final_df.column(rc)?;
        for i in 0..final_df.height() {
            let cat = cseries
                .get(i)
                .ok()
                .filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .unwrap_or_default();
            if cat.trim().is_empty() {
                continue;
            }
            if let Ok(v) = rseries.get(i) {
                if let Some(rv) = anyvalue_to_f64(v) {
                    *category_revenue.entry(cat).or_insert(0.0) += rv;
                }
            }
        }
    }
    let mut top_category_rev: Vec<(String, f64)> = category_revenue.into_iter().collect();
    top_category_rev
        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut content = String::new();
    content.push_str("LAPORAN PRESENTASI AUDIT DATA E-COMMERCE\n");
    content.push_str("============================================================\n\n");
    content.push_str(&format!(
        "Waktu Generate : {}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    content.push_str(&format!("Sumber Dataset : {}\n", source_csv_path.display()));
    content.push_str(&format!(
        "Mode Pipeline  : {}\n\n",
        if hard_reject {
            "KETAT (hard reject + karantina)"
        } else {
            "NORMAL"
        }
    ));

    content.push_str("1. Ringkasan Dataset\n");
    content.push_str(&format!(
        "- Output utama setelah cleaning: {} baris, {} kolom\n",
        total_final_rows, total_final_cols
    ));
    content.push_str(&format!("- Baris karantina: {}\n", total_quarantine_rows));
    content.push_str(&format!(
        "- Duplikat ID yang dibuang (post-impute): {}\n",
        dropped_dups_count
    ));
    if let (Some(miny), Some(maxy)) = (min_year, max_year) {
        content.push_str(&format!("- Rentang tahun analisis: {} - {}\n", miny, maxy));
    }
    content.push_str(&format!(
        "- Split data per tahun: {} file\n\n",
        yearly_file_count
    ));

    content.push_str("2. Kualitas Data (Karantina)\n");
    content.push_str(&format!("- Severity HIGH  : {}\n", sev_high));
    content.push_str(&format!("- Severity MEDIUM: {}\n", sev_medium));
    content.push_str(&format!("- Severity LOW   : {}\n", sev_low));
    if top_reasons.is_empty() {
        content.push_str("- Top alasan anomali: (tidak tersedia)\n\n");
    } else {
        content.push_str("- Top alasan anomali:\n");
        for (reason, count) in top_reasons.iter().take(5) {
            content.push_str(&format!("  * {}: {}\n", reason, count));
        }
        content.push('\n');
    }

    content.push_str("3. Temuan Utama untuk Audiens\n");
    content.push_str("- Pipeline sudah mengoreksi format angka (termasuk scientific notation) sehingga revenue tidak terdistorsi.\n");
    content.push_str("- Data anomali tidak dibuang diam-diam: dipisah ke karantina agar audit trail tetap transparan.\n");
    content.push_str("- Deduplikasi post-impute sudah aktif sehingga transaksi ganda seperti trx-1045 tidak dobel di hasil akhir.\n");
    if !top_category_rev.is_empty() {
        content.push_str("- Top kategori berdasarkan total revenue output utama:\n");
        for (cat, rev) in top_category_rev.iter().take(3) {
            content.push_str(&format!("  * {}: {:.2}\n", cat, rev));
        }
    }
    content.push_str("\n");

    content.push_str("4. Dampak Bisnis\n");
    content.push_str("- Keputusan bisnis berbasis dashboard menjadi lebih aman karena data outlier/invalid terkontrol.\n");
    content.push_str("- Tim audit mendapatkan prioritas investigasi yang jelas (HIGH/MEDIUM/LOW).\n");
    content.push_str("- Analisis tren tahun-ke-tahun bisa dilakukan lebih rapi lewat output per tahun.\n\n");

    content.push_str("5. Artefak untuk Presentasi\n");
    content.push_str("- Output bersih utama      : output/csv/dataset_transaksi_ecommerce.csv\n");
    content.push_str("- Ringkasan audit ketat    : output/quarantine/ringkasan_audit_ketat_senior.md\n");
    content.push_str("- Data tersembunyi         : output/quarantine/data_tersembunyi.md\n");
    content.push_str("- Data karantina detail    : output/quarantine/dataset_transaksi_ecommerce.csv\n");
    content.push_str("- Log duplikat dibuang     : output/quarantine/duplikat_dihapus_dataset_transaksi_ecommerce.csv\n");
    content.push_str("- Investigasi customer     : output/quarantine/investigasi_budi_santoso.md\n");
    content.push_str("- Split per tahun          : output/csv/per_tahun/<tahun>/dataset_transaksi_ecommerce.csv\n");

    let mut out = File::create(&out_path)
        .with_context(|| format!("failed to create {}", out_path.display()))?;
    out.write_all(content.as_bytes())
        .with_context(|| format!("failed to write {}", out_path.display()))?;

    let escaped = content
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let html_body = escaped
        .lines()
        .map(|line| {
            if line.trim().is_empty() {
                "<div class=\"space\"></div>".to_string()
            } else if line.ends_with("============================================================")
                || line == "LAPORAN PRESENTASI AUDIT DATA E-COMMERCE"
            {
                format!("<h1>{}</h1>", line)
            } else if line.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
                && line.contains('.')
            {
                format!("<h2>{}</h2>", line)
            } else if line.trim_start().starts_with("*") || line.trim_start().starts_with("-") {
                format!("<p class=\"bullet\">{}</p>", line)
            } else {
                format!("<p>{}</p>", line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let html = format!(
        "<!doctype html>\n<html lang=\"id\">\n<head>\n  <meta charset=\"utf-8\">\n  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n  <title>Presentasi Audit Data</title>\n  <style>\n    :root {{\n      --bg: #f5f7f2;\n      --card: #ffffff;\n      --ink: #1f2a1f;\n      --muted: #596559;\n      --accent: #1f7a4c;\n      --line: #d9e2d9;\n    }}\n    body {{\n      margin: 0;\n      font-family: 'Segoe UI', 'Trebuchet MS', sans-serif;\n      background: linear-gradient(145deg, #f5f7f2, #ecf2e8);\n      color: var(--ink);\n    }}\n    .wrap {{\n      max-width: 960px;\n      margin: 24px auto;\n      padding: 0 16px;\n    }}\n    .card {{\n      background: var(--card);\n      border: 1px solid var(--line);\n      border-radius: 16px;\n      box-shadow: 0 10px 28px rgba(31, 42, 31, 0.08);\n      padding: 24px;\n    }}\n    h1 {{\n      margin: 0 0 12px;\n      color: var(--accent);\n      font-size: 1.3rem;\n      line-height: 1.35;\n    }}\n    h2 {{\n      margin: 18px 0 8px;\n      color: #244f3a;\n      font-size: 1.05rem;\n      border-left: 4px solid #89b59f;\n      padding-left: 8px;\n    }}\n    p {{\n      margin: 6px 0;\n      line-height: 1.5;\n      white-space: pre-wrap;\n      word-break: break-word;\n    }}\n    .bullet {{ color: var(--muted); }}\n    .space {{ height: 8px; }}\n    @media (max-width: 640px) {{\n      .card {{ padding: 16px; border-radius: 12px; }}\n      h1 {{ font-size: 1.1rem; }}\n      h2 {{ font-size: 1rem; }}\n    }}\n  </style>\n</head>\n<body>\n  <main class=\"wrap\">\n    <section class=\"card\">\n      {}\n    </section>\n  </main>\n</body>\n</html>",
        html_body
    );

    let mut html_out = File::create(&html_path)
        .with_context(|| format!("failed to create {}", html_path.display()))?;
    html_out
        .write_all(html.as_bytes())
        .with_context(|| format!("failed to write {}", html_path.display()))?;

    Ok((out_path, html_path))
}

fn apply_hard_reject(
    clean_df: polars::prelude::DataFrame,
) -> Result<(polars::prelude::DataFrame, Option<polars::prelude::DataFrame>)> {
    let has_review_col = clean_df
        .get_column_names()
        .iter()
        .any(|c| *c == "Perlu_Review_Manual");
    if !has_review_col {
        return Ok((clean_df, None));
    }

    let review_series = clean_df
        .column("Perlu_Review_Manual")?
        .cast(&polars::prelude::DataType::Boolean)?;
    let review_ca = review_series.bool()?;

    let reject_mask_vals: Vec<bool> = review_ca.into_iter().map(|v| v.unwrap_or(false)).collect();
    let keep_mask_vals: Vec<bool> = reject_mask_vals.iter().map(|v| !*v).collect();

    let reject_mask = BooleanChunked::from_iter_values(
        "reject_mask".into(),
        reject_mask_vals.iter().copied(),
    );
    let keep_mask = BooleanChunked::from_iter_values(
        "keep_mask".into(),
        keep_mask_vals.iter().copied(),
    );

    let mut quarantined = clean_df.filter(&reject_mask)?;
    let filtered = clean_df.filter(&keep_mask)?;

    if quarantined.height() > 0 {
        enrich_quarantine_reason(&mut quarantined)?;
    }

    if quarantined.height() == 0 {
        Ok((filtered, None))
    } else {
        Ok((filtered, Some(quarantined)))
    }
}

fn enrich_quarantine_reason(df: &mut polars::prelude::DataFrame) -> Result<()> {
    let reason_map = [
        ("Qty_Ekstrem", "QTY_EKSTREM"),
        ("Rating_Tidak_Valid", "RATING_TIDAK_VALID"),
        ("Revenue_Anomali", "REVENUE_ANOMALI"),
        ("Duplikat_ID_Berbeda", "DUPLIKAT_ID_BERBEDA"),
        ("Harga_Satuan_Kosong_Awal", "HARGA_SATUAN_KOSONG"),
        ("Tanggal_DiLuar_Range", "TANGGAL_DILUAR_RANGE"),
        ("Qty_Nol", "QTY_NOL"),
    ];

    let available: Vec<(&str, &str)> = reason_map
        .into_iter()
        .filter(|(col, _)| df.get_column_names().iter().any(|c| *c == *col))
        .collect();

    if available.is_empty() {
        return Ok(());
    }

    let mut reason_text: Vec<Option<String>> = Vec::with_capacity(df.height());
    let mut reason_count: Vec<Option<i64>> = Vec::with_capacity(df.height());
    let mut severity_text: Vec<Option<String>> = Vec::with_capacity(df.height());
    let mut severity_score: Vec<Option<i64>> = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let mut reasons = Vec::new();
        let mut max_severity = 1_i64;
        for (col, label) in &available {
            if let Ok(series) = df.column(col) {
                if let Ok(v) = series.get(i) {
                    if anyvalue_to_bool(v) {
                        reasons.push((*label).to_string());
                        let sev = match *label {
                            "REVENUE_ANOMALI" | "DUPLIKAT_ID_BERBEDA" | "QTY_EKSTREM" => 3,
                            "RATING_TIDAK_VALID" | "HARGA_SATUAN_KOSONG" => 2,
                            "QTY_NOL" | "TANGGAL_DILUAR_RANGE" => 1,
                            _ => 1,
                        };
                        if sev > max_severity {
                            max_severity = sev;
                        }
                    }
                }
            }
        }

        if reasons.is_empty() {
            reason_text.push(Some("PERLU_REVIEW_MANUAL".to_string()));
            reason_count.push(Some(1));
            severity_text.push(Some("LOW".to_string()));
            severity_score.push(Some(1));
        } else {
            reason_count.push(Some(reasons.len() as i64));
            reason_text.push(Some(reasons.join("|")));
            let sev_label = match max_severity {
                3 => "HIGH",
                2 => "MEDIUM",
                _ => "LOW",
            }
            .to_string();
            severity_text.push(Some(sev_label));
            severity_score.push(Some(max_severity));
        }
    }

    df.with_column(polars::prelude::Series::new(
        "Alasan_Karantina".into(),
        reason_text,
    ))?;
    df.with_column(polars::prelude::Series::new(
        "Jumlah_Alasan_Karantina".into(),
        reason_count,
    ))?;
    df.with_column(polars::prelude::Series::new(
        "Severity_Karantina".into(),
        severity_text,
    ))?;
    df.with_column(polars::prelude::Series::new(
        "Skor_Severity_Karantina".into(),
        severity_score,
    ))?;

    Ok(())
}

fn anyvalue_to_bool(v: polars::prelude::AnyValue<'_>) -> bool {
    match v {
        polars::prelude::AnyValue::Boolean(b) => b,
        polars::prelude::AnyValue::String(s) => s.eq_ignore_ascii_case("true") || s == "1",
        polars::prelude::AnyValue::UInt8(x) => x > 0,
        polars::prelude::AnyValue::UInt16(x) => x > 0,
        polars::prelude::AnyValue::UInt32(x) => x > 0,
        polars::prelude::AnyValue::UInt64(x) => x > 0,
        polars::prelude::AnyValue::Int8(x) => x > 0,
        polars::prelude::AnyValue::Int16(x) => x > 0,
        polars::prelude::AnyValue::Int32(x) => x > 0,
        polars::prelude::AnyValue::Int64(x) => x > 0,
        _ => false,
    }
}

fn anyvalue_to_plain_string(v: polars::prelude::AnyValue<'_>) -> String {
    match v {
        polars::prelude::AnyValue::String(s) => s.to_string(),
        _ => v.to_string().trim_matches('"').to_string(),
    }
}

fn main() -> Result<()> {
    print_banner();

    let config = validate_args()?;
    let input_path = config.input_path.clone();
    let csv_paths = gather_csv_paths(&input_path)?;
    let started_at = chrono::Local::now();
    let output_dir = Path::new("./output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).context("failed to create output directory")?;
    }

    println!("  Input     : {}", input_path);
    println!("  File CSV  : {}", csv_paths.len());
    println!(
        "  Mode      : {}",
        if config.hard_reject {
            "HARD REJECT (karantina aktif)"
        } else {
            "normal"
        }
    );
    println!("  Mulai     : {}", started_at.format("%Y-%m-%d %H:%M:%S"));

    let mut station_summaries = Vec::with_capacity(csv_paths.len());

    for (idx, csv_path) in csv_paths.iter().enumerate() {
        let csv_path_str = csv_path.to_string_lossy().to_string();
        println!();
        println!(
            "{}",
            format!(
                "── [DATASET {}/{}] {}",
                idx + 1,
                csv_paths.len(),
                csv_path_str
            )
            .bold()
        );

        println!(
            "{}",
            "── [1/4] AUDIT DATA ──────────────────────────".bold()
        );
        let (raw_df, audit_report) = audit::run(&csv_path_str)?;

        println!();
        println!(
            "{}",
            "── [2/4] PEMBERSIHAN ─────────────────────────".bold()
        );
        let (clean_df, clean_report) = clean::run(raw_df.clone(), &audit_report)?;
        let (dedup_df, dropped_dups_df, dropped_dups_count) = deduplicate_transaction_ids(clean_df)?;
        if dropped_dups_count > 0 {
            println!(
                "  {} Dedup    : {} baris duplikat ID dihapus (post-impute)",
                "".yellow(),
                dropped_dups_count
            );
        }

        let analysis_df = dedup_df.clone();
        let (final_df, quarantine_df) = if config.hard_reject {
            apply_hard_reject(dedup_df)?
        } else {
            (dedup_df, None)
        };

        let cleaned_csv_path = write_cleaned_csv(&final_df, csv_path.as_path(), output_dir)?;
        println!("  {} CSV Clean: {}", "".cyan(), cleaned_csv_path.display());

        let yearly_paths = write_yearly_splits(&final_df, csv_path.as_path(), output_dir)?;
        if !yearly_paths.is_empty() {
            println!(
                "  {} Per Tahun: {} file di output/csv/per_tahun/",
                "".cyan(),
                yearly_paths.len()
            );
        }

        let budi_report_path = write_budi_santoso_investigation(&final_df, output_dir)?;
        println!("  {} Investigasi: {}", "".cyan(), budi_report_path.display());

        if let Some(ref dropped_df) = dropped_dups_df {
            let dropped_path = write_duplicate_drops_csv(&dropped_df, csv_path.as_path(), output_dir)?;
            println!("  {} Dedup Log : {}", "".yellow(), dropped_path.display());
        }

        if let Some(ref quarantine_df) = quarantine_df {
            let quarantine_path =
                write_quarantine_csv(&quarantine_df, csv_path.as_path(), output_dir)?;
            let summary_path =
                write_quarantine_summary(&quarantine_df, csv_path.as_path(), output_dir)?;
            let hidden_path = write_hidden_insights(
                &analysis_df,
                &final_df,
                &quarantine_df,
                dropped_dups_df.as_ref(),
                output_dir,
            )?;
            println!(
                "  {} Karantina: {} ({} baris)",
                "".yellow(),
                quarantine_path.display(),
                quarantine_df.height()
            );
            println!("  {} Ringkasan: {}", "".yellow(), summary_path.display());
            println!("  {} Hidden  : {}", "".yellow(), hidden_path.display());
        }

        let (presentasi_path, presentasi_html_path) = write_presentation_report(
            csv_path.as_path(),
            &final_df,
            quarantine_df.as_ref(),
            dropped_dups_count,
            yearly_paths.len(),
            output_dir,
            config.hard_reject,
        )?;
        println!("  {} Presentasi: {}", "".cyan(), presentasi_path.display());
        println!("  {} Presentasi: {}", "".cyan(), presentasi_html_path.display());

        station_summaries.push(viz::build_station_summary(
            &csv_path_str,
            &final_df,
            &audit_report,
            &clean_report,
        ));
    }

    println!();
    println!(
        "{}",
        "── [3/4] MEMBUAT VISUALISASI ─────────────────".bold()
    );
    let output_path = output_dir.join("html").join("report.html");
    let json_path = output_dir.join("html").join("report_data.json");
    let looker_path = output_dir.join("looker_studio");
    viz::run_station_comparison(&station_summaries, output_dir)?;

    let google_sheets_url = match google_sheets::sync_station_summaries_to_google_sheets(
        &station_summaries,
        output_dir,
    ) {
        Ok(status) => status.map(|s| s.spreadsheet_url),
        Err(err) => {
            eprintln!("  WARNING Google Sheets upload gagal: {err:#}");
            None
        }
    };

    print_final_summary(
        &output_path.to_string_lossy(),
        &json_path.to_string_lossy(),
        &looker_path.to_string_lossy(),
        google_sheets_url.as_deref(),
        station_summaries.len(),
    );

    tokio::runtime::Runtime::new()?.block_on(async {
        if let Err(err) = looker_auto::deploy_to_looker().await {
            println!("Looker optional: {}", err);
        }
    });

    Ok(())
}
