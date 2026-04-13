//! # Data Cleansing Module
//!
//! Adaptive and fully dynamic cleansing for arbitrary CSV schema.

use anyhow::{Context, Result};
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::audit::AuditReport;

/// Summary report for cleansing actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanReport {
    pub nulls_filled: Vec<(String, usize)>,
    pub outliers_capped: Vec<(String, usize)>,
    pub new_columns: Vec<String>,
    pub rows_before: usize,
    pub rows_after: usize,
}

/// Run full cleansing pipeline.
pub fn run(df: DataFrame, audit: &AuditReport) -> Result<(DataFrame, CleanReport)> {
    let rows_before = df.height();
    let mut cleaned_df = df;
    let mut nulls_filled = Vec::new();
    let mut outliers_capped = Vec::new();
    let mut new_columns = Vec::new();

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::with_template("{bar:40.cyan/blue} {pos}/{len} {msg}")
            .context("failed to set progress style")?,
    );

    pb.set_message("Normalisasi angka/tanggal/teks");
    cleaned_df = normalize_special_fields(cleaned_df)?;
    pb.inc(1);

    pb.set_message("Isi nilai yang hilang");
    cleaned_df = fill_missing_values(cleaned_df, audit, &mut nulls_filled)?;
    pb.inc(1);

    pb.set_message("Batasi outlier + tandai");
    cleaned_df = handle_outliers(cleaned_df, audit, &mut outliers_capped, &mut new_columns)?;
    pb.inc(1);

    pb.set_message("Standarisasi teks");
    cleaned_df = standardize_strings(cleaned_df, audit)?;
    pb.inc(1);

    pb.set_message("Rekayasa fitur");
    cleaned_df = feature_engineering(cleaned_df, audit, &mut new_columns)?;
    pb.inc(1);
    pb.finish_with_message("Pembersihan selesai");

    let report = CleanReport {
        nulls_filled,
        outliers_capped,
        new_columns,
        rows_before,
        rows_after: cleaned_df.height(),
    };

    print_clean_summary(&report);
    Ok((cleaned_df, report))
}

fn fill_missing_values(
    mut df: DataFrame,
    audit: &AuditReport,
    nulls_filled: &mut Vec<(String, usize)>,
) -> Result<DataFrame> {
    for col_name in &audit.numeric_cols {
        if let Some(profile) = audit.profiles.iter().find(|p| p.name == *col_name) {
            let null_count = profile.null_count;
            if null_count > 0 {
                let median_val = profile.median.unwrap_or(0.0);

                let s = df.column(col_name)?.cast(&DataType::Float64)?;
                let ca = s.f64()?;
                let filled: Float64Chunked = ca
                    .into_iter()
                    .map(|v| Some(v.unwrap_or(median_val)))
                    .collect();

                let mut series = filled.into_series();
                series.rename(col_name.as_str().into());
                df.with_column(series)?;
                nulls_filled.push((col_name.clone(), null_count));
            }
        }
    }

    for col_name in &audit.string_cols {
        if let Some(profile) = audit.profiles.iter().find(|p| p.name == *col_name) {
            let null_count = profile.null_count;
            if null_count > 0 {
                let replacement = profile
                    .top_value
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());

                let s = df.column(col_name)?;
                let mut filled: Vec<Option<String>> = Vec::with_capacity(s.len());
                for i in 0..s.len() {
                    let v = s.get(i)?;
                    if matches!(v, AnyValue::Null) {
                        filled.push(Some(replacement.clone()));
                    } else {
                        filled.push(Some(anyvalue_to_plain_string(v)));
                    }
                }

                let series = Series::new(col_name.as_str().into(), filled);
                df.with_column(series)?;
                nulls_filled.push((col_name.clone(), null_count));
            }
        }
    }

    Ok(df)
}

fn normalize_special_fields(mut df: DataFrame) -> Result<DataFrame> {
    let numeric_targets: Vec<String> = df
        .get_column_names()
        .iter()
        .filter_map(|name| {
            let n = name.to_ascii_lowercase();
            if n.contains("harga")
                || n.contains("price")
                || n.contains("jumlah")
                || n.contains("qty")
                || n.contains("quantity")
                || n.contains("diskon")
                || n.contains("discount")
            {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    for col_name in numeric_targets {
        let s = df.column(&col_name)?;
        let lower = col_name.to_ascii_lowercase();

        if lower.contains("jumlah") || lower.contains("qty") || lower.contains("quantity") {
            let vals: Vec<Option<i64>> = (0..s.len())
                .map(|i| {
                    s.get(i)
                        .ok()
                        .and_then(|v| parse_any_to_f64(v))
                        .map(|v| v.round() as i64)
                })
                .collect();

            // Keep negative quantity as-is and classify transaction type.
            let status_vals: Vec<Option<String>> = vals
                .iter()
                .map(|v| match v {
                    Some(x) if *x < 0 => Some("RETUR/REFUND".to_string()),
                    Some(_) => Some("NORMAL".to_string()),
                    None => Some("KOREKSI MANUAL".to_string()),
                })
                .collect();
            df.with_column(Series::new(col_name.as_str().into(), vals))?;
            df.with_column(Series::new("Status_Transaksi".into(), status_vals))?;
        } else {
            let vals: Vec<Option<f64>> = (0..s.len())
                .map(|i| {
                    s.get(i)
                        .ok()
                        .and_then(|v| parse_any_to_f64(v))
                        .map(|v| v.abs())
                })
                .collect();
            df.with_column(Series::new(col_name.as_str().into(), vals))?;
        }

    }

    let date_targets: Vec<String> = df
        .get_column_names()
        .iter()
        .filter_map(|name| {
            let n = name.to_ascii_lowercase();
            if n.contains("tanggal") || n.contains("date") || n == "tgl" {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    for col_name in date_targets {
        let s = df.column(&col_name)?;
        let normalized: Vec<Option<String>> = (0..s.len())
            .map(|i| {
                s.get(i).ok().and_then(|v| {
                    if matches!(v, AnyValue::Null) {
                        None
                    } else {
                        parse_flexible_date(&anyvalue_to_plain_string(v))
                    }
                })
            })
            .collect();
        df.with_column(Series::new(col_name.as_str().into(), normalized))?;
    }

    Ok(df)
}

fn parse_any_to_f64(v: AnyValue<'_>) -> Option<f64> {
    match v {
        AnyValue::Null => None,
        AnyValue::Int8(x) => Some(x as f64),
        AnyValue::Int16(x) => Some(x as f64),
        AnyValue::Int32(x) => Some(x as f64),
        AnyValue::Int64(x) => Some(x as f64),
        AnyValue::UInt8(x) => Some(x as f64),
        AnyValue::UInt16(x) => Some(x as f64),
        AnyValue::UInt32(x) => Some(x as f64),
        AnyValue::UInt64(x) => Some(x as f64),
        AnyValue::Float32(x) => Some(x as f64),
        AnyValue::Float64(x) => Some(x),
        AnyValue::String(s) => parse_mixed_number(s),
        _ => parse_mixed_number(&v.to_string()),
    }
}

fn parse_mixed_number(raw: &str) -> Option<f64> {
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }

    if let Some(word_num) = parse_number_words(s) {
        return Some(word_num as f64);
    }

    let mut cleaned = s.to_ascii_lowercase();
    cleaned = cleaned.replace("idr", "");
    cleaned = cleaned.replace("rp", "");
    cleaned = cleaned.replace(' ', "");

    let mut sign = 1.0;
    if cleaned.starts_with('-') {
        sign = -1.0;
        cleaned = cleaned.trim_start_matches('-').to_string();
    }

    // Extract suffix multiplier (k, m, b, etc.)
    let mut multiplier = 1.0;
    if cleaned.ends_with('k') {
        multiplier = 1_000.0;
        cleaned.pop();
    } else if cleaned.ends_with('m') {
        multiplier = 1_000_000.0;
        cleaned.pop();
    } else if cleaned.ends_with('b') {
        multiplier = 1_000_000_000.0;
        cleaned.pop();
    }

    let has_dot = cleaned.contains('.');
    let has_comma = cleaned.contains(',');
    let normalized = if has_dot && has_comma {
        cleaned.replace('.', "").replace(',', ".")
    } else if has_comma {
        cleaned.replace(',', ".")
    } else {
        cleaned
    };

    let filtered: String = normalized
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();

    if filtered.is_empty() {
        return None;
    }
    filtered.parse::<f64>().ok().map(|v| sign * v * multiplier)
}

fn parse_number_words(raw: &str) -> Option<i64> {
    let token = raw
        .trim()
        .to_ascii_lowercase()
        .replace('-', " ")
        .replace('_', " ");
    let t = token.trim();

    let direct = [
        ("nol", 0),
        ("satu", 1),
        ("dua", 2),
        ("tiga", 3),
        ("empat", 4),
        ("lima", 5),
        ("enam", 6),
        ("tujuh", 7),
        ("delapan", 8),
        ("sembilan", 9),
        ("sepuluh", 10),
        ("sebelas", 11),
    ];

    if let Some((_, v)) = direct.iter().find(|(k, _)| *k == t) {
        return Some(*v);
    }

    if let Some(base) = t.strip_suffix(" belas") {
        if let Some((_, v)) = direct.iter().find(|(k, _)| *k == base) {
            return Some(*v + 10);
        }
    }

    if let Some(base) = t.strip_suffix(" puluh") {
        if let Some((_, v)) = direct.iter().find(|(k, _)| *k == base) {
            return Some(*v * 10);
        }
    }

    if let Some((left, right)) = t.split_once(" puluh ") {
        let tens = direct.iter().find(|(k, _)| *k == left).map(|(_, v)| *v)?;
        let ones = direct.iter().find(|(k, _)| *k == right).map(|(_, v)| *v)?;
        return Some(tens * 10 + ones);
    }

    None
}

fn parse_flexible_date(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }

    let fmts = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%Y/%m/%d", "%d-%m-%Y"];
    for fmt in fmts {
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, fmt) {
            return Some(d.format("%Y-%m-%d").to_string());
        }
    }
    None
}

fn handle_outliers(
    mut df: DataFrame,
    audit: &AuditReport,
    outliers_capped: &mut Vec<(String, usize)>,
    new_columns: &mut Vec<String>,
) -> Result<DataFrame> {
    for col_name in &audit.numeric_cols {
        let profile_opt = audit.profiles.iter().find(|p| p.name == *col_name);
        if let Some(profile) = profile_opt {
            let lb = profile.lower_bound;
            let ub = profile.upper_bound;
            let out_cnt = profile.outlier_count.unwrap_or(0);

            if let (Some(lower), Some(upper)) = (lb, ub) {
                let flag_name = format!("is_outlier_{}", col_name);

                let s = df.column(col_name)?.cast(&DataType::Float64)?;
                let ca = s.f64()?;

                let capped_vals: Vec<Option<f64>> = ca
                    .into_iter()
                    .map(|v| match v {
                        Some(x) if x < lower => Some(lower),
                        Some(x) if x > upper => Some(upper),
                        Some(x) => Some(x),
                        None => None,
                    })
                    .collect();

                let flag_vals: Vec<Option<bool>> = ca
                    .into_iter()
                    .map(|v| v.map(|x| x < lower || x > upper))
                    .collect();

                let capped = Series::new(col_name.as_str().into(), capped_vals);
                let flag = Series::new(flag_name.as_str().into(), flag_vals);

                df.with_column(capped)?;
                df.with_column(flag)?;
                new_columns.push(flag_name);

                if out_cnt > 0 {
                    outliers_capped.push((col_name.clone(), out_cnt));
                }
            }
        }
    }

    Ok(df)
}

fn standardize_strings(mut df: DataFrame, audit: &AuditReport) -> Result<DataFrame> {
    let re = Regex::new(r"[^a-zA-Z0-9\s\-_.,]").context("invalid regex for string cleansing")?;
    let city_map = HashMap::from([
        ("BDG", "BANDUNG"),
        ("BANDUNG", "BANDUNG"),
        ("JKT", "JAKARTA"),
        ("JAKARTA", "JAKARTA"),
        ("SBY", "SURABAYA"),
        ("SURABAYA", "SURABAYA"),
        ("DPS", "DENPASAR"),
        ("DENPASAR", "DENPASAR"),
        ("SMG", "SEMARANG"),
        ("SEMARANG", "SEMARANG"),
        ("YK", "YOGYAKARTA"),
        ("YOGYAKARTA", "YOGYAKARTA"),
        ("MKS", "MAKASSAR"),
        ("MAKASSAR", "MAKASSAR"),
        ("MAKASAR", "MAKASSAR"),
    ]);
    let payment_map = HashMap::from([
        ("COD", "COD"),
        ("TRANSFERBANK", "TRANSFER BANK"),
        ("BANKTRANSFER", "TRANSFER BANK"),
        ("EWALLET", "E-WALLET"),
        ("EWALLETID", "E-WALLET"),
        ("DIGITALWALLET", "E-WALLET"),
        ("KARTUKREDIT", "KARTU KREDIT"),
        ("CREDITCARD", "KARTU KREDIT"),
        ("PAYLATER", "PAYLATER"),
    ]);

    for col_name in &audit.string_cols {
        let s = df.column(col_name)?;

        let is_city_col = {
            let n = col_name.to_ascii_lowercase();
            n.contains("kota") || n.contains("city")
        };
        let is_payment_col = {
            let n = col_name.to_ascii_lowercase();
            n.contains("pembayaran") || n.contains("payment") || n.contains("metode")
        };

        let mut cleaned: Vec<Option<String>> = Vec::with_capacity(s.len());
        for idx in 0..s.len() {
            let v = s.get(idx)?;
            if matches!(v, AnyValue::Null) {
                cleaned.push(None);
            } else {
                let raw = anyvalue_to_plain_string(v);
                let trimmed = raw.trim().to_string();
                let normalized = re.replace_all(&trimmed, "").to_string();

                if is_city_col {
                    let upper = normalized.to_ascii_uppercase();
                    let key = upper
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .collect::<String>();
                    let mapped = city_map
                        .get(key.as_str())
                        .map(|x| (*x).to_string())
                        .unwrap_or(upper);
                    cleaned.push(Some(mapped));
                } else if is_payment_col {
                    let upper = normalized.to_ascii_uppercase();
                    let key = upper
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .collect::<String>();
                    let mapped = payment_map
                        .get(key.as_str())
                        .map(|x| (*x).to_string())
                        .unwrap_or(upper);
                    cleaned.push(Some(mapped));
                } else {
                    cleaned.push(Some(normalized.to_ascii_lowercase()));
                }
            }
        }

        let series = Series::new(col_name.as_str().into(), cleaned);
        df.with_column(series)?;
    }

    Ok(df)
}

fn feature_engineering(
    mut df: DataFrame,
    audit: &AuditReport,
    new_columns: &mut Vec<String>,
) -> Result<DataFrame> {
    add_revenue_per_transaction(&mut df, new_columns)?;
    add_retention_count(&mut df, new_columns)?;

    if audit.numeric_cols.len() >= 2 {
        let col1 = &audit.numeric_cols[0];
        let col2 = &audit.numeric_cols[1];
        let ratio_name = format!("ratio_{}_per_{}", col1, col2);

        let s1 = df.column(col1)?.cast(&DataType::Float64)?;
        let s2 = df.column(col2)?.cast(&DataType::Float64)?;

        let c1 = s1.f64()?;
        let c2 = s2.f64()?;

        let ratio: Float64Chunked = c1
            .into_iter()
            .zip(c2.into_iter())
            .map(|(a, b)| match (a, b) {
                (Some(x), Some(y)) if y != 0.0 => Some(x / y),
                _ => None,
            })
            .collect();

        let mut ratio_series = ratio.into_series();
        ratio_series.rename(ratio_name.as_str().into());
        df.with_column(ratio_series)?;
        new_columns.push(ratio_name);
    }

    let age_candidate = audit.numeric_cols.iter().find(|name| {
        let n = name.to_lowercase();
        n.contains("age") || n.contains("tahun") || n.contains("year") || n.contains("umur")
    });

    if let Some(age_col) = age_candidate {
        let age_series = df.column(age_col)?.cast(&DataType::Float64)?;
        let age_ca = age_series.f64()?;

        let age_group: StringChunked = age_ca
            .into_iter()
            .map(|v| match v {
                Some(x) if x < 30.0 => Some("young"),
                Some(x) if x < 60.0 => Some("adult"),
                Some(_) => Some("senior"),
                None => None,
            })
            .collect();

        let mut age_group_series = age_group.into_series();
        age_group_series.rename("age_group".into());
        df.with_column(age_group_series)?;
        new_columns.push("age_group".to_string());
    }

    for col_name in &audit.numeric_cols {
        let profile_opt = audit.profiles.iter().find(|p| p.name == *col_name);
        if let Some(profile) = profile_opt {
            let med = profile.median.unwrap_or(0.0);
            let s = df.column(col_name)?.cast(&DataType::Float64)?;
            let ca = s.f64()?;

            let flag: BooleanChunked = ca.into_iter().map(|v| v.map(|x| x > med)).collect();
            let flag_name = format!("is_above_median_{}", col_name);
            let mut flag_series = flag.into_series();
            flag_series.rename(flag_name.as_str().into());
            df.with_column(flag_series)?;
            new_columns.push(flag_name);
        }
    }

    let cleaned_at = chrono::Utc::now().to_rfc3339();
    let cleaned_vec = vec![cleaned_at; df.height()];
    df.with_column(Series::new("cleaned_at".into(), cleaned_vec))?;
    new_columns.push("cleaned_at".to_string());

    Ok(df)
}

fn find_col_by_keywords(df: &DataFrame, words: &[&str]) -> Option<String> {
    df.get_column_names().iter().find_map(|n| {
        let low = n.to_ascii_lowercase();
        if words.iter().any(|w| low.contains(&w.to_ascii_lowercase())) {
            Some((*n).to_string())
        } else {
            None
        }
    })
}

fn add_revenue_per_transaction(df: &mut DataFrame, new_columns: &mut Vec<String>) -> Result<()> {
    let harga_col = find_col_by_keywords(df, &["harga", "price"]);
    let qty_col = find_col_by_keywords(df, &["jumlah", "qty", "quantity"]);
    let disc_col = find_col_by_keywords(df, &["diskon", "discount"]);

    let (Some(harga), Some(jumlah)) = (harga_col, qty_col) else {
        return Ok(());
    };

    let hs = df.column(&harga)?;
    let qs = df.column(&jumlah)?;
    let ds = disc_col.as_ref().and_then(|c| df.column(c).ok());

    let revenue: Vec<Option<f64>> = (0..df.height())
        .map(|i| {
            let h = hs.get(i).ok().and_then(parse_any_to_f64)?;
            let q = qs.get(i).ok().and_then(parse_any_to_f64)?;
            let d = ds
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .and_then(parse_any_to_f64)
                .unwrap_or(0.0);
            Some((h * q) - d)
        })
        .collect();

    df.with_column(Series::new("revenue_per_transaction".into(), revenue))?;
    new_columns.push("revenue_per_transaction".to_string());
    Ok(())
}

fn add_retention_count(df: &mut DataFrame, new_columns: &mut Vec<String>) -> Result<()> {
    let user_col = find_col_by_keywords(df, &["konsumen", "customer", "user", "nama"]);
    let Some(user_col_name) = user_col else {
        return Ok(());
    };

    let us = df.column(&user_col_name)?;
    let mut freq: HashMap<String, i64> = HashMap::new();

    for i in 0..us.len() {
        if let Ok(v) = us.get(i) {
            if !matches!(v, AnyValue::Null) {
                let key = anyvalue_to_plain_string(v).trim().to_ascii_lowercase();
                if !key.is_empty() {
                    *freq.entry(key).or_insert(0) += 1;
                }
            }
        }
    }

    let retention: Vec<Option<i64>> = (0..us.len())
        .map(|i| {
            us.get(i)
                .ok()
                .filter(|v| !matches!(v, AnyValue::Null))
                .and_then(|v| {
                    let key = anyvalue_to_plain_string(v).trim().to_ascii_lowercase();
                    freq.get(&key).copied()
                })
        })
        .collect();

    df.with_column(Series::new("retention_count".into(), retention))?;
    new_columns.push("retention_count".to_string());
    Ok(())
}

fn print_clean_summary(report: &CleanReport) {
    println!();
    println!(
        "{}",
        "── [2/4] PEMBERSIHAN ─────────────────────────".bold()
    );
    println!();

    let total_filled: usize = report.nulls_filled.iter().map(|(_, c)| *c).sum();
    let total_capped: usize = report.outliers_capped.iter().map(|(_, c)| *c).sum();

    println!(
        "  {} Terisi   : {} nilai kosong (median/modus)",
        "".cyan(),
        total_filled
    );
    println!(
        "  {} Dibatasi : {} outlier (winsorizing IQR)",
        "".cyan(),
        total_capped
    );

    println!();
    println!(
        "{}",
        "── [3/4] REKAYASA FITUR ──────────────────────".bold()
    );
    println!();
    println!(
        "  {} Dibuat   : {} kolom baru",
        "".cyan(),
        report.new_columns.len()
    );

    for col in &report.new_columns {
        println!("       → {}", format_feature_label(col));
    }
}

fn format_feature_label(name: &str) -> String {
    if let Some(rest) = name.strip_prefix("is_outlier_") {
        return title_case(&format!("penanda outlier {}", localize_field_label(rest)));
    }

    if let Some(rest) = name.strip_prefix("is_above_median_") {
        return title_case(&format!(
            "penanda di atas median {}",
            localize_field_label(rest)
        ));
    }

    if let Some(rest) = name.strip_prefix("ratio_") {
        if let Some((left, right)) = rest.split_once("_per_") {
            return title_case(&format!(
                "rasio {} per {}",
                localize_field_label(left),
                localize_field_label(right)
            ));
        }
    }

    match name {
        "revenue_per_transaction" => title_case("pendapatan per transaksi"),
        "retention_count" => title_case("jumlah retensi"),
        "age_group" => title_case("kelompok usia"),
        "cleaned_at" => title_case("waktu pembersihan"),
        _ => localize_field_label(name),
    }
}

fn localize_field_label(name: &str) -> String {
    title_case(&name.replace('_', " "))
}

fn title_case(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first
                    .to_uppercase()
                    .chain(chars.flat_map(|c| c.to_lowercase()))
                    .collect::<String>(),
                None => String::new(),
            }
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn anyvalue_to_plain_string(v: AnyValue<'_>) -> String {
    match v {
        AnyValue::String(s) => s.to_string(),
        _ => v.to_string().trim_matches('"').to_string(),
    }
}
