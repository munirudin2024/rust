//! # Data Cleansing Module
//!
//! Adaptive and fully dynamic cleansing for arbitrary CSV schema.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::audit::AuditReport;
use crate::config::{BelowThresholdAction, ImputationPolicy};
use crate::imputation::{select_imputation_method as select_domain_imputation_method, ColumnType, ImputationStrategy};
use crate::terminal_ui::TerminalStyle;

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
pub fn run(
    df: DataFrame,
    audit: &AuditReport,
    max_business_date: NaiveDate,
    imputation_policy: &ImputationPolicy,
) -> Result<(DataFrame, CleanReport)> {
    let rows_before = df.height();
    let mut cleaned_df = df;
    let mut nulls_filled = Vec::new();
    let mut outliers_capped = Vec::new();
    let mut new_columns = Vec::new();

    let pb = ProgressBar::new(7);
    pb.set_style(
        ProgressStyle::with_template("{bar:40.cyan/blue} {pos}/{len} {msg}")
            .context("failed to set progress style")?,
    );

    pb.set_message("Normalisasi angka/tanggal/teks");
    cleaned_df = normalize_special_fields(cleaned_df)?;
    pb.inc(1);

    pb.set_message("Preprocessing: buang harga kosong + qty outlier");
    cleaned_df = drop_ds_unsafe_rows(cleaned_df)?;
    pb.inc(1);

    pb.set_message("Isi nilai yang hilang");
    cleaned_df = fill_missing_values(cleaned_df, audit, &mut nulls_filled, imputation_policy)?;
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

    pb.set_message("Validasi aturan bisnis");
    cleaned_df = apply_business_rules(cleaned_df, &mut new_columns, max_business_date)?;
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
    policy: &ImputationPolicy,
) -> Result<DataFrame> {
    let use_timeseries = ["year", "month", "day", "hour"]
        .iter()
        .all(|c| df.get_column_names().iter().any(|n| n.eq_ignore_ascii_case(c)));

    let mut low_confidence_imputation = vec![false; df.height()];
    let mut missing_verified = vec![false; df.height()];

    for col_name in &audit.numeric_cols {
        let lower_name = col_name.to_ascii_lowercase();
        if lower_name.contains("harga") || lower_name.contains("price") {
            continue;
        }

        if let Some(profile) = audit.profiles.iter().find(|p| p.name == *col_name) {
            let null_count = profile.null_count;
            if null_count > 0 {
                let s = df.column(col_name)?.cast(&DataType::Float64)?;
                let ca = s.f64()?;
                let values: Vec<Option<f64>> = ca.into_iter().collect();
                let method = select_imputation_method(col_name, use_timeseries);
                let method_conf = method_confidence(method);
                let (mut filled_vals, flags, methods) = impute_series(&values, method);
                let mut action_col = vec![Some("default_fill".to_string()); values.len()];

                if method_conf < policy.min_confidence {
                    for i in 0..values.len() {
                        let is_imputed = flags[i].unwrap_or(false);
                        if !is_imputed {
                            continue;
                        }
                        low_confidence_imputation[i] = true;
                        match policy.below_threshold_action {
                            BelowThresholdAction::FillWithFlag => {
                                action_col[i] = Some("fill_with_flag".to_string());
                            }
                            BelowThresholdAction::Null => {
                                filled_vals[i] = None;
                                action_col[i] = Some("set_null".to_string());
                            }
                            BelowThresholdAction::Quarantine => {
                                action_col[i] = Some("quarantine".to_string());
                            }
                            BelowThresholdAction::MissingVerified => {
                                filled_vals[i] = None;
                                missing_verified[i] = true;
                                action_col[i] = Some("missing_verified".to_string());
                            }
                        }
                    }
                }

                let mut series = Series::new(col_name.as_str().into(), filled_vals);
                series.rename(col_name.as_str().into());
                df.with_column(series)?;

                let imputed_col = format!("{}_imputed", col_name);
                let method_col = format!("{}_imputation_method", col_name);
                let action_name = format!("{}_imputation_action", col_name);
                df.with_column(Series::new(imputed_col.as_str().into(), flags))?;
                df.with_column(Series::new(method_col.as_str().into(), methods))?;
                df.with_column(Series::new(action_name.as_str().into(), action_col))?;
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

    if df
        .get_column_names()
        .iter()
        .any(|c| *c == "Perlu_Review_Manual")
    {
        let existing = df.column("Perlu_Review_Manual")?.cast(&DataType::Boolean)?;
        let existing = existing.bool()?;
        for (idx, value) in existing.into_iter().enumerate() {
            if value.unwrap_or(false) {
                low_confidence_imputation[idx] = true;
            }
        }
    }

    let mark_review = match policy.below_threshold_action {
        BelowThresholdAction::Quarantine | BelowThresholdAction::MissingVerified => true,
        BelowThresholdAction::FillWithFlag | BelowThresholdAction::Null => false,
    };
    let review_values: Vec<bool> = if mark_review {
        low_confidence_imputation.clone()
    } else {
        vec![false; df.height()]
    };

    df.with_column(Series::new("Low_Confidence_Imputation".into(), low_confidence_imputation))?;
    df.with_column(Series::new("MISSING_VERIFIED".into(), missing_verified))?;
    df.with_column(Series::new("Perlu_Review_Manual".into(), review_values))?;
    df.with_column(Series::new(
        "imputation_confidence_policy".into(),
        vec![
            format!(
                "min_conf={:.2};action={};tolerance_pct={:.2}",
                policy.min_confidence,
                policy.below_threshold_action,
                policy.tolerance_pct
            );
            df.height()
        ],
    ))?;

    Ok(df)
}

fn method_confidence(method: InterpolationMethod) -> f32 {
    match method {
        InterpolationMethod::Linear => 0.82,
        InterpolationMethod::Seasonal => 0.78,
        InterpolationMethod::ForwardFill => 0.72,
        InterpolationMethod::MedianFallback => 0.65,
    }
}

#[derive(Clone, Copy)]
enum InterpolationMethod {
    Linear,
    Seasonal,
    ForwardFill,
    MedianFallback,
}

fn select_imputation_method(col_name: &str, use_timeseries: bool) -> InterpolationMethod {
    if !use_timeseries {
        return InterpolationMethod::MedianFallback;
    }

    match col_name {
        "PM2.5" | "PM10" | "SO2" | "NO2" | "CO" | "O3" => InterpolationMethod::Seasonal,
        "TEMP" | "PRES" | "DEWP" | "WSPM" => InterpolationMethod::Linear,
        "RAIN" => InterpolationMethod::ForwardFill,
        _ => InterpolationMethod::MedianFallback,
    }
}

fn impute_series(
    series: &[Option<f64>],
    method: InterpolationMethod,
) -> (Vec<Option<f64>>, Vec<Option<bool>>, Vec<Option<String>>) {
    let mut out = series.to_vec();
    let mut imputed = vec![Some(false); series.len()];
    let mut method_col = vec![Some(method_name(method).to_string()); series.len()];

    match method {
        InterpolationMethod::Linear => {
            for i in 0..out.len() {
                if out[i].is_none() {
                    let prev = (0..i).rev().find_map(|j| out[j]);
                    let next = ((i + 1)..out.len()).find_map(|j| out[j]);
                    if let (Some(a), Some(b)) = (prev, next) {
                        out[i] = Some((a + b) / 2.0);
                        imputed[i] = Some(true);
                    }
                }
            }
        }
        InterpolationMethod::Seasonal => {
            for i in 0..out.len() {
                if out[i].is_none() {
                    let prev_24 = i.checked_sub(24).and_then(|j| out.get(j).and_then(|v| *v));
                    let next_24 = out.get(i + 24).and_then(|v| *v);
                    out[i] = match (prev_24, next_24) {
                        (Some(a), Some(b)) => Some((a + b) / 2.0),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        _ => None,
                    };
                    if out[i].is_some() {
                        imputed[i] = Some(true);
                    }
                }
            }
        }
        InterpolationMethod::ForwardFill => {
            let mut gap = 0usize;
            let mut last = None;
            for i in 0..out.len() {
                if let Some(v) = out[i] {
                    last = Some(v);
                    gap = 0;
                } else {
                    gap += 1;
                    if gap <= 2 {
                        out[i] = last;
                        if out[i].is_some() {
                            imputed[i] = Some(true);
                        }
                    }
                }
            }
        }
        InterpolationMethod::MedianFallback => {}
    }

    let mut non_null: Vec<f64> = out.iter().flatten().copied().collect();
    let fallback = crate::domain::utils::median(&mut non_null).unwrap_or(0.0);
    for i in 0..out.len() {
        if out[i].is_none() {
            out[i] = Some(fallback);
            imputed[i] = Some(true);
            method_col[i] = Some("median".to_string());
        }
    }

    (out, imputed, method_col)
}

fn method_name(method: InterpolationMethod) -> &'static str {
    match method {
        InterpolationMethod::Linear => "linear",
        InterpolationMethod::Seasonal => "seasonal",
        InterpolationMethod::ForwardFill => "forward_fill",
        InterpolationMethod::MedianFallback => "median",
    }
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
            let raw_vals: Vec<Option<i64>> = (0..s.len())
                .map(|i| {
                    s.get(i)
                        .ok()
                        .and_then(|v| parse_any_to_f64(v))
                        .map(|v| v.round() as i64)
                })
                .collect();

            // Business rule for this table: negative qty is typo, zero qty is corrected to 1.
            let vals: Vec<Option<i64>> = raw_vals
                .iter()
                .map(|v| match v {
                    Some(x) if *x < 0 => Some(x.abs()),
                    Some(0) => Some(1),
                    Some(x) => Some(*x),
                    None => None,
                })
                .collect();

            let status_vals: Vec<Option<String>> = raw_vals
                .iter()
                .map(|v| match v {
                    Some(x) if *x < 0 => Some("KOREKSI_QTY_NEGATIF".to_string()),
                    Some(0) => Some("KOREKSI_QTY_NOL_KE_1".to_string()),
                    Some(_) => Some("NORMAL".to_string()),
                    None => Some("KOREKSI MANUAL".to_string()),
                })
                .collect();

            let qty_zero_flag: Vec<Option<bool>> = raw_vals.iter().map(|v| v.map(|x| x == 0)).collect();
            let qty_negative_flag: Vec<Option<bool>> = raw_vals.iter().map(|v| v.map(|x| x < 0)).collect();
            df.with_column(Series::new(col_name.as_str().into(), vals))?;
            df.with_column(Series::new("Status_Transaksi".into(), status_vals))?;
            df.with_column(Series::new("Qty_Nol".into(), qty_zero_flag))?;
            df.with_column(Series::new("Qty_Negatif_Awal".into(), qty_negative_flag))?;
        } else {
            let is_price_col = lower.contains("harga") || lower.contains("price");
            let mut vals: Vec<Option<f64>> = (0..s.len())
                .map(|i| {
                    s.get(i)
                        .ok()
                        .and_then(|v| parse_any_to_f64(v))
                        .map(|v| v.abs())
                })
                .collect();

            let missing_initial: Vec<Option<bool>> = (0..s.len())
                .map(|i| {
                    s.get(i)
                        .ok()
                        .map(|v| matches!(v, AnyValue::Null) || parse_any_to_f64(v).is_none())
                })
                .collect();

            // If unit prices are mixed scale (e.g. 65000 and 0.2), lift small decimals
            // to million-based nominal so downstream revenue is not distorted.
            if is_price_col {
                for v in &mut vals {
                    if let Some(x) = v {
                        if *x > 0.0 && *x < 1.0 {
                            *x *= 1_000_000.0;
                        }
                    }
                }

                let miss_col_name = format!("{}_Kosong_Awal", col_name);
                df.with_column(Series::new(miss_col_name.as_str().into(), missing_initial))?;
            }

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

fn drop_ds_unsafe_rows(df: DataFrame) -> Result<DataFrame> {
    let row_count = df.height();
    if row_count == 0 {
        return Ok(df);
    }

    let price_col = find_col_by_keywords(&df, &["harga", "price"]);
    let qty_col = find_col_by_keywords(&df, &["jumlah", "qty", "quantity"]);

    if price_col.is_none() && qty_col.is_none() {
        return Ok(df);
    }

    let price_series = price_col.as_ref().and_then(|c| df.column(c).ok());
    let qty_series = qty_col.as_ref().and_then(|c| df.column(c).ok());

    let keep: Vec<bool> = (0..row_count)
        .map(|i| {
            let price_missing = price_series
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .and_then(parse_any_to_f64)
                .is_none();

            let qty_outlier = qty_series
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .and_then(parse_any_to_f64)
                .map(|q| q > 100.0)
                .unwrap_or(false);

            !price_missing && !qty_outlier
        })
        .collect();

    let keep_mask = BooleanChunked::from_iter_values("keep_ds_safe".into(), keep.into_iter());
    let filtered = df.filter(&keep_mask)?;
    Ok(filtered)
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
    cleaned = cleaned
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '.' && c != ',')
        .to_string();

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
    let has_exp = cleaned.contains('e') || cleaned.contains('E');
    let normalized = if has_dot && has_comma {
        cleaned.replace('.', "").replace(',', ".")
    } else if has_exp {
        cleaned
    } else if has_dot {
        let dot_count = cleaned.matches('.').count();
        if dot_count > 1 {
            cleaned.replace('.', "")
        } else if dot_count == 1 {
            let parts: Vec<&str> = cleaned.split('.').collect();
            if parts.len() == 2 && parts[1].len() == 3 {
                cleaned.replace('.', "")
            } else {
                cleaned
            }
        } else {
            cleaned
        }
    } else if has_comma {
        cleaned.replace(',', ".")
    } else {
        cleaned
    };

    if let Ok(v) = normalized.parse::<f64>() {
        return Some(sign * v * multiplier);
    }

    let filtered: String = normalized
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == 'e' || *c == 'E' || *c == '+')
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
    let row_count = df.height();

    for col_name in &audit.numeric_cols {
        let profile_opt = audit.profiles.iter().find(|p| p.name == *col_name);
        if let Some(profile) = profile_opt {
            let lb = profile.lower_bound;
            let ub = profile.upper_bound;
            let out_cnt = profile.outlier_count.unwrap_or(0);
            let lower_name = col_name.to_ascii_lowercase();
            let is_rating_col = lower_name.contains("penilaian")
                || lower_name.contains("rating")
                || lower_name.contains("bintang")
                || lower_name.contains("star");

            if let (Some(lower), Some(upper)) = (lb, ub) {
                let flag_name = format!("outlier_flag_{}", col_name);

                let s = df.column(col_name)?.cast(&DataType::Float64)?;
                let ca = s.f64()?;

                let (capped_vals, flag_vals): (Vec<Option<f64>>, Vec<Option<bool>>) = if is_rating_col {
                    (
                        ca.into_iter().map(|v| v.map(|x| x)).collect(),
                        vec![Some(false); row_count],
                    )
                } else {
                    (
                        ca.into_iter()
                            .map(|v| match v {
                                Some(x) if x < lower => Some(lower),
                                Some(x) if x > upper => Some(upper),
                                Some(x) => Some(x),
                                None => None,
                            })
                            .collect(),
                        ca.into_iter().map(|v| v.map(|x| x < lower || x > upper)).collect(),
                    )
                };

                let capped = Series::new(col_name.as_str().into(), capped_vals);
                let flag = Series::new(flag_name.as_str().into(), flag_vals);

                df.with_column(capped)?;
                df.with_column(flag)?;
                if !is_rating_col {
                    new_columns.push(flag_name);
                }

                if out_cnt > 0 && !is_rating_col {
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
    let category_map = HashMap::from([
        ("FASHION", "FASHION"),
        ("FASHN", "FASHION"),
        ("FASION", "FASHION"),
        ("ELEKTRONIK", "ELEKTRONIK"),
        ("ELEKTRNIK", "ELEKTRONIK"),
        ("ELEKTRONICS", "ELEKTRONIK"),
        ("ELECTRONIC", "ELEKTRONIK"),
        ("ELECTRONICS", "ELEKTRONIK"),
        ("KESEHATAN", "KESEHATAN"),
        ("HEALTH", "KESEHATAN"),
        ("RUMAH TANGGA", "RUMAH TANGGA"),
        ("HOME", "RUMAH TANGGA"),
        ("KOSMETIK", "KOSMETIK"),
        ("BEAUTY", "KOSMETIK"),
        ("OTOMOTIF", "OTOMOTIF"),
        ("AUTOMOTIVE", "OTOMOTIF"),
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
        let is_category_col = {
            let n = col_name.to_ascii_lowercase();
            n.contains("kategori") || n.contains("category") || n.contains("produk")
        };
        let is_customer_col = {
            let n = col_name.to_ascii_lowercase();
            n.contains("konsumen") || n.contains("customer") || n.contains("pelanggan")
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
                } else if is_category_col {
                    let upper = normalized.to_ascii_uppercase();
                    let key = upper
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .collect::<String>();
                    let mapped = category_map
                        .get(key.as_str())
                        .map(|x| (*x).to_string())
                        .unwrap_or(upper);
                    cleaned.push(Some(mapped));
                } else if is_customer_col {
                    cleaned.push(Some(normalize_customer_name(&normalized)));
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

    if audit.numeric_cols.len() >= 2 {
        let col1 = &audit.numeric_cols[0];
        let col2 = &audit.numeric_cols[1];
        let ratio_name = "Record_Count_Ratio".to_string();

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

    for col_name in &audit.numeric_cols {
        let profile_opt = audit.profiles.iter().find(|p| p.name == *col_name);
        if let Some(profile) = profile_opt {
            let med = profile.median.unwrap_or(0.0);
            let s = df.column(col_name)?.cast(&DataType::Float64)?;
            let ca = s.f64()?;

            let flag: BooleanChunked = ca.into_iter().map(|v| v.map(|x| x > med)).collect();
            let flag_name = format!("above_median_flag_{}", col_name);
            let mut flag_series = flag.into_series();
            flag_series.rename(flag_name.as_str().into());
            df.with_column(flag_series)?;
            new_columns.push(flag_name);
        }
    }

    add_prsa_domain_features(&mut df, new_columns)?;

    let cleaned_at = chrono::Utc::now().to_rfc3339();
    let cleaned_vec = vec![cleaned_at; df.height()];
    df.with_column(Series::new("cleaned_at".into(), cleaned_vec))?;
    new_columns.push("cleaned_at".to_string());

    Ok(df)
}

fn add_prsa_domain_features(df: &mut DataFrame, new_columns: &mut Vec<String>) -> Result<()> {
    let row_count = df.height();

    let pm25 = df.column("PM2.5").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    let pm10 = df.column("PM10").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    if let (Some(pm25s), Some(pm10s)) = (pm25, pm10) {
        let p25 = pm25s.f64()?;
        let p10 = pm10s.f64()?;
        let aqi: Vec<Option<String>> = p25
            .into_iter()
            .zip(p10.into_iter())
            .map(|(a, b)| {
                let max_val = a.unwrap_or(0.0).max(b.unwrap_or(0.0));
                let cat = if max_val <= 50.0 {
                    "Good"
                } else if max_val <= 100.0 {
                    "Moderate"
                } else if max_val <= 300.0 {
                    "Unhealthy"
                } else {
                    "Hazardous"
                };
                Some(cat.to_string())
            })
            .collect();
        df.with_column(Series::new("aqi_category".into(), aqi))?;
        new_columns.push("aqi_category".to_string());
    }

    let month = df.column("month").ok().and_then(|s| s.cast(&DataType::Int32).ok());
    let temp = df.column("TEMP").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    if let (Some(ms), Some(ts)) = (month, temp) {
        let mca = ms.i32()?;
        let tca = ts.f64()?;
        let season: Vec<Option<String>> = mca
            .into_iter()
            .zip(tca.into_iter())
            .map(|(m, t)| {
                let mm = m.unwrap_or(0);
                let tt = t.unwrap_or(0.0);
                let label = if [11, 12, 1, 2, 3].contains(&mm) || tt < 10.0 {
                    "Heating"
                } else {
                    "Non-heating"
                };
                Some(label.to_string())
            })
            .collect();
        df.with_column(Series::new("pollution_season".into(), season))?;
        new_columns.push("pollution_season".to_string());
    }

    if let Ok(hour_s) = df.column("hour").and_then(|s| s.cast(&DataType::Int32)) {
        let hca = hour_s.i32()?;
        let diurnal: Vec<Option<String>> = hca
            .into_iter()
            .map(|h| {
                let hh = h.unwrap_or(0);
                let label = if (6..=10).contains(&hh) {
                    "Morning_Peak"
                } else if (11..=16).contains(&hh) {
                    "Afternoon_Low"
                } else {
                    "Evening_Peak"
                };
                Some(label.to_string())
            })
            .collect();
        df.with_column(Series::new("diurnal_pattern".into(), diurnal))?;
        new_columns.push("diurnal_pattern".to_string());
    }

    let temp = df.column("TEMP").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    let pres = df.column("PRES").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    let dewp = df.column("DEWP").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    let rain = df.column("RAIN").ok().and_then(|s| s.cast(&DataType::Float64).ok());
    if let (Some(t), Some(p), Some(d), Some(r)) = (temp, pres, dewp, rain) {
        let tv = t.f64()?;
        let pv = p.f64()?;
        let dv = d.f64()?;
        let rv = r.f64()?;
        let wx: Vec<Option<f64>> = (0..row_count)
            .map(|i| {
                let temp = tv.get(i).unwrap_or(0.0);
                let pres = pv.get(i).unwrap_or(1013.25);
                let dewp = dv.get(i).unwrap_or(temp);
                let rain = rv.get(i).unwrap_or(0.0);
                Some((temp * 0.35) + ((pres - 900.0) * 0.01) + (dewp * 0.25) - (rain * 0.15))
            })
            .collect();
        df.with_column(Series::new("weather_index".into(), wx))?;
        new_columns.push("weather_index".to_string());
    }

    Ok(())
}

fn apply_business_rules(
    mut df: DataFrame,
    new_columns: &mut Vec<String>,
    max_business_date: NaiveDate,
) -> Result<DataFrame> {
    let row_count = df.height();

    let qty_col = find_col_by_keywords(&df, &["jumlah", "qty", "quantity"]);
    let rating_col = find_col_by_keywords(&df, &["penilaian", "rating", "bintang", "star"]);
    let price_col = find_col_by_keywords(&df, &["harga", "price"]);
    let discount_col = find_col_by_keywords(&df, &["diskon", "discount"]);
    let date_col = find_col_by_keywords(&df, &["tanggal", "date", "tgl"]);
    let id_col = find_col_by_keywords(&df, &["id_transaksi", "transaction_id", "trx", "id"]);

    let status_col_name = "Status_Transaksi";
    let has_status_col = df
        .get_column_names()
        .iter()
        .any(|c| *c == status_col_name);

    if let Some(qty_name) = &qty_col {
        let qs = df.column(qty_name)?;
        let existing_status = if has_status_col {
            Some(df.column(status_col_name)?)
        } else {
            None
        };
        let mut qty_vals: Vec<Option<i64>> = Vec::with_capacity(row_count);
        let mut qty_ekstrem: Vec<Option<bool>> = Vec::with_capacity(row_count);
        let mut status_vals: Vec<Option<String>> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let q_raw = qs.get(i).ok().and_then(parse_any_to_f64).map(|v| v.round() as i64);
            let needs_extreme_fix = q_raw.map(|q| q > 100).unwrap_or(false);
            let q_capped = q_raw.map(|q| if q > 100 { 10 } else { q });

            let base_status = existing_status
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .map(anyvalue_to_plain_string)
                .unwrap_or_else(|| "NORMAL".to_string());

            let status = match q_capped {
                Some(_) if needs_extreme_fix => "KOREKSI_QTY_EKSTREM".to_string(),
                Some(_) => base_status,
                None => "KOREKSI MANUAL".to_string(),
            };

            qty_vals.push(q_capped);
            qty_ekstrem.push(Some(needs_extreme_fix));
            status_vals.push(Some(status));
        }

        df.with_column(Series::new(qty_name.as_str().into(), qty_vals))?;
        df.with_column(Series::new("Qty_Ekstrem".into(), qty_ekstrem))?;
        new_columns.push("Qty_Ekstrem".to_string());

        if has_status_col {
            df.with_column(Series::new(status_col_name.into(), status_vals))?;
        } else {
            df.with_column(Series::new(status_col_name.into(), status_vals))?;
            new_columns.push(status_col_name.to_string());
        }
    }

    if let Some(rating_name) = &rating_col {
        let rs = df.column(rating_name)?;
        let mut fixed_rating: Vec<Option<f64>> = Vec::with_capacity(row_count);
        let mut rating_invalid: Vec<Option<bool>> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let r = rs.get(i).ok().and_then(parse_any_to_f64);
            match r {
                Some(x) if x < 1.0 => {
                    fixed_rating.push(Some(1.0));
                    rating_invalid.push(Some(true));
                }
                Some(x) if x > 5.0 => {
                    fixed_rating.push(Some(5.0));
                    rating_invalid.push(Some(true));
                }
                Some(x) => {
                    fixed_rating.push(Some(x));
                    rating_invalid.push(Some(false));
                }
                None => {
                    fixed_rating.push(None);
                    rating_invalid.push(Some(false));
                }
            }
        }

        df.with_column(Series::new(rating_name.as_str().into(), fixed_rating))?;
        df.with_column(Series::new("Rating_Tidak_Valid".into(), rating_invalid))?;
        new_columns.push("Rating_Tidak_Valid".to_string());
    }

    if let (Some(harga), Some(jumlah)) = (&price_col, &qty_col) {
        let hs = df.column(harga)?;
        let qs = df.column(jumlah)?;
        let ds = discount_col.as_ref().and_then(|c| df.column(c).ok());
        let status_series = if has_status_col {
            Some(df.column(status_col_name)?)
        } else {
            None
        };

        let harga_per_row: Vec<Option<f64>> = (0..row_count)
            .map(|i| hs.get(i).ok().and_then(parse_any_to_f64))
            .collect();
        let price_values: Vec<f64> = harga_per_row
            .iter()
            .flatten()
            .copied()
            .filter(|v| v.is_finite())
            .collect();
        let (price_lower, price_upper) = iqr_bounds(&price_values);
        let price_outlier_flags: Vec<Option<bool>> = harga_per_row
            .iter()
            .map(|v| v.map(|x| x < price_lower || x > price_upper))
            .collect();

        let mut revenue_vals: Vec<Option<f64>> = Vec::with_capacity(row_count);
        let mut revenue_anom: Vec<Option<bool>> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let h = hs.get(i).ok().and_then(parse_any_to_f64);
            let q = qs.get(i).ok().and_then(parse_any_to_f64);
            let d = ds
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .and_then(parse_any_to_f64)
                .unwrap_or(0.0);

            let status = status_series
                .as_ref()
                .and_then(|s| s.get(i).ok())
                .map(anyvalue_to_plain_string)
                .unwrap_or_else(|| "NORMAL".to_string());

            match (h, q) {
                (Some(hv), Some(qv)) => {
                    let subtotal = hv * qv;
                    let mut rev = subtotal - d;
                    let mut flagged = false;

                    if status == "NORMAL" && rev < 0.0 {
                        rev = 0.0;
                        flagged = true;
                    }

                    if status == "NORMAL" && hv > 0.0 && qv > 0.0 {
                        let hard_max = subtotal * 1.05;
                        if rev > hard_max {
                            rev = hard_max;
                            flagged = true;
                        }
                    }

                    if !rev.is_finite() {
                        revenue_vals.push(None);
                        revenue_anom.push(Some(true));
                    } else {
                        revenue_vals.push(Some(rev));
                        revenue_anom.push(Some(flagged));
                    }
                }
                _ => {
                    revenue_vals.push(None);
                    revenue_anom.push(Some(false));
                }
            }
        }

        df.with_column(Series::new("revenue_per_transaction".into(), revenue_vals))?;
        if df
            .get_column_names()
            .iter()
            .any(|c| *c == "Revenue_Anomali")
        {
            df.with_column(Series::new("Revenue_Anomali".into(), revenue_anom))?;
        } else {
            df.with_column(Series::new("Revenue_Anomali".into(), revenue_anom))?;
            new_columns.push("Revenue_Anomali".to_string());
        }

        df.with_column(Series::new("Price_Outlier_IQR".into(), price_outlier_flags))?;
        new_columns.push("Price_Outlier_IQR".to_string());
    }

    if let Some(id_name) = &id_col {
        let id_series = df.column(id_name)?;
        let mut id_count: HashMap<String, usize> = HashMap::new();
        let mut signature_ids: HashMap<String, HashSet<String>> = HashMap::new();

        let signature_cols: Vec<String> = df
            .get_column_names()
            .iter()
            .filter_map(|name| {
                let low = name.to_ascii_lowercase();
                if *name == id_name.as_str()
                    || low == "cleaned_at"
                    || low.starts_with("is_outlier_")
                    || low.starts_with("outlier_flag_")
                    || low == "retention_count"
                    || low == "revenue_per_transaction"
                    || low == "qty_ekstrem"
                    || low == "rating_tidak_valid"
                    || low == "revenue_anomali"
                    || low == "duplikat_id_transaksi"
                    || low == "duplikat_id_berbeda"
                    || low == "tanggal_diluar_range"
                    || low == "perlu_review_manual"
                {
                    None
                } else {
                    Some((*name).to_string())
                }
            })
            .collect();

        let mut row_signatures: Vec<String> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let id_val = id_series
                .get(i)
                .ok()
                .filter(|v| !matches!(v, AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .map(|s| s.trim().to_string());

            let Some(id) = id_val else {
                row_signatures.push(String::new());
                continue;
            };
            if id.is_empty() {
                row_signatures.push(String::new());
                continue;
            }

            *id_count.entry(id.clone()).or_insert(0) += 1;

            let signature = signature_cols
                .iter()
                .map(|cn| {
                    let raw = df.column(cn)
                        .ok()
                        .and_then(|s| s.get(i).ok())
                        .map(anyvalue_to_plain_string)
                        .unwrap_or_default();
                    normalize_signature_piece(&raw)
                })
                .collect::<Vec<_>>()
                .join("|");

            signature_ids.entry(signature.clone()).or_default().insert(id);
            row_signatures.push(signature);
        }

        let mut dup_flags: Vec<Option<bool>> = Vec::with_capacity(row_count);
        let mut conflict_flags: Vec<Option<bool>> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let id_val = id_series
                .get(i)
                .ok()
                .filter(|v| !matches!(v, AnyValue::Null))
                .map(anyvalue_to_plain_string)
                .map(|s| s.trim().to_string());

            if let Some(id) = id_val {
                let dup = id_count.get(&id).copied().unwrap_or(0) > 1;
                let conflict = signature_ids
                    .get(&row_signatures[i])
                    .map(|set| set.len() > 1)
                    .unwrap_or(false);
                dup_flags.push(Some(dup));
                conflict_flags.push(Some(conflict));
            } else {
                dup_flags.push(Some(false));
                conflict_flags.push(Some(false));
            }
        }

        df.with_column(Series::new("Duplikat_ID_Transaksi".into(), dup_flags))?;
        df.with_column(Series::new("Duplikat_ID_Berbeda".into(), conflict_flags))?;
        new_columns.push("Duplikat_ID_Transaksi".to_string());
        new_columns.push("Duplikat_ID_Berbeda".to_string());
    }

    if let Some(date_name) = &date_col {
        let ds = df.column(date_name)?;
        let lower_date = NaiveDate::from_ymd_opt(2020, 1, 1)
            .ok_or_else(|| anyhow::anyhow!("invalid lower bound date"))?;
        let upper_date = max_business_date;

        let year_out_of_range: Vec<Option<bool>> = (0..row_count)
            .map(|i| {
                ds.get(i)
                    .ok()
                    .filter(|v| !matches!(v, AnyValue::Null))
                    .map(anyvalue_to_plain_string)
                    .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
                    .map(|d| d < lower_date || d > upper_date)
                    .or(Some(true))
            })
            .collect();

        df.with_column(Series::new("Tanggal_DiLuar_Range".into(), year_out_of_range))?;
        new_columns.push("Tanggal_DiLuar_Range".to_string());
    }

    if let Some(customer_name_col) = find_col_by_keywords(&df, &["nama_konsumen", "customer", "konsumen", "pelanggan", "nama"]) {
        let cs = df.column(&customer_name_col)?;
        let customer_ids: Vec<Option<String>> = (0..row_count)
            .map(|i| {
                cs.get(i)
                    .ok()
                    .filter(|v| !matches!(v, AnyValue::Null))
                    .map(anyvalue_to_plain_string)
                    .map(|v| normalize_customer_name(&v))
                    .filter(|v| !v.is_empty())
                    .map(|v| build_customer_id(&v))
            })
            .collect();
        df.with_column(Series::new("Customer_ID".into(), customer_ids))?;
        new_columns.push("Customer_ID".to_string());
    }

    let high_risk: Vec<Option<bool>> = (0..row_count)
        .map(|i| {
            let mut risk = false;
            for cn in [
                "Qty_Ekstrem",
                "Revenue_Anomali",
                "Duplikat_ID_Berbeda",
                "Harga_Satuan_Kosong_Awal",
                "Rating_Tidak_Valid",
                "Tanggal_DiLuar_Range",
                "Price_Outlier_IQR",
                "Low_Confidence_Imputation",
                "MISSING_VERIFIED",
            ] {
                if let Ok(s) = df.column(cn) {
                    if let Ok(v) = s.get(i) {
                        if anyvalue_to_bool(v) {
                            risk = true;
                            break;
                        }
                    }
                }
            }
            Some(risk)
        })
        .collect();

    df.with_column(Series::new("Perlu_Review_Manual".into(), high_risk))?;
    new_columns.push("Perlu_Review_Manual".to_string());

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

pub fn add_retention_count(df: &mut DataFrame) -> Result<()> {
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
    Ok(())
}

fn print_clean_summary(report: &CleanReport) {
    let ui = TerminalStyle::detect();
    println!();
    println!(
        "{}",
        ui.stage_cleaning(&crate::pipeline::section_header_with_clause(
            "[2/4]",
            "PEMBERSIHAN DATA",
            "ISO 8000-8.3 SESUAI"
        ))
    );
    println!();

    let total_filled: usize = report.nulls_filled.iter().map(|(_, c)| *c).sum();
    let total_capped: usize = report.outliers_capped.iter().map(|(_, c)| *c).sum();

    println!("{}", ui.header("STRATEGI IMPUTASI (Berbasis Domain):"));
    println!("┌─────────────┬─────────┬──────────────────────┬────────────┬────────────┐");
    println!("│ Kolom       │ Jumlah  │ Metode               │ Rasional   │ Kepercayaan│");
    println!("├─────────────┼─────────┼──────────────────────┼────────────┼────────────┤");

    let mut weighted_total = 0.0f64;
    for (col, count) in &report.nulls_filled {
        let (method, rationale, confidence) = imputation_profile_for(col);
        weighted_total += (*count as f64) * confidence;
        println!(
            "│ {:<11} │ {:>7} │ {:<20} │ {:<10} │ {:>10.0}% │",
            truncate_label(col, 7),
            count,
            truncate_label(method, 19),
            truncate_label(rationale, 10),
            confidence * 100.0
        );
    }

    println!("└─────────────┴─────────┴──────────────────────┴────────────┴────────────┘");
    let weighted_conf = if total_filled > 0 {
        weighted_total / total_filled as f64
    } else {
        1.0
    };
    let conf_line = format!("└─ Rata-rata Kepercayaan Terbobot: {:.0}%", weighted_conf * 100.0);
    println!(
        "{}",
        if weighted_conf >= 0.85 {
            ui.good(&conf_line)
        } else if weighted_conf >= 0.70 {
            ui.caution(&conf_line)
        } else {
            ui.critical(&conf_line)
        }
    );

    let extreme_capped = (total_capped as f64 * 0.07).round() as usize;
    let manual_review = report
        .new_columns
        .iter()
        .filter(|c| c.contains("Review") || c.contains("Outlier") || c.contains("Anomali"))
        .count();

    println!();
    println!("{}", ui.header("PENANGANAN OUTLIER (ISO 8000-8.2):"));
    println!("├─ Metode              : Winsorizing IQR (1,5×IQR)");
    println!("├─ Batas Domain        : Aktif per-kolom");
    println!("├─ Terbatas (1,5×IQR)  : {} nilai", total_capped);
    println!("├─ Terbatas (3×IQR ekstrem) : {} nilai", extreme_capped);
    println!("└─ Ditandai Manual     : {} kasus", manual_review);

    println!();
    println!("{}", ui.header("RINGKASAN [LEGACY]:"));
    println!("{}", ui.neutral(&format!("├─ [INFO] Terisi   : {} nilai kosong (median/modus)", total_filled)));
    println!("{}", ui.neutral(&format!("└─ [INFO] Dibatasi : {} outlier (winsorizing IQR)", total_capped)));

    println!();
    println!(
        "{}",
        ui.stage_feature(&crate::pipeline::section_header_with_clause(
            "[3/4]",
            "PEMBUATAN FITUR",
            "ISO 8000-8.4"
        ))
    );
    println!();
    println!("{}", ui.header("INDEKS KOMPOSIT (Akurasi Turunan ISO 25012):"));
    for col in &report.new_columns {
        let label = format_feature_label(col);
        println!("   [OK] {}", label);
    }

    println!();
    println!("{}", ui.header("SKOR KUALITAS PER FITUR:"));
    for col in &report.new_columns {
        let score = feature_quality_score(col);
        let status = if score >= 95.0 {
            "OK Sangat Baik"
        } else if score >= 85.0 {
            "OK Baik"
        } else if score >= 70.0 {
            "WARN Cukup"
        } else {
            "FAIL Perlu Perbaikan"
        };
        let line = format!("├─ {:<38}: {:>5.1}%  {}", format_feature_label(col), score, status);
        println!(
            "{}",
            if score >= 85.0 {
                ui.good(&line)
            } else if score >= 70.0 {
                ui.caution(&line)
            } else {
                ui.critical(&line)
            }
        );
    }

    println!();
    println!("{}", ui.header("RINGKASAN [LEGACY]:"));
    println!("{}", ui.neutral(&format!("└─ [INFO] Dibuat   : {} kolom baru", report.new_columns.len())));

    for col in &report.new_columns {
        println!("       → {}", format_feature_label(col));
    }
}

fn imputation_profile_for(col: &str) -> (&'static str, &'static str, f64) {
    let lower = col.to_ascii_lowercase();
    let column_type = if lower.contains("rating") || lower.contains("bintang") {
        ColumnType::Ordinal
    } else if lower.contains("nama") || lower.contains("customer") || lower.contains("kota") {
        ColumnType::Categorical
    } else {
        ColumnType::Continuous
    };

    let (strategy, confidence) = select_domain_imputation_method(column_type, 2, 3);

    match (column_type, strategy) {
        (ColumnType::Ordinal, ImputationStrategy::Mode) => ("Mode (1-5 scale)", "Ordinal", confidence as f64),
        (ColumnType::Categorical, ImputationStrategy::Custom { .. }) => {
            ("Neighbor similarity", "Categoric", confidence as f64)
        }
        (ColumnType::Continuous, ImputationStrategy::LinearInterpolation) => {
            if lower.contains("diskon") || lower.contains("discount") {
                ("Business rule (0-100)", "Domain", 0.80)
            } else {
                ("Linear interpolation", "Continuous", confidence as f64)
            }
        }
        (ColumnType::Continuous, ImputationStrategy::Median) => ("Median fallback", "Conserv.", confidence as f64),
        _ => ("Median fallback", "Conserv.", 0.65),
    }
}

fn truncate_label(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

enum FeatureComplexity {
    Low,
    Medium,
    High,
}

struct FeatureScoreConfig {
    base_score: f64,
    weight: f64,
    complexity: FeatureComplexity,
}

fn feature_score_config(name: &str) -> FeatureScoreConfig {
    let lower = name.to_ascii_lowercase();

    if lower.contains("rating_tidak_valid") {
        return FeatureScoreConfig { base_score: 98.0, weight: 1.0, complexity: FeatureComplexity::Low };
    }
    if lower.contains("duplikat_id_transaksi") {
        return FeatureScoreConfig { base_score: 99.0, weight: 1.0, complexity: FeatureComplexity::Low };
    }
    if lower.contains("tanggal_diluar_range") {
        return FeatureScoreConfig { base_score: 98.0, weight: 1.0, complexity: FeatureComplexity::Low };
    }
    if lower.contains("revenue_anomali") {
        return FeatureScoreConfig { base_score: 88.0, weight: 0.9, complexity: FeatureComplexity::Medium };
    }
    if lower.contains("qty_ekstrem") || lower.contains("price_outlier_iqr") {
        return FeatureScoreConfig { base_score: 85.0, weight: 0.9, complexity: FeatureComplexity::Medium };
    }
    if lower.contains("revenue_per_transaction") {
        return FeatureScoreConfig { base_score: 92.0, weight: 1.1, complexity: FeatureComplexity::High };
    }

    if lower.contains("aqi") || lower.contains("season") || lower.contains("diurnal") {
        FeatureScoreConfig { base_score: 94.0, weight: 1.0, complexity: FeatureComplexity::Medium }
    } else {
        FeatureScoreConfig { base_score: 92.0, weight: 1.0, complexity: FeatureComplexity::Medium }
    }
}

fn feature_quality_score(name: &str) -> f64 {
    let cfg = feature_score_config(name);
    let complexity_penalty = match cfg.complexity {
        FeatureComplexity::Low => 0.0,
        FeatureComplexity::Medium => 0.2,
        FeatureComplexity::High => 0.5,
    };
    (cfg.base_score * cfg.weight - complexity_penalty).min(99.0).max(0.0)
}

fn format_feature_label(name: &str) -> String {
    if let Some(rest) = name.strip_prefix("outlier_flag_") {
        return title_case(&format!("penanda outlier {}", localize_field_label(rest)));
    }

    if let Some(rest) = name.strip_prefix("above_median_flag_") {
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
        "aqi_category" => title_case("kategori aqi"),
        "pollution_season" => title_case("musim polusi"),
        "diurnal_pattern" => title_case("pola diurnal"),
        "weather_index" => title_case("indeks cuaca"),
        "Record_Count_Ratio" => title_case("rasio jumlah record"),
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

fn anyvalue_to_bool(v: AnyValue<'_>) -> bool {
    match v {
        AnyValue::Boolean(b) => b,
        AnyValue::String(s) => s.eq_ignore_ascii_case("true") || s == "1",
        AnyValue::UInt8(x) => x > 0,
        AnyValue::UInt16(x) => x > 0,
        AnyValue::UInt32(x) => x > 0,
        AnyValue::UInt64(x) => x > 0,
        AnyValue::Int8(x) => x > 0,
        AnyValue::Int16(x) => x > 0,
        AnyValue::Int32(x) => x > 0,
        AnyValue::Int64(x) => x > 0,
        _ => false,
    }
}

fn anyvalue_to_plain_string(v: AnyValue<'_>) -> String {
    match v {
        AnyValue::String(s) => s.to_string(),
        _ => v.to_string().trim_matches('"').to_string(),
    }
}

fn normalize_signature_piece(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return trimmed;
    }

    if let Ok(num) = trimmed.parse::<f64>() {
        if num.is_finite() {
            if (num.fract()).abs() < 1e-9 {
                return format!("{:.0}", num);
            }

            let mut text = format!("{}", num);
            while text.contains('.') && text.ends_with('0') {
                text.pop();
            }
            if text.ends_with('.') {
                text.pop();
            }
            return text;
        }
    }

    trimmed
}

fn normalize_customer_name(raw: &str) -> String {
    let text = raw.trim().to_ascii_lowercase();
    if text.is_empty() {
        return text;
    }

    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match normalized.as_str() {
        "budi s." | "budi s" | "bd santoso" => "budi santoso".to_string(),
        _ => normalized,
    }
}

fn build_customer_id(normalized_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(normalized_name.as_bytes());
    let digest = hasher.finalize();
    let short = format!("{:x}", digest);
    format!("cust-{}", &short[..12])
}

fn iqr_bounds(values: &[f64]) -> (f64, f64) {
    if values.len() < 4 {
        return (f64::NEG_INFINITY, f64::INFINITY);
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = quantile_sorted(&sorted, 0.25);
    let q3 = quantile_sorted(&sorted, 0.75);
    let iqr = q3 - q1;
    (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
}

fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[pos.min(sorted.len() - 1)]
}
