//! # Data Cleansing Module
//!
//! Adaptive and fully dynamic cleansing for arbitrary CSV schema.

use anyhow::{Context, Result};
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};

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

    let pb = ProgressBar::new(4);
    pb.set_style(
        ProgressStyle::with_template("{bar:40.cyan/blue} {pos}/{len} {msg}")
            .context("failed to set progress style")?,
    );

    pb.set_message("Fill missing values");
    cleaned_df = fill_missing_values(cleaned_df, audit, &mut nulls_filled)?;
    pb.inc(1);

    pb.set_message("Cap outliers + flags");
    cleaned_df = handle_outliers(cleaned_df, audit, &mut outliers_capped, &mut new_columns)?;
    pb.inc(1);

    pb.set_message("Standardize strings");
    cleaned_df = standardize_strings(cleaned_df, audit)?;
    pb.inc(1);

    pb.set_message("Feature engineering");
    cleaned_df = feature_engineering(cleaned_df, audit, &mut new_columns)?;
    pb.inc(1);
    pb.finish_with_message("Cleansing completed");

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
                        filled.push(Some(v.to_string()));
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

    for col_name in &audit.string_cols {
        let s = df.column(col_name)?;

        let mut cleaned: Vec<Option<String>> = Vec::with_capacity(s.len());
        for idx in 0..s.len() {
            let v = s.get(idx)?;
            if matches!(v, AnyValue::Null) {
                cleaned.push(None);
            } else {
                let lower = v.to_string().to_lowercase();
                let trimmed = lower.trim().to_string();
                let normalized = re.replace_all(&trimmed, "").to_string();
                cleaned.push(Some(normalized));
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

fn print_clean_summary(report: &CleanReport) {
    println!();
    println!("{}", "── [2/4] CLEANSING ───────────────────────────".bold());
    println!();

    let total_filled: usize = report.nulls_filled.iter().map(|(_, c)| *c).sum();
    let total_capped: usize = report.outliers_capped.iter().map(|(_, c)| *c).sum();

    println!(
        "  {} Filled   : {} null values (median/mode)",
        "⚡".cyan(),
        total_filled
    );
    println!(
        "  {} Capped   : {} outliers (IQR winsorizing)",
        "✂️".cyan(),
        total_capped
    );

    println!();
    println!("{}", "── [3/4] FEATURE ENGINEERING ─────────────────".bold());
    println!();
    println!(
        "  {} Created  : {} new columns",
        "🔧".cyan(),
        report.new_columns.len()
    );

    for col in &report.new_columns {
        println!("       → {}", col);
    }
}
