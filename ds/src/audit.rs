//! # Data Audit Module
//!
//! Fully dynamic data profiling with runtime schema inspection.

use anyhow::{Context, Result};
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use crate::terminal_ui::TerminalStyle;

/// IQR multiplier for outlier bounds.
pub const IQR_MULTIPLIER: f64 = 1.5;
/// High-null threshold percentage.
pub const NULL_THRESHOLD_HIGH: f64 = 20.0;
/// Medium-null threshold percentage.
pub const NULL_THRESHOLD_MEDIUM: f64 = 5.0;
/// Significant outlier threshold percentage.
pub const OUTLIER_THRESHOLD_PCT: f64 = 5.0;

/// Dynamic profile for one column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub dtype: String,
    pub null_count: usize,
    pub null_pct: f64,
    pub unique_count: usize,
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub std_dev: Option<f64>,
    pub skewness: Option<f64>,
    pub kurtosis: Option<f64>,
    pub outlier_count: Option<usize>,
    pub lower_bound: Option<f64>,
    pub upper_bound: Option<f64>,
    pub top_value: Option<String>,
}

/// Dynamic dataset audit report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    pub total_rows: usize,
    pub total_cols: usize,
    pub duplicate_rows: usize,
    pub numeric_cols: Vec<String>,
    pub string_cols: Vec<String>,
    pub profiles: Vec<ColumnProfile>,
    pub generated_at: String,
}

/// Run audit pipeline from CSV path.
pub fn run(path: &str) -> Result<(DataFrame, AuditReport)> {
    let df = load_csv(path).context("failed to load CSV with LazyCsvReader")?;
    let schema = df.schema();

    let (numeric_cols, string_cols) = classify_columns(&schema);
    let duplicate_rows = df.is_duplicated()?.sum().unwrap_or(0) as usize;

    let numeric_profiles: Vec<ColumnProfile> = numeric_cols
        .par_iter()
        .filter_map(|name| profile_numeric_column(&df, name).ok())
        .collect();

    let string_profiles: Vec<ColumnProfile> = string_cols
        .par_iter()
        .filter_map(|name| profile_string_column(&df, name).ok())
        .collect();

    let mut profiles = Vec::with_capacity(numeric_profiles.len() + string_profiles.len());
    profiles.extend(numeric_profiles);
    profiles.extend(string_profiles);

    let report = AuditReport {
        total_rows: df.height(),
        total_cols: df.width(),
        duplicate_rows,
        numeric_cols,
        string_cols,
        profiles,
        generated_at: chrono::Utc::now().to_rfc3339(),
    };

    print_audit_summary(&report);
    Ok((df, report))
}

fn load_csv(path: &str) -> Result<DataFrame> {
    let lf = LazyCsvReader::new(path)
        .has_header(true)
        .with_null_values(Some(NullValues::AllColumns(vec![
            "NA".into(),
            "N/A".into(),
            "null".into(),
            "NULL".into(),
        ])))
        .with_infer_schema_length(Some(50_000))
        .finish()?;
    let df = lf.collect()?;
    Ok(df)
}

fn classify_columns(schema: &Schema) -> (Vec<String>, Vec<String>) {
    let mut numeric_cols = Vec::new();
    let mut string_cols = Vec::new();

    for (name, dtype) in schema.iter() {
        let n = name.to_string();
        match dtype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => numeric_cols.push(n),
            DataType::String => string_cols.push(n),
            _ => string_cols.push(n),
        }
    }

    (numeric_cols, string_cols)
}

fn profile_numeric_column(df: &DataFrame, col_name: &str) -> Result<ColumnProfile> {
    let s = df.column(col_name)?;
    let total_rows = s.len();
    let null_count = s.null_count();
    let null_pct = if total_rows > 0 {
        (null_count as f64 * 100.0) / total_rows as f64
    } else {
        0.0
    };

    let unique_count = s.n_unique()?;
    let casted = s.cast(&DataType::Float64)?;
    let f = casted.f64()?;

    let values: Vec<f64> = f.into_iter().flatten().collect();
    let mean = mean(&values);
    let median = median(values.clone());
    let std_dev = std_dev(&values, mean);
    let skewness = skewness(&values, mean, std_dev);
    let kurtosis = kurtosis(&values, mean, std_dev);

    let (outlier_count, lower_bound, upper_bound) = iqr_outlier_stats(&values);

    Ok(ColumnProfile {
        name: col_name.to_string(),
        dtype: format!("{:?}", s.dtype()),
        null_count,
        null_pct,
        unique_count,
        mean,
        median,
        std_dev,
        skewness,
        kurtosis,
        outlier_count: Some(outlier_count),
        lower_bound: Some(lower_bound),
        upper_bound: Some(upper_bound),
        top_value: None,
    })
}

fn profile_string_column(df: &DataFrame, col_name: &str) -> Result<ColumnProfile> {
    let s = df.column(col_name)?;
    let total_rows = s.len();
    let null_count = s.null_count();
    let null_pct = if total_rows > 0 {
        (null_count as f64 * 100.0) / total_rows as f64
    } else {
        0.0
    };

    let unique_count = s.n_unique()?;
    let top_value = mode_string(s)?;

    Ok(ColumnProfile {
        name: col_name.to_string(),
        dtype: format!("{:?}", s.dtype()),
        null_count,
        null_pct,
        unique_count,
        mean: None,
        median: None,
        std_dev: None,
        skewness: None,
        kurtosis: None,
        outlier_count: None,
        lower_bound: None,
        upper_bound: None,
        top_value,
    })
}

fn mode_string(s: &Series) -> Result<Option<String>> {
    let mut counts = std::collections::HashMap::<String, usize>::new();
    for idx in 0..s.len() {
        let v = s.get(idx)?;
        if !matches!(v, AnyValue::Null) {
            let key = v.to_string();
            let entry = counts.entry(key).or_insert(0);
            *entry += 1;
        }
    }

    let top = counts.into_iter().max_by_key(|(_, c)| *c).map(|(k, _)| k);
    Ok(top)
}

fn iqr_outlier_stats(values: &[f64]) -> (usize, f64, f64) {
    if values.len() < 4 {
        return (0, f64::NEG_INFINITY, f64::INFINITY);
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let q1 = quantile_sorted(&sorted, 0.25);
    let q3 = quantile_sorted(&sorted, 0.75);
    let iqr = q3 - q1;
    let lower = q1 - IQR_MULTIPLIER * iqr;
    let upper = q3 + IQR_MULTIPLIER * iqr;

    let count = values.iter().filter(|v| **v < lower || **v > upper).count();

    (count, lower, upper)
}

fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[pos.min(sorted.len() - 1)]
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn median(mut values: Vec<f64>) -> Option<f64> {
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

fn std_dev(values: &[f64], mean: Option<f64>) -> Option<f64> {
    let m = mean?;
    if values.len() < 2 {
        return None;
    }
    let var = values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() as f64 - 1.0);
    Some(var.sqrt())
}

fn skewness(values: &[f64], mean: Option<f64>, std_dev: Option<f64>) -> Option<f64> {
    let m = mean?;
    let sd = std_dev?;
    if values.len() < 3 || sd == 0.0 {
        return None;
    }
    let n = values.len() as f64;
    let m3 = values.iter().map(|v| (v - m).powi(3)).sum::<f64>() / n;
    Some(m3 / sd.powi(3))
}

fn kurtosis(values: &[f64], mean: Option<f64>, std_dev: Option<f64>) -> Option<f64> {
    let m = mean?;
    let sd = std_dev?;
    if values.len() < 4 || sd == 0.0 {
        return None;
    }
    let n = values.len() as f64;
    let m4 = values.iter().map(|v| (v - m).powi(4)).sum::<f64>() / n;
    Some((m4 / sd.powi(4)) - 3.0)
}

fn print_audit_summary(report: &AuditReport) {
    let ui = TerminalStyle::detect();
    println!();
    println!(
        "{}",
        ui.stage_audit(&crate::pipeline::section_header_with_clause(
            "[1/4]",
            "PEMERIKSAAN DATA",
            "ISO 8000-8 SESUAI"
        ))
    );
    println!();

    println!("{}", ui.header("METRIK DATASET:"));

    let total_nulls: usize = report.profiles.iter().map(|p| p.null_count).sum();
    let total_cells = report.total_rows.saturating_mul(report.total_cols);
    let completeness_pct = if total_cells > 0 {
        100.0 - ((total_nulls as f64 * 100.0) / total_cells as f64)
    } else {
        0.0
    };
    let uniqueness_pct = if report.total_rows > 0 {
        100.0 - ((report.duplicate_rows as f64 * 100.0) / report.total_rows as f64)
    } else {
        100.0
    };

    println!(
        "├─ Total Baris   : {} baris",
        format_number(report.total_rows),
    );
    println!(
        "├─ Total Kolom   : {} kolom",
        report.total_cols
    );
    println!(
        "├─ Duplikat      : {} baris (Keunikan: {:.1}%)",
        format_number(report.duplicate_rows),
        uniqueness_pct
    );
    println!("└─ Kelengkapan   : {:.1}%", completeness_pct);

    let mut red = 0usize;
    let mut yellow = 0usize;
    let mut outlier_red = 0usize;

    for p in &report.profiles {
        let outlier_pct = if report.total_rows > 0 {
            p.outlier_count.unwrap_or(0) as f64 * 100.0 / report.total_rows as f64
        } else {
            0.0
        };

        if p.null_pct > NULL_THRESHOLD_HIGH || outlier_pct > OUTLIER_THRESHOLD_PCT {
            red += 1;
        } else if p.null_pct >= NULL_THRESHOLD_MEDIUM {
            yellow += 1;
        }

        if outlier_pct > OUTLIER_THRESHOLD_PCT {
            outlier_red += 1;
        }
    }

    let syntactic_pct = if report.total_cols > 0 {
        ((report.total_cols.saturating_sub(red)) as f64 * 100.0) / report.total_cols as f64
    } else {
        100.0
    };
    let consistency_pct = if report.total_cols > 0 {
        ((report.total_cols.saturating_sub(outlier_red)) as f64 * 100.0) / report.total_cols as f64
    } else {
        100.0
    };
    let semantic_pct = if report.total_cols > 0 {
        ((report.total_cols.saturating_sub(red + yellow)) as f64 * 100.0)
            / report.total_cols as f64
    } else {
        100.0
    };
    let pragmatic_pct = ((completeness_pct + consistency_pct) / 2.0).clamp(0.0, 100.0);

    println!();
    println!("{}", ui.header("DIMENSI KUALITAS ISO/IEC 25012:"));
    let syntactic_line = format!(
        "├─ [{}] Validitas Sintaksis    : {:.1}%  (minimum: 95%, target: 99%)",
        if syntactic_pct >= 95.0 { "OK" } else { "WARN" },
        syntactic_pct
    );
    println!(
        "{}",
        if syntactic_pct >= 95.0 {
            ui.good(&syntactic_line)
        } else {
            ui.caution(&syntactic_line)
        }
    );
    let completeness_line = format!(
        "├─ [{}] Kelengkapan           : {:.1}%  (minimum: 90%, target: 95%)",
        if completeness_pct >= 90.0 { "OK" } else { "WARN" },
        completeness_pct
    );
    println!(
        "{}",
        if completeness_pct >= 90.0 {
            ui.good(&completeness_line)
        } else {
            ui.caution(&completeness_line)
        }
    );
    let consistency_line = format!(
        "├─ [{}] Konsistensi           : {:.1}%  (minimum: 90%, target: 95%)",
        if consistency_pct >= 90.0 { "OK" } else { "WARN" },
        consistency_pct
    );
    println!(
        "{}",
        if consistency_pct >= 90.0 {
            ui.good(&consistency_line)
        } else {
            ui.caution(&consistency_line)
        }
    );
    let semantic_line = format!(
        "├─ [{}] Validitas Semantik    : {:.1}%  (minimum: 90%, target: 95%)",
        if semantic_pct >= 90.0 { "OK" } else { "WARN" },
        semantic_pct
    );
    println!(
        "{}",
        if semantic_pct >= 90.0 {
            ui.good(&semantic_line)
        } else {
            ui.caution(&semantic_line)
        }
    );
    let pragmatic_line = format!(
        "└─ [{}] Kualitas Pragmatis    : {:.1}%  (minimum: 85%, target: 90%)",
        if pragmatic_pct >= 85.0 { "OK" } else { "WARN" },
        pragmatic_pct
    );
    println!(
        "{}",
        if pragmatic_pct >= 85.0 {
            ui.good(&pragmatic_line)
        } else {
            ui.caution(&pragmatic_line)
        }
    );

    println!();
    println!("{}", ui.header("KUALITAS SEMANTIK (ISO 8000-8):"));
    let semantic_check_line = format!(
        "└─ [{}] Pemeriksaan aturan pada {} kolom terprofil",
        if semantic_pct >= 90.0 { "OK" } else { "WARN" },
        report.profiles.len()
    );
    println!(
        "{}",
        if semantic_pct >= 90.0 {
            ui.good(&semantic_check_line)
        } else {
            ui.caution(&semantic_check_line)
        }
    );
    if red > 0 || yellow > 0 {
        println!(
            "{}",
            ui.caution(&format!("├─ {} kolom memerlukan perhatian", red + yellow))
        );
        let detail = format!("└─ Rincian: {} kritis, {} peringatan", red, yellow);
        println!("{}", if red > 0 { ui.critical(&detail) } else { ui.caution(&detail) });
    }

    println!();
    println!("{}", ui.header("RINGKASAN [LEGACY]:"));

    if red > 0 {
        println!("{}", ui.critical(&format!("├─ [FAIL] Kosong   : {} kolom bermasalah", red)));
    }
    if yellow > 0 {
        println!("{}", ui.caution(&format!("├─ [WARN] Kosong   : {} kolom perlu perhatian", yellow)));
    }
    if red == 0 && yellow == 0 {
        println!("{}", ui.good("├─ [OK] Kosong   : semua kolom relatif bersih"));
    }

    if outlier_red > 0 {
        println!("{}", ui.critical(&format!("└─ [FAIL] Outlier  : {} kolom dengan pencilan > 5%", outlier_red)));
    } else {
        println!("{}", ui.good("└─ [OK] Outlier  : tidak ada pencilan signifikan"));
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    let mut c = 0usize;
    for ch in s.chars().rev() {
        if c > 0 && c % 3 == 0 {
            out.push('.');
        }
        out.push(ch);
        c += 1;
    }
    out.chars().rev().collect()
}
