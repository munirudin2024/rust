use anyhow::{Context, Result};
use polars::prelude::{CsvWriter, DataFrame, NamedFrom, SerWriter, Series};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::iso_standards::iso25012::QualityMetrics;
use crate::iso_standards::iso8000::{infer_collection_date_from_filename, ProvenanceMetadata};
use crate::validators::iso_compliance_validator::ValidationRun;

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub station: String,
    pub rows_total: usize,
    pub syntactic_error_rows: usize,
    pub semantic_error_rows: usize,
    pub manual_review_rows: usize,
    pub syntactic_error_count: usize,
    pub semantic_error_count: usize,
    pub syntactic_validity_rate: f64,
    pub semantic_validity_rate: f64,
}

pub struct IsoArtifacts {
    pub invalid_syntax_csv: Option<PathBuf>,
    pub invalid_semantic_csv: Option<PathBuf>,
    pub validation_report_json: PathBuf,
    pub provenance_json: PathBuf,
    pub manual_review_sample_csv: Option<PathBuf>,
    pub feedback_json: PathBuf,
    pub outlier_justification_json: PathBuf,
    pub quality_dashboard_html: Option<PathBuf>,
}

pub fn write_invalid_validation_csv(
    output_root: &Path,
    station: &str,
    syntax_df: &DataFrame,
    semantic_df: &DataFrame,
) -> Result<(Option<PathBuf>, Option<PathBuf>)> {
    let dir = output_root.join("validation");
    std::fs::create_dir_all(&dir).context("failed to create output/validation")?;

    let syntax_path = if syntax_df.height() > 0 {
        let p = dir.join(format!("invalid_syntax_{}.csv", station));
        let mut f = File::create(&p)?;
        let mut df = syntax_df.clone();
        CsvWriter::new(&mut f).finish(&mut df)?;
        Some(p)
    } else {
        None
    };

    let semantic_path = if semantic_df.height() > 0 {
        let p = dir.join(format!("invalid_semantic_{}.csv", station));
        let mut f = File::create(&p)?;
        let mut df = semantic_df.clone();
        CsvWriter::new(&mut f).finish(&mut df)?;
        Some(p)
    } else {
        None
    };

    Ok((syntax_path, semantic_path))
}

pub fn write_validation_report(
    output_root: &Path,
    station: &str,
    total_rows: usize,
    run: &ValidationRun,
) -> Result<(PathBuf, ValidationReport)> {
    let dir = output_root.join("validation");
    std::fs::create_dir_all(&dir)?;

    let synt_rows = run.invalid_syntax_indices.len();
    let sem_rows = run.invalid_semantic_indices.len();
    let valid_rows = total_rows.saturating_sub(synt_rows);
    let semantic_valid_rows = total_rows.saturating_sub(sem_rows);

    let report = ValidationReport {
        station: station.to_string(),
        rows_total: total_rows,
        syntactic_error_rows: synt_rows,
        semantic_error_rows: sem_rows,
        manual_review_rows: run.invalid_manual_review_indices.len(),
        syntactic_error_count: run.syntactic_error_count,
        semantic_error_count: run.semantic_error_count,
        syntactic_validity_rate: pct(valid_rows, total_rows),
        semantic_validity_rate: pct(semantic_valid_rows, total_rows),
    };

    let path = dir.join(format!("{}_validation_report.json", station));
    let content = serde_json::to_string_pretty(&report)?;
    std::fs::write(&path, content)?;

    Ok((path, report))
}

pub fn write_provenance_json(
    output_root: &Path,
    station: &str,
    source_path: &Path,
    cleaning_version: &str,
    quality_score: f64,
) -> Result<PathBuf> {
    let dir = output_root.join("validation");
    std::fs::create_dir_all(&dir)?;

    let provenance = ProvenanceMetadata {
        data_source: "PRSA_Beijing_AirQuality".to_string(),
        measurement_method: "Continuous_Ambient_Air_Monitoring".to_string(),
        collection_date: infer_collection_date_from_filename(source_path),
        cleaning_version: cleaning_version.to_string(),
        quality_score,
        sensor_calibration_date: None,
        data_collection_agency:
            "Beijing_Municipal_Environmental_Protection_Bureau".to_string(),
        qa_qc_procedure: "ISO_9001_certified".to_string(),
    };

    let path = dir.join(format!("{}_provenance.json", station));
    let content = serde_json::to_string_pretty(&provenance)?;
    std::fs::write(&path, content)?;
    Ok(path)
}

pub fn write_manual_review_sample(
    output_root: &Path,
    station: &str,
    df: &DataFrame,
    enabled: bool,
) -> Result<Option<PathBuf>> {
    if !enabled || df.height() == 0 {
        return Ok(None);
    }

    let review_col = if df.get_column_names().iter().any(|c| *c == "quality_flag") {
        "quality_flag"
    } else {
        return Ok(None);
    };

    let flags = df.column(review_col)?;
    let mut candidate_rows = Vec::new();
    for i in 0..df.height() {
        let is_review = flags
            .get(i)
            .ok()
            .map(|v| v.to_string().trim_matches('"') == "manual_review")
            .unwrap_or(false);
        if is_review {
            candidate_rows.push(i);
        }
    }

    if candidate_rows.is_empty() {
        return Ok(None);
    }

    let sample_size = ((candidate_rows.len() as f64) * 0.01).ceil().max(1.0) as usize;
    let mut picked = Vec::new();
    for (order, idx) in candidate_rows.iter().enumerate() {
        if order % (candidate_rows.len() / sample_size).max(1) == 0 {
            picked.push(*idx);
        }
        if picked.len() >= sample_size {
            break;
        }
    }

    let mut mask = vec![false; df.height()];
    for idx in picked {
        mask[idx] = true;
    }

    let mut sample = df.filter(Series::new("mask".into(), mask).bool().unwrap())?;
    sample.with_column(Series::new("suggested_action".into(), vec![Some("review"); sample.height()]))?;
    sample.with_column(Series::new("confidence_score".into(), vec![Some(0.5_f64); sample.height()]))?;
    sample.with_column(Series::new("reviewer_notes".into(), vec![Some(String::new()); sample.height()]))?;

    let dir = output_root.join("review");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("sample_manual_review_{}.csv", station));
    let mut f = File::create(&path)?;
    CsvWriter::new(&mut f).finish(&mut sample)?;
    Ok(Some(path))
}

pub fn write_feedback_template(output_root: &Path, station: &str) -> Result<PathBuf> {
    #[derive(Serialize)]
    struct FeedbackTemplate {
        reviewer_id: String,
        timestamp: String,
        record_id: String,
        original_flag: String,
        reviewer_decision: String,
        correction_applied: bool,
    }

    let dir = output_root.join("feedback");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("validation_results_{}.json", station));

    let template = vec![FeedbackTemplate {
        reviewer_id: String::new(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        record_id: String::new(),
        original_flag: String::new(),
        reviewer_decision: String::new(),
        correction_applied: false,
    }];

    let content = serde_json::to_string_pretty(&template)?;
    std::fs::write(&path, content)?;
    Ok(path)
}

pub fn write_outlier_justification_log(output_root: &Path, station: &str) -> Result<PathBuf> {
    #[derive(Serialize)]
    struct OutlierLogEntry {
        timestamp: String,
        column: String,
        raw_value: f64,
        capped_value: f64,
        reason: String,
        domain_expert_approved: Option<bool>,
    }

    let dir = output_root.join("audit");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("outlier_justification_{}.json", station));

    let placeholder: Vec<OutlierLogEntry> = Vec::new();
    std::fs::write(&path, serde_json::to_string_pretty(&placeholder)?)?;
    Ok(path)
}

pub fn write_quality_dashboard(
    output_root: &Path,
    reports: &[ValidationReport],
    metrics: &HashMap<String, QualityMetrics>,
    enabled: bool,
) -> Result<Option<PathBuf>> {
    if !enabled {
        return Ok(None);
    }

    let dir = output_root.join("laporan");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("quality_dashboard.html");

    let mut rows = String::new();
    for r in reports {
        let m = metrics.get(&r.station);
        let stale = m.map(|x| x.staleness_stale_pct).unwrap_or(0.0);
        rows.push_str(&format!(
            "<tr><td>{}</td><td>{:.2}%</td><td>{:.2}%</td><td>{:.2}%</td></tr>",
            r.station, r.syntactic_validity_rate, r.semantic_validity_rate, stale
        ));
    }

    let html = format!(
        "<!doctype html><html lang=\"id\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Quality Dashboard</title><style>body{{font-family:Segoe UI,sans-serif;background:#f4f8f7;color:#1d2f2a;padding:24px}}table{{width:100%;border-collapse:collapse;background:#fff}}th,td{{padding:10px;border:1px solid #dce7e4;text-align:left}}th{{background:#0f766e;color:#fff}}</style></head><body><h1>Dashboard Kualitas ISO</h1><table><thead><tr><th>Station</th><th>Syntactic Validity</th><th>Semantic Validity</th><th>Staleness (Stale %)</th></tr></thead><tbody>{}</tbody></table></body></html>",
        rows
    );

    std::fs::write(&path, html)?;
    Ok(Some(path))
}

pub fn estimate_quality_metrics(report: &ValidationReport) -> QualityMetrics {
    QualityMetrics {
        syntactic_validity_rate: report.syntactic_validity_rate,
        semantic_validity_rate: report.semantic_validity_rate,
        imputation_rate_linear: 0.0,
        imputation_rate_seasonal: 0.0,
        imputation_rate_forward_fill: 0.0,
        imputation_rate_median: 0.0,
        staleness_fresh_pct: 0.0,
        staleness_stale_pct: 0.0,
        staleness_archive_pct: 100.0,
    }
}

fn pct(n: usize, d: usize) -> f64 {
    if d == 0 {
        100.0
    } else {
        (n as f64 * 100.0) / d as f64
    }
}
