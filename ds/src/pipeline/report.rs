use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::DataFrame;
use std::path::{Path, PathBuf};

pub use crate::output::json::{CityRevenueStat, DatasetSummary, MetricStat, PaymentStat};

pub struct DatasetReportArtifacts {
    pub cleaned_csv: PathBuf,
    pub payload_csv: PathBuf,
    pub audit_log_csv: PathBuf,
    pub kpi_csv: PathBuf,
    pub yearly_csvs: Vec<PathBuf>,
    pub quarantine_csv: Option<PathBuf>,
    pub dropped_duplicates_csv: Option<PathBuf>,
    pub gap_report_md: Option<PathBuf>,
    pub quarantine_summary_md: Option<PathBuf>,
    pub hidden_insights_md: Option<PathBuf>,
    pub budi_investigation_md: PathBuf,
    pub presentasi_txt: PathBuf,
    pub presentasi_html: PathBuf,
    pub dropped_duplicates_count: usize,
    pub quarantine_rows: usize,
}

pub fn write_dataset_outputs(
    source_csv_path: &Path,
    analysis_df: &DataFrame,
    final_df: &DataFrame,
    quarantine_df: Option<&DataFrame>,
    dropped_dups_df: Option<&DataFrame>,
    dropped_dups_count: usize,
    max_business_date: NaiveDate,
    output_root: &Path,
    hard_reject: bool,
) -> Result<DatasetReportArtifacts> {
    let cleaned_csv =
        crate::output::csv_writer::write_cleaned_csv(final_df, source_csv_path, output_root)?;
    let (payload_csv, audit_log_csv) =
        crate::output::csv_writer::write_payload_audit_csvs(final_df, source_csv_path, output_root)?;
    let kpi_csv = crate::output::csv_writer::write_quality_kpi_csv(
        analysis_df,
        source_csv_path,
        output_root,
        max_business_date,
    )?;
    let yearly_csvs =
        crate::output::csv_writer::write_yearly_splits(final_df, source_csv_path, output_root)?;

    let gap_report_md = crate::output::quarantine::write_id_gap_report(final_df, source_csv_path, output_root)?;
    let budi_investigation_md =
        crate::output::quarantine::write_budi_santoso_investigation(final_df, output_root)?;

    let dropped_duplicates_csv = if let Some(dropped) = dropped_dups_df {
        Some(crate::output::quarantine::write_duplicate_drops_csv(
            dropped,
            source_csv_path,
            output_root,
        )?)
    } else {
        None
    };

    let (quarantine_csv, quarantine_summary_md, hidden_insights_md) =
        if let Some(quarantine) = quarantine_df {
            let q_csv = Some(crate::output::quarantine::write_quarantine_csv(
                quarantine,
                source_csv_path,
                output_root,
            )?);
            let q_summary = Some(crate::output::quarantine::write_quarantine_summary(
                quarantine,
                source_csv_path,
                output_root,
            )?);
            let hidden = Some(crate::output::quarantine::write_hidden_insights(
                analysis_df,
                final_df,
                quarantine,
                dropped_dups_df,
                output_root,
            )?);
            (q_csv, q_summary, hidden)
        } else {
            (None, None, None)
        };

    let (presentasi_txt, presentasi_html) = crate::output::laporan::write_presentation_report(
        source_csv_path,
        final_df,
        quarantine_df,
        dropped_dups_count,
        yearly_csvs.len(),
        output_root,
        hard_reject,
    )?;

    let quarantine_rows = quarantine_df.map(|q| q.height()).unwrap_or(0);

    Ok(DatasetReportArtifacts {
        cleaned_csv,
        payload_csv,
        audit_log_csv,
        kpi_csv,
        yearly_csvs,
        quarantine_csv,
        dropped_duplicates_csv,
        gap_report_md,
        quarantine_summary_md,
        hidden_insights_md,
        budi_investigation_md,
        presentasi_txt,
        presentasi_html,
        dropped_duplicates_count: dropped_dups_count,
        quarantine_rows,
    })
}

pub fn write_summary_json(
    summaries: &[DatasetSummary],
    output_root: &Path,
) -> Result<PathBuf> {
    crate::output::json::write_report_json(summaries, output_root)
}