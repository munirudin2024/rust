//! # Rust CLI Orchestrator
//!
//! Entry point for audit -> clean -> visualization pipeline.

use anyhow::{Context, Result};
use colored::*;
use polars::prelude::{CsvWriter, SerWriter};
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};

mod audit;
mod clean;
mod google_sheets;
mod looker_auto;
mod viz;

/// Validate CLI input and ensure path exists.
fn validate_args() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        anyhow::bail!("Usage: cargo run -- <path_to_csv_or_folder>");
    }

    let path = &args[1];
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

    Ok(path.to_string())
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

fn main() -> Result<()> {
    print_banner();

    let input_path = validate_args()?;
    let csv_paths = gather_csv_paths(&input_path)?;
    let started_at = chrono::Local::now();
    let output_dir = Path::new("./output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).context("failed to create output directory")?;
    }

    println!("  Input     : {}", input_path);
    println!("  File CSV  : {}", csv_paths.len());
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
        let cleaned_csv_path = write_cleaned_csv(&clean_df, csv_path.as_path(), output_dir)?;
        println!("  {} CSV Clean: {}", "".cyan(), cleaned_csv_path.display());

        station_summaries.push(viz::build_station_summary(
            &csv_path_str,
            &clean_df,
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
