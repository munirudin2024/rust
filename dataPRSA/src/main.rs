//! # Rust CLI Orchestrator
//!
//! Entry point for audit -> clean -> visualization pipeline.

use anyhow::{Context, Result};
use colored::*;
use std::env;
use std::path::{Path, PathBuf};

mod audit;
mod clean;
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
        anyhow::bail!(
            "Path harus berupa file CSV atau folder yang valid"
        );
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
    println!("{}", "╔══════════════════════════════════════════════╗".bold());
    println!("{}", "║              Data Cleansing Pipeline         ║".bold());
    println!("{}", "╚══════════════════════════════════════════════╝".bold());
    println!();
}

fn print_final_summary(output_path: &str, json_path: &str, station_count: usize) {
    println!();
    println!("{}", "── [4/4] GENERATING REPORT ───────────────────".bold());
    println!("  {} Stations : {}", "📍".cyan(), station_count);
    println!("  {} HTML     : {}", "💾".cyan(), output_path);
    println!("  {} JSON     : {}", "🗂️".cyan(), json_path);
    println!();
    println!("{}", "╔══════════════════════════════════════════════╗".bold());
    println!("{}", "║      PIPELINE COMPLETE — Open report.html    ║".bold());
    println!("{}", "╚══════════════════════════════════════════════╝".bold());
    println!();
}

fn main() -> Result<()> {
    print_banner();

    let input_path = validate_args()?;
    let csv_paths = gather_csv_paths(&input_path)?;
    let started_at = chrono::Local::now();

    println!("  📂 Input    : {}", input_path);
    println!("  🧾 CSV Files: {}", csv_paths.len());
    println!("  📅 Started  : {}", started_at.format("%Y-%m-%d %H:%M:%S"));

    let mut station_summaries = Vec::with_capacity(csv_paths.len());

    for (idx, csv_path) in csv_paths.iter().enumerate() {
        let csv_path_str = csv_path.to_string_lossy().to_string();
        println!();
        println!(
            "{}",
            format!("── [STATION {}/{}] {}", idx + 1, csv_paths.len(), csv_path_str).bold()
        );

        println!("{}", "── [1/4] AUDITING ────────────────────────────".bold());
        let (raw_df, audit_report) = audit::run(&csv_path_str)?;

        println!();
        println!("{}", "── [2/4] CLEANSING ───────────────────────────".bold());
        let (_clean_df, clean_report) = clean::run(raw_df.clone(), &audit_report)?;

        station_summaries.push(viz::build_station_summary(
            &csv_path_str,
            &raw_df,
            &audit_report,
            &clean_report,
        ));
    }

    println!();
    println!("{}", "── [3/4] GENERATING VISUALS ──────────────────".bold());

    let output_dir = Path::new("./output");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir).context("failed to create output directory")?;
    }

    let output_path = "./output/report.html";
    let json_path = "./output/report_data.json";
    viz::run_station_comparison(&station_summaries, output_path, json_path)?;

    print_final_summary(output_path, json_path, station_summaries.len());
    Ok(())
}
