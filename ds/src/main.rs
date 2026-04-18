use anyhow::Result;
use chrono::Local;
use data_cleaner::{config::Config, pipeline};

fn main() -> Result<()> {
    let started = Local::now();
    let config = Config::from_args()?;

    println!("Pipeline data cleaner");
    println!("Mulai     : {}", started.format("%Y-%m-%d %H:%M:%S"));
    println!("Dataset   : {} file", config.input_files.len());
    println!("Cutoff    : {}", config.max_date.format("%Y-%m-%d"));
    println!(
        "Mode      : {}",
        if config.hard_reject {
            "HARD REJECT"
        } else {
            "NORMAL"
        }
    );

    let result = pipeline::run_all(&config)?;
    let summary = pipeline::summarize_run(&result.datasets);

    for dataset in &result.datasets {
        println!("- {}", dataset.source_file.display());
        println!("  clean    : {}", dataset.artifacts.cleaned_csv.display());
        println!("  payload  : {}", dataset.artifacts.payload_csv.display());
        println!("  auditlog : {}", dataset.artifacts.audit_log_csv.display());
        println!("  kpi      : {}", dataset.artifacts.kpi_csv.display());
        println!("  presentasi: {}", dataset.artifacts.presentasi_html.display());
    }

    println!("Dataset diproses     : {}", summary.dataset_count);
    println!("Total baris akhir    : {}", summary.total_rows);
    println!("Duplikat terhapus    : {}", summary.total_dropped_duplicates);
    println!("Total baris karantina: {}", summary.total_quarantine_rows);
    println!("report_data.json: {}", result.report_json.display());
    println!("Selesai.");
    Ok(())
}
