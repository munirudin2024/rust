use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

fn env_or_default(name: &str, default_value: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default_value.to_string())
}

pub fn write_looker_helper_files(output_root: &Path) -> Result<Vec<PathBuf>> {
    let looker_dir = output_root.join("looker_studio");
    std::fs::create_dir_all(&looker_dir)
        .context("failed to create output/looker_studio directory")?;

    let project_id = env_or_default("LOOKER_GCP_PROJECT_ID", "<isi-project-id>");
    let dataset_id = env_or_default("LOOKER_BQ_DATASET", "dataset_transaksi_ecommerce");
    let table_id = env_or_default("LOOKER_BQ_TABLE", "dataset_transaksi_ecommerce");

    let quick_link = if project_id.starts_with('<') {
        "https://lookerstudio.google.com/navigation/reporting/create".to_string()
    } else {
        format!(
            "https://lookerstudio.google.com/reporting/create?c.datasource=bigquery&c.queryConfig={{\"projectId\":\"{}\",\"datasetId\":\"{}\",\"tableId\":\"{}\",\"useStandardSql\":true}}",
            project_id, dataset_id, table_id
        )
    };

    let quick_link_path = looker_dir.join("looker_quick_link.txt");
    let manual_connect_path = looker_dir.join("looker_manual_connect.txt");
    let readme_path = looker_dir.join("README.txt");

    let quick_link_content = format!(
        "Looker Studio Quick Link:\n{}\n\nProject: {}\nDataset: {}\nTable: {}\n",
        quick_link, project_id, dataset_id, table_id
    );

    let bq_console = if project_id.starts_with('<') {
        "https://console.cloud.google.com/bigquery".to_string()
    } else {
        format!(
            "https://console.cloud.google.com/bigquery?project={}&p={}&d={}&t={}&page=table",
            project_id, project_id, dataset_id, table_id
        )
    };

    let manual_content = format!(
        "Manual Connect Steps:\n1) Buka: https://lookerstudio.google.com/navigation/reporting/create\n2) Pilih BigQuery sebagai data source\n3) Pilih project/dataset/table berikut:\n   - Project: {}\n   - Dataset: {}\n   - Table  : {}\n4) Jika perlu, cek tabel di BigQuery console:\n{}\n",
        project_id, dataset_id, table_id, bq_console
    );

    let readme_content = "Folder ini dibuat otomatis oleh pipeline.\n\nIsi file:\n- looker_quick_link.txt   : link cepat buka Looker Studio\n- looker_manual_connect.txt : panduan koneksi manual\n\nCatatan:\n- Set env LOOKER_GCP_PROJECT_ID agar quick link langsung terisi target project BigQuery.\n- Tanpa env tersebut, quick link akan mengarah ke halaman create report umum.\n";

    std::fs::write(&quick_link_path, quick_link_content)
        .with_context(|| format!("failed to write {}", quick_link_path.display()))?;
    std::fs::write(&manual_connect_path, manual_content)
        .with_context(|| format!("failed to write {}", manual_connect_path.display()))?;
    std::fs::write(&readme_path, readme_content)
        .with_context(|| format!("failed to write {}", readme_path.display()))?;

    Ok(vec![quick_link_path, manual_connect_path, readme_path])
}
