use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub struct OutputStructure {
    pub iso_compliant: PathBuf,
    pub legacy_root: PathBuf,
}

pub fn ensure_dir_structure(output_root: &Path) -> Result<OutputStructure> {
    let iso_compliant = output_root.join("iso_compliant");
    let legacy_root = output_root.join("legacy");

    let legacy_dirs = [
        legacy_root.join("csv"),
        legacy_root.join("csv/clean"),
        legacy_root.join("csv/audit_log"),
        legacy_root.join("csv/kpi"),
        legacy_root.join("html"),
        legacy_root.join("report"),
    ];

    std::fs::create_dir_all(&iso_compliant)
        .with_context(|| format!("failed to create {}", iso_compliant.display()))?;

    for dir in legacy_dirs {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
    }

    let readme_path = output_root.join("README.md");
    if !readme_path.exists() {
        let content = "# Output Structure\n\n- iso_compliant/: Primary ISO 8000 outputs.\n- legacy/: Backward compatibility outputs for older consumers.\n\nMigration note: prefer reading outputs from iso_compliant/ for audit and certification workflows.\n";
        std::fs::write(&readme_path, content)
            .with_context(|| format!("failed to write {}", readme_path.display()))?;
    }

    Ok(OutputStructure {
        iso_compliant,
        legacy_root,
    })
}
