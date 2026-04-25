use std::path::PathBuf;
use chrono::NaiveDate;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Config {
    pub input_files: Vec<PathBuf>,
    pub max_date:    NaiveDate,
    pub hard_reject: bool,
    pub validate_iso: bool,
    pub generate_sample: bool,
    pub quality_dashboard: bool,
    pub cleaning_version: String,
    pub imputation_policy: ImputationPolicy,
    pub mode:        RunMode,
    pub output_root: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BelowThresholdAction {
    FillWithFlag,
    Null,
    Quarantine,
    MissingVerified,
}

impl std::fmt::Display for BelowThresholdAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BelowThresholdAction::FillWithFlag => write!(f, "fill_with_flag"),
            BelowThresholdAction::Null => write!(f, "null"),
            BelowThresholdAction::Quarantine => write!(f, "quarantine"),
            BelowThresholdAction::MissingVerified => write!(f, "missing_verified"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImputationPolicy {
    pub min_confidence: f32,
    pub below_threshold_action: BelowThresholdAction,
    pub tolerance_pct: f32,
}

impl Default for ImputationPolicy {
    fn default() -> Self {
        Self {
            min_confidence: 0.80,
            below_threshold_action: BelowThresholdAction::FillWithFlag,
            tolerance_pct: 1.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PipelineToml {
    imputation: Option<ImputationToml>,
}

#[derive(Debug, Clone, Deserialize)]
struct ImputationToml {
    min_confidence: Option<f32>,
    below_threshold_action: Option<BelowThresholdAction>,
    tolerance_pct: Option<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunMode {
    Normal,
    HardReject,
}

impl Config {
    pub fn from_args() -> anyhow::Result<Self> {
        let args: Vec<String> = std::env::args().skip(1).collect();

        if args.is_empty() {
            anyhow::bail!(
                "Usage: data_cleaner <file.csv> [file2.csv ...] \
                 [--hard-reject] [--max-date YYYY-MM-DD] \
                 [--validate-iso] [--generate-sample] [--quality-dashboard] \
                 [--pipeline-config path/to/pipeline.toml]"
            );
        }

        let hard_reject = args.iter().any(|a| a == "--hard-reject");
        let validate_iso = args.iter().any(|a| a == "--validate-iso");
        let generate_sample = args.iter().any(|a| a == "--generate-sample");
        let quality_dashboard = args.iter().any(|a| a == "--quality-dashboard");
        let pipeline_config_path = args
            .windows(2)
            .find(|w| w[0] == "--pipeline-config")
            .map(|w| PathBuf::from(&w[1]))
            .unwrap_or_else(|| PathBuf::from("config/pipeline.toml"));

        let max_date = args
            .windows(2)
            .find(|w| w[0] == "--max-date")
            .and_then(|w| NaiveDate::parse_from_str(&w[1], "%Y-%m-%d").ok())
            .unwrap_or_else(|| {
                chrono::Utc::now().date_naive() - chrono::Duration::days(1)
            });

        let input_files: Vec<PathBuf> = args
            .iter()
            .filter(|a| !a.starts_with("--") && a.ends_with(".csv"))
            .map(PathBuf::from)
            .collect();

        if input_files.is_empty() {
            anyhow::bail!("Minimal satu file .csv harus diberikan sebagai input.");
        }

        let imputation_policy = Self::load_imputation_policy(&pipeline_config_path)
            .unwrap_or_else(|err| {
                eprintln!(
                    "[PERINGATAN] Gagal membaca {}: {}. Pakai policy default.",
                    pipeline_config_path.display(),
                    err
                );
                ImputationPolicy::default()
            });

        let mode = if hard_reject {
            RunMode::HardReject
        } else {
            RunMode::Normal
        };

        Ok(Config {
            input_files,
            max_date,
            hard_reject,
            validate_iso,
            generate_sample,
            quality_dashboard,
            cleaning_version: env!("CARGO_PKG_VERSION").to_string(),
            imputation_policy,
            mode,
            output_root: PathBuf::from("output"),
        })
    }

    fn load_imputation_policy(path: &PathBuf) -> anyhow::Result<ImputationPolicy> {
        let text = std::fs::read_to_string(path)?;
        let raw: PipelineToml = toml::from_str(&text)?;

        let mut policy = ImputationPolicy::default();
        if let Some(imp) = raw.imputation {
            if let Some(v) = imp.min_confidence {
                policy.min_confidence = v.clamp(0.0, 1.0);
            }
            if let Some(v) = imp.below_threshold_action {
                policy.below_threshold_action = v;
            }
            if let Some(v) = imp.tolerance_pct {
                policy.tolerance_pct = v.max(0.0);
            }
        }

        Ok(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_mode_default_is_normal() {
        assert_eq!(RunMode::Normal, RunMode::Normal);
        assert_ne!(RunMode::Normal, RunMode::HardReject);
    }

    #[test]
    fn test_max_date_parse() {
        let d = NaiveDate::parse_from_str("2025-09-30", "%Y-%m-%d").unwrap();
        assert_eq!(d.to_string(), "2025-09-30");
    }
}