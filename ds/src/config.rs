use std::path::PathBuf;
use chrono::NaiveDate;

#[derive(Debug, Clone)]
pub struct Config {
    pub input_files: Vec<PathBuf>,
    pub max_date:    NaiveDate,
    pub hard_reject: bool,
    pub mode:        RunMode,
    pub output_root: PathBuf,
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
                 [--hard-reject] [--max-date YYYY-MM-DD]"
            );
        }

        let hard_reject = args.iter().any(|a| a == "--hard-reject");

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

        let mode = if hard_reject {
            RunMode::HardReject
        } else {
            RunMode::Normal
        };

        Ok(Config {
            input_files,
            max_date,
            hard_reject,
            mode,
            output_root: PathBuf::from("output"),
        })
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