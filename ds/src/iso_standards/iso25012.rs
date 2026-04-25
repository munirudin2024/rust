use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StalenessCategory {
    Fresh,
    Stale,
    Archive,
}

impl StalenessCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Archive => "archive",
        }
    }
}

pub fn calculate_staleness(
    record_timestamp: DateTime<Utc>,
    processing_timestamp: DateTime<Utc>,
) -> StalenessCategory {
    let hours = (processing_timestamp - record_timestamp).num_hours();
    match hours {
        0..=24 => StalenessCategory::Fresh,
        25..=168 => StalenessCategory::Stale,
        _ => StalenessCategory::Archive,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityMetrics {
    pub syntactic_validity_rate: f64,
    pub semantic_validity_rate: f64,
    pub imputation_rate_linear: f64,
    pub imputation_rate_seasonal: f64,
    pub imputation_rate_forward_fill: f64,
    pub imputation_rate_median: f64,
    pub staleness_fresh_pct: f64,
    pub staleness_stale_pct: f64,
    pub staleness_archive_pct: f64,
}
