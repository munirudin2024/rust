use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRequirementSpec {
    pub standard: String,
    pub version: String,
    pub domain: String,
    pub fields: HashMap<String, FieldSpec>,
    pub temporal_rules: TemporalRules,
    pub cross_field_rules: Vec<CrossFieldRule>,
    #[serde(default)]
    pub outlier_config: Option<OutlierConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub range: Option<[f64; 2]>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default, rename = "enum")]
    pub allowed_values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalRules {
    pub timestamp_format: String,
    pub future_date_check: bool,
    pub sequential_check: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossFieldRule {
    pub condition: String,
    pub implication: String,
    pub severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlierConfig {
    pub method: String,
    pub threshold: String,
    pub domain_adjustments: HashMap<String, DomainAdjustment>,
    pub require_manual_review: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainAdjustment {
    pub upper_cap: f64,
    pub justification: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceMetadata {
    pub data_source: String,
    pub measurement_method: String,
    pub collection_date: String,
    pub cleaning_version: String,
    pub quality_score: f64,
    pub sensor_calibration_date: Option<String>,
    pub data_collection_agency: String,
    pub qa_qc_procedure: String,
}

pub fn load_spec(path: &Path) -> Result<DataRequirementSpec> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading spec: {}", path.display()))?;
    let spec: DataRequirementSpec = serde_json::from_str(&content)
        .with_context(|| format!("failed parsing JSON spec: {}", path.display()))?;
    Ok(spec)
}

pub fn infer_collection_date_from_filename(source: &Path) -> String {
    let file_name = source.file_name().and_then(|s| s.to_str()).unwrap_or("unknown");
    let digits: String = file_name.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() >= 8 {
        format!("{}-{}-{}", &digits[0..4], &digits[4..6], &digits[6..8])
    } else {
        chrono::Utc::now().date_naive().to_string()
    }
}
