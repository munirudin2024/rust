//! ISO 8000-110 Configuration Parser & Manager
//! Struktur untuk parsing dan mangaging requirement specification config

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::{Context, Result};
use std::path::Path;

/// Root configuration structure untuk ISO 8000-110
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ISO8000Config {
    pub metadata: ConfigMetadata,
    pub global_settings: GlobalSettings,
    pub domains: HashMap<String, DomainConfig>,
    pub quality_thresholds: QualityThresholds,
    pub audit_configuration: AuditConfiguration,
}

/// Metadata untuk configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigMetadata {
    pub standard: String,
    pub version: String,
    pub generated_at: String,
    pub certification_target: String,
    pub pipeline_version: String,
}

/// Global settings untuk semua domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSettings {
    pub confidence_threshold: f64,
    pub imputation_default: String,
    pub outlier_method: String,
    pub audit_level: String,
    pub traceability: String,
}

/// Configuration untuk satu domain (e.g., environmental, retail)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainConfig {
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_agency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurement_standard: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iso_equivalent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    pub fields: HashMap<String, FieldConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub composite_indices: Option<HashMap<String, CompositeIndex>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub composite_features: Option<HashMap<String, CompositeFeature>>,
}

/// Configuration untuk satu field/kolom
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iso25012_dimensions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_constraints: Option<PhysicalConstraints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub business_rules: Option<Vec<BusinessRule>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imputation: Option<ImputationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outlier_handling: Option<OutlierHandling>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraints: Option<FieldConstraints>,
}

/// Physical/Domain constraints untuk field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalConstraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy_spec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diurnal_max_variation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_be_greater_than: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_be_less_than: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

/// Business rule untuk field validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessRule {
    pub id: String,
    pub description: String,
    pub rule_type: String,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_law: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formula: Option<String>,
    pub violation_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence_adjustment: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correction_logic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_hours: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_elevation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduplication_logic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_days: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub out_of_range_values: Option<String>,
}

/// Imputation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImputationConfig {
    pub primary_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence_calculation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_gap_hours: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fill_hours: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncertainty_propagation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
}

/// Outlier handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlierHandling {
    pub statistical_method: String,
    pub domain_cap_min: f64,
    pub domain_cap_max: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extreme_value_annotation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manual_review_threshold: Option<String>,
}

/// Field constraints (for fields with specific constraints)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConstraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_null: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Composite index calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeIndex {
    pub calculation_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakpoints: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub components: Option<HashMap<String, f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iso25012_mapping: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traffic_peak: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub night_low: Option<Vec<u32>>,
}

/// Composite feature calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeFeature {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calculation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detection_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// Quality thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityThresholds {
    pub completeness: DimensionThreshold,
    pub consistency: DimensionThreshold,
    pub accuracy: DimensionThreshold,
    pub syntactic_validity: DimensionThreshold,
    pub semantic_validity: DimensionThreshold,
    pub pragmatic_quality: DimensionThreshold,
}

/// Threshold untuk satu dimensi kualitas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionThreshold {
    pub minimum: f64,
    pub target: f64,
    pub measurement: String,
}

/// Audit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfiguration {
    pub format: String,
    pub traceability_level: String,
    pub provenance_tracking: bool,
    pub uncertainty_quantification: bool,
    pub confidence_intervals: bool,
    pub reasoning_preservation: bool,
}

/// Manager untuk ISO 8000 Config
impl ISO8000Config {
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .context(format!("Failed to read ISO config from {:?}", path))?;
        
        serde_json::from_str(&content)
            .context("Failed to parse ISO 8000 config JSON")
    }

    pub fn get_domain(&self, domain_name: &str) -> Option<&DomainConfig> {
        self.domains.get(domain_name)
    }

    pub fn get_field(&self, domain_name: &str, field_name: &str) -> Option<&FieldConfig> {
        self.domains
            .get(domain_name)
            .and_then(|d| d.fields.get(field_name))
    }

    pub fn get_business_rules(&self, domain_name: &str, field_name: &str) -> Vec<&BusinessRule> {
        self.domains
            .get(domain_name)
            .and_then(|d| d.fields.get(field_name))
            .and_then(|f| f.business_rules.as_ref())
            .map(|rules| rules.iter().collect())
            .unwrap_or_default()
    }

    pub fn validate_value(
        &self,
        domain: &str,
        field: &str,
        value: f64,
    ) -> ValidationResult {
        let mut result = ValidationResult::new();

        if let Some(field_cfg) = self.get_field(domain, field) {
            // Check physical constraints
            if let Some(constraints) = &field_cfg.physical_constraints {
                if let Some(min) = constraints.min {
                    if value < min {
                        result.add_violation(format!(
                            "Value {} is below minimum {}",
                            value, min
                        ));
                    }
                }
                if let Some(max) = constraints.max {
                    if value > max {
                        result.add_violation(format!(
                            "Value {} exceeds maximum {}",
                            value, max
                        ));
                    }
                }
            }
        }

        result
    }
}

/// Result dari validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub violations: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            violations: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn add_violation(&mut self, msg: String) {
        self.is_valid = false;
        self.violations.push(msg);
    }

    pub fn add_warning(&mut self, msg: String) {
        self.warnings.push(msg);
    }
}
