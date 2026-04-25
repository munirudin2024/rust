//! ISO 8000 & ISO/IEC 25012 Compliance Module
//! 
//! Implements:
//! - ISO 8000-110: Data Requirement Specification
//! - ISO 8000-8: Semantic Quality Framework  
//! - ISO 8000-61: Data Quality Management
//! - ISO/IEC 25012: Data Quality Model

pub mod iso8000_110;
pub mod iso8000_8;
pub mod iso8000_config;
pub mod iso25012;
pub mod audit_formatter;
pub mod final_report;
pub mod config_loader;
pub mod validators;
pub mod iso8000;
pub mod manual_review;

pub use final_report::FinalISOReport;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ISO 8000-110 Data Requirement Specification
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataRequirementSpec {
    pub metadata: SpecMetadata,
    pub global_settings: GlobalSettings,
    pub domains: HashMap<String, DomainSpec>,
    pub quality_thresholds: QualityThresholds,
    pub audit_configuration: AuditConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SpecMetadata {
    pub standard: String,
    pub version: String,
    pub generated_at: String,
    pub certification_target: String,
    pub pipeline_version: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalSettings {
    pub confidence_threshold: f32,
    pub imputation_default: String,
    pub outlier_method: String,
    pub audit_level: String,
    pub traceability: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DomainSpec {
    pub description: String,
    pub source_agency: Option<String>,
    pub measurement_standard: Option<String>,
    pub iso_equivalent: Option<String>,
    pub source_type: Option<String>,
    pub fields: HashMap<String, FieldSpec>,
    pub composite_indices: Option<HashMap<String, CompositeIndexSpec>>,
    pub composite_features: Option<HashMap<String, CompositeFeatureSpec>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FieldSpec {
    pub iso25012_dimensions: Option<Vec<String>>,
    pub physical_constraints: Option<PhysicalConstraints>,
    pub business_rules: Option<Vec<BusinessRule>>,
    pub imputation: Option<ImputationSpec>,
    pub outlier_handling: Option<OutlierSpec>,
    pub constraints: Option<FieldConstraints>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PhysicalConstraints {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub unit: Option<String>,
    pub precision: Option<f64>,
    pub accuracy_spec: Option<String>,
    pub must_be_greater_than: Option<Vec<String>>,
    pub must_be_less_than: Option<Vec<String>>,
    pub diurnal_max_variation: Option<f64>,
    #[serde(rename = "type")]
    pub data_type: Option<String>,
    pub unique: Option<bool>,
    pub non_null: Option<bool>,
    pub nullable: Option<bool>,
    #[serde(rename = "enum")]
    pub r#enum: Option<Vec<serde_json::Value>>,
    pub format: Option<String>,
    pub currency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FieldConstraints {
    pub unique: Option<bool>,
    pub non_null: Option<bool>,
    pub format: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BusinessRule {
    pub id: String,
    pub description: String,
    #[serde(rename = "type")]
    pub rule_type: String,
    pub condition: Option<String>,
    pub formula: Option<String>,
    pub tolerance: Option<f64>,
    #[serde(default)]
    pub violation_action: String,
    pub correction_logic: Option<String>,
    pub confidence_adjustment: Option<f32>,
    pub confidence_penalty: Option<f32>,
    pub physical_law: Option<String>,
    pub validation: Option<String>,
    pub deduplication_logic: Option<String>,
    pub max_age_days: Option<u32>,
    pub out_of_range_values: Option<String>,
    pub peak_hours: Option<Vec<u32>>,
    pub expected_elevation: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ImputationSpec {
    pub primary_method: String,
    pub fallback_method: Option<String>,
    pub rationale: Option<String>,
    pub max_gap_hours: Option<u32>,
    pub confidence_calculation: Option<String>,
    pub uncertainty_propagation: Option<bool>,
    pub max_fill_hours: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutlierSpec {
    pub statistical_method: String,
    pub domain_cap_min: Option<f64>,
    pub domain_cap_max: Option<f64>,
    pub extreme_value_annotation: Option<String>,
    pub manual_review_threshold: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompositeIndexSpec {
    pub calculation_method: String,
    pub breakpoints: Option<serde_json::Value>,
    pub rules: Option<Vec<serde_json::Value>>,
    pub components: Option<HashMap<String, f64>>,
    pub iso25012_mapping: Option<String>,
    pub traffic_peak: Option<Vec<u32>>,
    pub night_low: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompositeFeatureSpec {
    pub calculation: Option<String>,
    pub detection_method: Option<String>,
    pub threshold: Option<f64>,
    pub condition: Option<String>,
    #[serde(rename = "type")]
    pub feature_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QualityThresholds {
    pub completeness: ThresholdSpec,
    pub consistency: ThresholdSpec,
    pub accuracy: ThresholdSpec,
    pub syntactic_validity: ThresholdSpec,
    pub semantic_validity: ThresholdSpec,
    pub pragmatic_quality: ThresholdSpec,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThresholdSpec {
    pub minimum: f32,
    pub target: f32,
    pub measurement: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuditConfig {
    pub format: String,
    pub traceability_level: String,
    pub provenance_tracking: bool,
    pub uncertainty_quantification: bool,
    pub confidence_intervals: bool,
    pub reasoning_preservation: bool,
}

/// ISO 25012 Quality Dimension Score
#[derive(Debug, Clone, Serialize)]
pub struct QualityDimensionScore {
    pub dimension: String,
    pub score: f32,
    pub threshold_minimum: f32,
    pub threshold_target: f32,
    pub status: ComplianceStatus,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum ComplianceStatus {
    Compliant,
    Warning,
    NonCompliant,
    NotMeasured,
}

/// ISO 8000-8 Semantic Quality Result
#[derive(Debug, Clone, Serialize)]
pub struct SemanticQualityResult {
    pub field_name: String,
    pub business_rule_id: String,
    pub rule_description: String,
    pub total_checked: usize,
    pub passed: usize,
    pub failed: usize,
    pub confidence_score: f32,
    pub violations: Vec<RuleViolation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleViolation {
    pub record_index: usize,
    pub field_value: String,
    pub expected_condition: String,
    pub actual_condition: String,
    pub severity: ViolationSeverity,
    pub suggested_action: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum ViolationSeverity {
    Critical,    // Hard constraint violation
    Major,       // Business rule violation
    Minor,       // Contextual suspect
    Info,        // Annotation only
}

/// ISO 8000-61 Audit Trail Entry
#[derive(Debug, Clone, Serialize)]
pub struct AuditTrailEntry {
    pub timestamp: String,
    pub stage: String,
    pub operation: String,
    pub input_records: usize,
    pub output_records: usize,
    pub transformation_logic: String,
    pub confidence_impact: f32,
    pub reasoning: String,
    pub operator_id: Option<String>,  // For manual review
    pub automated: bool,
}

/// Complete ISO Compliant Audit Report
#[derive(Debug, Clone, Serialize)]
pub struct ISOCompliantAuditReport {
    pub metadata: ReportMetadata,
    pub data_requirement_spec: DataRequirementSpec,
    pub stage1_scores: HashMap<String, f32>,
    pub quality_dimensions: Vec<QualityDimensionScore>,
    pub semantic_quality: Vec<SemanticQualityResult>,
    pub audit_trail: Vec<AuditTrailEntry>,
    pub imputation_log: Vec<ImputationEntry>,
    pub outlier_log: Vec<OutlierEntry>,
    pub final_assessment: FinalAssessment,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportMetadata {
    pub report_id: String,
    pub generated_at: String,
    pub dataset_name: String,
    pub records_processed: usize,
    pub iso_standard_version: String,
    pub pipeline_version: String,
    pub git_commit_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImputationEntry {
    pub field: String,
    pub method: String,
    pub records_affected: usize,
    pub confidence_weighted: f32,
    pub rationale: String,
    pub uncertainty_propagated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutlierEntry {
    pub field: String,
    pub method: String,
    #[serde(rename = "capped_1.5iqr")]
    pub capped_1_5_iqr: usize,
    #[serde(rename = "capped_3iqr")]
    pub capped_3_iqr: usize,
    pub domain_cap_applied: usize,
    pub manual_review_flagged: usize,
    pub extreme_annotations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FinalAssessment {
    pub overall_compliance_level: u8,  // 1-5
    pub certification_ready: bool,
    pub critical_gaps: Vec<String>,
    pub recommendations: Vec<String>,
    pub next_audit_date: String,
}
