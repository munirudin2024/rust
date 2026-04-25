//! ISO 8000-110: Data Requirement Specification
//! Framework untuk mendefinisikan spesifikasi data dan validasi compliance

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Data Requirement Specification sesuai ISO 8000-110
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRequirementSpec {
    pub name: String,
    pub version: String,
    pub fields: Vec<FieldRequirement>,
    pub business_rules: Vec<BusinessRule>,
    pub reference_data: HashMap<String, Vec<String>>,
}

/// Persyaratan untuk satu field/kolom
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldRequirement {
    pub field_name: String,
    pub data_type: String,
    pub mandatory: bool,
    pub format_pattern: Option<String>,
    pub valid_values: Option<Vec<String>>,
    pub description: String,
}

/// Business rule untuk validasi data semantik
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessRule {
    pub rule_id: String,
    pub description: String,
    pub condition: String,
    pub severity: RuleSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RuleSeverity {
    Critical,
    High,
    Medium,
    Low,
}

impl DataRequirementSpec {
    pub fn new(name: String, version: String) -> Self {
        Self {
            name,
            version,
            fields: Vec::new(),
            business_rules: Vec::new(),
            reference_data: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field: FieldRequirement) {
        self.fields.push(field);
    }

    pub fn add_business_rule(&mut self, rule: BusinessRule) {
        self.business_rules.push(rule);
    }

    pub fn validate_field_count(&self, actual_count: usize) -> bool {
        self.fields.len() == actual_count
    }
}
