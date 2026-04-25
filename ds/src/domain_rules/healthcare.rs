//! Healthcare Domain Rules
//! Untuk data kesehatan (future expansion)

use super::{DomainBusinessRules, ValidationResult};

pub struct HealthcareRules {
    pub name: String,
}

impl HealthcareRules {
    pub fn new() -> Self {
        Self {
            name: "Healthcare & Medical Data".to_string(),
        }
    }
}

impl DomainBusinessRules for HealthcareRules {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self, _data: &serde_json::Value) -> Result<ValidationResult, String> {
        let result = ValidationResult::new();
        // Implementation akan di-expand di future versions
        Ok(result)
    }
}
