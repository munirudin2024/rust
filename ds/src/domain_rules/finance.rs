//! Financial Domain Rules
//! Untuk data finansial (future expansion)

use super::{DomainBusinessRules, ValidationResult};

pub struct FinanceRules {
    pub name: String,
}

impl FinanceRules {
    pub fn new() -> Self {
        Self {
            name: "Financial & Banking Data".to_string(),
        }
    }
}

impl DomainBusinessRules for FinanceRules {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self, _data: &serde_json::Value) -> Result<ValidationResult, String> {
        let result = ValidationResult::new();
        // Implementation akan di-expand di future versions
        Ok(result)
    }
}
