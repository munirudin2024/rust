//! Domain-specific Business Rules
//! Rules untuk berbagai industri/domain data

pub mod environmental;
pub mod retail;
pub mod healthcare;
pub mod finance;

pub use environmental::EnvironmentalRuleEngine;
pub use retail::RetailRuleEngine;
pub use healthcare::HealthcareRules;
pub use finance::FinanceRules;

/// Trait untuk business rules domain
pub trait DomainBusinessRules {
    fn name(&self) -> &str;
    fn validate(&self, data: &serde_json::Value) -> Result<ValidationResult, String>;
}

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
