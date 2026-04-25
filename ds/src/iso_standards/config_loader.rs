//! ISO 8000-110 Configuration Loader

use super::DataRequirementSpec;
use std::fs;
use std::path::Path;

pub struct ConfigLoader;

impl ConfigLoader {
    pub fn load_from_json<P: AsRef<Path>>(path: P) -> Result<DataRequirementSpec, ConfigError> {
        let content = fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(e))?;
        let spec: DataRequirementSpec = serde_json::from_str(&content)
            .map_err(|e| ConfigError::Parse(e))?;
        spec.validate()?;
        Ok(spec)
    }
    
    pub fn detect_domain_from_data(&self, headers: &[String]) -> Option<String> {
        // Auto-detect domain based on column names
        let header_set: std::collections::HashSet<String> = 
            headers.iter().map(|h| h.to_lowercase()).collect();
        
        if header_set.contains("pm2.5") || header_set.contains("pm10") || 
           header_set.contains("pm25") || header_set.contains("aqi") {
            Some("environmental_air_quality".to_string())
        } else if header_set.contains("transaction_id") || header_set.contains("revenue") ||
                  header_set.contains("customer_id") || header_set.contains("quantity") {
            Some("retail_ecommerce".to_string())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(serde_json::Error),
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "IO Error: {}", e),
            ConfigError::Parse(e) => write!(f, "JSON Parse Error: {}", e),
            ConfigError::Validation(msg) => write!(f, "Validation Error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self { 
        ConfigError::Io(e) 
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(e: serde_json::Error) -> Self { 
        ConfigError::Parse(e) 
    }
}

trait ConfigValidation {
    fn validate(&self) -> Result<(), ConfigError>;
}

impl ConfigValidation for DataRequirementSpec {
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate all domains have at least one field
        for (domain_name, domain) in &self.domains {
            if domain.fields.is_empty() {
                return Err(ConfigError::Validation(
                    format!("Domain {} has no fields defined", domain_name)
                ));
            }
            
            // Validate business rule IDs are unique per domain
            let mut rule_ids = std::collections::HashSet::new();
            for (field_name, field) in &domain.fields {
                if let Some(rules) = &field.business_rules {
                    for rule in rules {
                        if !rule_ids.insert(rule.id.clone()) {
                            return Err(ConfigError::Validation(
                                format!("Duplicate rule ID '{}' in domain '{}' field '{}'", 
                                    rule.id, domain_name, field_name)
                            ));
                        }
                    }
                }
            }
        }
        
        // Validate quality thresholds are sensible (minimum <= target)
        let thresholds = vec![
            ("completeness", &self.quality_thresholds.completeness),
            ("consistency", &self.quality_thresholds.consistency),
            ("accuracy", &self.quality_thresholds.accuracy),
            ("syntactic_validity", &self.quality_thresholds.syntactic_validity),
            ("semantic_validity", &self.quality_thresholds.semantic_validity),
            ("pragmatic_quality", &self.quality_thresholds.pragmatic_quality),
        ];
        
        for (name, threshold) in thresholds {
            if threshold.minimum > threshold.target {
                return Err(ConfigError::Validation(
                    format!("Threshold {} has minimum ({}) > target ({})", 
                        name, threshold.minimum, threshold.target)
                ));
            }
        }
        
        Ok(())
    }
}
