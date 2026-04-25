//! Fallback Strategy untuk Imputation
//! Strategi backup ketika primary method gagal

use serde::{Deserialize, Serialize};
use super::strategies::ImputationStrategy;

/// Konfigurasi fallback strategy chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackStrategy {
    pub primary: ImputationStrategy,
    pub secondary: Option<ImputationStrategy>,
    pub tertiary: Option<ImputationStrategy>,
}

impl FallbackStrategy {
    pub fn new(primary: ImputationStrategy) -> Self {
        Self {
            primary,
            secondary: None,
            tertiary: None,
        }
    }

    pub fn with_secondary(mut self, secondary: ImputationStrategy) -> Self {
        self.secondary = Some(secondary);
        self
    }

    pub fn with_tertiary(mut self, tertiary: ImputationStrategy) -> Self {
        self.tertiary = Some(tertiary);
        self
    }

    pub fn execute(&self, values: &[Option<f64>]) -> Vec<f64> {
        let result = self.primary.impute(values);
        
        // Check if primary worked (has non-zero values)
        if result.iter().any(|v| *v != 0.0) {
            return result;
        }

        // Try secondary
        if let Some(ref secondary) = self.secondary {
            let result = secondary.impute(values);
            if result.iter().any(|v| *v != 0.0) {
                return result;
            }
        }

        // Try tertiary
        if let Some(ref tertiary) = self.tertiary {
            return tertiary.impute(values);
        }

        result
    }
}
