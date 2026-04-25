//! Confidence Score untuk Imputed Values
//! Uncertainty quantification untuk nilai yang diisi

use serde::{Deserialize, Serialize};

/// Score kepercayaan untuk setiap nilai yang diisi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceScore {
    pub method: String,
    pub confidence_level: f64, // 0.0 - 1.0
    pub uncertainty: f64,       // estimation error
    pub data_density: f64,      // proporsi non-missing values
}

impl ConfidenceScore {
    pub fn new(method: String, confidence: f64, uncertainty: f64, density: f64) -> Self {
        Self {
            method,
            confidence_level: confidence.max(0.0).min(1.0),
            uncertainty: uncertainty.max(0.0),
            data_density: density.max(0.0).min(1.0),
        }
    }

    pub fn is_high_confidence(&self) -> bool {
        self.confidence_level >= 0.8
    }

    pub fn is_reliable(&self) -> bool {
        self.confidence_level >= 0.7 && self.data_density >= 0.5
    }

    pub fn quality_indicator(&self) -> String {
        match self.confidence_level {
            c if c >= 0.95 => "Excellent".to_string(),
            c if c >= 0.80 => "Good".to_string(),
            c if c >= 0.60 => "Fair".to_string(),
            c if c >= 0.40 => "Poor".to_string(),
            _ => "Very Poor".to_string(),
        }
    }
}
