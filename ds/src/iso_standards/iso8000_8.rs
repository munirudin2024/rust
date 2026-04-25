//! ISO 8000-8: Semantic Quality Framework
//! Evaluasi kualitas semantik data terhadap business context

use serde::{Deserialize, Serialize};

/// Report kualitas semantik sesuai ISO 8000-8
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticQualityReport {
    pub dataset_name: String,
    pub timestamp: String,
    pub semantic_assessments: Vec<SemanticAssessment>,
    pub overall_semantic_score: f64,
}

/// Penilaian semantik untuk satu aspek data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticAssessment {
    pub aspect: String,
    pub score: f64, // 0.0 - 100.0
    pub findings: Vec<String>,
    pub recommendations: Vec<String>,
}

/// Quality dimensions untuk semantic validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticDimension {
    pub name: String,
    pub description: String,
    pub weight: f64,
}

impl SemanticQualityReport {
    pub fn new(dataset_name: String) -> Self {
        Self {
            dataset_name,
            timestamp: chrono::Local::now().to_rfc3339(),
            semantic_assessments: Vec::new(),
            overall_semantic_score: 0.0,
        }
    }

    pub fn add_assessment(&mut self, assessment: SemanticAssessment) {
        self.semantic_assessments.push(assessment);
    }

    pub fn calculate_overall_score(&mut self) {
        if self.semantic_assessments.is_empty() {
            self.overall_semantic_score = 0.0;
            return;
        }

        let sum: f64 = self.semantic_assessments.iter().map(|a| a.score).sum();
        self.overall_semantic_score = sum / self.semantic_assessments.len() as f64;
    }
}
