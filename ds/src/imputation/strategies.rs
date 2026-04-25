//! Imputation Strategy Implementations
//! Mean, median, forward fill, backward fill, dll

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnType {
    Ordinal,
    Categorical,
    Continuous,
}

/// Enum untuk berbagai strategi imputation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ImputationStrategy {
    Mean,
    Median,
    Mode,
    ForwardFill,
    BackwardFill,
    LinearInterpolation,
    LastObservationCarriedForward,
    Custom { name: String, description: String },
}

impl ImputationStrategy {
    pub fn description(&self) -> String {
        match self {
            Self::Mean => "Isi dengan nilai rata-rata kolom".to_string(),
            Self::Median => "Isi dengan nilai median kolom".to_string(),
            Self::Mode => "Isi dengan nilai paling sering muncul".to_string(),
            Self::ForwardFill => "Isi dengan nilai terakhir yang diketahui (forward)".to_string(),
            Self::BackwardFill => "Isi dengan nilai terakhir yang diketahui (backward)".to_string(),
            Self::LinearInterpolation => "Interpolasi linear berdasarkan nilai sekitar".to_string(),
            Self::LastObservationCarriedForward => "Gunakan observasi terakhir".to_string(),
            Self::Custom { description, .. } => description.clone(),
        }
    }

    pub fn impute(&self, values: &[Option<f64>]) -> Vec<f64> {
        match self {
            Self::Mean => self.impute_mean(values),
            Self::Median => self.impute_median(values),
            Self::ForwardFill => self.impute_forward_fill(values),
            _ => values.iter().map(|v| v.unwrap_or(0.0)).collect(),
        }
    }

    fn impute_mean(&self, values: &[Option<f64>]) -> Vec<f64> {
        let valid: Vec<f64> = values.iter().filter_map(|v| *v).collect();
        let mean = valid.iter().sum::<f64>() / valid.len().max(1) as f64;
        values.iter().map(|v| v.unwrap_or(mean)).collect()
    }

    fn impute_median(&self, values: &[Option<f64>]) -> Vec<f64> {
        let mut valid: Vec<f64> = values.iter().filter_map(|v| *v).collect();
        valid.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = valid.get(valid.len() / 2).copied().unwrap_or(0.0);
        values.iter().map(|v| v.unwrap_or(median)).collect()
    }

    fn impute_forward_fill(&self, values: &[Option<f64>]) -> Vec<f64> {
        let mut result = Vec::new();
        let mut last_known = 0.0;
        for v in values {
            match v {
                Some(val) => {
                    last_known = *val;
                    result.push(*val);
                }
                None => result.push(last_known),
            }
        }
        result
    }
}

pub fn select_imputation_method(
    column_type: ColumnType,
    gap_size: usize,
    neighbors: usize,
) -> (ImputationStrategy, f32) {
    match column_type {
        ColumnType::Ordinal => {
            let confidence = if neighbors >= 2 { 0.75_f32 } else { 0.68_f32 };
            (ImputationStrategy::Mode, confidence)
        }
        ColumnType::Categorical => {
            let confidence = if neighbors >= 2 { 0.70_f32 } else { 0.62_f32 };
            (
                ImputationStrategy::Custom {
                    name: "NeighborSimilarity".to_string(),
                    description: "Neighbor similarity / frequent token match".to_string(),
                },
                confidence,
            )
        }
        ColumnType::Continuous => {
            let (strategy, mut confidence): (ImputationStrategy, f32) = if gap_size <= 3 {
                (ImputationStrategy::LinearInterpolation, 0.82_f32)
            } else {
                (ImputationStrategy::Median, 0.72_f32)
            };

            if neighbors < 2 {
                confidence -= 0.07_f32;
            }

            (strategy, confidence.max(0.5_f32))
        }
    }
}
