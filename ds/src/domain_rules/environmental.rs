//! Environmental Domain Rules - Air Quality
//! Implements ISO 8000-8 semantic quality for environmental data

use crate::iso_standards::*;
use polars::prelude::*;

pub struct EnvironmentalRuleEngine;

impl EnvironmentalRuleEngine {
    pub fn validate_pm25_vs_pm10(df: &DataFrame) -> SemanticQualityResult {
        let pm25 = df.column("PM2.5").unwrap();
        let pm10 = df.column("PM10").unwrap();
        
        let mut violations = Vec::new();
        let mut passed = 0;
        let mut failed = 0;
        
        for idx in 0..df.height() {
            let pm25_val = pm25.get(idx).unwrap().try_extract::<f64>().unwrap_or(0.0);
            let pm10_val = pm10.get(idx).unwrap().try_extract::<f64>().unwrap_or(999.0);
            
            if pm25_val > pm10_val {
                failed += 1;
                violations.push(RuleViolation {
                    record_index: idx,
                    field_value: format!("PM2.5={}, PM10={}", pm25_val, pm10_val),
                    expected_condition: "PM2.5 < PM10".to_string(),
                    actual_condition: format!("PM2.5 ({}) > PM10 ({})", pm25_val, pm10_val),
                    severity: ViolationSeverity::Critical,
                    suggested_action: "flag_invalid_or_swap".to_string(),
                    confidence: 0.0,
                });
            } else {
                passed += 1;
            }
        }
        
        SemanticQualityResult {
            field_name: "PM2.5 vs PM10".to_string(),
            business_rule_id: "ENV-R001".to_string(),
            rule_description: "PM2.5 must be less than PM10 (physical constraint)".to_string(),
            total_checked: passed + failed,
            passed,
            failed,
            confidence_score: if passed + failed > 0 {
                passed as f32 / (passed + failed) as f32
            } else {
                0.0
            },
            violations,
        }
    }
    
    pub fn validate_temp_vs_dewp(df: &DataFrame) -> SemanticQualityResult {
        let temp = df.column("TEMP").unwrap();
        let dewp = df.column("DEWP").unwrap();
        
        let mut violations = Vec::new();
        let mut passed = 0;
        let mut failed = 0;
        
        for idx in 0..df.height() {
            let temp_val = temp.get(idx).unwrap().try_extract::<f64>().unwrap_or(-999.0);
            let dewp_val = dewp.get(idx).unwrap().try_extract::<f64>().unwrap_or(999.0);
            
            if temp_val <= dewp_val {
                failed += 1;
                violations.push(RuleViolation {
                    record_index: idx,
                    field_value: format!("TEMP={}, DEWP={}", temp_val, dewp_val),
                    expected_condition: "TEMP > DEWP".to_string(),
                    actual_condition: format!("TEMP ({}) <= DEWP ({})", temp_val, dewp_val),
                    severity: ViolationSeverity::Critical,
                    suggested_action: "flag_physical_impossibility".to_string(),
                    confidence: 0.0,
                });
            } else {
                passed += 1;
            }
        }
        
        SemanticQualityResult {
            field_name: "TEMP vs DEWP".to_string(),
            business_rule_id: "ENV-R005".to_string(),
            rule_description: "Temperature must exceed dew point (thermodynamic)".to_string(),
            total_checked: passed + failed,
            passed,
            failed,
            confidence_score: if passed + failed > 0 {
                passed as f32 / (passed + failed) as f32
            } else {
                0.0
            },
            violations,
        }
    }
    
    pub fn detect_seasonal_anomalies(df: &DataFrame) -> Vec<RuleViolation> {
        let month = df.column("month").unwrap();
        let pm25 = df.column("PM2.5").unwrap();
        
        let mut anomalies = Vec::new();
        
        for idx in 0..df.height() {
            let m = month.get(idx).unwrap().try_extract::<i32>().unwrap_or(0);
            let pm25_val = pm25.get(idx).unwrap().try_extract::<f64>().unwrap_or(0.0);
            
            // Winter heating season (Nov-Mar): PM2.5 > 150 is normal
            // Summer (Jun-Aug): PM2.5 > 150 is suspect
            if m >= 6 && m <= 8 && pm25_val > 150.0 {
                anomalies.push(RuleViolation {
                    record_index: idx,
                    field_value: format!("month={}, PM2.5={}", m, pm25_val),
                    expected_condition: "Summer PM2.5 typically < 150".to_string(),
                    actual_condition: format!("Summer month {} with PM2.5={}", m, pm25_val),
                    severity: ViolationSeverity::Minor,
                    suggested_action: "annotate_suspect".to_string(),
                    confidence: 0.6,
                });
            }
        }
        
        anomalies
    }
    
    pub fn calculate_imputation_confidence(
        column: &str, 
        method: &str, 
        gap_hours: u32,
        neighbors_available: usize
    ) -> f32 {
        match (column, method) {
            ("PM2.5", "seasonal_interpolation") => {
                let base_confidence = 0.85;
                let gap_penalty = ((gap_hours as f32 / 72.0) * 0.3).min(0.3); // Max 72h
                base_confidence - gap_penalty
            },
            ("TEMP", "linear_interpolation") => {
                let base_confidence = 0.92;
                let gap_penalty = ((gap_hours as f32 / 6.0) * 0.2).min(0.2); // Max 6h
                base_confidence - gap_penalty
            },
            ("RAIN", "forward_fill") => {
                if gap_hours <= 2 {
                    0.78
                } else {
                    0.5 // Low confidence for longer gaps
                }
            },
            _ => {
                // Fallback median
                if neighbors_available >= 10 {
                    0.65
                } else {
                    0.4
                }
            }
        }
    }
}
