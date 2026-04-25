//! Retail Domain Rules - E-commerce
//! Implements ISO 8000-8 semantic quality for transactional data

use crate::iso_standards::*;
use polars::prelude::*;

pub struct RetailRuleEngine;

impl RetailRuleEngine {
    pub fn validate_revenue_calculation(df: &DataFrame) -> SemanticQualityResult {
        let qty = df.column("quantity").unwrap();
        let price = df.column("price").unwrap();
        let revenue = df.column("revenue").unwrap();
        
        let mut violations = Vec::new();
        let mut passed = 0;
        let mut failed = 0;
        
        for idx in 0..df.height() {
            let q = qty.get(idx).unwrap().try_extract::<f64>().unwrap_or(0.0);
            let p = price.get(idx).unwrap().try_extract::<f64>().unwrap_or(0.0);
            let r = revenue.get(idx).unwrap().try_extract::<f64>().unwrap_or(0.0);
            
            let expected = q * p;
            let tolerance = 0.01;
            
            if (r - expected).abs() > tolerance {
                failed += 1;
                violations.push(RuleViolation {
                    record_index: idx,
                    field_value: format!("qty={}, price={}, revenue={}", q, p, r),
                    expected_condition: format!("revenue = {} × {} = {}", q, p, expected),
                    actual_condition: format!("revenue = {}", r),
                    severity: ViolationSeverity::Major,
                    suggested_action: "recalculate_from_components".to_string(),
                    confidence: 0.0,
                });
            } else {
                passed += 1;
            }
        }
        
        SemanticQualityResult {
            field_name: "revenue".to_string(),
            business_rule_id: "RET-R002".to_string(),
            rule_description: "Revenue must equal quantity × price".to_string(),
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
    
    pub fn validate_rating_range(df: &DataFrame) -> SemanticQualityResult {
        let rating = df.column("rating").unwrap();
        
        let mut violations = Vec::new();
        let mut passed = 0;
        let mut failed = 0;
        
        for idx in 0..df.height() {
            if let Ok(r) = rating.get(idx).unwrap().try_extract::<i32>() {
                if r < 1 || r > 5 {
                    failed += 1;
                    violations.push(RuleViolation {
                        record_index: idx,
                        field_value: r.to_string(),
                        expected_condition: "1 <= rating <= 5".to_string(),
                        actual_condition: format!("rating = {}", r),
                        severity: ViolationSeverity::Major,
                        suggested_action: "clamp_or_null".to_string(),
                        confidence: 0.0,
                    });
                } else {
                    passed += 1;
                }
            } else {
                // Null is acceptable
                passed += 1;
            }
        }
        
        SemanticQualityResult {
            field_name: "rating".to_string(),
            business_rule_id: "RET-R004".to_string(),
            rule_description: "Rating must be 1-5 stars".to_string(),
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
    
    pub fn detect_high_value_transactions(df: &DataFrame) -> Vec<RuleViolation> {
        let revenue = df.column("revenue").unwrap();
        let mut suspects = Vec::new();
        
        for idx in 0..df.height() {
            if let Ok(r) = revenue.get(idx).unwrap().try_extract::<f64>() {
                if r > 10000.0 {
                    suspects.push(RuleViolation {
                        record_index: idx,
                        field_value: format!("revenue={}", r),
                        expected_condition: "Normal transaction < 10000".to_string(),
                        actual_condition: format!("High value transaction: {}", r),
                        severity: ViolationSeverity::Minor,
                        suggested_action: "flag_for_verification".to_string(),
                        confidence: 0.7,
                    });
                }
            }
        }
        
        suspects
    }
}
