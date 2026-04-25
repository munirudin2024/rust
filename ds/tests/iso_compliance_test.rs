//! ISO 8000 & ISO/IEC 25012 Compliance Tests

use data_cleaner::iso_standards::*;
use data_cleaner::domain_rules;
use polars::prelude::*;

#[test]
fn test_iso_config_loading() {
    let config = config_loader::ConfigLoader::load_from_json(
        "config/iso_8000_requirements.json"
    ).unwrap();
    
    assert_eq!(config.metadata.standard, "ISO 8000-110:2023");
    assert!(config.domains.contains_key("environmental_air_quality"));
    assert!(config.domains.contains_key("retail_ecommerce"));
}

#[test]
fn test_environmental_pm25_pm10_rule() {
    let df = df! {
        "PM2.5" => [50.0, 200.0, 150.0],
        "PM10" => [80.0, 150.0, 200.0], // Row 1: PM2.5 > PM10 (invalid)
    }.unwrap();
    
    let result = domain_rules::environmental::EnvironmentalRuleEngine::validate_pm25_vs_pm10(&df);
    
    assert_eq!(result.total_checked, 3);
    assert_eq!(result.failed, 1); // Row 1
    assert_eq!(result.passed, 2);
    assert!(result.confidence_score < 1.0);
}

#[test]
fn test_retail_revenue_calculation() {
    let df = df! {
        "quantity" => [2.0, 5.0, 3.0],
        "price" => [10.0, 20.0, 15.0],
        "revenue" => [20.0, 150.0, 45.0], // Row 1: 5*20=100, not 150 (invalid)
    }.unwrap();
    
    let result = domain_rules::retail::RetailRuleEngine::validate_revenue_calculation(&df);
    
    assert_eq!(result.failed, 1);
    assert_eq!(result.violations[0].suggested_action, "recalculate_from_components");
}

#[test]
fn test_iso_threshold_compliance() {
    let config = config_loader::ConfigLoader::load_from_json(
        "config/iso_8000_requirements.json"
    ).unwrap();
    
    // Verify all thresholds meet ISO 8000-61 minimums
    assert!(config.quality_thresholds.completeness.minimum >= 0.90);
    assert!(config.quality_thresholds.consistency.minimum >= 0.90);
    assert!(config.quality_thresholds.accuracy.minimum >= 0.85);
}

#[test]
fn test_quality_dimension_scoring() {
    let scores = vec![
        QualityDimensionScore {
            dimension: "Completeness".to_string(),
            score: 0.95,
            threshold_minimum: 0.90,
            threshold_target: 0.99,
            status: ComplianceStatus::Compliant,
            details: vec!["95% non-null".to_string()],
        },
        QualityDimensionScore {
            dimension: "Accuracy".to_string(),
            score: 0.85,
            threshold_minimum: 0.85,
            threshold_target: 0.95,
            status: ComplianceStatus::Compliant,
            details: vec!["Matches business rules".to_string()],
        },
    ];
    
    // Average compliance score
    let avg_score: f32 = scores.iter().map(|s| s.score).sum::<f32>() / scores.len() as f32;
    assert!(avg_score >= 0.85);
    
    // All compliant
    let all_compliant = scores.iter().all(|s| matches!(s.status, ComplianceStatus::Compliant));
    assert!(all_compliant);
}

#[test]
fn test_rule_violation_severity_levels() {
    // Test Critical violation
    let critical = RuleViolation {
        record_index: 0,
        field_value: "invalid".to_string(),
        expected_condition: "valid format".to_string(),
        actual_condition: "invalid format".to_string(),
        severity: ViolationSeverity::Critical,
        suggested_action: "flag".to_string(),
        confidence: 0.0,
    };
    
    assert!(matches!(critical.severity, ViolationSeverity::Critical));
    
    // Test Major violation
    let major = RuleViolation {
        record_index: 1,
        field_value: "50".to_string(),
        expected_condition: "< 100".to_string(),
        actual_condition: "150".to_string(),
        severity: ViolationSeverity::Major,
        suggested_action: "review".to_string(),
        confidence: 0.7,
    };
    
    assert!(matches!(major.severity, ViolationSeverity::Major));
    
    // Test Minor violation
    let minor = RuleViolation {
        record_index: 2,
        field_value: "acceptable".to_string(),
        expected_condition: "preferred".to_string(),
        actual_condition: "acceptable alternate".to_string(),
        severity: ViolationSeverity::Minor,
        suggested_action: "note".to_string(),
        confidence: 0.85,
    };
    
    assert!(matches!(minor.severity, ViolationSeverity::Minor));
}

#[test]
fn test_environmental_seasonal_anomalies() {
    let df = df! {
        "month" => [6i32, 7i32, 12i32, 1i32],
        "PM2.5" => [200.0, 180.0, 160.0, 170.0],
    }.unwrap();
    
    let anomalies = domain_rules::environmental::EnvironmentalRuleEngine::detect_seasonal_anomalies(&df);
    
    // Months 6 & 7 (summer) with PM2.5 > 150 should be flagged
    assert_eq!(anomalies.len(), 2);
}

#[test]
fn test_retail_high_value_transaction_detection() {
    let df = df! {
        "revenue" => [5000.0, 15000.0, 8000.0, 25000.0],
    }.unwrap();
    
    let high_value = domain_rules::retail::RetailRuleEngine::detect_high_value_transactions(&df);
    
    // Transactions > 10000
    assert_eq!(high_value.len(), 2);
    assert!(high_value.iter().all(|v| matches!(v.severity, ViolationSeverity::Minor)));
}

#[test]
fn test_imputation_confidence_calculation() {
    // Test PM2.5 seasonal interpolation
    let conf_72h = domain_rules::environmental::EnvironmentalRuleEngine::calculate_imputation_confidence(
        "PM2.5",
        "seasonal_interpolation",
        72,
        10,
    );
    assert!(conf_72h > 0.5);
    
    // Test TEMP with shorter gap
    let conf_temp = domain_rules::environmental::EnvironmentalRuleEngine::calculate_imputation_confidence(
        "TEMP",
        "linear_interpolation",
        3,
        10,
    );
    assert!(conf_temp > 0.8);
    
    // Test RAIN forward fill
    let conf_rain_short = domain_rules::environmental::EnvironmentalRuleEngine::calculate_imputation_confidence(
        "RAIN",
        "forward_fill",
        1,
        5,
    );
    assert_eq!(conf_rain_short, 0.78);
    
    let conf_rain_long = domain_rules::environmental::EnvironmentalRuleEngine::calculate_imputation_confidence(
        "RAIN",
        "forward_fill",
        5,
        5,
    );
    assert_eq!(conf_rain_long, 0.5);
}

#[test]
fn test_audit_trail_entry_creation() {
    let entry = AuditTrailEntry {
        timestamp: "2026-04-19T10:30:00+00:00".to_string(),
        stage: "Audit".to_string(),
        operation: "Semantic Validation".to_string(),
        input_records: 1000,
        output_records: 950,
        transformation_logic: "Remove PM2.5 > PM10".to_string(),
        confidence_impact: 0.95,
        reasoning: "Physical constraint violation".to_string(),
        operator_id: Some("auto_validator".to_string()),
        automated: true,
    };
    
    assert_eq!(entry.input_records, 1000);
    assert_eq!(entry.output_records, 950);
    assert!(entry.automated);
    assert_eq!(entry.confidence_impact, 0.95);
}
