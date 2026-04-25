//! ISO 8000-61 Compliant Audit Formatter

use super::*;
use std::fs;
use std::path::Path;

pub struct ISOAuditFormatter;

impl ISOAuditFormatter {
    pub fn format_stage_header(stage_num: u8, stage_name: &str, iso_clause: &str) -> String {
        format!(
            "── [{}/4] {} ───────────────────────── [{}]",
            stage_num, stage_name, iso_clause
        )
    }
    
    pub fn format_quality_dimensions(scores: &[QualityDimensionScore]) -> String {
        let mut output = String::from("\n   Dimensi Kualitas ISO 25012:\n");
        
        for score in scores {
            let icon = match score.status {
                ComplianceStatus::Compliant => "✅",
                ComplianceStatus::Warning => "⚠️ ",
                ComplianceStatus::NonCompliant => "❌",
                ComplianceStatus::NotMeasured => "➖",
            };
            
            output.push_str(&format!(
                "   {} {}: {:.1}% (min: {:.0}%, target: {:.0}%)\n",
                icon, score.dimension, score.score * 100.0, 
                score.threshold_minimum * 100.0, score.threshold_target * 100.0
            ));
            
            if !score.details.is_empty() {
                for detail in &score.details {
                    output.push_str(&format!("      → {}\n", detail));
                }
            }
        }
        
        output
    }
    
    pub fn format_imputation_log(entries: &[ImputationEntry]) -> String {
        let mut output = String::from("\n   Strategi Imputasi (ISO 8000-8.3):\n");
        output.push_str("   ┌─────────┬─────────┬─────────────────────┬────────────┬────────────┐\n");
        output.push_str("   │ Column  │ Count   │ Method              │ Rationale  │ Confidence │\n");
        output.push_str("   ├─────────┼─────────┼─────────────────────┼────────────┼────────────┤\n");
        
        let total_confidence: f32 = entries.iter().map(|e| e.confidence_weighted).sum();
        let avg_confidence = if !entries.is_empty() {
            total_confidence / entries.len() as f32
        } else {
            1.0
        };
        
        for entry in entries {
            let rationale_short = entry.rationale.chars().take(10).collect::<String>();
            output.push_str(&format!(
                "   │ {:<7} │ {:>7} │ {:<19} │ {:<10} │ {:>10.2} │\n",
                entry.field, entry.records_affected, entry.method, 
                rationale_short, entry.confidence_weighted
            ));
        }
        
        output.push_str("   └─────────┴─────────┴─────────────────────┴────────────┴────────────┘\n");
        output.push_str(&format!("   Weighted Average Confidence: {:.2}\n", avg_confidence));
        
        output
    }
    
    pub fn format_semantic_quality(results: &[SemanticQualityResult]) -> String {
        let mut output = String::from("\n   Kualitas Semantik (ISO 8000-8):\n");
        output.push_str("   Validasi Aturan Bisnis:\n");
        
        for result in results {
            let pass_rate = if result.total_checked > 0 {
                (result.passed as f32 / result.total_checked as f32) * 100.0
            } else {
                0.0
            };
            
            let status_icon = if pass_rate >= 95.0 { "✅" } 
                             else if pass_rate >= 80.0 { "⚠️ " } 
                             else { "❌" };
            
            output.push_str(&format!(
                "   {} [{}] {}: {:.1}% lolos ({}/{})\n",
                status_icon, result.business_rule_id, result.rule_description, 
                pass_rate, result.passed, result.total_checked
            ));
            
            // Show sample violations (max 3)
            for (i, violation) in result.violations.iter().take(3).enumerate() {
                let severity_str = format!("{:?}", violation.severity);
                output.push_str(&format!(
                    "      {}. Rekaman {}: {} (diharapkan: {}, didapat: {})\n",
                    i + 1, violation.record_index, severity_str,
                    violation.expected_condition, violation.actual_condition
                ));
            }
            if result.violations.len() > 3 {
                output.push_str(&format!(
                    "      ... dan {} pelanggaran lainnya\n", result.violations.len() - 3
                ));
            }
        }
        
        output
    }

    pub fn format_outlier_log(entries: &[OutlierEntry]) -> String {
        let mut output = String::from("\n   Penanganan Outlier (Winsorizing + Domain Capping):\n");
        
        for entry in entries {
            output.push_str(&format!(
                "   Field: {}\n\
                 ├─ Metode: {}\n\
                 ├─ Dibatasi (1,5 IQR): {}\n\
                 ├─ Dibatasi (3 IQR): {}\n\
                 ├─ Pembatasan Domain Diterapkan: {}\n\
                 └─ Ditandai untuk Tinjauan: {}\n",
                entry.field,
                entry.method,
                entry.capped_1_5_iqr,
                entry.capped_3_iqr,
                entry.domain_cap_applied,
                entry.manual_review_flagged
            ));
        }
        
        output
    }
    
    pub fn generate_final_report(report: &ISOCompliantAuditReport) -> String {
        let mut output = String::new();
        
        // Header
        output.push_str(&format!(
            "╔══════════════════════════════════════════════════════════════════════════════╗\n\
             ║     LAPORAN AUDIT KUALITAS DATA SESUAI ISO 8000 & ISO/IEC 25012           ║\n\
             ║                          Tingkat Sertifikasi {}                              ║\n\
             ╚══════════════════════════════════════════════════════════════════════════════╝\n",
            report.final_assessment.overall_compliance_level
        ));
        
        // Metadata
        output.push_str(&format!(
            "\nID Laporan: {}\n\
             Dibuat: {}\n\
             Dataset: {}\n\
             Rekaman: {}\n\
             Standar: {}\n\
             Pipeline: {}\n",
            report.metadata.report_id,
            report.metadata.generated_at,
            report.metadata.dataset_name,
            report.metadata.records_processed,
            report.metadata.iso_standard_version,
            report.metadata.pipeline_version
        ));
        
        // Quality Dimensions
        output.push_str(&Self::format_quality_dimensions(&report.quality_dimensions));
        
        // Semantic Quality
        output.push_str(&Self::format_semantic_quality(&report.semantic_quality));
        
        // Imputation Log
        output.push_str(&Self::format_imputation_log(&report.imputation_log));

        // Outlier Log
        output.push_str(&Self::format_outlier_log(&report.outlier_log));
        
        // Final Assessment
        output.push_str("\n╔══════════════════════════════════════════════════════════════════════════════╗\n");
        output.push_str("║                           PENILAIAN AKHIR                                    ║\n");
        output.push_str("╚══════════════════════════════════════════════════════════════════════════════╝\n");
        output.push_str(&format!(
            "Tingkat Kepatuhan Keseluruhan: {}/5\n\
             Siap Sertifikasi: {}\n",
            report.final_assessment.overall_compliance_level,
            if report.final_assessment.certification_ready { "YA ✅" } else { "TIDAK ❌" }
        ));
        
        if !report.final_assessment.critical_gaps.is_empty() {
            output.push_str("\nCelah Kritis:\n");
            for gap in &report.final_assessment.critical_gaps {
                output.push_str(&format!("  ❌ {}\n", gap));
            }
        }
        
        if !report.final_assessment.recommendations.is_empty() {
            output.push_str("\nRekomendasi:\n");
            for rec in &report.final_assessment.recommendations {
                output.push_str(&format!("  ➡️  {}\n", rec));
            }
        }
        
        output.push_str(&format!(
            "\nTanggal Audit Berikutnya: {}\n",
            report.final_assessment.next_audit_date
        ));
        
        output
    }
    
    pub fn save_json_report<P: AsRef<Path>>(
        report: &ISOCompliantAuditReport, 
        path: P,
    ) -> Result<(), std::io::Error> {
        let json = serde_json::to_string_pretty(report)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn save_text_report<P: AsRef<Path>>(
        report: &ISOCompliantAuditReport,
        path: P,
    ) -> Result<(), std::io::Error> {
        let text = Self::generate_final_report(report);
        fs::write(path, text)?;
        Ok(())
    }
}
