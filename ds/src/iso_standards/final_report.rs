//! Final ISO report formatter and exporter.

use chrono::Utc;
use std::collections::HashMap;

use crate::iso_standards::manual_review::ManualReviewTracker;
use crate::iso_standards::*;
use crate::time_utils::{format_iso_timestamp, TimestampContext};

pub struct FinalISOReport;

impl FinalISOReport {
    pub fn finalize(report: &ISOCompliantAuditReport) -> std::io::Result<()> {
        Self::finalize_with_review(report, None)
    }

    pub fn finalize_with_review(
        report: &ISOCompliantAuditReport,
        review_tracker: Option<&ManualReviewTracker>,
    ) -> std::io::Result<()> {
        let (timestamp, safe_name) = Self::path_context(report);
        println!(
            "{}",
            Self::create_boxed_report_with_paths(report, review_tracker, &timestamp, &safe_name)
        );
        Self::export_all_formats_with_paths(report, review_tracker, &timestamp, &safe_name)
    }

    fn create_boxed_report_with_paths(
        report: &ISOCompliantAuditReport,
        review_tracker: Option<&ManualReviewTracker>,
        timestamp: &str,
        safe_name: &str,
    ) -> String {
        let mut lines = Vec::new();

        lines.push("╔══════════════════════════════════════════════════════════════════════════════╗".to_string());
        lines.push("║             🏆  LAPORAN KEPATUHAN ISO 8000 & ISO/IEC 25012                  ║".to_string());
        lines.push(format!(
            "║{:^78}║",
            format!(
                "LAPORAN AUDIT KUALITAS DATA - TINGKAT {}",
                report.final_assessment.overall_compliance_level
            )
        ));
        lines.push("╚══════════════════════════════════════════════════════════════════════════════╝".to_string());

        lines.push(Self::section_header("📋 METADATA LAPORAN"));
        lines.push(Self::kv_line("ID Laporan", &report.metadata.report_id));
        lines.push(Self::kv_line("Waktu Dibuat", &report.metadata.generated_at));
        lines.push(Self::kv_line("Nama Dataset", &report.metadata.dataset_name));
        lines.push(Self::kv_line(
            "Baris Diproses",
            &Self::format_number(report.metadata.records_processed),
        ));
        lines.push(Self::kv_line("Versi Pipeline", &report.metadata.pipeline_version));
        lines.push(Self::kv_line("Git Commit", &report.metadata.git_commit_hash));

        lines.push(Self::section_header("🧭 SKOR 3-LEVEL ISO 8000-8"));
        let syntactic = Self::find_dimension_score(report, "Syntactic");
        let semantic = Self::find_dimension_score(report, "Semantic");
        let pragmatic = Self::find_dimension_score(report, "Pragmatic");
        lines.push(format!("  Syntactic : {:>6.1}%", syntactic * 100.0));
        lines.push(format!("  Semantic  : {:>6.1}%", semantic * 100.0));
        lines.push(format!("  Pragmatic : {:>6.1}%", pragmatic * 100.0));

        lines.push(Self::section_header("📊 DIMENSI KUALITAS ISO/IEC 25012"));
        for score in &report.quality_dimensions {
            let (icon, status_str) = Self::status_icon(&score.status);
            lines.push(format!(
                "  {} {:<20} {:>6.1}%  {}",
                icon,
                format!("{}:", score.dimension),
                score.score * 100.0,
                status_str
            ));
        }

        if let Some(tracker) = review_tracker {
            lines.push(Self::section_header("🚨 ANTRIAN TINJAUAN MANUAL (ISO 8000-8.5)"));
            lines.push(tracker.display_queue());
            lines.push(Self::section_header("🧩 RINGKASAN KATEGORI AKAR MASALAH"));
            for (category, count) in Self::root_cause_categories(tracker) {
                lines.push(format!("  - {:<28} {} kasus", category, count));
            }
        }

        lines.push(Self::section_header("🏁 PENILAIAN AKHIR"));
        lines.push(format!(
            "  Tingkat Kepatuhan: {}/5",
            report.final_assessment.overall_compliance_level
        ));
        lines.push(format!(
            "  Status Sertifikasi: {}",
            if report.final_assessment.certification_ready {
                "SIAP ✅"
            } else {
                "BELUM SIAP ❌"
            }
        ));
        lines.push(format!(
            "  Audit Berikutnya: {}",
            report.final_assessment.next_audit_date
        ));

        lines.push(String::new());
        lines.push("╔══════════════════════════════════════════════════════════════════════════════╗".to_string());
        lines.push(format!("║  📁 AUDIT:  output/iso_compliant/{}_{}_audit.json", timestamp, safe_name));
        lines.push(format!("║  📁 ASAL:   output/iso_compliant/{}_{}_provenance.json", timestamp, safe_name));
        lines.push(format!("║  📁 RINGK.: output/iso_compliant/{}_{}_summary.json", timestamp, safe_name));
        lines.push(format!("║  📁 DASH:   output/iso_compliant/{}_{}_dashboard.html", timestamp, safe_name));
        lines.push(format!("║  📁 METRIK: output/iso_compliant/{}_{}_metrics.csv", timestamp, safe_name));
        if review_tracker.is_some() {
            lines.push(format!(
                "║  📁 TINJAU: output/iso_compliant/{}_{}_manual_review.json",
                timestamp, safe_name
            ));
            lines.push(format!(
                "║  📁 JEJAK:  output/iso_compliant/{}_{}_record_provenance.json",
                timestamp, safe_name
            ));
            lines.push(format!(
                "║  📁 KARAN:  output/iso_compliant/{}_{}_quarantine_candidates.json",
                timestamp, safe_name
            ));
            lines.push(format!(
                "║  📁 TIKET:  output/iso_compliant/{}_{}_upstream_tickets.json",
                timestamp, safe_name
            ));
        }
        lines.push("╚══════════════════════════════════════════════════════════════════════════════╝".to_string());

        lines.join("\n")
    }

    fn section_header(title: &str) -> String {
        format!("\n┌─{:─<77}┐", format!(" {} ", title))
    }

    fn kv_line(key: &str, value: &str) -> String {
        format!("  {:<25} {}", format!("{}:", key), value)
    }

    fn status_icon(status: &ComplianceStatus) -> (&'static str, &'static str) {
        match status {
            ComplianceStatus::Compliant => ("✅", "LULUS"),
            ComplianceStatus::Warning => ("⚠️", "PERINGATAN"),
            ComplianceStatus::NonCompliant => ("❌", "GAGAL"),
            ComplianceStatus::NotMeasured => ("ℹ️", "TIDAK DINILAI"),
        }
    }

    fn sanitize_filename(name: &str) -> String {
        name.replace(|c: char| !c.is_alphanumeric() && c != '-' && c != '_', "_")
            .to_lowercase()
    }

    fn find_dimension_score(report: &ISOCompliantAuditReport, needle: &str) -> f32 {
        report
            .quality_dimensions
            .iter()
            .find(|d| d.dimension.to_ascii_lowercase().contains(&needle.to_ascii_lowercase()))
            .map(|d| d.score)
            .unwrap_or(0.0)
    }

    fn root_cause_categories(tracker: &ManualReviewTracker) -> Vec<(String, usize)> {
        let mut map: HashMap<String, usize> = HashMap::new();
        for case in tracker.get_cases() {
            let source = case
                .root_cause
                .as_deref()
                .unwrap_or("unknown");
            let lower = source.to_ascii_lowercase();
            let category = if lower.contains("ingest") || lower.contains("upstream") {
                "ingest/upstream error"
            } else if lower.contains("timezone") || lower.contains("clock") {
                "timezone issue"
            } else if lower.contains("duplikasi") || lower.contains("duplicate") {
                "upstream duplication"
            } else if lower.contains("kepadatan") || lower.contains("historis") {
                "low historical data"
            } else {
                "other"
            };
            *map.entry(category.to_string()).or_insert(0) += 1;
        }
        let mut entries: Vec<(String, usize)> = map.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries
    }

    fn path_context(report: &ISOCompliantAuditReport) -> (String, String) {
        (
            format_iso_timestamp(Utc::now(), TimestampContext::Filename),
            Self::sanitize_filename(&report.metadata.dataset_name),
        )
    }

    fn format_number(value: usize) -> String {
        let s = value.to_string();
        let mut out = String::new();
        let mut count = 0usize;
        for ch in s.chars().rev() {
            if count > 0 && count % 3 == 0 {
                out.push(',');
            }
            out.push(ch);
            count += 1;
        }
        out.chars().rev().collect()
    }

    fn export_all_formats_with_paths(
        report: &ISOCompliantAuditReport,
        review_tracker: Option<&ManualReviewTracker>,
        timestamp: &str,
        safe_name: &str,
    ) -> std::io::Result<()> {
        let output_dir = "output/iso_compliant";
        std::fs::create_dir_all(output_dir)?;

        let audit_file = Self::export_json_with_paths(report, timestamp, safe_name)?;
        let provenance_file = format!("{}/{}_{}_provenance.json", output_dir, timestamp, safe_name);
        let summary_file = format!("{}/{}_{}_summary.json", output_dir, timestamp, safe_name);

        let html_file = format!("{}/{}_{}_dashboard.html", output_dir, timestamp, safe_name);
        std::fs::write(&html_file, Self::create_html_content(report))?;

        let csv_file = format!("{}/{}_{}_metrics.csv", output_dir, timestamp, safe_name);
        std::fs::write(&csv_file, Self::generate_csv(report))?;

        let manual_review_file = if let Some(tracker) = review_tracker {
            let filepath = format!("{}/{}_{}_manual_review.json", output_dir, timestamp, safe_name);
            std::fs::write(&filepath, tracker.export_json())?;
            Some(filepath)
        } else {
            None
        };

        let detailed_record_provenance = if let Some(tracker) = review_tracker {
            let filepath = format!(
                "{}/{}_{}_record_provenance.json",
                output_dir, timestamp, safe_name
            );
            std::fs::write(
                &filepath,
                Self::generate_record_provenance_json(report, Some(tracker)),
            )?;
            Some(filepath)
        } else {
            None
        };

        let quarantine_candidates_file = if let Some(tracker) = review_tracker {
            let filepath = format!(
                "{}/{}_{}_quarantine_candidates.json",
                output_dir, timestamp, safe_name
            );
            std::fs::write(&filepath, Self::generate_quarantine_candidates_json(tracker, 0.80))?;
            Some(filepath)
        } else {
            None
        };

        let upstream_tickets_file = if let Some(tracker) = review_tracker {
            let filepath = format!(
                "{}/{}_{}_upstream_tickets.json",
                output_dir, timestamp, safe_name
            );
            std::fs::write(&filepath, Self::generate_upstream_tickets_json(report, tracker))?;
            Some(filepath)
        } else {
            None
        };

        println!(
            "✅ Diekspor {} file ke {}/",
            if manual_review_file.is_some() { 9 } else { 5 },
            output_dir
        );
        println!("   ├── {}", audit_file);
        println!("   ├── {}", provenance_file);
        println!("   ├── {}", summary_file);
        println!("   ├── {}", html_file);
        println!("   ├── {}", csv_file);
        if let Some(path) = manual_review_file {
            println!("   ├── {}", path);
        }
        if let Some(path) = detailed_record_provenance {
            println!("   ├── {}", path);
        }
        if let Some(path) = quarantine_candidates_file {
            println!("   ├── {}", path);
        }
        if let Some(path) = upstream_tickets_file {
            println!("   └── {}", path);
        }

        Ok(())
    }

    fn export_json_with_paths(
        report: &ISOCompliantAuditReport,
        timestamp: &str,
        safe_name: &str,
    ) -> std::io::Result<String> {
        let output_dir = "output/iso_compliant";
        std::fs::create_dir_all(output_dir)?;

        let filename = format!("{}_{}_audit.json", timestamp, safe_name);
        let filepath = format!("{}/{}", output_dir, filename);

        let mut json_value = serde_json::to_value(report)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        if let Some(obj) = json_value.as_object_mut() {
            obj.insert(
                "compliance_level".to_string(),
                serde_json::json!(report.final_assessment.overall_compliance_level),
            );
            obj.insert(
                "certification_ready".to_string(),
                serde_json::json!(report.final_assessment.certification_ready),
            );
        }

        let json = serde_json::to_string_pretty(&json_value)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        std::fs::write(&filepath, json)?;

        let prov_filename = format!("{}_{}_provenance.json", timestamp, safe_name);
        let prov_filepath = format!("{}/{}", output_dir, prov_filename);
        std::fs::write(&prov_filepath, Self::generate_provenance_json(report))?;

        let summary_filename = format!("{}_{}_summary.json", timestamp, safe_name);
        let summary_filepath = format!("{}/{}", output_dir, summary_filename);
        std::fs::write(&summary_filepath, Self::generate_summary_json(report))?;

        Ok(filepath)
    }

    fn generate_provenance_json(report: &ISOCompliantAuditReport) -> String {
        let avg_quality_score = if report.quality_dimensions.is_empty() {
            0.0
        } else {
            report.quality_dimensions.iter().map(|q| q.score).sum::<f32>()
                / report.quality_dimensions.len() as f32
        };

        let provenance = serde_json::json!({
            "@context": {
                "prov": "http://www.w3.org/ns/prov#",
                "iso": "http://iso.org/8000/"
            },
            "@type": "prov:Activity",
            "prov:startedAtTime": report.audit_trail.first().map(|a| a.timestamp.clone()).unwrap_or_default(),
            "prov:endedAtTime": report.audit_trail.last().map(|a| a.timestamp.clone()).unwrap_or_default(),
            "prov:used": report.metadata.dataset_name,
            "iso:averageQualityScore": avg_quality_score,
            "iso:complianceLevel": report.final_assessment.overall_compliance_level,
            "iso:certificationReady": report.final_assessment.certification_ready,
            "prov:generatedAtTime": report.metadata.generated_at,
        });

        serde_json::to_string_pretty(&provenance).unwrap_or_else(|_| "{}".to_string())
    }

    fn generate_record_provenance_json(
        report: &ISOCompliantAuditReport,
        review_tracker: Option<&ManualReviewTracker>,
    ) -> String {
        let record_events = if let Some(tracker) = review_tracker {
            tracker
                .get_cases()
                .iter()
                .map(|case| {
                    serde_json::json!({
                        "record_index": case.record_index,
                        "affected_records": {
                            "primary": case.record_index,
                            "related": case.related_record_index,
                        },
                        "duplicate_handling": {
                            "kept_record_index": case.related_record_index,
                            "rejected_record_index": case.record_index,
                            "decision": case.decision.as_ref().map(|v| v.to_string()),
                            "decision_reason": case.decision_reason,
                        },
                        "field": case.field,
                        "issue_type": case.issue_type,
                        "detected_at": case.detected_at,
                        "severity": case.severity.to_string(),
                        "root_cause": case.root_cause,
                        "decision": case.decision.as_ref().map(|v| v.to_string()),
                        "decision_reason": case.decision_reason,
                        "approver": case.approver,
                        "approved_at": case.approved_at,
                        "quarantine_recommended": case.quarantine_recommended,
                        "confidence_score": case.confidence_score,
                        "expected": case.expected,
                        "actual": case.actual,
                        "suggested_action": case.suggested_action,
                    })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let value = serde_json::json!({
            "dataset": report.metadata.dataset_name,
            "generated_at": report.metadata.generated_at,
            "provenance_scope": "record-level-change-tracking",
            "events": record_events,
            "audit_trail": report.audit_trail,
        });

        serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
    }

    fn generate_quarantine_candidates_json(
        tracker: &ManualReviewTracker,
        min_imputation_confidence: f32,
    ) -> String {
        let cases = tracker
            .quarantine_candidates(min_imputation_confidence)
            .iter()
            .map(|case| {
                serde_json::json!({
                    "case_id": case.case_id,
                    "record_index": case.record_index,
                    "field": case.field,
                    "severity": case.severity.to_string(),
                    "issue_type": case.issue_type,
                    "confidence_score": case.confidence_score,
                    "quarantine_recommended": case.quarantine_recommended,
                    "decision": case.decision.as_ref().map(|v| v.to_string()),
                    "approver": case.approver,
                    "decision_reason": case.decision_reason,
                })
            })
            .collect::<Vec<_>>();

        let value = serde_json::json!({
            "policy": {
                "min_imputation_confidence": min_imputation_confidence,
                "critical_severity_auto_quarantine": true,
                "low_confidence_auto_quarantine": true,
            },
            "total_candidates": cases.len(),
            "candidates": cases,
        });

        serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
    }

    fn generate_upstream_tickets_json(
        report: &ISOCompliantAuditReport,
        tracker: &ManualReviewTracker,
    ) -> String {
        let tickets = tracker
            .get_cases()
            .iter()
            .map(|case| {
                serde_json::json!({
                    "title": format!("[DataQuality] {} pada baris #{}", case.issue_type, case.record_index),
                    "dataset": report.metadata.dataset_name,
                    "severity": case.severity.to_string(),
                    "root_cause": case.root_cause,
                    "suggested_action": case.suggested_action,
                    "decision": case.decision.as_ref().map(|v| v.to_string()),
                    "decision_reason": case.decision_reason,
                    "approver": case.approver,
                    "labels": ["data-quality", "upstream", "iso-8000"],
                    "payload": {
                        "case_id": case.case_id,
                        "record_index": case.record_index,
                        "field": case.field,
                        "expected": case.expected,
                        "actual": case.actual,
                        "related_record_index": case.related_record_index,
                    }
                })
            })
            .collect::<Vec<_>>();

        let value = serde_json::json!({
            "integration_hint": "Import JSON ini ke Jira/Linear menggunakan automation rule.",
            "total_tickets": tickets.len(),
            "tickets": tickets,
        });

        serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
    }

    fn generate_summary_json(report: &ISOCompliantAuditReport) -> String {
        let scores: HashMap<String, f32> = report
            .quality_dimensions
            .iter()
            .map(|q| (q.dimension.clone(), q.score))
            .collect();

        let summary = serde_json::json!({
            "report_id": report.metadata.report_id,
            "generated_at": report.metadata.generated_at,
            "dataset": report.metadata.dataset_name,
            "records": report.metadata.records_processed,
            "compliance_level": report.final_assessment.overall_compliance_level,
            "certification_ready": report.final_assessment.certification_ready,
            "quality_scores": scores,
            "critical_gaps_count": report.final_assessment.critical_gaps.len(),
            "critical_gaps": report.final_assessment.critical_gaps,
            "recommendations": report.final_assessment.recommendations,
            "next_audit": report.final_assessment.next_audit_date,
        });

        serde_json::to_string_pretty(&summary).unwrap_or_else(|_| "{}".to_string())
    }

    fn generate_csv(report: &ISOCompliantAuditReport) -> String {
        let mut lines = vec!["dimension,score,threshold_min,threshold_target,status".to_string()];
        for q in &report.quality_dimensions {
            lines.push(format!(
                "{},{:.4},{:.2},{:.2},{:?}",
                q.dimension, q.score, q.threshold_minimum, q.threshold_target, q.status
            ));
        }
        lines.join("\n")
    }

    fn create_html_content(report: &ISOCompliantAuditReport) -> String {
        format!(
            r#"<!DOCTYPE html>
<html>
<head><title>ISO 8000 Dashboard - {}</title></head>
<body>
<h1>ISO 8000 Quality Dashboard</h1>
<p>Level {} | {} | {} records</p>
</body>
</html>"#,
            report.metadata.dataset_name,
            report.final_assessment.overall_compliance_level,
            report.metadata.generated_at,
            report.metadata.records_processed
        )
    }
}
