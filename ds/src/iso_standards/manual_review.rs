//! Manual Review Tracking Module (ISO 8000-8.5)
//! Track, display, and resolve manual review cases.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualReviewCase {
    pub case_id: u32,
    pub record_index: usize,
    pub field: String,
    pub issue_type: String,
    pub expected: String,
    pub actual: String,
    pub related_record_index: Option<usize>,
    pub severity: ReviewSeverity,
    pub auto_flagged: bool,
    pub detected_at: DateTime<Utc>,
    pub suggested_action: String,
    pub resolution: ResolutionStatus,
    pub root_cause: Option<String>,
    pub decision: Option<ReviewDecision>,
    pub decision_reason: Option<String>,
    pub approver: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub quarantine_recommended: bool,
    pub confidence_score: Option<f32>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReviewSeverity {
    Critical,
    Major,
    Minor,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolutionStatus {
    Pending,
    UnderReview,
    Resolved { action_taken: String },
    Escalated { to: String },
    Dismissed { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReviewDecision {
    Approve,
    Reject,
    Quarantine,
}

impl fmt::Display for ReviewSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReviewSeverity::Critical => write!(f, "Critical"),
            ReviewSeverity::Major => write!(f, "Major"),
            ReviewSeverity::Minor => write!(f, "Minor"),
            ReviewSeverity::Info => write!(f, "Info"),
        }
    }
}

impl fmt::Display for ResolutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolutionStatus::Pending => write!(f, "Pending review"),
            ResolutionStatus::UnderReview => write!(f, "Under review"),
            ResolutionStatus::Resolved { action_taken } => write!(f, "Resolved: {}", action_taken),
            ResolutionStatus::Escalated { to } => write!(f, "Escalated to {}", to),
            ResolutionStatus::Dismissed { reason } => write!(f, "Dismissed: {}", reason),
        }
    }
}

impl fmt::Display for ReviewDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReviewDecision::Approve => write!(f, "Approve"),
            ReviewDecision::Reject => write!(f, "Reject"),
            ReviewDecision::Quarantine => write!(f, "Quarantine"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualReviewTracker {
    cases: Vec<ManualReviewCase>,
    next_case_id: u32,
    escalation_contact: String,
}

impl ManualReviewTracker {
    pub fn new(escalation_contact: &str) -> Self {
        Self {
            cases: Vec::new(),
            next_case_id: 1,
            escalation_contact: escalation_contact.to_string(),
        }
    }

    pub fn flag_validation_failure(
        &mut self,
        record_index: usize,
        field: &str,
        expected: &str,
        actual: &str,
        severity: ReviewSeverity,
        suggested_action: &str,
    ) -> u32 {
        let case = ManualReviewCase {
            case_id: self.next_case_id,
            record_index,
            field: field.to_string(),
            issue_type: "Validation Failure".to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
            related_record_index: None,
            severity: severity.clone(),
            auto_flagged: true,
            detected_at: Utc::now(),
            suggested_action: suggested_action.to_string(),
            resolution: ResolutionStatus::Pending,
            root_cause: None,
            decision: None,
            decision_reason: None,
            approver: None,
            approved_at: None,
            quarantine_recommended: matches!(severity, ReviewSeverity::Critical),
            confidence_score: None,
            resolved_by: None,
            resolved_at: None,
            notes: None,
        };

        self.cases.push(case);
        let id = self.next_case_id;
        self.next_case_id += 1;
        id
    }

    pub fn flag_duplicate(
        &mut self,
        record_index: usize,
        field: &str,
        duplicate_value: &str,
        existing_record: usize,
        existing_date: &str,
    ) -> u32 {
        let case = ManualReviewCase {
            case_id: self.next_case_id,
            record_index,
            field: field.to_string(),
            issue_type: "Duplicate Detection".to_string(),
            expected: format!("Unique {}", field),
            actual: format!("{} (duplicate of record #{})", duplicate_value, existing_record),
            related_record_index: Some(existing_record),
            severity: ReviewSeverity::Critical,
            auto_flagged: true,
            detected_at: Utc::now(),
            suggested_action: "Merge records or remove duplicate".to_string(),
            resolution: ResolutionStatus::Pending,
            root_cause: Some(
                "Kemungkinan duplikasi ingest upstream atau idempotency key tidak konsisten"
                    .to_string(),
            ),
            decision: None,
            decision_reason: None,
            approver: None,
            approved_at: None,
            quarantine_recommended: true,
            confidence_score: None,
            resolved_by: None,
            resolved_at: None,
            notes: Some(format!("Existing record from {}", existing_date)),
        };

        self.cases.push(case);
        let id = self.next_case_id;
        self.next_case_id += 1;
        id
    }

    pub fn flag_temporal_anomaly(
        &mut self,
        record_index: usize,
        field: &str,
        value: &str,
        cutoff_date: &str,
        anomaly_type: &str,
    ) -> u32 {
        let case = ManualReviewCase {
            case_id: self.next_case_id,
            record_index,
            field: field.to_string(),
            issue_type: format!("Temporal Anomaly: {}", anomaly_type),
            expected: format!("Date <= {}", cutoff_date),
            actual: value.to_string(),
            related_record_index: None,
            severity: ReviewSeverity::Critical,
            auto_flagged: true,
            detected_at: Utc::now(),
            suggested_action: "Verify date accuracy or quarantine record".to_string(),
            resolution: ResolutionStatus::Pending,
            root_cause: Some(
                "Kemungkinan perbedaan timezone, data uji ikut masuk produksi, atau clock source tidak sinkron"
                    .to_string(),
            ),
            decision: None,
            decision_reason: None,
            approver: None,
            approved_at: None,
            quarantine_recommended: true,
            confidence_score: None,
            resolved_by: None,
            resolved_at: None,
            notes: None,
        };

        self.cases.push(case);
        let id = self.next_case_id;
        self.next_case_id += 1;
        id
    }

    pub fn flag_low_confidence_imputation(
        &mut self,
        record_index: usize,
        field: &str,
        imputed_value: &str,
        confidence: f32,
        threshold: f32,
    ) -> u32 {
        let case = ManualReviewCase {
            case_id: self.next_case_id,
            record_index,
            field: field.to_string(),
            issue_type: "Low Confidence Imputation".to_string(),
            expected: format!("Confidence >= {}", threshold),
            actual: format!("Confidence = {:.2}", confidence),
            related_record_index: None,
            severity: ReviewSeverity::Minor,
            auto_flagged: true,
            detected_at: Utc::now(),
            suggested_action: format!(
                "Review imputed value '{}' or collect actual data",
                imputed_value
            ),
            resolution: ResolutionStatus::Pending,
            root_cause: Some(
                "Kepadatan data sekitar rendah sehingga estimasi imputasi kurang andal".to_string(),
            ),
            decision: None,
            decision_reason: None,
            approver: None,
            approved_at: None,
            quarantine_recommended: confidence < threshold,
            confidence_score: Some(confidence),
            resolved_by: None,
            resolved_at: None,
            notes: Some(format!("Imputed value: {}", imputed_value)),
        };

        self.cases.push(case);
        let id = self.next_case_id;
        self.next_case_id += 1;
        id
    }

    pub fn set_root_cause(&mut self, case_id: u32, cause: &str) -> bool {
        if let Some(case) = self.cases.iter_mut().find(|c| c.case_id == case_id) {
            case.root_cause = Some(cause.to_string());
            return true;
        }
        false
    }

    pub fn apply_decision(
        &mut self,
        case_id: u32,
        decision: ReviewDecision,
        approver: &str,
        reason: &str,
    ) -> bool {
        if let Some(case) = self.cases.iter_mut().find(|c| c.case_id == case_id) {
            case.decision = Some(decision.clone());
            case.decision_reason = Some(reason.to_string());
            case.approver = Some(approver.to_string());
            case.approved_at = Some(Utc::now());
            case.resolution = match decision {
                ReviewDecision::Approve => ResolutionStatus::Resolved {
                    action_taken: "Approved with correction".to_string(),
                },
                ReviewDecision::Reject => ResolutionStatus::Resolved {
                    action_taken: "Rejected or removed from accepted set".to_string(),
                },
                ReviewDecision::Quarantine => ResolutionStatus::Escalated {
                    to: "Quarantine workflow".to_string(),
                },
            };
            return true;
        }
        false
    }

    pub fn quarantine_candidates(&self, min_confidence: f32) -> Vec<&ManualReviewCase> {
        self.cases
            .iter()
            .filter(|c| {
                c.quarantine_recommended
                    || matches!(c.severity, ReviewSeverity::Critical)
                    || c.confidence_score
                        .map(|value| value < min_confidence)
                        .unwrap_or(false)
            })
            .collect()
    }

    pub fn display_queue(&self) -> String {
        if self.cases.is_empty() {
            return "  ✅ Tidak ada tinjauan manual yang diperlukan\n".to_string();
        }

        let mut lines = Vec::new();

        let pending = self
            .cases
            .iter()
            .filter(|c| matches!(c.resolution, ResolutionStatus::Pending))
            .count();
        let resolved = self
            .cases
            .iter()
            .filter(|c| matches!(c.resolution, ResolutionStatus::Resolved { .. }))
            .count();
        let total = self.cases.len();

        lines.push(format!(
            "  Total Ditandai: {} kasus | Terselesaikan: {} | Tertunda: {}",
            total, resolved, pending
        ));
        lines.push("".to_string());

        for case in &self.cases {
            lines.push(format!("  Kasus #{}: Baris #{}", case.case_id, case.record_index));
            lines.push(format!("  ├── Kolom: {}", case.field));
            lines.push(format!("  ├── Masalah: {}", case.issue_type));
            lines.push(format!("  ├── Seharusnya: {}", case.expected));
            lines.push(format!("  ├── Aktual: {}", case.actual));
            if let Some(related_idx) = case.related_record_index {
                lines.push(format!("  ├── Rekaman Terkait: Baris #{}", related_idx));
            }

            if let Some(ref notes) = case.notes {
                lines.push(format!("  ├── Catatan: {}", notes));
            }

            lines.push(format!("  ├── Tingkat Keparahan: {}", case.severity));
            lines.push(format!("  ├── Ditandai Otomatis: {}", if case.auto_flagged { "ya" } else { "tidak" }));
            if let Some(ref cause) = case.root_cause {
                lines.push(format!("  ├── Akar Masalah: {}", cause));
            }
            if let Some(ref decision) = case.decision {
                lines.push(format!("  ├── Keputusan: {}", decision));
            }
            if let Some(ref approver) = case.approver {
                lines.push(format!("  ├── Penyetuju: {}", approver));
            }
            lines.push(format!("  ├── Tindakan Disarankan: {}", case.suggested_action));
            lines.push(format!("  └── Status: {}", case.resolution));
            lines.push("".to_string());
        }

        if pending > 0 {
            lines.push(format!(
                "  ⚠️  Eskalasi: {} (pemberitahuan otomatis tertunda)",
                self.escalation_contact
            ));
        }

        lines.join("\n")
    }

    pub fn export_json(&self) -> String {
        let payload: Vec<serde_json::Value> = self
            .cases
            .iter()
            .map(|case| {
                serde_json::json!({
                    "case_id": case.case_id,
                    "record_index": case.record_index,
                    "field": case.field,
                    "issue_type": case.issue_type,
                    "expected": case.expected,
                    "actual": case.actual,
                    "related_record_index": case.related_record_index,
                    "severity": case.severity.to_string(),
                    "auto_flagged": case.auto_flagged,
                    "detected_at": case.detected_at,
                    "suggested_action": case.suggested_action,
                    "resolution": case.resolution.to_string(),
                    "root_cause": case.root_cause,
                    "decision": case.decision.as_ref().map(|v| v.to_string()),
                    "decision_reason": case.decision_reason,
                    "approver": case.approver,
                    "approved_at": case.approved_at,
                    "quarantine_recommended": case.quarantine_recommended,
                    "confidence_score": case.confidence_score,
                    "resolved_by": case.resolved_by,
                    "resolved_at": case.resolved_at,
                    "notes": case.notes,
                })
            })
            .collect();

        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "[]".to_string())
    }

    pub fn get_cases(&self) -> &[ManualReviewCase] {
        &self.cases
    }

    pub fn len(&self) -> usize {
        self.cases.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cases.is_empty()
    }
}
