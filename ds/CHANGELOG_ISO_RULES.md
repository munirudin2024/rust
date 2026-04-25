# Changelog ISO Rules

## 1.0.0 - 2026-04-19

- Menambahkan `config/data_requirement_spec.json` (ISO 8000-110).
- Menambahkan modul `src/validators/iso_compliance_validator.rs`.
- Menambahkan namespace `src/iso_standards/` untuk ISO 8000 dan ISO 25012.
- Menambahkan Stage 1.5 validasi ISO sebelum cleaning.
- Menambahkan output:
  - `output/validation/invalid_syntax_*.csv`
  - `output/validation/invalid_semantic_*.csv`
  - `output/validation/*_validation_report.json`
  - `output/validation/*_provenance.json`
  - `output/review/sample_manual_review_*.csv`
  - `output/feedback/validation_results_*.json`
  - `output/audit/outlier_justification_*.json`
  - `output/laporan/quality_dashboard.html`
- Menambahkan metadata currentness dan credibility ke output CSV.
- Menambahkan alert threshold syntactic < 95% dan semantic < 90%.
