# ISO Compliance Guide

Pipeline ini menambahkan kontrol mutu untuk ISO 8000 dan ISO/IEC 25012.

## Fitur yang Diaktifkan

- Stage 1.5 validasi ISO (`--validate-iso`)
- Output invalid syntax/semantic per dataset di `output/validation/`
- Quality flag per record: `valid`, `syntactic_error`, `semantic_error`, `manual_review`
- Metadata provenance per record dan per file JSON
- Metadata currentness: `data_freshness_hours`, `staleness_flag`, `measurement_quality_index`
- Sample review manual 1% (`--generate-sample`)
- Feedback loop template reviewer di `output/feedback/`
- Dashboard kualitas (`--quality-dashboard`)

## Perintah

```bash
cargo run -- data/PRSA_Data_20130301-20170228/*.csv --validate-iso --generate-sample --quality-dashboard
```

## Monitoring Ambang

- Alert jika syntactic validity < 95%
- Alert jika semantic validity < 90%

## Artefak Bukti Audit

- `config/data_requirement_spec.json`
- `output/validation/*_validation_report.json`
- `output/validation/*_provenance.json`
- `output/review/sample_manual_review_*.csv`
- `output/feedback/validation_results_*.json`
- `output/laporan/quality_dashboard.html`
