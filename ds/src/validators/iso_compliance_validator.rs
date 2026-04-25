use chrono::{NaiveDate, NaiveDateTime, Utc};
use polars::prelude::{AnyValue, DataFrame, NamedFrom, Series};
use serde::Serialize;
use std::collections::HashMap;
use thiserror::Error;

use crate::domain::utils::anyvalue_to_plain_string;
use crate::iso_standards::iso8000::{DataRequirementSpec, FieldSpec};

#[derive(Debug, Clone)]
pub struct Record {
    pub row_idx: usize,
    pub values: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Error)]
pub enum ValidationError {
    #[error("missing required field: {0}")]
    MissingRequired(String),
    #[error("invalid timestamp format")]
    InvalidTimestamp,
    #[error("numeric format error on field: {0}")]
    NumericFormat(String),
    #[error("string length overflow on field: {0}")]
    StringLength(String),
    #[error("out of range on field: {0}")]
    OutOfRange(String),
    #[error("invalid enum value on field: {0}")]
    InvalidEnum(String),
    #[error("future date detected")]
    FutureDate,
    #[error("temporal range violation: {0}")]
    TemporalRangeViolation(String),
    #[error("cross field violation: {0}")]
    CrossFieldViolation(String),
}

#[derive(Debug, Clone)]
pub struct ValidationRun {
    pub quality_flags: Vec<String>,
    pub invalid_syntax_indices: Vec<usize>,
    pub invalid_semantic_indices: Vec<usize>,
    pub invalid_manual_review_indices: Vec<usize>,
    pub syntactic_error_count: usize,
    pub semantic_error_count: usize,
}

pub fn validate_syntactic_quality(record: &Record) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    if parse_record_timestamp(record).is_none() {
        errors.push(ValidationError::InvalidTimestamp);
    }

    for (k, v) in &record.values {
        // String di Rust sudah UTF-8, tapi kita cek karakter replacement sebagai sinyal rusak.
        if v.contains('\u{FFFD}') {
            errors.push(ValidationError::StringLength(k.clone()));
        }

        if v.len() > 255 {
            errors.push(ValidationError::StringLength(k.clone()));
        }

        let looks_numeric = [
            "No", "year", "month", "day", "hour", "PM2.5", "PM10", "SO2", "NO2", "CO", "O3",
            "TEMP", "PRES", "DEWP", "RAIN", "WSPM",
        ]
        .contains(&k.as_str());

        if looks_numeric && !v.trim().is_empty() {
            if v.contains(',') {
                errors.push(ValidationError::NumericFormat(k.clone()));
            } else if v.parse::<f64>().is_err() {
                errors.push(ValidationError::NumericFormat(k.clone()));
            }
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

pub fn validate_semantic_quality(
    record: &Record,
    spec: &DataRequirementSpec,
) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    for (name, field) in &spec.fields {
        let raw = record.values.get(name).map(|s| s.trim()).unwrap_or("");

        if field.required && raw.is_empty() {
            errors.push(ValidationError::MissingRequired(name.clone()));
            continue;
        }

        if raw.is_empty() {
            continue;
        }

        validate_field_range(name, raw, field, &mut errors);
        validate_field_enum(name, raw, field, &mut errors);
    }

    if let Some(ts) = parse_record_timestamp(record) {
        let timezone = "UTC";
        let now = Utc::now().naive_utc();
        let upper_bound = now + chrono::Duration::days(1);
        let lower_bound = NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("valid lower-bound date")
            .and_hms_opt(0, 0, 0)
            .expect("valid lower-bound time");

        if ts < lower_bound || ts > upper_bound {
            errors.push(ValidationError::TemporalRangeViolation(format!(
                "timestamp={} outside [2000-01-01T00:00:00, {}] timezone={}",
                ts,
                upper_bound,
                timezone
            )));
        }

        if ts > now {
            errors.push(ValidationError::FutureDate);
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

pub fn validate_cross_field(record: &Record) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    let rain = record.values.get("RAIN").and_then(|v| v.parse::<f64>().ok());
    let dewp = record.values.get("DEWP").and_then(|v| v.parse::<f64>().ok());
    let temp = record.values.get("TEMP").and_then(|v| v.parse::<f64>().ok());
    let pm25 = record.values.get("PM2.5").and_then(|v| v.parse::<f64>().ok());
    let pm10 = record.values.get("PM10").and_then(|v| v.parse::<f64>().ok());
    let co = record.values.get("CO").and_then(|v| v.parse::<f64>().ok());
    let no2 = record.values.get("NO2").and_then(|v| v.parse::<f64>().ok());

    if let (Some(r), Some(d), Some(t)) = (rain, dewp, temp) {
        if r > 0.0 && d <= t - 5.0 {
            errors.push(ValidationError::CrossFieldViolation(
                "RAIN > 0 requires DEWP > TEMP - 5".to_string(),
            ));
        }
    }

    if let (Some(t), Some(r)) = (temp, rain) {
        if t < 0.0 && r > 0.0 {
            errors.push(ValidationError::CrossFieldViolation(
                "TEMP < 0 requires RAIN == 0 or null".to_string(),
            ));
        }
    }

    if let (Some(a), Some(b)) = (pm25, pm10) {
        if a > 300.0 && b < a {
            errors.push(ValidationError::CrossFieldViolation(
                "PM2.5 > 300 and PM10 < PM2.5 is physically implausible".to_string(),
            ));
        }
    }

    if let (Some(c), Some(n)) = (co, no2) {
        if c > 5.0 && n < 10.0 {
            errors.push(ValidationError::CrossFieldViolation(
                "CO > 5 with NO2 < 10 requires manual review".to_string(),
            ));
        }
    }

    if let (Some(t), Some(r)) = (temp, rain) {
        if t > 35.0 && r > 0.0 {
            errors.push(ValidationError::CrossFieldViolation(
                "TEMP > 35 with RAIN > 0 requires manual review".to_string(),
            ));
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

pub fn validate_dataframe_iso(df: &DataFrame, spec: &DataRequirementSpec) -> ValidationRun {
    let mut quality_flags = Vec::with_capacity(df.height());
    let mut invalid_syntax_indices = Vec::new();
    let mut invalid_semantic_indices = Vec::new();
    let mut invalid_manual_review_indices = Vec::new();
    let mut syntactic_error_count = 0usize;
    let mut semantic_error_count = 0usize;

    let mut prev_ts: Option<NaiveDateTime> = None;

    for row_idx in 0..df.height() {
        let record = to_record(df, row_idx);
        let syntax_errs = validate_syntactic_quality(&record).err().unwrap_or_default();
        let semantic_errs = validate_semantic_quality(&record, spec).err().unwrap_or_default();
        let cross_errs = validate_cross_field(&record).err().unwrap_or_default();

        let ts_now = parse_record_timestamp(&record);
        let sequential_err = match (prev_ts, ts_now) {
            (Some(prev), Some(curr)) if curr < prev => true,
            (_, Some(curr)) => {
                prev_ts = Some(curr);
                false
            }
            _ => false,
        };

        let has_syntax = !syntax_errs.is_empty();
        let has_semantic = !semantic_errs.is_empty() || sequential_err;
        let has_manual = !cross_errs.is_empty();

        if has_syntax {
            syntactic_error_count += syntax_errs.len();
            invalid_syntax_indices.push(row_idx);
            quality_flags.push("syntactic_error".to_string());
            continue;
        }

        if has_semantic {
            semantic_error_count += semantic_errs.len() + usize::from(sequential_err);
            invalid_semantic_indices.push(row_idx);
            quality_flags.push("semantic_error".to_string());
            continue;
        }

        if has_manual {
            invalid_manual_review_indices.push(row_idx);
            quality_flags.push("manual_review".to_string());
            continue;
        }

        quality_flags.push("valid".to_string());
    }

    ValidationRun {
        quality_flags,
        invalid_syntax_indices,
        invalid_semantic_indices,
        invalid_manual_review_indices,
        syntactic_error_count,
        semantic_error_count,
    }
}

pub fn filter_rows_by_indices(df: &DataFrame, idxs: &[usize]) -> DataFrame {
    if idxs.is_empty() {
        return DataFrame::default();
    }

    let mut mask = vec![false; df.height()];
    for idx in idxs {
        if *idx < mask.len() {
            mask[*idx] = true;
        }
    }

    let mask_series = Series::new("iso_filter".into(), mask);
    let mask_chunked = mask_series.bool().expect("bool mask conversion should not fail");
    df.filter(mask_chunked).unwrap_or_else(|_| DataFrame::default())
}

fn validate_field_range(name: &str, raw: &str, field: &FieldSpec, errors: &mut Vec<ValidationError>) {
    if let Some([min, max]) = field.range {
        if let Ok(v) = raw.parse::<f64>() {
            if v < min || v > max {
                errors.push(ValidationError::OutOfRange(name.to_string()));
            }
        }
    }
}

fn validate_field_enum(name: &str, raw: &str, field: &FieldSpec, errors: &mut Vec<ValidationError>) {
    if let Some(values) = &field.allowed_values {
        if !values.iter().any(|x| x == raw) {
            errors.push(ValidationError::InvalidEnum(name.to_string()));
        }
    }
}

fn parse_record_timestamp(record: &Record) -> Option<NaiveDateTime> {
    let year = record.values.get("year")?.parse::<i32>().ok()?;
    let month = record.values.get("month")?.parse::<u32>().ok()?;
    let day = record.values.get("day")?.parse::<u32>().ok()?;
    let hour = record.values.get("hour")?.parse::<u32>().ok()?;

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    date.and_hms_opt(hour, 0, 0)
}

fn to_record(df: &DataFrame, row_idx: usize) -> Record {
    let mut values = HashMap::new();
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) {
            let value = series
                .get(row_idx)
                .map(anyvalue_to_plain_string)
                .unwrap_or_else(|_| String::new());
            values.insert(name.to_string(), clean_anyvalue_like_string(&value));
        }
    }
    Record { row_idx, values }
}

fn clean_anyvalue_like_string(raw: &str) -> String {
    if raw == format!("{}", AnyValue::Null) {
        String::new()
    } else {
        raw.trim_matches('"').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_syntactic_quality_timestamp() {
        let mut values = HashMap::new();
        values.insert("year".to_string(), "2024".to_string());
        values.insert("month".to_string(), "13".to_string());
        values.insert("day".to_string(), "10".to_string());
        values.insert("hour".to_string(), "12".to_string());
        let record = Record { row_idx: 0, values };
        assert!(validate_syntactic_quality(&record).is_err());
    }
}
