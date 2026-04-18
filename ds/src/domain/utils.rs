use polars::prelude::{AnyValue, DataFrame};

pub fn anyvalue_to_f64(v: AnyValue<'_>) -> Option<f64> {
	match v {
		AnyValue::Float64(x) => Some(x),
		AnyValue::Float32(x) => Some(x as f64),
		AnyValue::Int64(x) => Some(x as f64),
		AnyValue::Int32(x) => Some(x as f64),
		AnyValue::Int16(x) => Some(x as f64),
		AnyValue::Int8(x) => Some(x as f64),
		AnyValue::UInt64(x) => Some(x as f64),
		AnyValue::UInt32(x) => Some(x as f64),
		AnyValue::UInt16(x) => Some(x as f64),
		AnyValue::UInt8(x) => Some(x as f64),
		AnyValue::String(s) => s.parse::<f64>().ok(),
		_ => anyvalue_to_plain_string(v).parse::<f64>().ok(),
	}
}

pub fn anyvalue_to_bool(v: AnyValue<'_>) -> bool {
	match v {
		AnyValue::Boolean(b) => b,
		AnyValue::String(s) => s.eq_ignore_ascii_case("true") || s == "1",
		AnyValue::UInt8(x) => x > 0,
		AnyValue::UInt16(x) => x > 0,
		AnyValue::UInt32(x) => x > 0,
		AnyValue::UInt64(x) => x > 0,
		AnyValue::Int8(x) => x > 0,
		AnyValue::Int16(x) => x > 0,
		AnyValue::Int32(x) => x > 0,
		AnyValue::Int64(x) => x > 0,
		_ => false,
	}
}

pub fn anyvalue_to_plain_string(v: AnyValue<'_>) -> String {
	match v {
		AnyValue::String(s) => s.to_string(),
		_ => v.to_string().trim_matches('"').to_string(),
	}
}

pub fn find_col_by_keywords(df: &DataFrame, words: &[&str]) -> Option<String> {
	df.get_column_names().iter().find_map(|n| {
		let low = n.to_ascii_lowercase();
		if words.iter().any(|w| low.contains(&w.to_ascii_lowercase())) {
			Some((*n).to_string())
		} else {
			None
		}
	})
}

pub fn normalize_loose_text(raw: &str) -> String {
	raw.trim()
		.to_ascii_lowercase()
		.chars()
		.filter(|c| c.is_ascii_alphanumeric() || c.is_ascii_whitespace())
		.collect::<String>()
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ")
}

pub fn normalize_signature_piece(raw: &str) -> String {
	let trimmed = raw.trim().to_ascii_lowercase();
	if trimmed.is_empty() {
		return trimmed;
	}
	if let Ok(num) = trimmed.parse::<f64>() {
		if num.is_finite() {
			if (num.fract()).abs() < 1e-9 {
				return format!("{:.0}", num);
			}
			let mut text = format!("{}", num);
			while text.contains('.') && text.ends_with('0') {
				text.pop();
			}
			if text.ends_with('.') {
				text.pop();
			}
			return text;
		}
	}
	trimmed
}

pub fn looks_like_abbreviated_name(raw: &str) -> bool {
	let n = normalize_loose_text(raw);
	if n.is_empty() {
		return false;
	}
	let parts: Vec<&str> = n.split_whitespace().collect();
	if parts.len() <= 1 {
		return false;
	}
	parts.iter().any(|p| p.len() == 1)
}

pub fn is_canonical_payment_method(raw: &str) -> bool {
	matches!(
		normalize_loose_text(raw).as_str(),
		"cod" | "transfer bank" | "e wallet" | "kartu kredit" | "paylater"
	)
}

pub fn parse_trx_suffix(raw: &str) -> Option<i64> {
	let trimmed = raw.trim();
	if trimmed.is_empty() {
		return None;
	}
	if let Some((_, right)) = trimmed.rsplit_once('-') {
		return right.trim().parse::<i64>().ok();
	}
	let digits: String = trimmed.chars().filter(|c| c.is_ascii_digit()).collect();
	if digits.is_empty() { None } else { digits.parse::<i64>().ok() }
}

pub fn median(values: &mut [f64]) -> Option<f64> {
	if values.is_empty() {
		return None;
	}
	values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
	let n = values.len();
	if n % 2 == 0 {
		Some((values[n / 2 - 1] + values[n / 2]) / 2.0)
	} else {
		Some(values[n / 2])
	}
}

pub fn row_has_non_missing_value(df: &DataFrame, col_name: &str, row_idx: usize) -> bool {
	let Ok(series) = df.column(col_name) else { return false; };
	let Ok(v) = series.get(row_idx) else { return false; };
	match v {
		AnyValue::Null => false,
		AnyValue::String(s) => {
			let t = s.trim();
			!t.is_empty() && !t.eq_ignore_ascii_case("unknown")
		}
		_ => true,
	}
}

pub fn flag_at(df: &DataFrame, col_name: &str, row_idx: usize) -> bool {
	df.column(col_name)
		.ok()
		.and_then(|s| s.get(row_idx).ok())
		.map(anyvalue_to_bool)
		.unwrap_or(false)
}

pub fn csv_escape(value: &str) -> String {
	if value.contains(',') || value.contains('"') || value.contains('\n') {
		format!("\"{}\"", value.replace('"', "\"\""))
	} else {
		value.to_string()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_trx_suffix() {
		assert_eq!(parse_trx_suffix("trx-1045"), Some(1045));
		assert_eq!(parse_trx_suffix("trx-999"), Some(999));
		assert_eq!(parse_trx_suffix(""), None);
	}

	#[test]
	fn test_csv_escape() {
		assert_eq!(csv_escape("normal"), "normal");
		assert_eq!(csv_escape("has,comma"), "\"has,comma\"");
		assert_eq!(csv_escape("has\"quote"), "\"has\"\"quote\"");
	}

	#[test]
	fn test_is_canonical_payment() {
		assert!(is_canonical_payment_method("COD"));
		assert!(is_canonical_payment_method("Transfer Bank"));
		assert!(!is_canonical_payment_method("GOPAY"));
	}

	#[test]
	fn test_median_odd() {
		let mut v = vec![3.0, 1.0, 2.0];
		assert_eq!(median(&mut v), Some(2.0));
	}

	#[test]
	fn test_median_even() {
		let mut v = vec![1.0, 2.0, 3.0, 4.0];
		assert_eq!(median(&mut v), Some(2.5));
	}

	#[test]
	fn test_looks_like_abbreviated() {
		assert!(looks_like_abbreviated_name("budi s."));
		assert!(!looks_like_abbreviated_name("budi santoso"));
	}
}
