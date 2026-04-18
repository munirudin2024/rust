use polars::prelude::DataFrame;
use std::collections::{HashMap, HashSet};

use crate::domain::utils::{
	anyvalue_to_plain_string, flag_at, is_canonical_payment_method,
	looks_like_abbreviated_name, normalize_loose_text, row_has_non_missing_value,
};

pub fn count_completeness_passed(df: &DataFrame, required_cols: &[String]) -> usize {
	(0..df.height())
		.filter(|i| {
			required_cols
				.iter()
				.all(|col| row_has_non_missing_value(df, col, *i))
		})
		.count()
}

pub fn count_uniqueness_passed(df: &DataFrame) -> usize {
	let row_count = df.height();
	if df
		.get_column_names()
		.iter()
		.any(|c| *c == "Duplikat_ID_Transaksi")
	{
		(0..row_count)
			.filter(|i| !flag_at(df, "Duplikat_ID_Transaksi", *i))
			.count()
	} else {
		row_count
	}
}

pub fn passes_validity(df: &DataFrame, row_idx: usize) -> bool {
	!flag_at(df, "Rating_Tidak_Valid", row_idx)
		&& !flag_at(df, "Qty_Negatif_Awal", row_idx)
		&& !flag_at(df, "Qty_Nol", row_idx)
		&& !flag_at(df, "Harga_Satuan_Kosong_Awal", row_idx)
}

pub fn has_cross_id_duplicate(df: &DataFrame, row_idx: usize) -> bool {
	flag_at(df, "Duplikat_ID_Berbeda", row_idx)
}

pub fn passes_timeliness(df: &DataFrame, row_idx: usize) -> bool {
	!flag_at(df, "Tanggal_DiLuar_Range", row_idx)
}

pub fn needs_manual_review(df: &DataFrame, row_idx: usize) -> bool {
	flag_at(df, "Perlu_Review_Manual", row_idx)
}

pub fn has_accuracy_flag(df: &DataFrame, row_idx: usize) -> bool {
	flag_at(df, "Revenue_Anomali", row_idx) || flag_at(df, "Price_Outlier_IQR", row_idx)
}

pub fn has_revenue_formula_mismatch(
	price: Option<f64>,
	qty: Option<f64>,
	revenue: Option<f64>,
	discount: Option<f64>,
) -> bool {
	match (price, qty, revenue) {
		(Some(hv), Some(qv), Some(rv)) => {
			let expected = (hv * qv) - discount.unwrap_or(0.0);
			let tolerance = expected.abs().max(1.0) * 0.01;
			(rv - expected).abs() > tolerance
		}
		_ => true,
	}
}

pub fn has_customer_name_abbreviation(name: &str) -> bool {
	looks_like_abbreviated_name(name)
}

pub fn collect_conflicting_customer_ids(
	df: &DataFrame,
	customer_col: &str,
	customer_id_col: &str,
) -> HashSet<String> {
	let Ok(name_series) = df.column(customer_col) else {
		return HashSet::new();
	};
	let Ok(id_series) = df.column(customer_id_col) else {
		return HashSet::new();
	};

	let mut names_by_cid: HashMap<String, HashSet<String>> = HashMap::new();
	for i in 0..df.height() {
		let id = id_series
			.get(i)
			.ok()
			.map(anyvalue_to_plain_string)
			.unwrap_or_default();
		let name = name_series
			.get(i)
			.ok()
			.map(anyvalue_to_plain_string)
			.unwrap_or_default();

		if id.trim().is_empty() || name.trim().is_empty() {
			continue;
		}

		names_by_cid
			.entry(id)
			.or_default()
			.insert(normalize_loose_text(&name));
	}

	names_by_cid
		.into_iter()
		.filter(|(_, variants)| variants.len() > 1)
		.map(|(cid, _)| cid)
		.collect()
}

pub fn is_noncanonical_payment_value(value: &str) -> bool {
	!value.trim().is_empty() && !is_canonical_payment_method(value)
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::prelude::{DataFrame, NamedFrom, Series};

	#[test]
	fn test_passes_validity() {
		let df = DataFrame::new(vec![
			Series::new("Rating_Tidak_Valid".into(), vec![Some(false), Some(true)]),
			Series::new("Qty_Negatif_Awal".into(), vec![Some(false), Some(false)]),
			Series::new("Qty_Nol".into(), vec![Some(false), Some(false)]),
			Series::new("Harga_Satuan_Kosong_Awal".into(), vec![Some(false), Some(false)]),
		])
		.unwrap();

		assert!(passes_validity(&df, 0));
		assert!(!passes_validity(&df, 1));
	}

	#[test]
	fn test_row_flags() {
		let df = DataFrame::new(vec![
			Series::new("Duplikat_ID_Berbeda".into(), vec![Some(false), Some(true)]),
			Series::new("Tanggal_DiLuar_Range".into(), vec![Some(false), Some(true)]),
			Series::new("Perlu_Review_Manual".into(), vec![Some(false), Some(true)]),
		])
		.unwrap();

		assert!(!has_cross_id_duplicate(&df, 0));
		assert!(has_cross_id_duplicate(&df, 1));
		assert!(passes_timeliness(&df, 0));
		assert!(!passes_timeliness(&df, 1));
		assert!(!needs_manual_review(&df, 0));
		assert!(needs_manual_review(&df, 1));
	}

	#[test]
	fn test_revenue_formula_mismatch() {
		assert!(!has_revenue_formula_mismatch(
			Some(100.0),
			Some(2.0),
			Some(200.0),
			Some(0.0)
		));
		assert!(has_revenue_formula_mismatch(
			Some(100.0),
			Some(2.0),
			Some(170.0),
			Some(0.0)
		));
		assert!(has_revenue_formula_mismatch(None, Some(2.0), Some(200.0), Some(0.0)));
	}

	#[test]
	fn test_customer_name_abbreviation() {
		assert!(has_customer_name_abbreviation("budi s."));
		assert!(!has_customer_name_abbreviation("budi santoso"));
	}

	#[test]
	fn test_noncanonical_payment_value() {
		assert!(is_noncanonical_payment_value("GOPAY"));
		assert!(!is_noncanonical_payment_value("COD"));
		assert!(!is_noncanonical_payment_value("   "));
	}

	#[test]
	fn test_collect_conflicting_customer_ids() {
		let df = DataFrame::new(vec![
			Series::new("Customer_ID".into(), vec!["c1", "c1", "c2"]),
			Series::new("nama_konsumen".into(), vec!["Budi Santoso", "budi s.", "Ani"]) 
		])
		.unwrap();

		let conflicts = collect_conflicting_customer_ids(&df, "nama_konsumen", "Customer_ID");
		assert!(conflicts.contains("c1"));
		assert!(!conflicts.contains("c2"));
	}

	#[test]
	fn test_count_completeness_passed() {
		let df = DataFrame::new(vec![
			Series::new("id_transaksi".into(), vec![Some("trx-1"), Some("trx-2")]),
			Series::new("kota".into(), vec![Some("Jakarta"), Some("")]),
		])
		.unwrap();

		let required = vec!["id_transaksi".to_string(), "kota".to_string()];
		assert_eq!(count_completeness_passed(&df, &required), 1);
	}

	#[test]
	fn test_count_uniqueness_passed() {
		let df = DataFrame::new(vec![
			Series::new("Duplikat_ID_Transaksi".into(), vec![Some(false), Some(true), Some(false)]),
		])
		.unwrap();
		assert_eq!(count_uniqueness_passed(&df), 2);
	}
}