use anyhow::Result;
use polars::prelude::DataFrame;
use std::collections::HashSet;

use crate::domain::kpi::KpiDimension;
use crate::domain::utils::{
	anyvalue_to_f64, anyvalue_to_plain_string, flag_at, find_col_by_keywords,
	parse_trx_suffix,
};
use crate::domain::validation::{
	collect_conflicting_customer_ids, has_accuracy_flag, has_cross_id_duplicate,
	has_customer_name_abbreviation, has_revenue_formula_mismatch,
	is_noncanonical_payment_value, needs_manual_review,
};

#[derive(Debug, Clone)]
pub struct ScoreSummary {
	pub overall_score: f64,
	pub weighted_score: f64,
	pub dimensions: usize,
}

pub fn calculate_score(total_checked: usize, passed: usize) -> f64 {
	if total_checked == 0 {
		100.0
	} else {
		(passed as f64 * 100.0) / total_checked as f64
	}
}

pub fn aggregate_scores(dimensions: &[KpiDimension]) -> ScoreSummary {
	if dimensions.is_empty() {
		return ScoreSummary {
			overall_score: 100.0,
			weighted_score: 100.0,
			dimensions: 0,
		};
	}

	let overall_score =
		dimensions.iter().map(|d| d.score).sum::<f64>() / dimensions.len() as f64;

	let total_checked: usize = dimensions.iter().map(|d| d.total_checked).sum();
	let weighted_score = if total_checked == 0 {
		100.0
	} else {
		let passed_total: usize = dimensions.iter().map(|d| d.passed).sum();
		calculate_score(total_checked, passed_total)
	};

	ScoreSummary {
		overall_score,
		weighted_score,
		dimensions: dimensions.len(),
	}
}

pub struct ConsistencyEval {
	pub passed: usize,
	pub notes: String,
}

pub struct AccuracyEval {
	pub passed: usize,
	pub notes: String,
}

pub struct IntegrityEval {
	pub passed: usize,
	pub notes: String,
}

pub fn evaluate_consistency(df: &DataFrame, row_count: usize) -> Result<ConsistencyEval> {
	let mut consistency_issue_rows: HashSet<usize> = (0..row_count)
		.filter(|i| has_cross_id_duplicate(df, *i))
		.collect();

	if let Some(customer_col) =
		find_col_by_keywords(df, &["nama_konsumen", "customer", "konsumen", "pelanggan"])
	{
		let cs = df.column(&customer_col)?;
		for i in 0..row_count {
			let name = cs.get(i).ok().map(anyvalue_to_plain_string).unwrap_or_default();
			if has_customer_name_abbreviation(&name) {
				consistency_issue_rows.insert(i);
			}
		}

		if df.get_column_names().iter().any(|c| *c == "Customer_ID") {
			let cid = df.column("Customer_ID")?;
			let conflicts = collect_conflicting_customer_ids(df, &customer_col, "Customer_ID");
			if !conflicts.is_empty() {
				for i in 0..row_count {
					let id = cid.get(i).ok().map(anyvalue_to_plain_string).unwrap_or_default();
					if conflicts.contains(&id) {
						consistency_issue_rows.insert(i);
					}
				}
			}
		}
	}

	let mut payment_noncanonical = 0usize;
	if let Some(payment_col) =
		find_col_by_keywords(df, &["pembayaran", "payment", "metode"])
	{
		let ps = df.column(&payment_col)?;
		for i in 0..row_count {
			let val = ps.get(i).ok().map(anyvalue_to_plain_string).unwrap_or_default();
			if is_noncanonical_payment_value(&val) {
				consistency_issue_rows.insert(i);
				payment_noncanonical += 1;
			}
		}
	}

	let cross_dup = (0..row_count)
		.filter(|i| has_cross_id_duplicate(df, *i))
		.count();
	let notes = format!(
		"cross_id_dup_rows: {}; customer_entity_issue_rows: {}; payment_noncanonical_rows: {}",
		cross_dup,
		consistency_issue_rows.len().saturating_sub(cross_dup),
		payment_noncanonical,
	);

	Ok(ConsistencyEval {
		passed: row_count.saturating_sub(consistency_issue_rows.len()),
		notes,
	})
}

pub fn evaluate_accuracy(df: &DataFrame, row_count: usize) -> Result<AccuracyEval> {
	let mut accuracy_issue_rows: HashSet<usize> = (0..row_count)
		.filter(|i| has_accuracy_flag(df, *i))
		.collect();

	let price_col = find_col_by_keywords(df, &["harga", "price"]);
	let qty_col = find_col_by_keywords(df, &["jumlah", "qty", "quantity"]);
	let discount_col = find_col_by_keywords(df, &["diskon", "discount"]);
	let revenue_col = find_col_by_keywords(df, &["revenue_per_transaction", "revenue"]);

	if let (Some(hc), Some(qc), Some(rc)) = (&price_col, &qty_col, &revenue_col) {
		let hs = df.column(hc)?;
		let qs = df.column(qc)?;
		let rs = df.column(rc)?;
		let ds = discount_col.as_ref().and_then(|cn| df.column(cn).ok());

		for i in 0..row_count {
			let h = hs.get(i).ok().and_then(anyvalue_to_f64);
			let q = qs.get(i).ok().and_then(anyvalue_to_f64);
			let r = rs.get(i).ok().and_then(anyvalue_to_f64);
			let d = ds
				.as_ref()
				.and_then(|s| s.get(i).ok())
				.and_then(anyvalue_to_f64)
				.unwrap_or(0.0);
			if has_revenue_formula_mismatch(h, q, r, Some(d)) {
				accuracy_issue_rows.insert(i);
			}
		}
	}

	let notes = format!(
		"revenue_formula_mismatch_rows: {}; revenue_anomali_rows: {}; price_outlier_rows: {}",
		accuracy_issue_rows.len(),
		(0..row_count)
			.filter(|i| flag_at(df, "Revenue_Anomali", *i))
			.count(),
		(0..row_count)
			.filter(|i| flag_at(df, "Price_Outlier_IQR", *i))
			.count(),
	);

	Ok(AccuracyEval {
		passed: row_count.saturating_sub(accuracy_issue_rows.len()),
		notes,
	})
}

pub fn evaluate_integrity(df: &DataFrame, row_count: usize) -> Result<IntegrityEval> {
	let mut integrity_issue_rows: HashSet<usize> = (0..row_count)
		.filter(|i| needs_manual_review(df, *i))
		.collect();

	let mut gap_id_count = 0usize;
	if let Some(id_col) = find_col_by_keywords(df, &["id_transaksi", "transaction_id", "trx"]) {
		let id_series = df.column(&id_col)?;
		let mut id_numbers: Vec<i64> = (0..row_count)
			.filter_map(|i| {
				id_series
					.get(i)
					.ok()
					.map(anyvalue_to_plain_string)
					.and_then(|s| parse_trx_suffix(&s))
			})
			.collect();
		id_numbers.sort_unstable();
		id_numbers.dedup();
		if let (Some(&min_id), Some(&max_id)) = (id_numbers.first(), id_numbers.last()) {
			let id_set: HashSet<i64> = id_numbers.into_iter().collect();
			gap_id_count = (min_id..=max_id).filter(|n| !id_set.contains(n)).count();
		}
	}

	let (payload_df, audit_df) = crate::output::csv_writer::split_payload_and_audit_tables(df)?;
	let mut referential_mismatch_count = 0usize;
	if let (Some(pid_col), Some(aid_col)) = (
		find_col_by_keywords(&payload_df, &["id_transaksi", "transaction_id", "trx"]),
		find_col_by_keywords(&audit_df, &["id_transaksi", "transaction_id", "trx"]),
	) {
		let pid_set: HashSet<String> = (0..payload_df.height())
			.filter_map(|i| {
				payload_df
					.column(&pid_col)
					.ok()?
					.get(i)
					.ok()
					.map(anyvalue_to_plain_string)
			})
			.filter(|s| !s.trim().is_empty())
			.collect();
		let aid_set: HashSet<String> = (0..audit_df.height())
			.filter_map(|i| {
				audit_df
					.column(&aid_col)
					.ok()?
					.get(i)
					.ok()
					.map(anyvalue_to_plain_string)
			})
			.filter(|s| !s.trim().is_empty())
			.collect();
		referential_mismatch_count = pid_set.symmetric_difference(&aid_set).count();
	}

	for i in 0..integrity_issue_rows
		.len()
		.saturating_add(gap_id_count + referential_mismatch_count)
		.min(row_count)
	{
		integrity_issue_rows.insert(i);
	}

	let notes = format!(
		"gap_id_count: {}; referential_mismatch_count: {}; manual_review_rows: {}",
		gap_id_count,
		referential_mismatch_count,
		(0..row_count)
			.filter(|i| needs_manual_review(df, *i))
			.count(),
	);

	Ok(IntegrityEval {
		passed: row_count.saturating_sub(integrity_issue_rows.len()),
		notes,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_calculate_score() {
		assert_eq!(calculate_score(0, 0), 100.0);
		assert_eq!(calculate_score(10, 8), 80.0);
	}

	#[test]
	fn test_aggregate_scores() {
		let dims = vec![
			KpiDimension {
				dimension: "Completeness".to_string(),
				score: 90.0,
				total_checked: 10,
				passed: 9,
				failed: 1,
				notes: String::new(),
			},
			KpiDimension {
				dimension: "Validity".to_string(),
				score: 70.0,
				total_checked: 10,
				passed: 7,
				failed: 3,
				notes: String::new(),
			},
		];

		let s = aggregate_scores(&dims);
		assert_eq!(s.dimensions, 2);
		assert_eq!(s.overall_score, 80.0);
		assert_eq!(s.weighted_score, 80.0);
	}
}
