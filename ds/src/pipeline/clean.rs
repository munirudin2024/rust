use anyhow::Result;
use polars::prelude::{BooleanChunked, DataFrame, NamedFrom, NewChunkedArray, Series};
use std::collections::HashMap;

use crate::domain::utils::{
	anyvalue_to_bool, anyvalue_to_plain_string, find_col_by_keywords,
	normalize_signature_piece,
};

pub struct DedupResult {
	pub df:            DataFrame,
	pub dropped:       Option<DataFrame>,
	pub dropped_count: usize,
}

pub fn deduplicate_transaction_ids(df: DataFrame) -> Result<DedupResult> {
	let id_col = find_col_by_keywords(&df, &["id_transaksi", "transaction_id", "trx"]);
	let Some(id_name) = id_col else {
		return Ok(DedupResult { df, dropped: None, dropped_count: 0 });
	};

	let id_series  = df.column(&id_name)?;
	let col_names  = df.get_column_names();
	let mut best_by_id:  HashMap<String, (usize, i64)> = HashMap::new();
	let mut rows_by_id:  HashMap<String, Vec<usize>>   = HashMap::new();

	for i in 0..df.height() {
		let id = id_series.get(i).ok()
			.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
			.map(anyvalue_to_plain_string)
			.map(|s| s.trim().to_string())
			.filter(|s| !s.is_empty());
		let Some(id) = id else { continue; };

		let score: i64 = col_names.iter().filter_map(|col| {
			df.column(col).ok()?.get(i).ok().map(|v| {
				let is_null = matches!(v, polars::prelude::AnyValue::Null);
				let txt = anyvalue_to_plain_string(v);
				if !is_null
					&& !txt.trim().is_empty()
					&& txt.trim() != "unknown" { 1 } else { 0 }
			})
		}).sum();

		rows_by_id.entry(id.clone()).or_default().push(i);
		let entry = best_by_id.entry(id).or_insert((i, score));
		if score > entry.1 { *entry = (i, score); }
	}

	let mut keep = vec![true; df.height()];
	let mut dropped_count = 0usize;
	for (id, rows) in &rows_by_id {
		if rows.len() <= 1 { continue; }
		if let Some((best_idx, _)) = best_by_id.get(id) {
			for idx in rows {
				if idx != best_idx { keep[*idx] = false; dropped_count += 1; }
			}
		}
	}

	if dropped_count == 0 {
		return Ok(DedupResult { df, dropped: None, dropped_count: 0 });
	}

	let keep_mask = BooleanChunked::from_iter_values("keep_dedup".into(), keep.iter().copied());
	let drop_mask = BooleanChunked::from_iter_values("drop_dedup".into(), keep.iter().map(|v| !v));
	Ok(DedupResult {
		dropped:       Some(df.filter(&drop_mask)?),
		df:            df.filter(&keep_mask)?,
		dropped_count,
	})
}

pub fn deduplicate_cross_id(df: DataFrame) -> Result<DedupResult> {
	let id_col = find_col_by_keywords(&df, &["id_transaksi", "transaction_id", "trx"]);
	let Some(id_name) = id_col else {
		return Ok(DedupResult { df, dropped: None, dropped_count: 0 });
	};

	let id_series  = df.column(&id_name)?;
	let col_names  = df.get_column_names();

	let skip: &[&str] = &[
		"cleaned_at", "retention_count", "revenue_per_transaction",
		"qty_ekstrem", "rating_tidak_valid", "revenue_anomali",
		"duplikat_id_transaksi", "duplikat_id_berbeda",
		"tanggal_diluar_range", "perlu_review_manual",
	];

	let sig_cols: Vec<String> = col_names.iter().filter_map(|n| {
		let low = n.to_ascii_lowercase();
		if *n == id_name.as_str()
			|| low.starts_with("is_outlier_")
			|| low.starts_with("outlier_flag_")
			|| skip.contains(&low.as_str()) { None }
		else { Some((*n).to_string()) }
	}).collect();

	let mut best_by_sig: HashMap<String, (usize, i64)>        = HashMap::new();
	let mut rows_by_sig: HashMap<String, Vec<(usize, String)>> = HashMap::new();

	for i in 0..df.height() {
		let id = id_series.get(i).ok()
			.filter(|v| !matches!(v, polars::prelude::AnyValue::Null))
			.map(anyvalue_to_plain_string)
			.map(|s| s.trim().to_string())
			.filter(|s| !s.is_empty());
		let Some(id) = id else { continue; };

		let sig = sig_cols.iter().map(|cn| {
			let raw = df.column(cn).ok()
				.and_then(|s| s.get(i).ok())
				.map(anyvalue_to_plain_string)
				.unwrap_or_default();
			normalize_signature_piece(&raw)
		}).collect::<Vec<_>>().join("|");

		let score: i64 = col_names.iter().filter_map(|col| {
			df.column(col).ok()?.get(i).ok().map(|v| {
				let is_null = matches!(v, polars::prelude::AnyValue::Null);
				let txt = anyvalue_to_plain_string(v);
				if !is_null
					&& !txt.trim().is_empty()
					&& txt.trim() != "unknown" { 1 } else { 0 }
			})
		}).sum();

		rows_by_sig.entry(sig.clone()).or_default().push((i, id));
		let entry = best_by_sig.entry(sig).or_insert((i, score));
		if score > entry.1 { *entry = (i, score); }
	}

	let mut keep = vec![true; df.height()];
	let mut dropped_count = 0usize;
	for (sig, rows) in &rows_by_sig {
		let distinct_ids: std::collections::HashSet<&String> =
			rows.iter().map(|(_, id)| id).collect();
		if distinct_ids.len() <= 1 { continue; }
		if let Some((best_idx, _)) = best_by_sig.get(sig) {
			for (idx, _) in rows {
				if idx != best_idx { keep[*idx] = false; dropped_count += 1; }
			}
		}
	}

	if dropped_count == 0 {
		return Ok(DedupResult { df, dropped: None, dropped_count: 0 });
	}

	let keep_mask = BooleanChunked::from_iter_values("keep_cross".into(), keep.iter().copied());
	let drop_mask = BooleanChunked::from_iter_values("drop_cross".into(), keep.iter().map(|v| !v));
	Ok(DedupResult {
		dropped:       Some(df.filter(&drop_mask)?),
		df:            df.filter(&keep_mask)?,
		dropped_count,
	})
}

pub fn merge_dropped(
	left:  Option<DataFrame>,
	right: Option<DataFrame>,
) -> Result<Option<DataFrame>> {
	match (left, right) {
		(None, None)         => Ok(None),
		(Some(df), None)
		| (None, Some(df))   => Ok(Some(df)),
		(Some(mut a), Some(b)) => { a.vstack_mut(&b)?; Ok(Some(a)) }
	}
}

pub fn refresh_post_dedup_flags(df: &mut DataFrame) -> Result<()> {
	let has_dup_id = df
		.get_column_names()
		.iter()
		.any(|c| *c == "Duplikat_ID_Transaksi");
	let has_dup_cross = df
		.get_column_names()
		.iter()
		.any(|c| *c == "Duplikat_ID_Berbeda");
	let has_review = df
		.get_column_names()
		.iter()
		.any(|c| *c == "Perlu_Review_Manual");

	if has_dup_id {
		df.with_column(Series::new("Duplikat_ID_Transaksi".into(), vec![Some(false); df.height()]))?;
	}
	if has_dup_cross {
		df.with_column(Series::new("Duplikat_ID_Berbeda".into(), vec![Some(false); df.height()]))?;
	}

	if has_review {
		let risk: Vec<Option<bool>> = (0..df.height()).map(|i| {
			let bad = [
				"Qty_Ekstrem",
				"Revenue_Anomali",
				"Duplikat_ID_Berbeda",
				"Harga_Satuan_Kosong_Awal",
				"Low_Confidence_Imputation",
				"MISSING_VERIFIED",
			]
				.iter()
				.any(|cn| df.column(cn).ok()
					.and_then(|s| s.get(i).ok())
					.map(anyvalue_to_bool)
					.unwrap_or(false));
			Some(bad)
		}).collect();
		df.with_column(Series::new("Perlu_Review_Manual".into(), risk))?;
	}
	Ok(())
}

pub fn apply_hard_reject(
	df: DataFrame,
) -> Result<(DataFrame, Option<DataFrame>)> {
	if !df.get_column_names().iter().any(|c| *c == "Perlu_Review_Manual") {
		return Ok((df, None));
	}

	let review = df.column("Perlu_Review_Manual")?
		.cast(&polars::prelude::DataType::Boolean)?;
	let ca = review.bool()?;

	let reject: Vec<bool> = ca.into_iter().map(|v| v.unwrap_or(false)).collect();
	let keep:   Vec<bool> = reject.iter().map(|v| !v).collect();

	let reject_mask = BooleanChunked::from_iter_values("reject_mask".into(), reject.iter().copied());
	let keep_mask   = BooleanChunked::from_iter_values("keep_mask".into(),   keep.iter().copied());

	let filtered = df.filter(&keep_mask)?;
	let mut quarantined = df.filter(&reject_mask)?;

	if quarantined.height() > 0 {
		enrich_quarantine_reason(&mut quarantined)?;
	}

	if quarantined.height() == 0 { Ok((filtered, None)) }
	else { Ok((filtered, Some(quarantined))) }
}

pub fn enrich_quarantine_reason(df: &mut DataFrame) -> Result<()> {
	let reason_map: &[(&str, &str, i64)] = &[
		("Qty_Ekstrem",              "QTY_EKSTREM",              3),
		("Revenue_Anomali",          "REVENUE_ANOMALI",          3),
		("Duplikat_ID_Berbeda",      "DUPLIKAT_ID_BERBEDA",      3),
		("Price_Outlier_IQR",        "PRICE_OUTLIER_IQR",        2),
		("Rating_Tidak_Valid",       "RATING_TIDAK_VALID",       2),
		("Harga_Satuan_Kosong_Awal", "HARGA_SATUAN_KOSONG",      2),
		("Tanggal_DiLuar_Range",     "TANGGAL_DILUAR_RANGE",     1),
		("Qty_Nol",                  "QTY_NOL",                  1),
	];

	let available: Vec<(&str, &str, i64)> = reason_map.iter()
		.filter(|(col, _, _)| df.get_column_names().iter().any(|c| *c == *col))
		.copied()
		.collect();

	if available.is_empty() { return Ok(()); }

	let mut reason_text:  Vec<Option<String>> = Vec::with_capacity(df.height());
	let mut reason_count: Vec<Option<i64>>    = Vec::with_capacity(df.height());
	let mut severity_txt: Vec<Option<String>> = Vec::with_capacity(df.height());
	let mut severity_scr: Vec<Option<i64>>    = Vec::with_capacity(df.height());

	for i in 0..df.height() {
		let mut reasons = Vec::new();
		let mut max_sev = 1i64;

		for (col, label, sev) in &available {
			if df.column(col).ok()
				.and_then(|s| s.get(i).ok())
				.map(anyvalue_to_bool)
				.unwrap_or(false)
			{
				reasons.push((*label).to_string());
				if *sev > max_sev { max_sev = *sev; }
			}
		}

		if reasons.is_empty() {
			reason_text.push(Some("PERLU_REVIEW_MANUAL".into()));
			reason_count.push(Some(1));
			severity_txt.push(Some("LOW".into()));
			severity_scr.push(Some(1));
		} else {
			let sev_label = match max_sev { 3 => "HIGH", 2 => "MEDIUM", _ => "LOW" };
			reason_count.push(Some(reasons.len() as i64));
			reason_text.push(Some(reasons.join("|")));
			severity_txt.push(Some(sev_label.into()));
			severity_scr.push(Some(max_sev));
		}
	}

	df.with_column(Series::new("Alasan_Karantina".into(),          reason_text))?;
	df.with_column(Series::new("Jumlah_Alasan_Karantina".into(),   reason_count))?;
	df.with_column(Series::new("Severity_Karantina".into(),        severity_txt))?;
	df.with_column(Series::new("Skor_Severity_Karantina".into(),   severity_scr))?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_merge_dropped_both_none() {
		let result = merge_dropped(None, None).unwrap();
		assert!(result.is_none());
	}
}
