//! Google Sheets synchronization for Looker Studio workflows.

use anyhow::{Context, Result};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::viz::{CityRevenueStat, Observation, PaymentStat, StationSummary};

const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets";
const SHEETS_API_BASE: &str = "https://sheets.googleapis.com/v4/spreadsheets";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleSheetsUploadStatus {
    pub spreadsheet_id: String,
    pub spreadsheet_url: String,
    pub created_new_spreadsheet: bool,
    pub uploaded_at: String,
    pub tabs: Vec<String>,
    pub rows_uploaded: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: String,
}

#[derive(Debug, Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: usize,
    iat: usize,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct SpreadsheetMetadata {
    sheets: Option<Vec<SheetMetadata>>,
}

#[derive(Debug, Deserialize)]
struct SheetMetadata {
    properties: SheetProperties,
}

#[derive(Debug, Deserialize)]
struct SheetProperties {
    title: String,
}

#[derive(Debug, Clone)]
struct SheetTable {
    title: String,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

pub fn sync_station_summaries_to_google_sheets(
    summaries: &[StationSummary],
    output_root: &Path,
) -> Result<Option<GoogleSheetsUploadStatus>> {
    let Some(config) = SheetsConfig::from_env()? else {
        return Ok(None);
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .context("failed to build HTTP client for Google Sheets")?;

    let access_token = config.fetch_access_token(&client)?;
    let (spreadsheet_id, created_new_spreadsheet) = config.ensure_spreadsheet(&client, &access_token)?;
    let tables = build_sheet_tables(summaries);
    config.ensure_sheet_tabs(&client, &access_token, &spreadsheet_id, &tables)?;

    let mut total_rows = 0usize;
    for table in &tables {
        total_rows += table.rows.len();
        config.clear_sheet(&client, &access_token, &spreadsheet_id, &table.title)?;
        config.write_table(&client, &access_token, &spreadsheet_id, table)?;
    }

    let spreadsheet_url = config.spreadsheet_url(&spreadsheet_id);
    let status = GoogleSheetsUploadStatus {
        spreadsheet_id: spreadsheet_id.clone(),
        spreadsheet_url: spreadsheet_url.clone(),
        created_new_spreadsheet,
        uploaded_at: Utc::now().to_rfc3339(),
        tabs: tables.iter().map(|t| t.title.clone()).collect(),
        rows_uploaded: total_rows,
    };

    write_upload_status(output_root, &status)?;

    Ok(Some(status))
}

struct SheetsConfig {
    spreadsheet_id: Option<String>,
    spreadsheet_title: String,
    service_account: ServiceAccountKey,
}

impl SheetsConfig {
    fn from_env() -> Result<Option<Self>> {
        let spreadsheet_id = env::var("GOOGLE_SHEETS_SPREADSHEET_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let spreadsheet_title = env::var("GOOGLE_SHEETS_SPREADSHEET_TITLE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Data Cleaner Report".to_string());

        let Some(service_account) = load_service_account_key()? else {
            if spreadsheet_id.is_some() {
                anyhow::bail!(
                    "GOOGLE_SHEETS_SPREADSHEET_ID is set but no service account credentials were found. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SERVICE_ACCOUNT_JSON."
                );
            }
            return Ok(None);
        };

        Ok(Some(Self {
            spreadsheet_id,
            spreadsheet_title,
            service_account,
        }))
    }

    fn spreadsheet_url(&self, spreadsheet_id: &str) -> String {
        format!(
            "https://docs.google.com/spreadsheets/d/{}/edit",
            spreadsheet_id
        )
    }

    fn token_uri(&self) -> &str {
        if self.service_account.token_uri.trim().is_empty() {
            DEFAULT_TOKEN_URI
        } else {
            self.service_account.token_uri.as_str()
        }
    }

    fn fetch_access_token(&self, client: &Client) -> Result<String> {
        let header = Header::new(Algorithm::RS256);
        let now = Utc::now().timestamp() as usize;
        let claims = JwtClaims {
            iss: &self.service_account.client_email,
            scope: SHEETS_SCOPE,
            aud: self.token_uri(),
            exp: now + 3600,
            iat: now,
        };

        let encoding_key = EncodingKey::from_rsa_pem(self.service_account.private_key.as_bytes())
            .context("failed to load service account private key")?;
        let jwt = encode(&header, &claims, &encoding_key)
            .context("failed to create Google OAuth JWT assertion")?;

        let response = client
            .post(self.token_uri())
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ])
            .send()
            .context("failed to request Google access token")?
            .error_for_status()
            .context("Google OAuth token request returned an error")?;

        let token: TokenResponse = response
            .json()
            .context("failed to decode Google OAuth token response")?;
        Ok(token.access_token)
    }

    fn ensure_spreadsheet(&self, client: &Client, access_token: &str) -> Result<(String, bool)> {
        if let Some(spreadsheet_id) = &self.spreadsheet_id {
            return Ok((spreadsheet_id.clone(), false));
        }

        let response = client
            .post(SHEETS_API_BASE)
            .bearer_auth(access_token)
            .json(&serde_json::json!({
                "properties": { "title": self.spreadsheet_title },
                "sheets": [{ "properties": { "title": "dataset_summary" } }]
            }))
            .send()
            .context("failed to create Google Sheets spreadsheet")?
            .error_for_status()
            .context("Google Sheets create spreadsheet returned an error")?;

        #[derive(Debug, Deserialize)]
        struct CreateSpreadsheetResponse {
            #[serde(rename = "spreadsheetId")]
            spreadsheet_id: String,
        }

        let created: CreateSpreadsheetResponse = response
            .json()
            .context("failed to decode create spreadsheet response")?;

        Ok((created.spreadsheet_id, true))
    }

    fn ensure_sheet_tabs(
        &self,
        client: &Client,
        access_token: &str,
        spreadsheet_id: &str,
        tables: &[SheetTable],
    ) -> Result<()> {
        let existing = self.fetch_existing_sheet_titles(client, access_token, spreadsheet_id)?;
        let missing: Vec<String> = tables
            .iter()
            .filter(|table| !existing.contains(&table.title))
            .map(|table| table.title.clone())
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        let requests: Vec<serde_json::Value> = missing
            .into_iter()
            .map(|title| serde_json::json!({"addSheet": {"properties": {"title": title}}}))
            .collect();

        client
            .post(format!("{}/{}:batchUpdate", SHEETS_API_BASE, spreadsheet_id))
            .bearer_auth(access_token)
            .json(&serde_json::json!({"requests": requests}))
            .send()
            .context("failed to add missing Google Sheets tabs")?
            .error_for_status()
            .context("Google Sheets tab creation returned an error")?;

        Ok(())
    }

    fn fetch_existing_sheet_titles(
        &self,
        client: &Client,
        access_token: &str,
        spreadsheet_id: &str,
    ) -> Result<HashSet<String>> {
        let response = client
            .get(format!("{}/{}?fields=sheets.properties.title", SHEETS_API_BASE, spreadsheet_id))
            .bearer_auth(access_token)
            .send()
            .context("failed to fetch spreadsheet metadata")?
            .error_for_status()
            .context("Google Sheets metadata request returned an error")?;

        let metadata: SpreadsheetMetadata = response
            .json()
            .context("failed to decode spreadsheet metadata")?;

        Ok(metadata
            .sheets
            .unwrap_or_default()
            .into_iter()
            .map(|sheet| sheet.properties.title)
            .collect())
    }

    fn clear_sheet(
        &self,
        client: &Client,
        access_token: &str,
        spreadsheet_id: &str,
        sheet_title: &str,
    ) -> Result<()> {
        let url = format!(
            "{}/{}/values/{}:clear",
            SHEETS_API_BASE, spreadsheet_id, sheet_title
        );

        client
            .post(url)
            .bearer_auth(access_token)
            .json(&serde_json::json!({}))
            .send()
            .context("failed to clear Google Sheet tab")?
            .error_for_status()
            .with_context(|| format!("Google Sheets clear failed for tab {}", sheet_title))?;

        Ok(())
    }

    fn write_table(
        &self,
        client: &Client,
        access_token: &str,
        spreadsheet_id: &str,
        table: &SheetTable,
    ) -> Result<()> {
        const CHUNK_SIZE: usize = 1000;

        let mut all_rows = Vec::with_capacity(table.rows.len() + 1);
        all_rows.push(table.headers.clone());
        all_rows.extend(table.rows.clone());

        for (chunk_index, chunk) in all_rows.chunks(CHUNK_SIZE).enumerate() {
            let start_row = chunk_index * CHUNK_SIZE + 1;
            let range = format!("{}!A{}", table.title, start_row);
            client
                .put(format!(
                    "{}/{}/values/{}?valueInputOption=RAW",
                    SHEETS_API_BASE, spreadsheet_id, range
                ))
                .bearer_auth(access_token)
                .json(&serde_json::json!({
                    "range": range,
                    "majorDimension": "ROWS",
                    "values": chunk,
                }))
                .send()
                .with_context(|| format!("failed to write Google Sheets tab {}", table.title))?
                .error_for_status()
                .with_context(|| format!("Google Sheets update failed for tab {}", table.title))?;
        }

        Ok(())
    }
}

fn load_service_account_key() -> Result<Option<ServiceAccountKey>> {
    let raw = if let Ok(value) = env::var("GOOGLE_SERVICE_ACCOUNT_JSON") {
        Some(value)
    } else if let Ok(path) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        Some(
            fs::read_to_string(&path)
                .with_context(|| format!("failed to read service account file {}", path))?,
        )
    } else {
        None
    };

    let Some(raw) = raw else {
        return Ok(None);
    };

    let mut key: ServiceAccountKey =
        serde_json::from_str(&raw).context("failed to parse Google service account JSON")?;
    if key.token_uri.trim().is_empty() {
        key.token_uri = DEFAULT_TOKEN_URI.to_string();
    }

    Ok(Some(key))
}

fn write_upload_status(output_root: &Path, status: &GoogleSheetsUploadStatus) -> Result<()> {
    let looker_dir = output_root.join("looker_studio");
    fs::create_dir_all(&looker_dir).context("failed to create output/looker_studio directory")?;

    fs::write(
        looker_dir.join("google_sheets_upload.json"),
        serde_json::to_string_pretty(status).context("failed to serialize upload status")?,
    )
    .context("failed to write google_sheets_upload.json")?;

    fs::write(
        looker_dir.join("google_sheets_url.txt"),
        format!("{}\n", status.spreadsheet_url),
    )
    .context("failed to write google_sheets_url.txt")?;

    Ok(())
}

fn build_sheet_tables(summaries: &[StationSummary]) -> Vec<SheetTable> {
    vec![
        build_dataset_summary_table(summaries),
        build_observations_table(summaries),
        build_payment_stats_table(summaries),
        build_city_revenue_table(summaries),
    ]
}

fn build_dataset_summary_table(summaries: &[StationSummary]) -> SheetTable {
    let headers = vec![
        "station",
        "file_path",
        "total_rows",
        "total_cols",
        "duplicate_rows",
        "null_cells",
        "null_pct",
        "nulls_filled",
        "outliers_capped",
        "new_columns",
        "pm25_mean",
        "pm10_mean",
        "no2_mean",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let rows = summaries
        .iter()
        .map(|summary| {
            vec![
                summary.station.clone(),
                summary.file_path.clone(),
                summary.total_rows.to_string(),
                summary.total_cols.to_string(),
                summary.duplicate_rows.to_string(),
                summary.null_cells.to_string(),
                format!("{:.4}", summary.null_pct),
                summary.nulls_filled.to_string(),
                summary.outliers_capped.to_string(),
                summary.new_columns.to_string(),
                optional_float(metric_value(summary, "PM2.5")),
                optional_float(metric_value(summary, "PM10")),
                optional_float(metric_value(summary, "NO2")),
            ]
        })
        .collect();

    SheetTable {
        title: "dataset_summary".to_string(),
        headers,
        rows,
    }
}

fn build_observations_table(summaries: &[StationSummary]) -> SheetTable {
    let headers = vec![
        "station",
        "file_path",
        "date",
        "month",
        "weekday",
        "hour",
        "pm25",
        "pm10",
        "so2",
        "no2",
        "o3",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let mut rows = Vec::new();
    for summary in summaries {
        for obs in &summary.observations {
            rows.push(observation_row(summary, obs));
        }
    }

    SheetTable {
        title: "observations".to_string(),
        headers,
        rows,
    }
}

fn build_payment_stats_table(summaries: &[StationSummary]) -> SheetTable {
    let headers = vec!["station", "method", "count"]
        .into_iter()
        .map(String::from)
        .collect();

    let mut rows = Vec::new();
    for summary in summaries {
        for stat in &summary.payment_stats {
            rows.push(payment_row(summary, stat));
        }
    }

    SheetTable {
        title: "payment_stats".to_string(),
        headers,
        rows,
    }
}

fn build_city_revenue_table(summaries: &[StationSummary]) -> SheetTable {
    let headers = vec!["station", "city", "total_revenue"]
        .into_iter()
        .map(String::from)
        .collect();

    let mut rows = Vec::new();
    for summary in summaries {
        for stat in &summary.city_revenue_stats {
            rows.push(city_row(summary, stat));
        }
    }

    SheetTable {
        title: "city_revenue_stats".to_string(),
        headers,
        rows,
    }
}

fn observation_row(summary: &StationSummary, obs: &Observation) -> Vec<String> {
    vec![
        summary.station.clone(),
        summary.file_path.clone(),
        obs.date.clone(),
        obs.month.to_string(),
        obs.weekday.to_string(),
        obs.hour.to_string(),
        optional_float(obs.pm25),
        optional_float(obs.pm10),
        optional_float(obs.so2),
        optional_float(obs.no2),
        optional_float(obs.o3),
    ]
}

fn payment_row(summary: &StationSummary, stat: &PaymentStat) -> Vec<String> {
    vec![
        summary.station.clone(),
        stat.method.clone(),
        stat.count.to_string(),
    ]
}

fn city_row(summary: &StationSummary, stat: &CityRevenueStat) -> Vec<String> {
    vec![
        summary.station.clone(),
        stat.city.clone(),
        format!("{:.2}", stat.total_revenue),
    ]
}

fn metric_value(summary: &StationSummary, name: &str) -> Option<f64> {
    summary
        .metrics
        .iter()
        .find(|metric| metric.name.eq_ignore_ascii_case(name))
        .and_then(|metric| metric.mean)
}

fn optional_float(value: Option<f64>) -> String {
    value.map(|v| format!("{:.4}", v)).unwrap_or_default()
}
