use chrono::{DateTime, Datelike, Timelike, Utc};

#[derive(Debug, Clone, Copy)]
pub enum TimestampContext {
    Display,
    Json,
    Filename,
}

pub fn format_iso_timestamp(dt: DateTime<Utc>, ctx: TimestampContext) -> String {
    match ctx {
        TimestampContext::Display => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        TimestampContext::Json => dt.to_rfc3339(),
        TimestampContext::Filename => dt.format("%Y%m%dT%H%M%SZ").to_string(),
    }
}

pub fn format_indonesian_timestamp(dt: DateTime<Utc>) -> String {
    let month_name = match dt.month() {
        1 => "Januari",
        2 => "Februari",
        3 => "Maret",
        4 => "April",
        5 => "Mei",
        6 => "Juni",
        7 => "Juli",
        8 => "Agustus",
        9 => "September",
        10 => "Oktober",
        11 => "November",
        12 => "Desember",
        _ => "",
    };

    format!(
        "{:02} {} {:04}, {:02}:{:02}:{:02} UTC",
        dt.day(),
        month_name,
        dt.year(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}
