use anyhow::{Context, Result};
use std::path::Path;

const REPORT_HTML_TEMPLATE: &str = include_str!("templates/report.html");

pub fn ensure_report_html_exists(html_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(html_dir).context("failed to create output/html")?;

    let report_html = html_dir.join("report.html");
    if !report_html.exists() {
        std::fs::write(&report_html, REPORT_HTML_TEMPLATE)
            .with_context(|| format!("failed to write {}", report_html.display()))?;
    }

    Ok(())
}

pub fn build_report_html(content: &str) -> String {
    let escaped = content
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");

    let body = escaped
        .lines()
        .map(|line| {
            if line.trim().is_empty() {
                "<div class=\"space\"></div>".to_string()
            } else if line.starts_with("LAPORAN") || line.starts_with("====") {
                format!("<h1>{}</h1>", line)
            } else if line.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
                && line.contains('.')
            {
                format!("<h2>{}</h2>", line)
            } else if line.trim_start().starts_with('*') || line.trim_start().starts_with('-') {
                format!("<p class=\"bullet\">{}</p>", line)
            } else {
                format!("<p>{}</p>", line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!doctype html>
<html lang="id">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Presentasi Audit Data</title>
  <style>
    :root {{ --bg:#f5f7f2; --card:#fff; --ink:#1f2a1f; --muted:#596559; --accent:#1f7a4c; --line:#d9e2d9; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:'Segoe UI',sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:960px; margin:24px auto; padding:0 16px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:24px; }}
    h1 {{ margin:0 0 12px; color:var(--accent); font-size:1.3rem; }}
    h2 {{ margin:18px 0 8px; color:#244f3a; font-size:1.05rem; border-left:4px solid #89b59f; padding-left:8px; }}
    p {{ margin:6px 0; line-height:1.5; white-space:pre-wrap; word-break:break-word; }}
    .bullet {{ color:var(--muted); }}
    .space {{ height:8px; }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">{}</section>
  </main>
</body>
</html>"#,
        body
    )
}