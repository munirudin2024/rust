//! # Visualization Module
//!
//! Build lightweight multi-station HTML + JSON report.

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate};
use polars::prelude::{AnyValue, DataFrame, Series};
use serde::{Deserialize, Serialize};

use crate::audit::AuditReport;
use crate::clean::CleanReport;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationMetric {
    pub name: String,
    pub mean: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationSummary {
    pub station: String,
    pub file_path: String,
    pub total_rows: usize,
    pub total_cols: usize,
    pub duplicate_rows: usize,
    pub null_cells: usize,
    pub null_pct: f64,
    pub nulls_filled: usize,
    pub outliers_capped: usize,
    pub new_columns: usize,
    pub metrics: Vec<StationMetric>,
    pub observations: Vec<Observation>,
}

  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct Observation {
    pub date: String,
    pub month: u8,
    pub weekday: u8,
    pub hour: u8,
    pub pm25: Option<f64>,
    pub pm10: Option<f64>,
    pub so2: Option<f64>,
    pub no2: Option<f64>,
    pub o3: Option<f64>,
}

pub fn build_station_summary(
    file_path: &str,
    raw_df: &DataFrame,
    audit: &AuditReport,
    clean: &CleanReport,
) -> StationSummary {
    let station = station_name_from_path(file_path);
    let null_cells: usize = audit.profiles.iter().map(|p| p.null_count).sum();
    let total_cells = audit.total_rows.saturating_mul(audit.total_cols);
    let null_pct = if total_cells > 0 {
        null_cells as f64 * 100.0 / total_cells as f64
    } else {
        0.0
    };

    let target_metrics = [
        "PM2.5", "PM10", "SO2", "NO2", "CO", "O3", "TEMP", "PRES", "DEWP", "RAIN", "WSPM",
    ];

    let mut metrics = Vec::new();
    for metric_name in &target_metrics {
      let mean_val = audit
        .profiles
        .iter()
        .find(|p| p.name.eq_ignore_ascii_case(metric_name))
        .and_then(|p| p.mean);
      metrics.push(StationMetric {
        name: (*metric_name).to_string(),
        mean: mean_val,
      });
    }

    let observations = build_observations(raw_df);

    StationSummary {
        station,
        file_path: file_path.to_string(),
        total_rows: audit.total_rows,
        total_cols: audit.total_cols,
        duplicate_rows: audit.duplicate_rows,
        null_cells,
        null_pct,
        nulls_filled: clean.nulls_filled.iter().map(|(_, c)| *c).sum(),
        outliers_capped: clean.outliers_capped.iter().map(|(_, c)| *c).sum(),
        new_columns: clean.new_columns.len(),
        metrics,
        observations,
    }
}

pub fn run_station_comparison(
    summaries: &[StationSummary],
    output_html_path: &str,
    output_json_path: &str,
) -> Result<()> {
    let json =
        serde_json::to_string_pretty(summaries).context("failed to serialize station summaries")?;
    std::fs::write(output_json_path, json).context("failed to write report_data.json")?;

    let html = build_station_comparison_html(summaries);
    std::fs::write(output_html_path, &html).context("failed to write report.html")?;
  let index_path = std::path::Path::new(output_html_path)
    .parent()
    .map(|dir| dir.join("index.html"))
    .unwrap_or_else(|| std::path::PathBuf::from("index.html"));
    std::fs::write(index_path, &html)
      .context("failed to write index.html")?;
    Ok(())
}

fn build_station_comparison_html(summaries: &[StationSummary]) -> String {
    let stations: Vec<String> = summaries.iter().map(|s| s.station.clone()).collect();
    let missing_pct: Vec<f64> = summaries.iter().map(|s| s.null_pct).collect();
    let duplicate_rows: Vec<f64> = summaries.iter().map(|s| s.duplicate_rows as f64).collect();
    let pm25: Vec<f64> = summaries
        .iter()
        .map(|s| metric_value(s, "PM2.5").unwrap_or(f64::NAN))
        .collect();
    let pm10: Vec<f64> = summaries
        .iter()
        .map(|s| metric_value(s, "PM10").unwrap_or(f64::NAN))
        .collect();
    let no2: Vec<f64> = summaries
      .iter()
      .map(|s| metric_value(s, "NO2").unwrap_or(f64::NAN))
      .collect();
    let total_rows: usize = summaries.iter().map(|s| s.total_rows).sum();

    let summaries_json = serde_json::to_string(summaries).unwrap_or_else(|_| "[]".to_string());

    let mut table_rows = String::new();
    for s in summaries {
        table_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}%</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            s.station,
            s.total_rows,
            s.total_cols,
            s.null_pct,
            s.duplicate_rows,
            s.nulls_filled,
            s.outliers_capped,
        ));
    }

    let generated_at = chrono::Utc::now().format("%Y-%m-%d").to_string();
    format!(
        r#"<!doctype html>
<html lang="id">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dashboard Kualitas Udara PRSA</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    :root {{
      --bg: #f8f4ec;
      --bg-soft: #fffdf8;
      --ink: #1e2930;
      --card: #ffffff;
      --accent: #0f766e;
      --accent-2: #f97316;
      --muted: #64748b;
      --line: #e7ddd1;
      --ok: #16a34a;
      --warn: #f59e0b;
      --bad: #dc2626;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Nunito Sans", "Poppins", sans-serif;
      background:
        radial-gradient(1200px 600px at -10% -10%, #fde68a55, transparent 65%),
        radial-gradient(900px 500px at 110% 0%, #0ea5e933, transparent 60%),
        linear-gradient(180deg, var(--bg-soft), var(--bg));
    }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 18px; }}
    .hero {{
      background: linear-gradient(120deg, #0f766e 0%, #14b8a6 40%, #f97316 110%);
      color:#fff;
      border-radius: 20px;
      padding: 24px;
      margin-bottom: 16px;
      box-shadow: 0 20px 35px -24px rgba(15, 118, 110, 0.65);
      animation: slideIn 600ms ease-out;
    }}
    .hero h1 {{ margin: 0 0 8px 0; }}
    .hero p {{ margin: 0; opacity: 0.95; }}
    .actions {{ margin-top: 12px; display: flex; gap: 10px; flex-wrap: wrap; }}
    .btn {{
      border: 1px solid rgba(255,255,255,0.5);
      color: #fff;
      background: rgba(255,255,255,0.1);
      border-radius: 999px;
      padding: 8px 14px;
      cursor: pointer;
      font-weight: 600;
    }}
    .btn:hover {{ background: rgba(255,255,255,0.2); }}
    .layout {{ display: grid; grid-template-columns: 300px 1fr; gap: 14px; align-items: start; }}
    .sidebar {{
      position: sticky;
      top: 12px;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 10px 20px -18px rgba(0, 0, 0, 0.45);
    }}
    .sidebar h3 {{ margin: 0 0 8px 0; color: var(--accent); }}
    .field {{ margin-bottom: 10px; }}
    .field label {{ display: block; font-weight: 700; font-size: 13px; margin-bottom: 6px; }}
    .field select, .field input {{
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 10px;
      padding: 9px 10px;
      font: inherit;
    }}
    .section {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      margin-bottom: 12px;
      animation: fadeUp 500ms ease-out;
    }}
    .section h2 {{ margin-top: 0; color: var(--accent); }}
    .tabs {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }}
    .tab-btn {{
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      border-radius: 999px;
      padding: 8px 14px;
      font-weight: 700;
      cursor: pointer;
      transition: all .2s ease;
    }}
    .tab-btn.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .grid {{ display:grid; gap:10px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }}
    .kpi {{
      background: linear-gradient(160deg, #ffffff, #fdf6ef);
      border:1px solid var(--line);
      border-radius:12px;
      padding:12px;
    }}
    .kpi b {{ display:block; font-size: 24px; }}
    .kpi small {{ color: var(--muted); }}
    .chart {{ min-height: 360px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding:8px; border-bottom:1px solid var(--line); text-align:left; font-size: 14px; }}
    th {{ color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    details {{ border: 1px dashed var(--line); border-radius: 10px; padding: 8px 10px; margin-top: 8px; }}
    details summary {{ cursor: pointer; font-weight: 700; color: var(--accent); }}
    .insight {{ color: #334155; margin: 8px 0 4px; }}
    .legend-badge {{
      display: inline-block;
      border-radius: 999px;
      padding: 3px 8px;
      border: 1px solid var(--line);
      font-size: 12px;
      margin-right: 6px;
      margin-bottom: 6px;
      background: #fff;
    }}
    .footnote {{ font-size: 12px; color: var(--muted); }}
    @keyframes fadeUp {{
      from {{ opacity: 0; transform: translateY(10px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @keyframes slideIn {{
      from {{ opacity: 0; transform: translateY(-8px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @media (max-width: 980px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .sidebar {{ position: static; }}
    }}
    @media print {{
      body {{ background: #fff; }}
      .wrap {{ max-width: 100%; padding: 0; }}
      .hero {{ background: #0f766e; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
      .actions {{ display: none; }}
      .sidebar {{ display: none; }}
      .section {{ break-inside: avoid; page-break-inside: avoid; }}
      .chart {{ min-height: 300px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Dashboard Kualitas Udara PRSA</h1>
      <p>Dibuat: {} • Data stasiun: {} • Total data: {} baris</p>
      <p>Fokus bisnis: memahami pola polusi berbasis waktu, wilayah, dan kategori risiko agar keputusan lebih cepat dan tepat.</p>
      <div class="actions">
        <button class="btn" onclick="exportPdf()">Cetak / Simpan PDF</button>
      </div>
    </section>

    <div class="layout">
      <aside class="sidebar">
        <h3>Filter Eksplorasi</h3>
        <div class="field">
          <label for="station_filter">Stasiun</label>
          <select id="station_filter"></select>
        </div>
        <div class="field">
          <label for="date_start">Tanggal Mulai</label>
          <input id="date_start" type="date" />
        </div>
        <div class="field">
          <label for="date_end">Tanggal Akhir</label>
          <input id="date_end" type="date" />
        </div>
        <div class="field">
          <button class="tab-btn" id="btn_apply" style="width:100%;">Terapkan Filter</button>
        </div>
        <p class="footnote">Tip: pilih satu stasiun untuk analisis tren detail, atau pilih "Semua Stasiun" untuk perbandingan wilayah.</p>
      </aside>

      <main>
        <section class="section">
          <h2>Ringkasan Utama</h2>
          <div class="grid" id="metric_cards"></div>
        </section>

        <section class="section">
          <div class="tabs">
            <button class="tab-btn active" data-tab="tab_tren">Analisis Tren</button>
            <button class="tab-btn" data-tab="tab_wilayah">Analisis Wilayah</button>
            <button class="tab-btn" data-tab="tab_kategori">Kategori Udara</button>
          </div>

          <div id="tab_tren" class="tab-panel active">
            <div id="heatmap_chart" class="chart"></div>
            <details>
              <summary>Lihat Penjelasan Insight</summary>
              <p class="insight">Heatmap menunjukkan jam dan hari ketika PM2.5 paling tinggi. Area warna lebih pekat menandakan periode kritis yang perlu intervensi kebijakan (misal manajemen lalu lintas atau kontrol emisi jam sibuk).</p>
            </details>
            <div id="weekday_weekend_chart" class="chart"></div>
            <details>
              <summary>Lihat Penjelasan Insight</summary>
              <p class="insight">Perbedaan pola weekday vs weekend membantu memisahkan dugaan sumber polusi dari aktivitas komuter/industri dibanding aktivitas akhir pekan.</p>
            </details>
            <div id="monthly_chart" class="chart"></div>
            <details>
              <summary>Lihat Penjelasan Insight</summary>
              <p class="insight">Grafik bulanan memperlihatkan siklus musiman. Lonjakan konsisten pada bulan tertentu bisa dijadikan dasar kampanye mitigasi musiman.</p>
            </details>
          </div>

          <div id="tab_wilayah" class="tab-panel">
            <div id="gas_stack_chart" class="chart"></div>
            <details>
              <summary>Lihat Penjelasan Insight</summary>
              <p class="insight">Komposisi SO2, NO2, dan O3 membantu melihat profil polusi tiap stasiun, sehingga strategi kebijakan bisa lebih spesifik lokasi.</p>
            </details>
            <div id="pm_station_chart" class="chart"></div>
            <div id="missing_chart" class="chart"></div>
          </div>

          <div id="tab_kategori" class="tab-panel">
            <div>
              <span class="legend-badge">Good: PM2.5 ≤ 35</span>
              <span class="legend-badge">Moderate: 35-75</span>
              <span class="legend-badge">Unhealthy: 75-115</span>
              <span class="legend-badge">Very Unhealthy: 115-150</span>
              <span class="legend-badge">Hazardous: &gt; 150</span>
            </div>
            <div id="air_quality_pie" class="chart"></div>
            <div id="dup_chart" class="chart"></div>
          </div>
        </section>

        <section class="section">
          <h2>Kualitas Data per Stasiun</h2>
          <div style="overflow-x:auto; margin-top:8px;">
            <table>
              <thead><tr><th>Stasiun</th><th>Rows</th><th>Cols</th><th>Missing</th><th>Duplicates</th><th>Nulls Filled</th><th>Outliers Capped</th></tr></thead>
              <tbody>{}</tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  </div>

  <script>
    const reportData = {};
    const stations = {};
    const missingPct = {};
    const pm25 = {};
    const pm10 = {};
    const no2 = {};
    const duplicates = {};

    const monthLabel = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];
    const weekdayLabel = ['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu'];

    const stationFilter = document.getElementById('station_filter');
    const dateStart = document.getElementById('date_start');
    const dateEnd = document.getElementById('date_end');
    const btnApply = document.getElementById('btn_apply');

    initializeFilters();
    initializeTabs();
    renderAll();

    btnApply.addEventListener('click', renderAll);

    function initializeFilters() {{
      stationFilter.innerHTML = `<option value="ALL">Semua Stasiun</option>` + stations.map((s) => `<option value="${{s}}">${{s}}</option>`).join('');

      const allDates = reportData
        .flatMap((s) => (s.observations || []).map((o) => o.date))
        .filter(Boolean)
        .sort();

      if (allDates.length > 0) {{
        dateStart.value = allDates[0];
        dateEnd.value = allDates[allDates.length - 1];
        dateStart.min = allDates[0];
        dateStart.max = allDates[allDates.length - 1];
        dateEnd.min = allDates[0];
        dateEnd.max = allDates[allDates.length - 1];
      }}
    }}

    function initializeTabs() {{
      const tabBtns = Array.from(document.querySelectorAll('[data-tab]'));
      tabBtns.forEach((btn) => {{
        btn.addEventListener('click', () => {{
          tabBtns.forEach((x) => x.classList.remove('active'));
          btn.classList.add('active');
          document.querySelectorAll('.tab-panel').forEach((p) => p.classList.remove('active'));
          document.getElementById(btn.dataset.tab).classList.add('active');
        }});
      }});
    }}

    function renderAll() {{
      const selectedStation = stationFilter.value || 'ALL';
      const start = dateStart.value;
      const end = dateEnd.value;

      const filteredStations = reportData
        .filter((s) => selectedStation === 'ALL' || s.station === selectedStation)
        .map((s) => ({{
          ...s,
          observations: (s.observations || []).filter((o) => (!start || o.date >= start) && (!end || o.date <= end)),
        }}));

      renderMetricCards(filteredStations);
      renderHeatmap(filteredStations);
      renderWeekdayWeekend(filteredStations);
      renderMonthly(filteredStations);
      renderGasStack(filteredStations);
      renderPMStation(filteredStations);
      renderMissingChart(filteredStations);
      renderQualityPie(filteredStations);
      renderDuplicateChart(filteredStations);
    }}

    function renderMetricCards(stationsData) {{
      const card = document.getElementById('metric_cards');
      const obs = stationsData.flatMap((s) => s.observations || []);
      const pm25Vals = obs.map((o) => o.pm25).filter((v) => Number.isFinite(v));
      const pm10Vals = obs.map((o) => o.pm10).filter((v) => Number.isFinite(v));
      const no2Vals = obs.map((o) => o.no2).filter((v) => Number.isFinite(v));
      const avg = (arr) => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length) : 0;

      const cards = [
        {{ title: 'Rata-rata PM2.5', value: avg(pm25Vals).toFixed(2), note: 'ug/m3' }},
        {{ title: 'Rata-rata PM10', value: avg(pm10Vals).toFixed(2), note: 'ug/m3' }},
        {{ title: 'Rata-rata NO2', value: avg(no2Vals).toFixed(2), note: 'ug/m3' }},
        {{ title: 'Total Data Terfilter', value: obs.length.toLocaleString('id-ID'), note: 'baris observasi' }},
      ];

      card.innerHTML = cards
        .map((c) => `<article class="kpi"><small>${{c.title}}</small><b>${{c.value}}</b><small>${{c.note}}</small></article>`)
        .join('');
    }}

    function renderHeatmap(stationsData) {{
      const obs = stationsData.flatMap((s) => s.observations || []).filter((o) => Number.isFinite(o.pm25));
      const z = Array.from({{ length: 7 }}, () => Array.from({{ length: 24 }}, () => null));
      const bucket = Array.from({{ length: 7 }}, () => Array.from({{ length: 24 }}, () => []));

      obs.forEach((o) => {{
        if (o.weekday >= 0 && o.weekday < 7 && o.hour >= 0 && o.hour < 24) {{
          bucket[o.weekday][o.hour].push(o.pm25);
        }}
      }});

      for (let d = 0; d < 7; d++) {{
        for (let h = 0; h < 24; h++) {{
          if (bucket[d][h].length) {{
            z[d][h] = bucket[d][h].reduce((a, b) => a + b, 0) / bucket[d][h].length;
          }}
        }}
      }}

      Plotly.newPlot('heatmap_chart', [{{
        type: 'heatmap',
        x: Array.from({{ length: 24 }}, (_, i) => i),
        y: weekdayLabel,
        z,
        colorscale: 'YlOrRd',
        hovertemplate: 'Hari: %{{y}}<br>Jam: %{{x}}:00<br>PM2.5: %{{z:.2f}}<extra></extra>'
      }}], {{
        title: 'Heatmap PM2.5 (Jam vs Hari)',
        margin: {{ t: 50, l: 50, r: 20, b: 40 }}
      }}, {{ responsive: true }});
    }}

    function renderWeekdayWeekend(stationsData) {{
      const obs = stationsData.flatMap((s) => s.observations || []).filter((o) => Number.isFinite(o.pm25));
      const wk = Array.from({{ length: 24 }}, () => []);
      const we = Array.from({{ length: 24 }}, () => []);

      obs.forEach((o) => {{
        if (o.hour < 0 || o.hour > 23) return;
        if (o.weekday <= 4) wk[o.hour].push(o.pm25);
        else we[o.hour].push(o.pm25);
      }});

      const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
      const x = Array.from({{ length: 24 }}, (_, i) => i);

      Plotly.newPlot('weekday_weekend_chart', [
        {{ type: 'scatter', mode: 'lines+markers', name: 'Hari Kerja', x, y: wk.map(avg), line: {{ color: '#0f766e', width: 3 }} }},
        {{ type: 'scatter', mode: 'lines+markers', name: 'Akhir Pekan', x, y: we.map(avg), line: {{ color: '#f97316', width: 3 }} }}
      ], {{
        title: 'Perbandingan Pola PM2.5: Hari Kerja vs Akhir Pekan',
        xaxis: {{ title: 'Jam' }},
        yaxis: {{ title: 'PM2.5 (ug/m3)' }}
      }}, {{ responsive: true }});
    }}

    function renderMonthly(stationsData) {{
      const obs = stationsData.flatMap((s) => s.observations || []).filter((o) => Number.isFinite(o.pm25));
      const bucket = Array.from({{ length: 12 }}, () => []);
      obs.forEach((o) => {{
        if (o.month >= 1 && o.month <= 12) bucket[o.month - 1].push(o.pm25);
      }});

      const y = bucket.map((arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);

      Plotly.newPlot('monthly_chart', [{{
        type: 'bar',
        x: monthLabel,
        y,
        marker: {{ color: '#14b8a6' }}
      }}], {{
        title: 'Pola Musiman PM2.5 (Rata-rata Bulanan)',
        yaxis: {{ title: 'PM2.5 (ug/m3)' }}
      }}, {{ responsive: true }});
    }}

    function renderGasStack(stationsData) {{
      const all = (stationsData.length > 0 ? stationsData : reportData);
      const names = all.map((s) => s.station);
      const avgMetric = (records, key) => {{
        const vals = records.map((o) => o[key]).filter((v) => Number.isFinite(v));
        return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
      }};

      Plotly.newPlot('gas_stack_chart', [
        {{ type: 'bar', name: 'SO2', x: names, y: all.map((s) => avgMetric(s.observations || [], 'so2')), marker: {{ color: '#1d4ed8' }} }},
        {{ type: 'bar', name: 'NO2', x: names, y: all.map((s) => avgMetric(s.observations || [], 'no2')), marker: {{ color: '#ea580c' }} }},
        {{ type: 'bar', name: 'O3',  x: names, y: all.map((s) => avgMetric(s.observations || [], 'o3')), marker: {{ color: '#16a34a' }} }}
      ], {{
        barmode: 'stack',
        title: 'Komposisi Gas Polutan per Stasiun (SO2 + NO2 + O3)',
        yaxis: {{ title: 'Konsentrasi rata-rata (ug/m3)' }}
      }}, {{ responsive: true }});
    }}

    function renderPMStation(stationsData) {{
      const all = (stationsData.length > 0 ? stationsData : reportData);
      const names = all.map((s) => s.station);

      Plotly.newPlot('pm_station_chart', [
        {{ type:'scatter', mode:'lines+markers', name:'PM2.5 mean', x: names, y: all.map((s) => (s.metrics.find((m) => m.name === 'PM2.5') || {{}}).mean ?? null), line: {{color:'#d1495b'}} }},
        {{ type:'scatter', mode:'lines+markers', name:'PM10 mean', x: names, y: all.map((s) => (s.metrics.find((m) => m.name === 'PM10') || {{}}).mean ?? null), line: {{color:'#00798c'}} }},
        {{ type:'scatter', mode:'lines+markers', name:'NO2 mean', x: names, y: all.map((s) => (s.metrics.find((m) => m.name === 'NO2') || {{}}).mean ?? null), line: {{color:'#7c3aed'}} }}
      ], {{ title: 'Perbandingan Rata-rata Polutan Utama antar Stasiun' }}, {{ responsive: true }});
    }}

    function renderMissingChart(stationsData) {{
      const all = (stationsData.length > 0 ? stationsData : reportData);
      Plotly.newPlot('missing_chart', [{{
        type: 'bar',
        x: all.map((s) => s.station),
        y: all.map((s) => s.null_pct),
        marker: {{ color: '#0f766e' }}
      }}], {{ title: 'Persentase Missing Value per Stasiun (%)' }}, {{ responsive: true }});
    }}

    function renderQualityPie(stationsData) {{
      const obs = stationsData.flatMap((s) => s.observations || []).filter((o) => Number.isFinite(o.pm25));
      const label = ['Good', 'Moderate', 'Unhealthy', 'Very Unhealthy', 'Hazardous'];
      const c = [0, 0, 0, 0, 0];

      obs.forEach((o) => {{
        const v = o.pm25;
        if (v <= 35) c[0] += 1;
        else if (v <= 75) c[1] += 1;
        else if (v <= 115) c[2] += 1;
        else if (v <= 150) c[3] += 1;
        else c[4] += 1;
      }});

      Plotly.newPlot('air_quality_pie', [{{
        type: 'pie',
        labels: label,
        values: c,
        hole: 0.45,
        marker: {{ colors: ['#22c55e', '#eab308', '#f97316', '#ef4444', '#991b1b'] }}
      }}], {{ title: 'Proporsi Kategori Kualitas Udara (berdasarkan PM2.5)' }}, {{ responsive: true }});
    }}

    function renderDuplicateChart(stationsData) {{
      const all = (stationsData.length > 0 ? stationsData : reportData);
      Plotly.newPlot('dup_chart', [{{
        type: 'bar',
        x: all.map((s) => s.station),
        y: all.map((s) => s.duplicate_rows),
        marker: {{ color: '#f4a259' }}
      }}], {{ title: 'Jumlah Duplikasi Data per Stasiun' }}, {{ responsive: true }});
    }}

    function exportPdf() {{
      window.print();
    }}

    // Tambahkan ?autoprint=1 di URL jika ingin langsung membuka dialog print.
    const url = new URL(window.location.href);
    if (url.searchParams.get('autoprint') === '1') {{
      setTimeout(() => window.print(), 700);
    }}
  </script>
</body>
</html>"#,
        generated_at,
        summaries.len(),
        total_rows,
        table_rows,
        summaries_json,
        js_array_str(&stations),
        js_array_num(&missing_pct),
        js_array_num(&pm25),
        js_array_num(&pm10),
        js_array_num(&no2),
        js_array_num(&duplicate_rows),
    )
}

fn build_observations(df: &DataFrame) -> Vec<Observation> {
    let year_col = find_column_name(df, &["year"]);
    let month_col = find_column_name(df, &["month"]);
    let day_col = find_column_name(df, &["day"]);
    let hour_col = find_column_name(df, &["hour"]);

    let pm25_col = find_column_name(df, &["PM2.5", "pm2.5", "pm25"]);
    let pm10_col = find_column_name(df, &["PM10", "pm10"]);
    let so2_col = find_column_name(df, &["SO2", "so2"]);
    let no2_col = find_column_name(df, &["NO2", "no2"]);
    let o3_col = find_column_name(df, &["O3", "o3"]);

    let (Some(y), Some(m), Some(d), Some(h)) = (year_col, month_col, day_col, hour_col) else {
        return Vec::new();
    };

    let year_s = match df.column(&y) {
        Ok(s) => s.clone(),
        Err(_) => return Vec::new(),
    };
    let month_s = match df.column(&m) {
        Ok(s) => s.clone(),
        Err(_) => return Vec::new(),
    };
    let day_s = match df.column(&d) {
        Ok(s) => s.clone(),
        Err(_) => return Vec::new(),
    };
    let hour_s = match df.column(&h) {
        Ok(s) => s.clone(),
        Err(_) => return Vec::new(),
    };

    let pm25_s = pm25_col
        .as_ref()
        .and_then(|n| df.column(n).ok())
        .map(|c| c.clone());
    let pm10_s = pm10_col
        .as_ref()
        .and_then(|n| df.column(n).ok())
        .map(|c| c.clone());
    let so2_s = so2_col
        .as_ref()
        .and_then(|n| df.column(n).ok())
        .map(|c| c.clone());
    let no2_s = no2_col
        .as_ref()
        .and_then(|n| df.column(n).ok())
        .map(|c| c.clone());
    let o3_s = o3_col
        .as_ref()
        .and_then(|n| df.column(n).ok())
        .map(|c| c.clone());

    let mut observations = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let year = get_i32_at(&year_s, i);
        let month = get_i32_at(&month_s, i);
        let day = get_i32_at(&day_s, i);
        let hour = get_i32_at(&hour_s, i);

        let (Some(year), Some(month), Some(day), Some(hour)) = (year, month, day, hour) else {
          continue;
        };

        if !(1..=12).contains(&month) || !(1..=31).contains(&day) || !(0..=23).contains(&hour) {
          continue;
        }

        let Some(date) = NaiveDate::from_ymd_opt(year, month as u32, day as u32) else {
          continue;
        };

        let weekday = date.weekday().num_days_from_monday() as u8;
        observations.push(Observation {
          date: date.format("%Y-%m-%d").to_string(),
          month: month as u8,
          weekday,
          hour: hour as u8,
          pm25: pm25_s.as_ref().and_then(|s| get_f64_at(s, i)),
          pm10: pm10_s.as_ref().and_then(|s| get_f64_at(s, i)),
          so2: so2_s.as_ref().and_then(|s| get_f64_at(s, i)),
          no2: no2_s.as_ref().and_then(|s| get_f64_at(s, i)),
          o3: o3_s.as_ref().and_then(|s| get_f64_at(s, i)),
        });
    }

    observations
}

    fn find_column_name(df: &DataFrame, candidates: &[&str]) -> Option<String> {
      for name in df.get_column_names() {
        if candidates.iter().any(|c| name.eq_ignore_ascii_case(c)) {
          return Some(name.to_string());
        }
      }
      None
    }

    fn get_i32_at(s: &Series, idx: usize) -> Option<i32> {
      match s.get(idx).ok()? {
        AnyValue::Int8(v) => Some(v as i32),
        AnyValue::Int16(v) => Some(v as i32),
        AnyValue::Int32(v) => Some(v),
        AnyValue::Int64(v) => i32::try_from(v).ok(),
        AnyValue::UInt8(v) => Some(v as i32),
        AnyValue::UInt16(v) => Some(v as i32),
        AnyValue::UInt32(v) => i32::try_from(v).ok(),
        AnyValue::UInt64(v) => i32::try_from(v).ok(),
        AnyValue::Float32(v) => Some(v.round() as i32),
        AnyValue::Float64(v) => Some(v.round() as i32),
        AnyValue::String(v) => v.parse::<i32>().ok(),
        _ => None,
      }
    }

    fn get_f64_at(s: &Series, idx: usize) -> Option<f64> {
      match s.get(idx).ok()? {
        AnyValue::Int8(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Float64(v) => Some(v),
        AnyValue::String(v) => v.parse::<f64>().ok(),
        _ => None,
      }
    }

fn metric_value(summary: &StationSummary, name: &str) -> Option<f64> {
    summary
        .metrics
        .iter()
        .find(|m| m.name.eq_ignore_ascii_case(name))
        .and_then(|m| m.mean)
}

fn station_name_from_path(path: &str) -> String {
    let file = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(path);

    let without_prefix = file.strip_prefix("PRSA_Data_").unwrap_or(file);
    let station = without_prefix
        .split("_201")
        .next()
        .unwrap_or(without_prefix);
    station.to_string()
}

fn js_array_num(v: &[f64]) -> String {
    let body = v
        .iter()
        .map(|x| {
            if x.is_finite() {
                format!("{:.8}", x)
            } else {
                "null".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{}]", body)
}

fn js_array_str(v: &[String]) -> String {
    let body = v
        .iter()
        .map(|s| format!("\"{}\"", s.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{}]", body)
}
