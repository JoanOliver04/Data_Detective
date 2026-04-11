<div align="center">

# 🔍 Data Detective Valencia

### Urban Environmental Intelligence — Air Quality · Weather · Traffic · Events

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.54+-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Plotly](https://img.shields.io/badge/Plotly-Interactive_Charts-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)](https://plotly.com)
[![Folium](https://img.shields.io/badge/Folium-Geospatial_Maps-77B829?style=for-the-badge&logo=leaflet&logoColor=white)](https://python-visualization.github.io/folium/)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

**An end-to-end data engineering platform that ingests, processes, and visualizes 70+ years of environmental and mobility data from Valencia, Spain — combining historical archives with live sensor feeds to measure the real impact of mass events like Fallas, football matches, and concerts on air quality and urban traffic.**

[Features](#-key-features) · [Architecture](#-architecture) · [Data Sources](#-data-sources) · [Getting Started](#-getting-started) · [Tech Stack](#-tech-stack) · [Testing](#-testing) · [Screenshots](#-screenshots)

---

</div>

## 📌 What Is This Project?

**Data Detective Valencia** is a full-cycle Big Data project developed as an academic portfolio piece for a graduate-level data engineering course. It addresses a concrete urban analytics question: _how do mass events affect air quality and traffic congestion in Valencia?_

The platform integrates data from **8 official sources** — including the Generalitat Valenciana, AEMET, the European Environment Agency, and real-time APIs — spanning meteorological records from **1956** and air quality measurements from **1963** through to live streaming data captured today. All data flows through a modular ETL pipeline before being served through an interactive Streamlit dashboard that covers all **19 official districts** of Valencia.

The dashboard runs a **background streaming daemon** that continuously captures live data from four sources (AQICN, OpenWeather, AVAMET, DGT) while serving the UI — no separate process needed. Real-time sensor readings are fused into an **Urban Quality Index** (0–10) that combines pollution, weather, and traffic into a single score per district.

The analytical core is a **quasi-experimental baseline comparison model** that quantifies how events like Fallas (a 19-day UNESCO-heritage festival) or a Valencia CF match deviate NO₂, PM2.5, and traffic incident counts from a carefully constructed baseline of comparable non-event days — controlling for season, day of week, and precipitation. A **Pattern Detective** mode extends this with anomaly detection across arbitrary date ranges.

The result is a production-grade, self-contained analytics environment: from raw HTTP responses to polished, exportable visualizations — entirely on a single Windows machine, no cloud infrastructure required.

---

## ✨ Key Features

- 📊 **Composite Air Quality Index (0–10)** — Weighted synthetic score per station and neighborhood, computed from WHO thresholds across all six pollutants and displayed as a KPI with progress bar and grade label
- 🏙️ **Urban Quality Index (0–10)** — Three-axis fusion score combining pollution (50%), meteorology (25%), and traffic (25%), with automatic weight redistribution when data sources are unavailable
- 🗺️ **Interactive Sensor Map** — Folium map with real-time sensor layers (AQICN + OpenWeather live data in popups), district heatmap overlay, per-station circle markers color-coded by pollution ratio, and the Turia Garden polyline as a green reference corridor
- 🚶 **Clean Air Walking Routes** — Five predefined low-pollution routes across Valencia's green corridors (Jardí del Túria, Vivers, Albufera, Malvarrosa, Huerta Nord) with real-time AQI color overlay per segment
- 🏆 **Neighborhood Ranking** — Dynamic league table covering all **19 official Valencia districts** with a 3-parameter urban fusion score (pollution + weather + traffic), medal icons, worst-pollutant identification, and progress column
- 🚨 **Real-Time Contamination Alerts** — Dual alert system: historical alerts scanning the most recent 30 days against WHO thresholds, plus live real-time alerts triggered when any neighborhood currently exceeds WHO limits
- 📈 **Historical Comparator with Pattern Detective** — Triple-mode comparison tool: _Event vs. Baseline_ (bar chart), _Period vs. Period_ (radar chart), and _Pattern Detective_ mode with anomaly detection to uncover unusual pollution behavior
- 🔮 **Dual Forecast System** — 72-hour weather-based risk forecast (atmospheric washout heuristic) plus a statistical pollution forecast for tomorrow based on historical trends, interannual tendency correction, and real-time data weighting
- 🎆 **Mass Event Impact Analysis** — Gantt-style timeline and grouped bar charts quantifying NO₂, PM10, PM2.5, and traffic deviation (%) during Fallas, Valencia CF matches, concerts, and other classified events
- 🔄 **Background Streaming & Auto-Refresh** — Daemon thread captures live data from all four sources (AQICN, OpenWeather, AVAMET, DGT) in the background while the dashboard runs; `streamlit-autorefresh` polls for updates every 5 minutes; sidebar shows data freshness indicators per source
- 🌗 **Dark / Light Theme Toggle** — Full theme system with coherent color tokens across Plotly charts, Folium maps, and inline HTML; user preference persisted in session state
- 📥 **Multi-Format Data Export** — Every tab includes a collapsible export panel offering CSV (`;` separator, UTF-8 BOM for Excel), JSON (with metadata envelope), XML (auto-sanitized element names), and PDF (formatted table with header and metadata) — for any filtered slice of data
- 🧪 **Integration Test Suite** — Pytest-based tests validating core dashboard components (quality index, data loading, configuration) without requiring real data or network access

---

## 🏗️ Architecture

```
DATA_DETECTIVE/
│
├── 1.DATOS_EN_CRUDO/              ← Landing Zone (git-ignored)
│   ├── historicos/                    GVA, AEMET, EEA — CSV / Parquet archives
│   ├── dinamicos/                     AQICN, OpenWeather, AVAMET, DGT — JSON / XML streams
│   └── eventos/                       .ics feeds + classified event records
│
├── 2.SCRIPTS/
│   ├── recopilacion/                  11 scripts: API clients, web scrapers, .ics parsers
│   │   ├── streaming_master.py        Orchestrates all 4 live sources sequentially
│   │   ├── streaming_aqicn.py         Real-time air quality (AQICN/WAQI)
│   │   ├── streaming_openweather.py   Weather + 72h forecast (OpenWeatherMap)
│   │   ├── scraping_avamet.py         Precipitation scraping (AVAMET)
│   │   └── streaming_dgt.py           Traffic feed (DGT DATEX II v3.6)
│   └── procesamiento/                 ETL + visualization generation
│       ├── pipeline_etl.py            Orchestrates phases 5.1 → 5.5 in order
│       ├── normalizar_contaminacion.py  5.1 — WHO/EU normalization + quality flags
│       ├── limpiar_meteorologia.py      5.2 — Meteorological cleaning
│       ├── limpiar_trafico.py           5.3 — Traffic incident parsing
│       ├── calcular_estadisticas.py     5.4 — Annual/neighborhood aggregations
│       ├── correlacion_eventos.py       5.5 — Quasi-experimental baseline model
│       ├── generar_mapas.py             Folium HTML maps (pre-generated for speed)
│       ├── generar_graficos.py          Plotly HTML charts
│       └── generar_pronostico.py        72h forecast visualization
│
├── 3.DATOS_LIMPIOS/               ← Curated datasets (git-ignored)
│   ├── contaminacion_normalizada.parquet   121K+ records, columnar format
│   ├── meteorologia_limpio.csv             2.8K+ hourly records
│   ├── trafico_limpio.csv                  750+ incident records
│   ├── impacto_eventos.csv                 24 event×variable pairs
│   └── estadisticas/                       Annual and monthly aggregations
│
├── 4.VISUALIZACIONES/             ← Pre-generated HTML (git-ignored)
│   ├── mapas/                     Folium maps: NO₂, PM2.5, traffic
│   ├── graficos/                  Plotly time-series charts
│   └── pronostico/                72h interactive forecast chart
│
├── 5.DASHBOARD/                   ← Streamlit Application
│   ├── app.py                     Main orchestrator — 6 tabs, global filters, CSS
│   ├── config.py                  Centralized paths, WHO/EU thresholds, 19 districts
│   ├── data_loader.py             Cached loading layer (@st.cache_data, Parquet/CSV)
│   ├── streaming_background.py    Daemon thread for live data capture while dashboard runs
│   ├── theme.py                   Dark/light theme system with Plotly + Folium tokens
│   ├── components/
│   │   ├── sidebar.py             Year range, variable, district filters + freshness indicators
│   │   ├── kpis.py                Air quality KPIs + composite quality index
│   │   ├── alertas.py             WHO threshold alerts (historical + real-time)
│   │   ├── ranking_barrios.py     19-district ranking with 3-axis urban fusion score
│   │   ├── maps.py                Folium map with RT sensors layer + district heatmap
│   │   ├── rutas_limpias.py       Clean air walking routes with live AQI overlay
│   │   ├── realtime.py            Collapsible real-time data panel (AQICN + OWM + DGT)
│   │   ├── trends.py              Plotly time-series + annual trend metrics
│   │   ├── meteorologia.py        Precipitation tab (adaptive granularity + climatology)
│   │   ├── trafico.py             Traffic tab (weekly distribution + embedded map)
│   │   ├── eventos.py             Event impact tab (Gantt timeline + grouped bars)
│   │   ├── comparador.py          Historical comparator + Pattern Detective anomaly mode
│   │   ├── pronostico.py          72h forecast + statistical next-day prediction
│   │   └── exportar.py            Per-tab export panel (CSV / JSON / XML / PDF)
│   └── utils/
│       ├── quality_index.py       Composite 0–10 air quality score engine
│       ├── urban_quality_index.py  3-axis urban quality fusion (pollution+weather+traffic)
│       ├── pronostico_estadistico.py  Statistical next-day pollution forecast
│       ├── exportador.py          Format converters (BOM, XML sanitization)
│       └── formatters.py          Number and percentage formatting helpers
│
├── tests/                         ← Integration test suite
│   ├── conftest.py                Pytest config (sys.path, st.cache_data patch)
│   ├── test_dashboard_basico.py   Component tests (quality index, data loader, config)
│   └── run_tests.py               Test runner script
│
├── utils/
│   └── paths.py                   PROJECT_ROOT resolver (portable, .env-anchored)
├── logs/                          Structured log files (rotating)
├── .env                           API keys (git-ignored)
├── requirements.txt
├── CLAUDE.md
└── README.md
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  GVA · AEMET · EEA · AQICN · OpenWeatherMap · AVAMET · DGT     │
│  REST APIs · Web scraping · DATEX II XML · .ics calendars       │
└──────────────┬─────────────────────────────┬────────────────────┘
               │  Historical + batch capture  │  Live streaming
               ▼                              │  (background daemon)
  1.DATOS_EN_CRUDO  (Landing Zone)            │
         JSON · CSV · XML · .ics              │
               │                              │
               │  pipeline_etl.py             │
               │  (5 sequential stages)       │
               ▼                              │
 ┌─────────────────────────────────────┐      │
 │   5.1  Normalize pollutants         │      │
 │   5.2  Clean meteorology            │      │
 │   5.3  Parse traffic incidents      │      │
 │   5.4  Compute annual aggregates    │      │
 │   5.5  Quasi-experimental baseline  │      │
 └────────────────┬────────────────────┘      │
                  │                            │
                  ▼                            │
 3.DATOS_LIMPIOS  +  4.VISUALIZACIONES        │
   Parquet · CSV · Folium · Plotly HTML        │
                  │                            │
                  │  streamlit run app.py       │
                  ▼                            ▼
         ┌─────────────────────────────────────┐
         │        STREAMLIT DASHBOARD          │
         │  Dark/Light theme toggle            │
         │  Sidebar: filters + freshness       │
         │  Real-time data panel (collapsible) │
         │  Background streaming daemon        │
         │                                     │
         │  Tab 1 — Air Quality                │
         │    KPIs · Sensor map · RT layers    │
         │    District heatmap · Walking routes│
         │    Ranking (19 districts)           │
         │    Alerts (historical + real-time)  │
         │  Tab 2 — Precipitation              │
         │  Tab 3 — Traffic                    │
         │  Tab 4 — Event Impact               │
         │  Tab 5 — Historical Comparator      │
         │    + Pattern Detective (anomalies)  │
         │  Tab 6 — Forecast                   │
         │    72h weather + statistical next-day│
         │                                     │
         │  Each tab → Export CSV/JSON/XML/PDF │
         └─────────────────────────────────────┘
```

---

## 📡 Data Sources

| Source                                 | Type              | Variables                                        | Period         |
| -------------------------------------- | ----------------- | ------------------------------------------------ | -------------- |
| **GVA** — Generalitat Valenciana       | REST API          | NO₂, O₃, PM10, PM2.5, SO₂, CO                    | 1963 – present |
| **AEMET** OpenData                     | REST API          | Temperature, humidity, precipitation             | 1956 – present |
| **European Environment Agency**        | Bulk download     | EU-wide air quality records                      | Multi-decade   |
| **AQICN / WAQI**                       | REST API          | Real-time AQI (6 stations)                       | Live           |
| **OpenWeatherMap**                     | REST API          | Current weather + 72h forecast                   | Live           |
| **AVAMET**                             | Web scraping      | Precipitation, temperature (Valencia network)    | Live           |
| **DGT** — Dirección General de Tráfico | DATEX II v3.6 XML | Traffic intensity, speed, incidents              | Live           |
| **Visit Valencia / City Council**      | .ics + scraping   | Mass events calendar (Fallas, concerts, matches) | Current season |

**Monitoring stations (GVA network, Valencia city):**

| Station ID | Location                      | District        |
| ---------- | ----------------------------- | --------------- |
| 46250001   | Avd. Francia / Pista de Silla | Quatre Carreres |
| 46250004   | Viveros                       | Jesús           |
| 46250030   | Molí del Sol                  | Jesús           |
| 46250047   | Politècnic                    | Benimaclet      |
| 46250050   | Molí del Sol (zona Patraix)   | Patraix         |
| 46250054   | Centre                        | Ciutat Vella    |

Additionally, the dashboard maps real-time AQICN sensor data to all **19 official Valencia districts** (Ciutat Vella, L'Eixample, Extramurs, Campanar, La Saïdia, El Pla del Real, L'Olivereta, Patraix, Jesús, Quatre Carreres, Poblats Marítims, Camins al Grau, Algirós, Benimaclet, Rascanya, Benicalap, Pobles del Nord, Pobles de l'Oest, and Pobles del Sud), using proximity-weighted interpolation when direct readings are unavailable.

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.10+** on Windows 10/11
- Free API keys (no credit card required):
  - [AEMET OpenData](https://opendata.aemet.es/centrodedescargas/altaUsuario) — Spanish meteorological historical data
  - [OpenWeatherMap](https://openweathermap.org/api) — 72h weather forecasts (1,000 calls/day on free tier)
  - [AQICN / WAQI](https://aqicn.org/data-platform/token/) — Real-time air quality index

### Installation

```powershell
# Clone the repository
git clone https://github.com/joan-oliver/Data_Detective.git
cd Data_Detective

# Create and activate the virtual environment
python -m venv env_data_detective
.\env_data_detective\Scripts\Activate.ps1

# Install all dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root (already listed in `.gitignore`):

```ini
# .env — never commit this file
AQI_TOKEN=your_aqicn_token_here
OWM_API_KEY=your_openweathermap_key_here
AEMET_API_KEY=your_aemet_key_here
```

### Running the Pipeline

```powershell
# Step 1 — Capture live data (run once or schedule it)
python 2.SCRIPTS/recopilacion/streaming_master.py

# Step 2 — Run the full ETL pipeline (normalize → clean → aggregate → correlate)
python 2.SCRIPTS/procesamiento/pipeline_etl.py

# Step 3 — Generate pre-rendered visualizations (maps and charts)
python 2.SCRIPTS/procesamiento/generar_mapas.py
python 2.SCRIPTS/procesamiento/generar_graficos.py
python 2.SCRIPTS/procesamiento/generar_visualizaciones_eventos.py
python 2.SCRIPTS/procesamiento/generar_pronostico.py

# Step 4 — Launch the dashboard
streamlit run 5.DASHBOARD/app.py
```

Open your browser at `http://localhost:8501`.

> **Note:** The dashboard automatically starts a background streaming thread that captures live data from AQICN, OpenWeather, AVAMET, and DGT while the UI is running. Step 1 is only needed to seed initial data before the first launch.

### Automating Data Collection (Windows Task Scheduler)

To schedule live data capture every 10 minutes without keeping a terminal open:

1. Open **Task Scheduler** → _Create Basic Task_
2. **Program/script**: `C:\...\env_data_detective\Scripts\python.exe`
3. **Add arguments**: `2.SCRIPTS\recopilacion\streaming_master.py`
4. **Start in** _(mandatory)_: `C:\...\Data_Detective\`
5. **Trigger**: Repeat every 10 minutes, indefinitely

---

## 🛠️ Tech Stack

| Category             | Library / Tool        | Purpose                                      |
| -------------------- | --------------------- | -------------------------------------------- |
| **Language**         | Python 3.10+          | Core runtime                                 |
| **Dashboard**        | Streamlit 1.54+       | Interactive web UI, tab layout, caching      |
| **Data processing**  | pandas, pyarrow, numpy | DataFrames, Parquet I/O, ETL, statistics    |
| **Visualization**    | Plotly                | Interactive time-series, bar, radar charts   |
| **Geospatial**       | Folium                | Leaflet.js maps embedded in Streamlit        |
| **PDF generation**   | fpdf2                 | Formatted PDF export with tables and headers |
| **HTTP / APIs**      | requests, aiohttp     | REST API calls with retry and backoff        |
| **Web scraping**     | BeautifulSoup4, lxml  | AVAMET and event calendar scraping           |
| **Calendar parsing** | icalendar, icalevents | .ics event feed processing                   |
| **Configuration**    | python-dotenv         | Secure API key management                    |
| **Scheduling**       | schedule              | Windows-compatible task scheduling           |
| **Paths**            | pathlib               | Portable cross-version path handling         |
| **Logging**          | logging (stdlib)      | Structured logs with rotation, no `print()`  |
| **Streaming**        | threading (stdlib)    | Background daemon for live data capture      |
| **Auto-refresh**     | streamlit-autorefresh | Dashboard self-refresh every 5 minutes       |
| **Testing**          | pytest                | Integration test suite for dashboard         |
| **Version control**  | Git                   | Conventional commits spec                    |

---

## 🧪 Testing

The project includes an integration test suite that validates core dashboard components without requiring real data files or network access.

```powershell
# Run all tests
pytest tests/ -v

# Or use the bundled runner
python tests/run_tests.py
```

**What's covered:**
- Air quality index calculation (0–10 scale, grade labels, edge cases)
- Data loader functions (schema validation, column normalization)
- Configuration integrity (WHO/EU thresholds, station mappings, 19 districts)

Tests use mock DataFrames and patch `st.cache_data` so they run fully offline.

---

## 🖼️ Screenshots

> Run the dashboard with `streamlit run 5.DASHBOARD/app.py` and capture each tab. Save images to `docs/screenshots/`.

|                    Air Quality Tab                     |            Event Impact Analysis            |
| :----------------------------------------------------: | :-----------------------------------------: |
| ![Air Quality](docs/screenshots/tab_contaminacion.png) | ![Events](docs/screenshots/tab_eventos.png) |

|        Neighborhood Ranking & Alerts         |               Historical Comparator                |
| :------------------------------------------: | :------------------------------------------------: |
| ![Ranking](docs/screenshots/tab_ranking.png) | ![Comparator](docs/screenshots/tab_comparador.png) |

|                Precipitation & Climatology                 |                   72h Forecast                   |
| :--------------------------------------------------------: | :----------------------------------------------: |
| ![Precipitation](docs/screenshots/tab_precipitaciones.png) | ![Forecast](docs/screenshots/tab_pronostico.png) |

---

## 🧪 Methodology: Event Impact Analysis

The analytical core of the project is a **quasi-experimental baseline comparison** implemented in `correlacion_eventos.py`. For each event:

```
1. DEFINE event window  →  [start_date, end_date]

2. BUILD baseline from "comparable days" satisfying ALL of:
     ✓ Same calendar month           (seasonal control)
     ✓ Same day-of-week pattern      (weekly rhythm control)
     ✓ No overlap with any event     (cross-contamination prevention)
     ✓ Precipitation ≤ 5 mm          (weather confound control)
     ✓ data quality == "ok"          (measurement validity)

3. COMPUTE impact metric:
     Δ% = ((mean_event − mean_baseline) / mean_baseline) × 100

4. RECORD meteorological conditions for both windows
     (temperature, precipitation) as descriptive controls
```

This produces a dataset (`impacto_eventos.csv`) with one row per event × pollutant combination, enabling grouped analysis — e.g., whether prolonged high-impact events like Fallas generate sustained NO₂ increases versus short spikes from a single football match.

---

## 📂 Key Output Files

| File                                   | Format  | Records | Description                                            |
| -------------------------------------- | ------- | ------- | ------------------------------------------------------ |
| `contaminacion_normalizada.parquet`    | Parquet | 121K+   | All pollutant readings, normalized and quality-flagged |
| `meteorologia_limpio.csv`              | CSV     | 2.8K+   | Hourly meteorological records                          |
| `trafico_limpio.csv`                   | CSV     | 750+    | Traffic incident records with day-of-week              |
| `impacto_eventos.csv`                  | CSV     | 24      | Event × variable impact pairs (Δ%)                     |
| `contaminacion_media_anual_barrio.csv` | CSV     | 130     | Annual means by district and pollutant                 |
| `tendencias_historicas.csv`            | CSV     | 13      | City-wide annual averages since first available year   |

---

## ⚠️ Disclaimer

This project is an **academic and analytical platform**. It does not provide official health recommendations or regulatory-grade air quality assessments. WHO and EU thresholds are used for contextual comparison only. For official information, consult [GVA Qualitat de l'Aire](https://agroambient.gva.es) and your local health authority.

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Joan V. Oliver Rosell**
Big Data · Data Engineering · Analytics

_Big Data course project — 2026_

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/joan-v-oliver-rosell-84b260257)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?style=flat&logo=gmail)](mailto:joanoliverrosell@gmail.com)

---

<div align="center">

_Built with data — from Valencia 🇪🇸_

</div>
