<div align="center">

# 🔍 Data Detective — Valencia

### Urban Intelligence Platform: Air Quality, Weather & Traffic Analysis

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![Folium](https://img.shields.io/badge/Folium-Geospatial_Maps-77B829?style=for-the-badge&logo=leaflet&logoColor=white)](https://python-visualization.github.io/folium/)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows_10-0078D6?style=for-the-badge&logo=windows&logoColor=white)](https://www.microsoft.com/windows)

**A full-cycle data engineering and urban analytics platform that ingests, processes, and visualizes environmental and mobility data from 8+ official sources — spanning 70 years of historical records and real-time sensor feeds — to uncover how mass events like Fallas, football matches, and concerts impact air quality and traffic in Valencia, Spain.**

_Designed and built independently as a portfolio-level demonstration of data engineering, ETL automation, and analytical modeling skills._

[Dashboard Preview](#-dashboard-preview) · [Key Features](#-key-features) · [Architecture](#-architecture) · [Methodology](#-methodology) · [Getting Started](#-getting-started) · [Skills Demonstrated](#-skills-demonstrated)

---

</div>

## 📊 Project at a Glance

| Metric                           | Value                                                           |
| -------------------------------- | --------------------------------------------------------------- |
| **Data sources integrated**      | 8 (APIs, web scraping, XML feeds, .ics calendars)               |
| **Historical depth**             | 70 years (meteorology since 1956, air quality since 1963)       |
| **Real-time stations monitored** | 5 air quality + weather + traffic sensors                       |
| **Pollutants tracked**           | 6 (NO₂, O₃, PM10, PM2.5, SO₂, CO)                             |
| **Event sources**                | 3 (Visit Valencia, City Council, Valencia CF)                   |
| **Streaming frequency**          | Every 5–60 min via Windows Task Scheduler                       |
| **ETL pipeline modules**         | 5 sequential stages with integrity validation                   |
| **Dashboard tabs**               | 5 (Air Quality, Precipitation, Traffic, Event Impact, Forecast) |
| **Export formats**               | 3 (CSV with European `;` separator, JSON with metadata, XML)   |
| **Data provenance tracking**     | Every dataset labeled as Static, Dynamic, or Mixed              |
| **Total Python scripts**         | 37 (collection, processing, visualization, dashboard)           |

## 📌 The Problem

Valencia hosts major events throughout the year — **Fallas** (UNESCO heritage festival with city-wide fireworks), **Valencia CF matches** at Mestalla stadium, large-scale concerts, and seasonal celebrations. Each event brings hundreds of thousands of people into the city, but **what is their measurable impact on air quality and urban mobility?**

This project answers that question with data.

## 🎯 What This Project Does

Data Detective is an **end-to-end data engineering and analytics platform** that:

1. **Collects** data from 8+ heterogeneous sources (REST APIs, web scraping, XML feeds, .ics calendars) — both historical archives dating back to 1956 and real-time sensor streams updated every 5–60 minutes.
2. **Processes** raw data through a modular ETL pipeline — normalizing pollutant concentrations against WHO/EU thresholds, cleaning meteorological records, and parsing traffic feeds.
3. **Correlates** pollution and traffic anomalies with a classified event calendar using a quasi-experimental baseline comparison model.
4. **Visualizes** everything through an interactive Streamlit dashboard with geospatial heatmaps, historical trend analysis, and 72-hour air quality risk forecasts.
5. **Exports** filtered datasets in three formats (CSV, JSON, XML), with full provenance metadata and European locale compatibility.

## 🖼️ Dashboard Preview

<!--
  📸 TO ADD YOUR OWN SCREENSHOTS:
  1. Run the dashboard: streamlit run 5.DASHBOARD/app.py
  2. Take screenshots of each tab
  3. Save them in docs/screenshots/
  4. Uncomment the img lines below and update paths
-->

|                   Air Quality Tab                    |              Event Impact Analysis               |
| :--------------------------------------------------: | :----------------------------------------------: |
| ![Air Quality](docs/screenshots/air_quality_tab.png) | ![Events](docs/screenshots/event_impact_tab.png) |

|                 Precipitation & Forecast                 |               Traffic Overview               |
| :------------------------------------------------------: | :------------------------------------------: |
| ![Precipitation](docs/screenshots/precipitation_tab.png) | ![Traffic](docs/screenshots/traffic_tab.png) |

> **Note:** To generate screenshots, run the dashboard locally and capture each tab. Place images in `docs/screenshots/`.

## ✨ Key Features

- **70+ years of historical depth** — meteorological records from 1956 (AEMET) and air quality data from 1963 (GVA) through 2026
- **Real-time streaming** — automated data capture from 5 air quality stations and 4 live sources via Windows Task Scheduler
- **Multi-source ETL pipeline** — orchestrated sequential execution with independent fault isolation, retry logic, and post-run integrity validation
- **Geospatial heatmaps** — Folium-based pollution and traffic maps by neighborhood, embedded in the dashboard
- **Event impact analysis** — quasi-experimental baseline model quantifying NO₂, PM2.5, and traffic deviation during mass events
- **72-hour air quality forecast** — heuristic risk model combining precipitation probability with atmospheric washout effects
- **WHO/EU threshold monitoring** — every pollutant reading is validated against both WHO and European Union air quality guidelines
- **Data provenance tracking** — every dataset and dashboard section is labeled with its origin type: `STATIC` (historical archives), `DYNAMIC` (real-time streaming), or `MIXED` (combined sources) — giving users immediate context about data freshness and reliability
- **Multi-format data export** — filtered datasets can be downloaded as CSV (European `;` separator with UTF-8 BOM for Excel compatibility), JSON (with project metadata envelope), or XML (sanitized element names) — directly from each dashboard tab
- **Modular architecture** — 37 scripts, each self-contained, documented, and independently executable
- **Production-grade logging** — structured logs with rotation, centralized for monitoring scheduled tasks
- **Full UTF-8 compliance** — proper Spanish character rendering (ñ, tildes, em dashes) throughout the interface, with explicit Unicode escapes to prevent encoding drift

## 🧪 Methodology

### Event Impact Analysis

The core analytical contribution of this project is a **quasi-experimental baseline comparison model** that quantifies how mass events affect urban environmental variables. The methodology is implemented in `correlacion_eventos.py`.

#### How It Works

```
For each event:
  1. DEFINE event window  →  [fecha_inicio, fecha_fin]
  2. BUILD baseline from "comparable days" that satisfy ALL of:
       ✓ Same month            (seasonal control)
       ✓ Same day of week      (weekly pattern control)
       ✓ No overlap with ANY event  (cross-contamination prevention)
       ✓ Precipitation ≤ 5 mm      (weather confound control)
       ✓ Data quality == "ok"       (measurement validity)
  3. COMPUTE impact metric:
       Δ% = ((mean_event − mean_baseline) / mean_baseline) × 100
  4. RECORD meteorological conditions during both periods
       (temperature, precipitation) as descriptive controls
```

#### Event Classification

Events are automatically classified using keyword-based heuristics:

| Dimension           | Categories                | Examples                             |
| ------------------- | ------------------------- | ------------------------------------ |
| **Duration**        | `punctual` · `prolonged`  | Match (2h) vs. Fallas (19 days)      |
| **Expected impact** | `high` · `medium` · `low` | Fallas / Concert vs. Workshop / Talk |

This classification enables grouped analysis — for example, comparing whether prolonged high-impact events like Fallas cause sustained pollution increases versus short spikes from a single football match.

#### Variables Analyzed Per Event

| Category    | Variables                            | Source                 |
| ----------- | ------------------------------------ | ---------------------- |
| Air quality | NO₂, O₃, PM10, PM2.5 (µg/m³)       | GVA + EEA + AQICN      |
| Traffic     | Incident count delta                 | DGT DATEX II           |
| Controls    | Temperature (°C), Precipitation (mm) | AEMET + OpenWeatherMap |

### 72-Hour Air Quality Risk Forecast

The forecast module combines OpenWeatherMap precipitation data with a heuristic risk model:

| Condition                           | Risk Level      | Rationale                                    |
| ----------------------------------- | --------------- | -------------------------------------------- |
| Total rain > 10 mm                  | 🟢 **LOW**      | Atmospheric washout likely clears pollutants |
| Max precipitation probability > 60% | 🟡 **MODERATE** | Partial cleansing possible                   |
| Otherwise                           | 🔴 **HIGH**     | Pollutant accumulation probable              |

## 🏗️ Architecture

```
Data_Detective/
│
├── 1.DATOS_EN_CRUDO/              # Raw Data Lake
│   ├── estaticos/                 #   GVA, AEMET, EEA historical archives
│   ├── dinamicos/                 #   AQICN, OpenWeather, AVAMET, DGT streams
│   └── eventos/                   #   .ics feeds + classified events JSON
│
├── 2.SCRIPTS/
│   ├── recopilacion/              # 11 scripts: APIs, scraping, .ics parsing
│   ├── procesamiento/             # 6 scripts: ETL pipeline + event correlation
│   └── visualizacion/             # 4 scripts: maps, charts, forecast generation
│
├── 3.DATOS_LIMPIOS/               # Curated: Parquet + CSV + aggregated stats
├── 4.VISUALIZACIONES/             # Generated: Folium HTML maps + Plotly charts
│
├── 5.DASHBOARD/                   # Streamlit Application
│   ├── app.py                     #   Main orchestrator + global filters
│   ├── config.py                  #   Centralized paths, thresholds, palettes,
│   │                              #     data origin registry (ORIGEN_DATOS)
│   ├── data_loader.py             #   Cached data loading layer
│   └── components/                #   10 modular UI components:
│       ├── sidebar.py             #     Global filters + data status panel
│       ├── kpis.py                #     Air quality KPI metrics
│       ├── maps.py                #     Folium geospatial maps
│       ├── trends.py              #     Temporal charts + annual trends
│       ├── meteorologia.py        #     Precipitation tab (adaptive granularity)
│       ├── trafico.py             #     Traffic tab (weekly distribution + map)
│       ├── eventos.py             #     Event impact tab (Gantt timeline)
│       ├── pronostico.py          #     72h forecast tab (risk heuristic)
│       ├── exportar.py            #     Export panel (CSV/JSON/XML download)
│       └── utils/
│           ├── exportador.py      #       Format converters (BOM, sanitization)
│           └── formatters.py      #       Number/percentage formatting
│
├── logs/                          # Structured logging (file rotation)
├── .env                           # API keys (git-ignored)
└── requirements.txt
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                      │
├──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┤
│ GVA API  │  AEMET   │   EEA    │  AQICN   │OpenWeath.│  AVAMET  │   DGT    │
│(air '63) │(met '56) │(EU bulk) │(live AQ) │(forecast)│(precip.) │(traffic) │
└────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┘
     │          │          │          │          │          │          │
     ▼          ▼          ▼          ▼          ▼          ▼          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    1.DATOS_EN_CRUDO (Landing Zone)                          │
│                    JSON · CSV · XML · Parquet · .ics                        │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │  pipeline_etl.py (5 sequential stages)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│          ETL: Normalize → Clean → Aggregate → Correlate → Validate          │
│     WHO/EU thresholds · Chunk processing · Baseline comparison model        │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               3.DATOS_LIMPIOS + 4.VISUALIZACIONES                           │
│          Parquet/CSV · Folium heatmaps · Plotly charts                      │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │  streamlit run 5.DASHBOARD/app.py
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     📊 STREAMLIT DASHBOARD                                  │
│  ┌────────────────┐ ┌─────────────────────────────────────────────────┐    │
│  │ Sidebar        │ │  5 Tabs with origin badges:                     │    │
│  │ ─────────────  │ │                                                 │    │
│  │ Filters        │ │  🏭 Air Quality    [STATIC]                     │    │
│  │ Data Status:   │ │  🌧️ Precipitation  [STATIC]                     │    │
│  │ ✅ Contam.     │ │  🚗 Traffic        [STATIC]                     │    │
│  │    [STATIC]    │ │  🎆 Event Impact   [MIXED]                      │    │
│  │ ✅ Forecast    │ │  🔮 72h Forecast   [DYNAMIC]                    │    │
│  │    [DYNAMIC]   │ │                                                 │    │
│  │ ─────────────  │ │  Each tab includes:                             │    │
│  │ Legend:        │ │  · KPIs · Charts · Maps                         │    │
│  │ 🔵 Static     │ │  · Export panel (CSV / JSON / XML)              │    │
│  │ 🟢 Dynamic    │ │                                                 │    │
│  │ 🟠 Mixed      │ └─────────────────────────────────────────────────┘    │
│  └────────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📡 Data Sources

| Source                            | Type            | Data                                 | Period         | Format             | Auth           | Origin   |
| --------------------------------- | --------------- | ------------------------------------ | -------------- | ------------------ | -------------- | -------- |
| **GVA** (Generalitat Valenciana)  | REST API        | NO₂, O₃, PM10, PM2.5                | 1963–present   | JSON               | None           | Static   |
| **AEMET** OpenData                | REST API        | Temperature, humidity, precipitation | 1956–present   | JSON               | API Key (free) | Static   |
| **European Environment Agency**   | Bulk download   | EU air quality records               | Multi-decade   | Parquet (multi-GB) | None           | Static   |
| **AQICN / WAQI**                  | REST API        | Real-time AQI from 5 stations        | Live           | JSON               | Token (free)   | Dynamic  |
| **OpenWeatherMap**                | REST API        | Current weather + 72h forecast       | Live           | JSON               | API Key (free) | Dynamic  |
| **AVAMET**                        | Web scraping    | Precipitation, temperature           | Live           | HTML → parsed      | None           | Dynamic  |
| **DGT** (Traffic Authority)       | DATEX II        | Traffic intensity, speed, incidents  | Live           | XML                | None           | Dynamic  |
| **Visit Valencia / City Council** | .ics + scraping | Mass events calendar                 | Current season | .ics / HTML        | None           | Mixed    |

## 📊 Interactive Dashboard

The Streamlit dashboard provides five analytical tabs, each with **data origin badges** and **built-in export functionality**:

| Tab                  | Origin        | What It Shows                                                                                                                               |
| -------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **🏭 Air Quality**   | `STATIC`      | Real-time pollutant levels, Folium heatmaps by neighborhood, historical trends with WHO/EU threshold overlays, annual averages per district |
| **🌧️ Precipitation** | `STATIC`      | Current conditions, monthly climatology with standard deviation bands, 70-year precipitation patterns                                       |
| **🚗 Traffic**       | `STATIC`      | Live traffic intensity map, incident feed, comparison against historical baselines                                                          |
| **🎆 Event Impact**  | `MIXED`       | NO₂/PM2.5/traffic deviation during Fallas, Valencia CF matches, and concerts — grouped by event type and expected impact                    |
| **🔮 72h Forecast**  | `DYNAMIC`     | Air quality risk prediction combining precipitation forecasts with atmospheric washout heuristics                                           |

**Sidebar filters** allow slicing all views by date range, neighborhood/district, and specific pollutant. The sidebar also displays **data status** for all five datasets with their origin badges and a color-coded legend.

### Data Provenance Badges

Every tab header and sidebar entry displays a colored badge indicating the data origin:

| Badge          | Color      | Meaning                                                                              |
| -------------- | ---------- | ------------------------------------------------------------------------------------ |
| **`STATIC`**   | 🔵 Blue   | Historical data from bulk archives (GVA, AEMET, EEA). Does not change once ingested. |
| **`DYNAMIC`**  | 🟢 Green  | Real-time streaming data (AQICN, OpenWeatherMap, DGT). Refreshes periodically.       |
| **`MIXED`**    | 🟠 Orange | Combines static baselines with dynamic event feeds.                                  |

This is centralized in `config.py` via the `ORIGEN_DATOS` registry and the `badge_origen_html()` function, ensuring a single source of truth across all components.

### Data Export System

Every tab includes a collapsible **Export Data** panel that allows downloading the currently filtered dataset in three formats:

| Format   | Features                                                                                          |
| -------- | ------------------------------------------------------------------------------------------------- |
| **CSV**  | European `;` separator (comma-as-decimal safe), UTF-8 with BOM (Excel opens accents correctly)    |
| **JSON** | `records` format wrapped in a metadata envelope (`_metadata`, `total_registros`, `columnas`, `datos`) |
| **XML**  | Auto-sanitized element names (e.g., `PM2.5` → `PM2_5`), `<dataset>/<registro>` structure          |

The export panel also shows:

- **Origin badge** — whether the dataset is static, dynamic, or mixed
- **Source attribution** — which official data sources contributed
- **Record count** — number of rows × columns being exported
- **Data preview** — popover with the first 50 rows before downloading

Implementation: `exportar.py` (UI component) + `exportador.py` (format converters).

## 🚀 Getting Started

### Prerequisites

- **Python 3.9+** on Windows 10
- Free API keys from:
  - [AEMET OpenData](https://opendata.aemet.es/centrodedescargas/altaUsuario) — Spanish meteorological data
  - [OpenWeatherMap](https://openweathermap.org/api) — Weather forecasts (1,000 calls/day free)
  - [AQICN](https://aqicn.org/data-platform/token/) — Air quality index

### Installation

```powershell
# Clone the repository
git clone https://github.com/YOUR_USERNAME/Data_Detective.git
cd Data_Detective

# Create and activate virtual environment
python -m venv env_data_detective
.\env_data_detective\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root:

```ini
# .env — NEVER commit this file (already in .gitignore)
AEMET_API_KEY=your_aemet_key_here
OPENWEATHER_API_KEY=your_openweather_key_here
AQICN_TOKEN=your_aqicn_token_here
```

### Quick Run

```powershell
# 1. Collect real-time data (single execution)
python 2.SCRIPTS/recopilacion/streaming_master.py

# 2. Run the full ETL pipeline
python 2.SCRIPTS/procesamiento/pipeline_etl.py

# 3. Generate visualizations
python 2.SCRIPTS/visualizacion/generar_mapas.py
python 2.SCRIPTS/visualizacion/generar_graficos.py

# 4. Launch the dashboard
streamlit run 5.DASHBOARD/app.py
```

### Automating Real-Time Collection (Windows Task Scheduler)

To schedule streaming data capture every 10 minutes:

1. Open **Task Scheduler** → Create Basic Task
2. **Program/script**: `C:\...\env_data_detective\Scripts\python.exe`
3. **Arguments**: `2.SCRIPTS\recopilacion\streaming_master.py`
4. **Start in**: `C:\...\Data_Detective\` _(critical for relative paths)_
5. **Trigger**: Repeat every 10 minutes

## 📋 Project Phases

| Phase | Description                                           | Key Scripts                                                                                             | Status      |
| ----- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ----------- |
| **1** | Repository setup, environment, `.gitignore`           | —                                                                                                       | ✅ Complete |
| **2** | Historical data collection (GVA, AEMET, EEA)          | `descargar_gva_historico.py`, `descargar_aemet_historico.py`, `procesar_eea_historico.py`               | ✅ Complete |
| **3** | Real-time streaming (4 sources orchestrated)          | `streaming_master.py` + 4 individual scripts                                                            | ✅ Complete |
| **4** | Event parsing & heuristic classification              | `eventos_visitvalencia.py`, `eventos_ayuntamiento.py`, `eventos_valenciacf.py`, `clasificar_eventos.py` | ✅ Complete |
| **5** | ETL pipeline (normalize, clean, aggregate, correlate) | `pipeline_etl.py` orchestrating 5 processing scripts                                                    | ✅ Complete |
| **6** | Visualization generation (maps, charts, forecasts)    | `generar_mapas.py`, `generar_graficos.py`, `generar_pronostico.py`                                      | ✅ Complete |
| **7** | Interactive Streamlit dashboard                       | `app.py` + 10 modular components + export system                                                        | ✅ Complete |

## 🛠️ Tech Stack

| Category            | Technologies                                                          |
| ------------------- | --------------------------------------------------------------------- |
| **Language**        | Python 3.x                                                            |
| **Data Processing** | pandas, NumPy (chunk-based processing for multi-GB datasets)          |
| **Data Collection** | requests, BeautifulSoup4, icalevents                                  |
| **Visualization**   | Streamlit, Plotly, Folium, Matplotlib                                 |
| **Geospatial**      | Folium (Leaflet.js) — heatmaps, marker clusters                      |
| **Data Export**      | CSV (BOM UTF-8), JSON (metadata envelope), XML (etree sanitization)  |
| **Automation**      | Windows Task Scheduler, `schedule` library                            |
| **Security**        | python-dotenv (API key management)                                    |
| **Logging**         | Python `logging` module with file rotation                            |
| **Environment**     | Windows 10, virtual environments                                      |
| **Version Control** | Git with [Conventional Commits](https://www.conventionalcommits.org/) |

## 💡 Skills Demonstrated

This project showcases a broad set of **data engineering** and **analytics** competencies:

- **ETL Pipeline Design** — Multi-stage orchestrated pipeline with fault isolation, integrity validation, and structured logging
- **Quasi-Experimental Analysis** — Baseline comparison model with seasonal, weekly, and meteorological controls for causal inference
- **API Integration** — REST APIs with various auth patterns (AEMET, OpenWeatherMap, AQICN), DATEX II XML feeds (DGT), and .ics calendar parsing
- **Web Scraping** — Ethical scraping with custom headers, rate limiting, `robots.txt` compliance, and retry logic
- **Big Data Handling** — Chunk-based processing for multi-GB European Environment Agency datasets; memory-efficient generators
- **Data Cleaning & Normalization** — WHO/EU threshold mapping, outlier detection, temporal alignment across heterogeneous sources with quality flags
- **Data Provenance & Lineage** — Centralized origin registry classifying every dataset as static/dynamic/mixed, with visual badges propagated across sidebar, tab headers, and export panels
- **Multi-Format Data Export** — CSV with European locale (`;` separator, UTF-8 BOM), JSON with metadata envelopes, XML with auto-sanitized element names — all integrated into the dashboard UI
- **Geospatial Analysis** — Folium heatmaps with station-level granularity and neighborhood-based aggregations
- **Dashboard Development** — Production-grade Streamlit app with caching (`@st.cache_data`), 10 modular components, and responsive layout
- **Internationalization (i18n)** — Full Spanish UI with proper Unicode handling (explicit escapes for ñ, tildes, em dashes) to prevent encoding drift across Windows environments
- **Automation** — Windows Task Scheduler integration for continuous data ingestion
- **Security Best Practices** — `.env`-based secret management, comprehensive `.gitignore`, no credentials in code
- **Software Engineering** — Modular architecture, type hints, docstrings, conventional commits, reproducible environments

## ⚠️ Disclaimer

This project is an **analytical and educational platform**. It does not provide official health recommendations or regulatory-grade air quality assessments. Pollutant thresholds referenced (WHO, EU) are used for contextual analysis only. For official air quality data and health advisories, consult [GVA Qualitat de l'Aire](https://agroambient.gva.es) and your local health authority.

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Joan V. Oliver Rosell** — Data Engineering & Analytics

If you'd like to discuss this project or potential opportunities, feel free to reach out.

<!-- Add your contact links here:
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat&logo=linkedin)](https://linkedin.com/in/YOUR_PROFILE)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?style=flat&logo=gmail)](mailto:your@email.com)
-->

---

<div align="center">

_Built with curiosity and data — from Valencia 🇪🇸_

</div>
