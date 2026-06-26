# CLAUDE.md — Instrucciones para Claude Code

# Proyecto: Data Detective Valencia

# Autor: Joan Oliver | 2026

## Descripción del proyecto

Data Detective es un proyecto académico de Big Data que analiza la calidad del aire,
precipitaciones y tráfico en Valencia (España). Incluye datos históricos (desde 1956)
y captura en tiempo real (streaming). El resultado final es un dashboard interactivo
con Streamlit.

## Estructura de carpetas

```
DATA_DETECTIVE/                    ← RAÍZ DEL PROYECTO (contiene .env)
├── 1.DATOS_EN_CRUDO/              ← Datos sin procesar (NO están en git)
│   ├── historicos/                 ← CSVs descargados de AEMET, GVA, EEA, DGT
│   ├── dinamicos/                 ← JSONs capturados por streaming
│   │   ├── contaminacion/         ← aqicn_*.json
│   │   ├── meteorologia/          ← openweather_*.json, avamet_*.json
│   │   └── trafico/               ← dgt_*.json
│   └── eventos/                   ← Calendarios iCal y datos scraped
├── 2.SCRIPTS/
│   ├── recopilacion/              ← Scripts de captura (streaming + históricos)
│   │   ├── streaming_master.py    ← Orquestador de los 4 scripts de streaming
│   │   ├── streaming_aqicn.py     ← Calidad del aire (API WAQI/AQICN)
│   │   ├── streaming_openweather.py ← Meteorología (API OpenWeatherMap)
│   │   ├── scraping_avamet.py     ← Precipitaciones (scraping AVAMET)
│   │   ├── streaming_dgt.py       ← Tráfico (DATEX II v3.6 de DGT)
│   │   ├── descargar_aemet_historico.py
│   │   ├── descargar_gva_historico.py
│   │   ├── descargar_dgt_historico.py
│   │   ├── procesar_eea_historico.py
│   │   └── eventos_*.py           ← Scrapers de eventos
│   └── procesamiento/             ← ETL (limpieza, normalización, estadísticas)
│       ├── pipeline_etl.py        ← Orquestador ETL (ejecuta 5.1 a 5.5 en orden)
│       ├── normalizar_contaminacion.py  ← Fase 5.1
│       ├── limpiar_meteorologia.py      ← Fase 5.2
│       ├── limpiar_trafico.py           ← Fase 5.3
│       ├── calcular_estadisticas.py     ← Fase 5.4
│       ├── correlacion_eventos.py       ← Fase 5.5
│       ├── clasificar_eventos.py
│       ├── generar_graficos.py          ← Fase 6: genera HTMLs plotly
│       ├── generar_mapas.py             ← Fase 6: genera mapas Folium
│       ├── generar_visualizaciones_eventos.py
│       └── generar_pronostico.py
├── 3.DATOS_LIMPIOS/               ← Datos procesados (NO están en git)
│   ├── contaminacion_normalizada.parquet  ← 121K+ registros
│   ├── meteorologia_limpio.csv            ← 50K+ registros
│   ├── trafico_limpio.csv                 ← 4.7K+ registros
│   ├── impacto_eventos.csv                ← 42 registros
│   └── estadisticas/
│       ├── contaminacion_media_anual_barrio.csv
│       ├── precipitacion_media_mensual.csv
│       └── tendencias_historicas.csv
├── 4.VISUALIZACIONES/             ← HTMLs pre-generados (NO están en git)
│   ├── mapas/                     ← mapa_no2.html, mapa_pm25.html, mapa_trafico.html
│   ├── graficos/                  ← evolucion_no2.html, precipitaciones_anuales.html
│   ├── eventos/                   ← impacto_no2_por_tipo.html, timeline_eventos.html
│   └── pronostico/                ← pronostico_72h.html
├── 5.DASHBOARD/                   ← Dashboard Streamlit
│   ├── app.py                     ← Orquestador principal (main)
│   ├── config.py                  ← Config centralizada (rutas, umbrales, colores)
│   ├── data_loader.py             ← Carga y validación de datos
│   ├── components/                ← Componentes modulares del dashboard
│   │   ├── sidebar.py             ← Filtros globales
│   │   ├── kpis.py                ← Métricas de contaminación
│   │   ├── trends.py              ← Gráficos temporales Plotly
│   │   ├── maps.py                ← Mapas Folium embebidos
│   │   ├── meteorologia.py        ← Tab precipitaciones
│   │   ├── trafico.py             ← Tab tráfico
│   │   ├── eventos.py             ← Tab eventos masivos
│   │   ├── pronostico.py          ← Tab pronóstico 72h
│   │   └── exportar.py            ← Panel exportación CSV/JSON/XML
│   └── utils/                     ← Utilidades del dashboard
│       └── exportador.py          ← Conversión DataFrame → bytes
├── utils/                         ← Utilidades globales del proyecto
│   ├── __init__.py
│   └── paths.py                   ← get_project_root() y rutas centralizadas
├── logs/                          ← Logs de ejecución
├── .env                           ← Claves API (NO en git)
├── requirements.txt
├── README.md
└── CLAUDE.md                      ← Este archivo
```

## Stack tecnológico

- **Lenguaje:** Python 3.10+
- **SO:** Windows 10 (NO usar comandos Linux/bash, usar CMD/PowerShell)
- **Dashboard:** Streamlit 1.54+
- **Datos:** pandas, pyarrow (parquet)
- **Visualización:** Plotly, Folium, streamlit-folium
- **HTTP:** requests, aiohttp
- **Scraping:** BeautifulSoup4, lxml
- **Calendarios:** icalendar, icalevents
- **Config:** python-dotenv
- **Scheduler:** schedule (compatible con Windows Task Scheduler)
- **Entorno virtual:** env_data_detective

## Reglas de código obligatorias

### Estilo y calidad

- Encoding: `# -*- coding: utf-8 -*-` en la primera línea de CADA archivo .py
- Docstrings: estilo Google (con Args, Returns, Raises)
- Logging: SIEMPRE `logging.getLogger(__name__)`, NUNCA `print()` para debug
- Type hints: en todas las funciones públicas
- Guard clause: `if __name__ == "__main__":` en scripts ejecutables
- Formateo: `black` + `ruff` para linting

### Manejo de errores

- Toda lectura de archivo: try/except con logging.error
- Toda llamada API: try/except con reintentos y backoff
- Toda función de dashboard: guard clause si df is None or df.empty → st.warning()
- NUNCA dejar que un error crashee el dashboard entero

### Rutas y portabilidad

- SIEMPRE usar `pathlib.Path`, NUNCA strings con `/` o `\\` hardcoded
- Importar PROJECT_ROOT desde `utils.paths` (no calcular con **file**)
- El archivo `.env` en la raíz sirve como marcador para encontrar PROJECT_ROOT
- Archivos de datos referenciados desde config.py, NUNCA rutas hardcoded en componentes

### Datos y rendimiento

- Datos de contaminación en formato Parquet (rápido, comprimido)
- Caché Streamlit: `@st.cache_data(ttl=...)` en toda carga de datos
- Chunks y generators para archivos > 100MB
- Variables internas: "anio" (sin tilde, para evitar problemas de encoding en código)
- Variables visibles al usuario: "año", "µg/m³" (con acentos y símbolos correctos)

### Seguridad

- API keys SOLO en .env, cargadas con `python-dotenv`
- .env está en .gitignore
- NUNCA hardcodear tokens en código

## Convención de commits (en inglés)

```
feat:     Nueva funcionalidad para el usuario
fix:      Corrección de bug
docs:     Cambios solo en documentación
style:    Formato, sin cambio de lógica (NO es CSS)
refactor: Mejora de estructura sin cambiar funcionalidad
perf:     Mejora de rendimiento
test:     Añadir o corregir tests
build:    Cambios en dependencias
ci:       Configuración CI/CD
chore:    Tareas rutinarias (actualizar .gitignore, etc.)
revert:   Revertir commit anterior
```

NO poner "Commit sugerido:" dentro de archivos Python.

## Configuración clave (config.py)

### Estaciones de medición GVA en Valencia

```python
ESTACION_BARRIO_MAP = {
    "46250001": "Quatre Carreres",   # Avd. Francia / Pista de Silla
    "46250004": "Jesus",             # Viveros
    "46250030": "Jesus",             # Molí del Sol
    "46250047": "Benimaclet",        # Politècnic
    "46250050": "Patraix",           # Molí del Sol (zona Patraix)
    "46250054": "Ciutat Vella",      # Centre
}
```

### Coordenadas de estaciones (maps.py)

```python
ESTACION_COORDS = {
    "46250001": {"lat": 39.4561, "lon": -0.3522, "nombre": "Valencia - Pista de Silla"},
    "46250004": {"lat": 39.4600, "lon": -0.3850, "nombre": "Valencia - Viveros"},
    "46250030": {"lat": 39.4580, "lon": -0.3900, "nombre": "Valencia - Moli del Sol"},
    "46250047": {"lat": 39.4830, "lon": -0.3590, "nombre": "Valencia - Benimaclet"},
    "46250050": {"lat": 39.4620, "lon": -0.3920, "nombre": "Valencia - Patraix"},
    "46250054": {"lat": 39.4750, "lon": -0.3760, "nombre": "Valencia - Ciutat Vella"},
}
```

### Umbrales de contaminación

```python
UMBRALES_OMS = {"NO2": 10.0, "O3": 60.0, "PM10": 15.0, "PM2.5": 5.0, "SO2": 40.0, "CO": 4000.0}
UMBRALES_UE  = {"NO2": 40.0, "O3": 120.0, "PM10": 40.0, "PM2.5": 25.0, "SO2": 125.0, "CO": 10000.0}
```

### Variables de contaminación

```python
VARIABLES_CONTAMINACION = ["NO2", "O3", "PM10", "PM2.5", "SO2", "CO"]
VARIABLES_PRINCIPALES = ["NO2", "O3", "PM10", "PM2.5"]
```

## Esquemas de datos (columnas esperadas)

### contaminacion_normalizada.parquet (121K registros)

`fecha_utc` (datetime), `estacion_id` (int), `estacion_nombre` (str), `fuente` (str),
`variable` (str: NO2/O3/PM10/PM2.5/SO2/CO), `valor` (float, µg/m³), `unidad` (str),
`calidad_dato` (str: ok/sospechoso)
→ data_loader añade: `barrio` (str), `anio` (int)

### meteorologia_limpio.csv (50K registros)

`fecha` (date), `hora` (int), `precipitacion_mm` (float), `temp_c` (float),
`humedad_pct` (float), `fuente` (str), `calidad_dato` (str)
→ data_loader añade: `anio` (int), `mes` (int)

### trafico_limpio.csv (4.7K registros)

`fecha` (date), `hora` (int), `ubicacion` (str), `intensidad` (float),
`velocidad` (float), `incidencias` (str), `fuente` (str), `calidad_dato` (str)
→ data_loader añade: `anio` (int), `mes` (int), `dia_semana` (str en inglés)

### impacto_eventos.csv (42 registros)

`evento_id`, `nombre_evento`, `tipo_evento`, `categoria_evento`, `subcategoria_evento`,
`duracion_tipo`, `impacto_esperado`, `impacto_score`, `fecha_inicio`, `fecha_fin`,
`variable`, `media_evento`, `media_baseline`, `impacto_pct`, `n_dias_evento`,
`n_dias_baseline`, `media_temp_evento`, `media_temp_baseline`,
`media_precip_evento`, `media_precip_baseline`, `impacto_trafico_pct`

### tendencias_historicas.csv (13 registros)

`año` (float), `CO_ugm3`, `NO2_ugm3`, `O3_ugm3`, `PM10_ugm3`, `PM2.5_ugm3`, `SO2_ugm3`,
`*_n_registros`, `temp_media_c`, `precipitacion_media_mm`, `humedad_media_pct`

### contaminacion_media_anual_barrio.csv (397 registros)

`año` (int), `barrio` (str), `variable` (str), `media_anual` (float),
`n_registros` (int), `unidad` (str)

## Flujo de datos (pipeline)

```
1. CAPTURA (Fase 3):
   streaming_master.py → ejecuta secuencialmente:
     streaming_aqicn.py      → 1.DATOS_EN_CRUDO/dinamicos/contaminacion/
     streaming_openweather.py → 1.DATOS_EN_CRUDO/dinamicos/meteorologia/
     scraping_avamet.py      → 1.DATOS_EN_CRUDO/dinamicos/meteorologia/
     streaming_dgt.py        → 1.DATOS_EN_CRUDO/dinamicos/trafico/

2. ETL (Fase 5):
   pipeline_etl.py → ejecuta secuencialmente:
     5.1 normalizar_contaminacion.py → contaminacion_normalizada.parquet
     5.2 limpiar_meteorologia.py     → meteorologia_limpio.csv
     5.3 limpiar_trafico.py          → trafico_limpio.csv
     5.4 calcular_estadisticas.py    → estadisticas/*.csv
     5.5 correlacion_eventos.py      → impacto_eventos.csv

3. VISUALIZACIONES (Fase 6):
   generar_mapas.py, generar_graficos.py → 4.VISUALIZACIONES/*.html

4. DASHBOARD (Fase 7):
   streamlit run 5.DASHBOARD/app.py
   → Lee de 3.DATOS_LIMPIOS/ y 4.VISUALIZACIONES/
   → Prioriza HTML pre-generados, fallback a generación dinámica
```

## APIs y fuentes de datos

| Fuente         | Variable .env         | Uso                                |
| -------------- | --------------------- | ---------------------------------- |
| AQICN/WAQI     | `AQI_TOKEN`           | Contaminación tiempo real          |
| OpenWeatherMap | `OPENWEATHER_API_KEY` | Meteorología + pronóstico 72h      |
| AEMET          | `AEMET_API_KEY`       | Datos históricos meteorología      |
| DGT DATEX II   | (sin clave)           | Tráfico tiempo real                |
| AVAMET         | (sin clave, scraping) | Precipitaciones Valencia           |
| GVA            | (sin clave)           | Referencia histórica contaminación |

## Comandos de ejecución

```powershell
# Activar entorno virtual
.\env_data_detective\Scripts\Activate.ps1

# Ejecutar streaming (captura datos nuevos)
python 2.SCRIPTS/recopilacion/streaming_master.py

# Ejecutar ETL completo
python 2.SCRIPTS/procesamiento/pipeline_etl.py

# Generar visualizaciones
python 2.SCRIPTS/procesamiento/generar_mapas.py
python 2.SCRIPTS/procesamiento/generar_graficos.py
python 2.SCRIPTS/procesamiento/generar_visualizaciones_eventos.py
python 2.SCRIPTS/procesamiento/generar_pronostico.py

# Lanzar dashboard
streamlit run 5.DASHBOARD/app.py

# Formatear código
black . && ruff check --fix .

# Tests
pytest
```

## Cosas que NUNCA hacer

- NUNCA usar cron jobs (estamos en Windows, usar Task Scheduler)
- NUNCA usar Docker (proyecto local en Windows)
- NUNCA usar Angular, React u otros frameworks frontend (solo Streamlit)
- NUNCA hardcodear API keys en código fuente
- NUNCA usar `print()` para logging
- NUNCA poner "Commit sugerido:" dentro de archivos Python
- NUNCA usar PySpark, Polars ni Dask (el proyecto usa pandas + pyarrow)
- NUNCA romper la compatibilidad con Windows 10 (paths con \, encoding UTF-8, etc.)
- NUNCA crear archivos fuera de la estructura de carpetas existente sin consultarme

## Cosas a tener en cuenta

- El dashboard usa tema oscuro (plotly_dark, CartoDB dark_matter en Folium)
- Los colores de variables están en config.py VARIABLE_COLORS
- La columna "año" se normaliza internamente a "anio" en data_loader.py (\_normalizar_col_anio)
- Contaminación usa Parquet, todo lo demás CSV
- El mapa tiene 2 estrategias: 1) HTML pre-generado, 2) Folium dinámico (fallback)
- El dashboard tiene 5 tabs: Contaminación, Precipitaciones, Tráfico, Eventos, Pronóstico
- Cada tab incluye panel de exportación al final
- Filtros globales en sidebar: rango años, variable, barrios, tipos evento
