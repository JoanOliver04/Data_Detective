# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Configuracion Centralizada del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/config.py
Autor: Joan | Fecha: 2026
"""

import importlib.util
from pathlib import Path

# Cargamos utils/paths.py por su ruta absoluta para evitar colision de nombres
# con 5.DASHBOARD/utils/ (el paquete utils del dashboard tiene exportador.py).
# NO modificamos sys.path: el dashboard mantiene su propio 'utils' intacto.
_CONFIG_DIR = Path(__file__).resolve().parent          # 5.DASHBOARD/
_PROJECT_ROOT_CANDIDATE = _CONFIG_DIR.parent           # DATA_DETECTIVE/
_PATHS_PY = _PROJECT_ROOT_CANDIDATE / "utils" / "paths.py"

_spec = importlib.util.spec_from_file_location("project_utils.paths", _PATHS_PY)
_paths_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_paths_module)

PROJECT_ROOT: Path = _paths_module.PROJECT_ROOT
DATOS_LIMPIOS_DIR: Path = _paths_module.DATOS_LIMPIOS_DIR
VISUALIZACIONES_DIR: Path = _paths_module.VISUALIZACIONES_DIR
DATOS_CRUDOS_DIR: Path = _paths_module.DATOS_CRUDOS_DIR

# ==============================================================================
# RUTAS BASE
# ==============================================================================
ESTADISTICAS_DIR = DATOS_LIMPIOS_DIR / "estadisticas"

# ==============================================================================
# RUTAS DE DATOS LIMPIOS (Fases 5.1 - 5.5)
# ==============================================================================
CONTAMINACION_PARQUET = DATOS_LIMPIOS_DIR / "contaminacion_normalizada.parquet"
METEOROLOGIA_CSV = DATOS_LIMPIOS_DIR / "meteorologia_limpio.csv"
TRAFICO_CSV = DATOS_LIMPIOS_DIR / "trafico_limpio.csv"
IMPACTO_EVENTOS_CSV = DATOS_LIMPIOS_DIR / "impacto_eventos.csv"

# ==============================================================================
# RUTAS DE ESTADISTICAS AGREGADAS (Fase 5.4)
# ==============================================================================
CONTAM_ANUAL_BARRIO_CSV = ESTADISTICAS_DIR / \
    "contaminacion_media_anual_barrio.csv"
PRECIP_MENSUAL_CSV = ESTADISTICAS_DIR / "precipitacion_media_mensual.csv"
TENDENCIAS_CSV = ESTADISTICAS_DIR / "tendencias_historicas.csv"

# ==============================================================================
# RUTAS DE VISUALIZACIONES PRE-GENERADAS (Fase 6)
# ==============================================================================
MAPAS_DIR = VISUALIZACIONES_DIR / "mapas"
GRAFICOS_DIR = VISUALIZACIONES_DIR / "graficos"
EVENTOS_VIS_DIR = VISUALIZACIONES_DIR / "eventos"
PRONOSTICO_VIS_DIR = VISUALIZACIONES_DIR / "pronostico"

MAPA_NO2_HTML = MAPAS_DIR / "mapa_no2.html"
MAPA_PM25_HTML = MAPAS_DIR / "mapa_pm25.html"
MAPA_TRAFICO_HTML = MAPAS_DIR / "mapa_trafico.html"

GRAFICO_EVOLUCION_NO2_HTML = GRAFICOS_DIR / "evolucion_no2.html"
GRAFICO_PRECIPITACIONES_HTML = GRAFICOS_DIR / "precipitaciones_anuales.html"
GRAFICO_ESTACIONAL_HTML = GRAFICOS_DIR / "comparativa_estacional.html"

VIS_IMPACTO_NO2_HTML = EVENTOS_VIS_DIR / "impacto_no2_por_tipo.html"
VIS_IMPACTO_TRAFICO_HTML = EVENTOS_VIS_DIR / "impacto_trafico_por_tipo.html"
VIS_TIMELINE_HTML = EVENTOS_VIS_DIR / "timeline_eventos.html"

VIS_PRONOSTICO_72H_HTML = PRONOSTICO_VIS_DIR / "pronostico_72h.html"

# ==============================================================================
# RUTAS DE DATOS DINAMICOS
# ==============================================================================
METEO_DINAMICA_DIR = DATOS_CRUDOS_DIR / "dinamicos" / "meteorologia"
FORECAST_GLOB_PATTERN = "openweather_*.json"

# ==============================================================================
# CONFIGURACION DE PAGINA STREAMLIT
# ==============================================================================
PAGE_CONFIG = {
    "page_title": "Data Detective - Valencia",
    "page_icon": "\U0001f50d",
    "layout": "wide",
    "initial_sidebar_state": "expanded",
}

# ==============================================================================
# MAPEO ESTACION -> BARRIO / DISTRITO
# ==============================================================================
ESTACION_BARRIO_MAP = {
    "46250001": "Quatre Carreres",
    "46250004": "Jesus",
    "46250030": "Jesus",
    "46250047": "Benimaclet",
    "46250050": "Patraix",
    "46250054": "Ciutat Vella",
}

# ==============================================================================
# VARIABLES DE CONTAMINACION
# ==============================================================================
VARIABLES_CONTAMINACION = ["NO2", "O3", "PM10", "PM2.5", "SO2", "CO"]
VARIABLES_PRINCIPALES = ["NO2", "O3", "PM10", "PM2.5"]

UMBRALES_OMS = {
    "NO2": 10.0, "O3": 60.0, "PM10": 15.0, "PM2.5": 5.0,
    "SO2": 40.0, "CO": 4000.0,
}
UMBRALES_UE = {
    "NO2": 40.0, "O3": 120.0, "PM10": 40.0, "PM2.5": 25.0,
    "SO2": 125.0, "CO": 10000.0,
}

# ==============================================================================
# COORDENADAS
# ==============================================================================
VALENCIA_CENTER_LAT = 39.4699
VALENCIA_CENTER_LON = -0.3763

# ==============================================================================
# PALETA DE COLORES
# ==============================================================================
COLORS = {
    "primary": "#1f77b4", "secondary": "#ff7f0e",
    "success": "#2ca02c", "danger": "#d62728",
    "warning": "#ffbb33", "info": "#17becf", "muted": "#7f7f7f",
    "no2": "#e74c3c", "o3": "#3498db", "pm10": "#e67e22",
    "pm25": "#9b59b6", "so2": "#1abc9c", "co": "#95a5a6",
}
VARIABLE_COLORS = {
    "NO2": COLORS["no2"], "O3": COLORS["o3"],
    "PM10": COLORS["pm10"], "PM2.5": COLORS["pm25"],
    "SO2": COLORS["so2"], "CO": COLORS["co"],
}

# ==============================================================================
# NOMBRES DE TABS Y DESCRIPCIONES
# ==============================================================================
TAB_NAMES = {
    "contaminacion":  "Contaminación",
    "precipitaciones": "Precipitaciones",
    "trafico":        "Tráfico",
    "eventos":        "Eventos Masivos",
    "comparador":     "📊 Comparador",
    "pronostico":     "Pronóstico 72h",
}
DESCRIPCION_TABS = {
    "contaminacion": (
        "Análisis de calidad del aire en Valencia: NO₂, O₃, PM10, PM2.5. "
        "Datos históricos desde 1963 y sensores en tiempo real."
    ),
    "precipitaciones": (
        "Registro de precipitaciones y condiciones meteorológicas. "
        "Datos AEMET históricos y streaming OpenWeatherMap."
    ),
    "trafico": (
        "Incidencias de tráfico en la red viaria. "
        "Datos DGT para la Comunidad Valenciana."
    ),
    "eventos": (
        "Impacto de eventos masivos (Fallas, Valencia CF, conciertos) "
        "sobre contaminación y tráfico, comparado con baseline."
    ),
    "comparador": (
        "Compara contaminación entre un evento y su baseline, "
        "o entre dos periodos históricos libremente seleccionables."
    ),
    "pronostico": (
        "Pronóstico meteorológico de 72 horas con indicadores "
        "heurísticos de riesgo de calidad del aire."
    ),
}

# ==============================================================================
# ESQUEMAS ESPERADOS (para validacion en data_loader)
# ==============================================================================
ESQUEMA_CONTAMINACION = [
    "fecha_utc", "estacion_id", "variable", "valor", "calidad_dato"
]
ESQUEMA_METEOROLOGIA = [
    "fecha", "precipitacion_mm", "temp_c", "humedad_pct", "calidad_dato"
]
ESQUEMA_TRAFICO = [
    "fecha", "ubicacion", "incidencias", "calidad_dato"
]
ESQUEMA_IMPACTO_EVENTOS = [
    "evento_id",
    "nombre_evento",
    "tipo_evento",              # backward compat
    "categoria_evento",
    "subcategoria_evento",
    "duracion_tipo",
    "impacto_esperado",
    "impacto_score",
    "variable",
    "impacto_pct",
    "impacto_trafico_pct",
    "fecha_inicio",
    "fecha_fin",
]

ESQUEMA_CONTAM_ANUAL = [
    "anio", "barrio", "variable", "media_anual", "n_registros"
]
ESQUEMA_PRECIP_MENSUAL = ["anio", "mes"]
ESQUEMA_TENDENCIAS = ["anio"]
