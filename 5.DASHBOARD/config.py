# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Configuracion Centralizada del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/config.py
Autor: Joan | Fecha: 2026
"""

from pathlib import Path

# ==============================================================================
# RUTAS BASE
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATOS_LIMPIOS_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
ESTADISTICAS_DIR = DATOS_LIMPIOS_DIR / "estadisticas"
VISUALIZACIONES_DIR = PROJECT_ROOT / "4.VISUALIZACIONES"
DATOS_CRUDOS_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO"

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
CONTAM_ANUAL_BARRIO_CSV = ESTADISTICAS_DIR / "contaminacion_media_anual_barrio.csv"
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
    "page_icon": "🔍",
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
    "contaminacion": "Contaminacion",
    "precipitaciones": "Precipitaciones",
    "trafico": "Trafico",
    "eventos": "Eventos Masivos",
    "pronostico": "Pronostico 72h",
}
DESCRIPCION_TABS = {
    "contaminacion": (
        "Analisis de calidad del aire en Valencia: NO2, O3, PM10, PM2.5. "
        "Datos historicos desde 1963 y sensores en tiempo real."
    ),
    "precipitaciones": (
        "Registro de precipitaciones y condiciones meteorologicas. "
        "Datos AEMET historicos y streaming OpenWeatherMap."
    ),
    "trafico": (
        "Incidencias de trafico en la red viaria. "
        "Datos DGT para la Comunidad Valenciana."
    ),
    "eventos": (
        "Impacto de eventos masivos (Fallas, Valencia CF, conciertos) "
        "sobre contaminacion y trafico, comparado con baseline."
    ),
    "pronostico": (
        "Pronostico meteorologico de 72 horas con indicadores "
        "heuristicos de riesgo de calidad del aire."
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
    "evento_id", "nombre_evento", "tipo_evento", "variable",
    "impacto_pct", "fecha_inicio", "fecha_fin"
]
ESQUEMA_CONTAM_ANUAL = [
    "anio", "barrio", "variable", "media_anual", "n_registros"
]
ESQUEMA_PRECIP_MENSUAL = ["anio", "mes"]
ESQUEMA_TENDENCIAS = ["anio"]
