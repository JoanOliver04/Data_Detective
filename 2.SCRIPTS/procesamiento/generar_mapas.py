# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 6.1: Generación de Mapas de Calor con Folium
==============================================================================

Descripción:
    Genera 3 mapas coropléticos (choropleth) de la ciudad de Valencia:
      1) NO₂ — Media anual por distrito (último año disponible)
      2) PM2.5 — Media anual por distrito (último año disponible)
      3) Tráfico — Conteo diario medio de incidencias por distrito

    Cada mapa se exporta como archivo HTML interactivo con:
      - Escala de colores accesible (YlOrRd)
      - Tooltips con nombre del distrito, valor y nº de registros
      - Leyenda automática
      - Capa base OpenStreetMap

    El join geoespacial entre datos tabulares y polígonos se realiza
    por nombre de distrito normalizado (lowercase, sin tildes).

Datos de entrada:
    1. 3.DATOS_LIMPIOS/estadisticas/contaminacion_media_anual_barrio.csv
    2. 3.DATOS_LIMPIOS/trafico_limpio.csv
    3. 1.DATOS_EN_CRUDO/geo/barrios_valencia.geojson
       (si no existe, se autogenera con polígonos simplificados
        de los distritos con estaciones de medición)

Datos de salida:
    4.VISUALIZACIONES/mapas/mapa_no2.html
    4.VISUALIZACIONES/mapas/mapa_pm25.html
    4.VISUALIZACIONES/mapas/mapa_trafico.html

Ruta esperada del script:
    2.SCRIPTS/procesamiento/generar_mapas.py

Uso:
    python generar_mapas.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd
import folium


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Entrada ---
CONTAM_STATS_PATH = (
    PROJECT_ROOT / "3.DATOS_LIMPIOS" / "estadisticas"
    / "contaminacion_media_anual_barrio.csv"
)
TRAFICO_PATH = PROJECT_ROOT / "3.DATOS_LIMPIOS" / "trafico_limpio.csv"
GEOJSON_PATH = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / \
    "geo" / "barrios_valencia.geojson"

# --- Salida ---
MAPAS_DIR = PROJECT_ROOT / "4.VISUALIZACIONES" / "mapas"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# --- Coordenadas centro de Valencia ---
VALENCIA_CENTER = [39.4699, -0.3763]
ZOOM_START = 13

# --- Variables de contaminación a mapear ---
POLLUTION_VARIABLES = {
    "NO2": {
        "output_file": "mapa_no2.html",
        "title": "NO\u2082 \u2014 Media anual por distrito",
        "legend": "NO\u2082 (\u00b5g/m\u00b3)",
        "color_scale": "YlOrRd",
    },
    "PM2.5": {
        "output_file": "mapa_pm25.html",
        "title": "PM2.5 \u2014 Media anual por distrito",
        "legend": "PM2.5 (\u00b5g/m\u00b3)",
        "color_scale": "YlOrRd",
    },
}

# ==============================================================================
# GEOJSON EMBEBIDO — DISTRITOS DE VALENCIA
# ==============================================================================
# Polígonos simplificados de los distritos de Valencia donde tenemos
# estaciones de medición de calidad del aire (ESTACION_BARRIO_MAP en
# calcular_estadisticas.py):
#
#   Estación 46250001  →  Quatre Carreres  (Avda. Francia)
#   Estación 46250004  →  Jesús            (Pista de Silla)
#   Estación 46250030  →  Jesús            (Pista de Silla actual)
#   Estación 46250047  →  Benimaclet       (Politècnic UPV)
#   Estación 46250050  →  Patraix          (Molí del Sol)
#   Estación 46250054  →  Ciutat Vella     (Conselleria Meteo)
#
# Además se incluyen distritos complementarios para enriquecer el
# mapa de tráfico (Camins al Grau, L'Eixample, Poblats Marítims,
# Extramurs, Campanar).
#
# Los vértices están tomados de OpenStreetMap y simplificados a ~8-12
# puntos por polígono. El CRS es EPSG:4326 (WGS84 lat/lon).
#
# Para un GeoJSON completo con los 19 distritos a resolución oficial,
# se puede descargar desde:
#   https://valencia.opendatasoft.com → Distritos de Valencia
#   https://geoportal.valencia.es → Descarga de cartografía
#
# Si se descarga un GeoJSON oficial, basta con colocarlo en:
#   1.DATOS_EN_CRUDO/geo/barrios_valencia.geojson
# y este script lo usará automáticamente en lugar del embebido.
# La propiedad de nombre debe llamarse "nombre" dentro de "properties".

DISTRITOS_VALENCIA_GEOJSON = {
    "type": "FeatureCollection",
    "crs": {
        "type": "name",
        "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}
    },
    "features": [
        {
            "type": "Feature",
            "properties": {"nombre": "Ciutat Vella", "codigo": "01"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3860, 39.4780], [-0.3780, 39.4790],
                    [-0.3720, 39.4760], [-0.3700, 39.4720],
                    [-0.3730, 39.4680], [-0.3800, 39.4670],
                    [-0.3850, 39.4700], [-0.3870, 39.4740],
                    [-0.3860, 39.4780],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "L'Eixample", "codigo": "02"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3850, 39.4700], [-0.3800, 39.4670],
                    [-0.3730, 39.4680], [-0.3680, 39.4650],
                    [-0.3650, 39.4610], [-0.3720, 39.4580],
                    [-0.3810, 39.4590], [-0.3880, 39.4630],
                    [-0.3870, 39.4670], [-0.3850, 39.4700],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Extramurs", "codigo": "03"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3950, 39.4740], [-0.3870, 39.4740],
                    [-0.3850, 39.4700], [-0.3880, 39.4630],
                    [-0.3960, 39.4600], [-0.4020, 39.4640],
                    [-0.4010, 39.4700], [-0.3950, 39.4740],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Campanar", "codigo": "04"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.4010, 39.4870], [-0.3900, 39.4870],
                    [-0.3860, 39.4820], [-0.3870, 39.4770],
                    [-0.3950, 39.4740], [-0.4010, 39.4760],
                    [-0.4060, 39.4800], [-0.4050, 39.4850],
                    [-0.4010, 39.4870],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Camins al Grau", "codigo": "12"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3700, 39.4720], [-0.3620, 39.4740],
                    [-0.3530, 39.4710], [-0.3500, 39.4660],
                    [-0.3550, 39.4620], [-0.3650, 39.4610],
                    [-0.3680, 39.4650], [-0.3730, 39.4680],
                    [-0.3700, 39.4720],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Quatre Carreres", "codigo": "10"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3650, 39.4610], [-0.3550, 39.4620],
                    [-0.3450, 39.4570], [-0.3420, 39.4500],
                    [-0.3480, 39.4430], [-0.3580, 39.4400],
                    [-0.3680, 39.4430], [-0.3740, 39.4500],
                    [-0.3720, 39.4580], [-0.3650, 39.4610],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Poblats Maritims", "codigo": "11"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3500, 39.4660], [-0.3380, 39.4680],
                    [-0.3250, 39.4620], [-0.3200, 39.4540],
                    [-0.3280, 39.4470], [-0.3420, 39.4500],
                    [-0.3450, 39.4570], [-0.3530, 39.4610],
                    [-0.3500, 39.4660],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Jesus", "codigo": "08"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3880, 39.4630], [-0.3810, 39.4590],
                    [-0.3720, 39.4580], [-0.3740, 39.4500],
                    [-0.3780, 39.4440], [-0.3880, 39.4420],
                    [-0.3960, 39.4470], [-0.3990, 39.4540],
                    [-0.3960, 39.4600], [-0.3880, 39.4630],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Patraix", "codigo": "09"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.4020, 39.4640], [-0.3960, 39.4600],
                    [-0.3990, 39.4540], [-0.3960, 39.4470],
                    [-0.4050, 39.4440], [-0.4130, 39.4480],
                    [-0.4140, 39.4560], [-0.4080, 39.4620],
                    [-0.4020, 39.4640],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"nombre": "Benimaclet", "codigo": "14"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-0.3780, 39.4900], [-0.3680, 39.4910],
                    [-0.3600, 39.4870], [-0.3580, 39.4820],
                    [-0.3620, 39.4780], [-0.3720, 39.4770],
                    [-0.3780, 39.4790], [-0.3810, 39.4840],
                    [-0.3780, 39.4900],
                ]]
            }
        },
    ]
}


# ==============================================================================
# MAPEO TRÁFICO → DISTRITO
# ==============================================================================
# La columna 'ubicacion' de trafico_limpio.csv tiene formato:
#   "carretera | municipio | provincia"
#
# Este mapeo heurístico asigna palabras clave que aparecen en la
# ubicación del tráfico al distrito más probable.  Es una aproximación
# razonable para un proyecto académico; un join geoespacial real
# requeriría coordenadas lat/lon que el endpoint DGT SituationPublication
# no siempre proporciona.
#
# Prioridad: se busca en orden; la primera coincidencia gana.

TRAFICO_UBICACION_KEYWORDS = [
    # (keyword_en_ubicacion_lower, distrito_asignado)
    ("pista de silla", "Jesus"),
    ("pista silla", "Jesus"),
    ("v-30", "Jesus"),
    ("francia", "Quatre Carreres"),
    ("nazaret", "Poblats Maritims"),
    ("malvarrosa", "Poblats Maritims"),
    ("puerto", "Poblats Maritims"),
    ("port", "Poblats Maritims"),
    ("grau", "Camins al Grau"),
    ("serreria", "Camins al Grau"),
    ("blasco", "Camins al Grau"),
    ("mestalla", "Benimaclet"),
    ("primat reig", "Benimaclet"),
    ("upv", "Benimaclet"),
    ("politecnic", "Benimaclet"),
    ("patraix", "Patraix"),
    ("moli del sol", "Patraix"),
    ("cid", "Patraix"),
    ("tres forques", "Patraix"),
    ("extramurs", "Extramurs"),
    ("guillem de castro", "Extramurs"),
    ("angel guimera", "Extramurs"),
    ("turia", "Ciutat Vella"),
    ("colon", "Ciutat Vella"),
    ("xativa", "Ciutat Vella"),
    ("estacio", "Ciutat Vella"),
    ("plaza del ayuntamiento", "Ciutat Vella"),
    ("ayuntamiento", "Ciutat Vella"),
    ("campanar", "Campanar"),
    ("mislata", "Campanar"),
    ("ademuz", "Campanar"),
    ("burjassot", "Campanar"),
    ("eixample", "L'Eixample"),
    ("gran via", "L'Eixample"),
    ("ruzafa", "L'Eixample"),
    ("russafa", "L'Eixample"),
    ("quatre carreres", "Quatre Carreres"),
    ("ballester", "Quatre Carreres"),
    ("en corts", "Quatre Carreres"),
    # Fallback: "Valencia" genérico → Ciutat Vella (centro)
    ("valencia", "Ciutat Vella"),
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola).
    Mismo patrón que fases anteriores del proyecto.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "generar_mapas.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Generar_Mapas")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# UTILIDADES DE NORMALIZACIÓN
# ==============================================================================

def _normalize_name(name: str) -> str:
    """
    Normaliza un nombre de distrito para que el join entre datos
    tabulares y propiedades del GeoJSON sea robusto.

    Transformaciones:
      1. Lowercase
      2. Eliminar tildes/diacríticos (á→a, ü→u, etc.)
      3. Strip espacios laterales

    Ejemplos:
      "Jesús"        → "jesus"
      "Quatre Carreres" → "quatre carreres"
      "Ciutat Vella" → "ciutat vella"
      "L'Eixample"   → "l'eixample"

    Args:
        name: Nombre original

    Returns:
        Nombre normalizado
    """
    if not isinstance(name, str):
        return ""
    # NFKD descompone los caracteres acentuados en base + combinación
    nfkd = unicodedata.normalize("NFKD", name)
    # Filtrar solo caracteres que no son marcas combinadas (Mn)
    sin_tildes = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    return sin_tildes.lower().strip()


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

def load_data(logger: logging.Logger) -> Tuple[
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    Optional[dict]
]:
    """
    Carga los tres datasets necesarios para generar los mapas.

    Returns:
        Tupla: (df_contam_stats, df_trafico, geojson_dict)
        Cualquiera puede ser None si falla la carga.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 1: Carga de datos")
    logger.info("=" * 60)

    df_contam = None
    df_trafico = None
    geojson = None

    # --- 1A: Estadísticas de contaminación por barrio ---
    logger.info(f"  1A: Contaminación → {CONTAM_STATS_PATH.name}")
    if CONTAM_STATS_PATH.exists():
        try:
            df_contam = pd.read_csv(CONTAM_STATS_PATH)
            logger.info(f"      {len(df_contam):,} filas cargadas")
            logger.info(f"      Columnas: {list(df_contam.columns)}")
            logger.info(
                f"      Barrios: {sorted(df_contam['barrio'].unique())}"
            )
            logger.info(
                f"      Variables: {sorted(df_contam['variable'].unique())}"
            )
            logger.info(
                f"      Años: {df_contam['año'].min()} → {df_contam['año'].max()}"
            )
        except Exception as e:
            logger.error(f"      Error leyendo CSV: {e}")
    else:
        logger.warning(
            f"      No encontrado: {CONTAM_STATS_PATH}\n"
            f"      Ejecuta primero calcular_estadisticas.py (Fase 5.4)"
        )

    # --- 1B: Tráfico limpio ---
    logger.info(f"  1B: Tráfico → {TRAFICO_PATH.name}")
    if TRAFICO_PATH.exists():
        try:
            df_trafico = pd.read_csv(TRAFICO_PATH, parse_dates=["fecha"])
            logger.info(f"      {len(df_trafico):,} registros cargados")
        except Exception as e:
            logger.error(f"      Error leyendo CSV: {e}")
    else:
        logger.warning(
            f"      No encontrado: {TRAFICO_PATH}\n"
            f"      Ejecuta primero limpiar_trafico.py (Fase 5.3)"
        )

    # --- 1C: GeoJSON ---
    logger.info(f"  1C: GeoJSON → {GEOJSON_PATH.name}")
    if GEOJSON_PATH.exists():
        try:
            with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
                geojson = json.load(f)
            n_features = len(geojson.get("features", []))
            logger.info(
                f"      {n_features} polígonos cargados (archivo externo)")
        except Exception as e:
            logger.error(f"      Error leyendo GeoJSON: {e}")
    else:
        logger.info(
            "      Archivo no encontrado → usando GeoJSON embebido "
            "(distritos simplificados)"
        )
        geojson = DISTRITOS_VALENCIA_GEOJSON

        # Guardar copia en disco para referencia y reutilización
        try:
            GEOJSON_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(GEOJSON_PATH, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, indent=2)
            logger.info(f"      Guardado en: {GEOJSON_PATH}")
        except Exception as e:
            logger.warning(f"      No se pudo guardar GeoJSON: {e}")

    # Listar nombres de distritos en el GeoJSON
    if geojson:
        nombres = [
            f.get("properties", {}).get("nombre", "?")
            for f in geojson.get("features", [])
        ]
        logger.info(f"      Distritos en GeoJSON: {nombres}")

    return df_contam, df_trafico, geojson


# ==============================================================================
# ASIGNACIÓN DE DISTRITO PARA TRÁFICO
# ==============================================================================

def _assign_traffic_distrito(ubicacion: str) -> Optional[str]:
    """
    Asigna un distrito a una incidencia de tráfico basándose en
    keywords presentes en la cadena de ubicación.

    El campo 'ubicacion' de trafico_limpio.csv tiene formato:
      "carretera | municipio | provincia"

    Se busca la primera coincidencia en TRAFICO_UBICACION_KEYWORDS.

    Args:
        ubicacion: Cadena de ubicación del tráfico

    Returns:
        Nombre del distrito asignado, o None si no coincide nada
    """
    if not isinstance(ubicacion, str):
        return None

    ubicacion_lower = ubicacion.lower()

    for keyword, distrito in TRAFICO_UBICACION_KEYWORDS:
        if keyword in ubicacion_lower:
            return distrito

    return None


def prepare_traffic_by_distrito(
    df_trafico: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[pd.DataFrame]:
    """
    Agrega incidencias de tráfico por distrito.

    Flujo:
    1. Asigna distrito a cada incidencia (heurística por keywords)
    2. Cuenta incidencias totales por distrito
    3. Calcula media diaria (total / días únicos con datos)

    Args:
        df_trafico: DataFrame de tráfico limpio
        logger: Logger

    Returns:
        DataFrame con columnas: [distrito, n_incidencias, media_diaria]
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("Preparación de datos de tráfico por distrito")
    logger.info("─" * 40)

    df = df_trafico.copy()

    # Paso 1: Asignar distrito
    df["distrito"] = df["ubicacion"].apply(_assign_traffic_distrito)

    n_total = len(df)
    n_asignados = df["distrito"].notna().sum()
    n_sin_distrito = n_total - n_asignados

    logger.info(f"  Incidencias totales:   {n_total:,}")
    logger.info(
        f"  Asignadas a distrito:  {n_asignados:,} ({n_asignados/n_total*100:.1f}%)")
    logger.info(f"  Sin distrito (excluidas): {n_sin_distrito:,}")

    if n_asignados == 0:
        logger.warning("  Ninguna incidencia pudo asignarse a un distrito")
        return None

    # Paso 2: Filtrar y agrupar
    df_ok = df[df["distrito"].notna()].copy()

    # Contar días únicos con datos (para media diaria)
    df_ok["fecha_dia"] = pd.to_datetime(df_ok["fecha"], utc=True).dt.date
    n_dias_totales = df_ok["fecha_dia"].nunique()

    trafico_agg = (
        df_ok
        .groupby("distrito", as_index=False)
        .agg(
            n_incidencias=("fecha", "count"),
            n_dias=("fecha_dia", "nunique"),
        )
    )

    # Media diaria = incidencias / días únicos con datos en ese distrito
    trafico_agg["media_diaria"] = (
        trafico_agg["n_incidencias"] / trafico_agg["n_dias"]
    ).round(1)

    logger.info(f"  Días únicos con datos: {n_dias_totales}")
    logger.info(f"  Distritos con tráfico: {len(trafico_agg)}")
    for _, row in trafico_agg.iterrows():
        logger.debug(
            f"    {row['distrito']:>20}: "
            f"{row['n_incidencias']} incidencias, "
            f"{row['media_diaria']:.1f}/día"
        )

    return trafico_agg


# ==============================================================================
# CREACIÓN DE MAPAS DE CONTAMINACIÓN
# ==============================================================================

def create_pollution_map(
    df_contam: pd.DataFrame,
    geojson: dict,
    variable: str,
    config: Dict[str, str],
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un mapa coroplético de una variable de contaminación.

    Flujo:
    1. Filtra por variable y último año disponible
    2. Normaliza nombres de barrios para matching con GeoJSON
    3. Crea mapa base Folium centrado en Valencia
    4. Añade capa Choropleth con la escala de colores
    5. Añade GeoJsonTooltip con nombre, valor y nº registros
    6. Guarda HTML

    Args:
        df_contam: DataFrame de estadísticas de contaminación
        geojson: Diccionario GeoJSON
        variable: Nombre de la variable ("NO2", "PM2.5")
        config: Diccionario con output_file, title, legend, color_scale
        logger: Logger

    Returns:
        Path al archivo HTML guardado, o None si falla
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info(f"Mapa: {config['title']}")
    logger.info("─" * 40)

    # --- 1. Filtrar por variable ---
    df_var = df_contam[df_contam["variable"] == variable].copy()

    if df_var.empty:
        logger.warning(f"  Sin datos para variable '{variable}'")
        return None

    # Último año disponible
    latest_year = df_var["año"].max()
    df_year = df_var[df_var["año"] == latest_year].copy()

    logger.info(f"  Variable: {variable}")
    logger.info(f"  Año seleccionado: {latest_year}")
    logger.info(f"  Barrios con datos: {len(df_year)}")

    if df_year.empty:
        logger.warning(f"  Sin datos para {variable} en {latest_year}")
        return None

    # --- 2. Normalizar nombres para join ---
    # Crear clave normalizada en los datos tabulares
    df_year["barrio_norm"] = df_year["barrio"].apply(_normalize_name)

    # Crear clave normalizada en el GeoJSON (como propiedad adicional)
    # Esto permite que el join funcione aunque haya diferencias de tildes
    geojson_copy = json.loads(json.dumps(geojson))  # Deep copy
    for feature in geojson_copy["features"]:
        nombre_original = feature["properties"].get("nombre", "")
        feature["properties"]["barrio_norm"] = _normalize_name(nombre_original)
        # Inicializar campos para tooltip (se rellenan después)
        feature["properties"]["valor"] = None
        feature["properties"]["n_registros"] = None
        feature["properties"]["info"] = "Sin datos"

    # --- 3. Join: inyectar valores en las properties del GeoJSON ---
    # Esto permite que GeoJsonTooltip muestre datos personalizados
    datos_por_barrio = {
        row["barrio_norm"]: {
            "valor": row["media_anual"],
            "n_registros": int(row["n_registros"]),
        }
        for _, row in df_year.iterrows()
    }

    matched = 0
    for feature in geojson_copy["features"]:
        barrio_norm = feature["properties"]["barrio_norm"]
        if barrio_norm in datos_por_barrio:
            datos = datos_por_barrio[barrio_norm]
            feature["properties"]["valor"] = datos["valor"]
            feature["properties"]["n_registros"] = datos["n_registros"]
            feature["properties"]["info"] = (
                f"{datos['valor']:.1f} \u00b5g/m\u00b3 "
                f"({datos['n_registros']:,} registros)"
            )
            matched += 1

    logger.info(f"  Distritos con join exitoso: {matched}/{len(df_year)}")

    if matched == 0:
        logger.warning(
            "  No se pudo hacer match entre datos y GeoJSON. "
            "Verifica nombres de barrios."
        )
        # Mostrar qué nombres hay en cada lado para debug
        logger.debug(
            f"  Datos:   {sorted(datos_por_barrio.keys())}"
        )
        logger.debug(
            f"  GeoJSON: {sorted(f['properties']['barrio_norm'] for f in geojson_copy['features'])}"
        )
        return None

    # --- 4. Crear mapa Folium ---
    mapa = folium.Map(
        location=VALENCIA_CENTER,
        zoom_start=ZOOM_START,
        tiles="OpenStreetMap",
    )

    # Preparar DataFrame para Choropleth
    # Choropleth necesita: key_on (propiedad del GeoJSON), columns (id + valor)
    df_for_choro = df_year[["barrio_norm", "media_anual"]].copy()

    # --- 5. Añadir capa Choropleth ---
    choropleth = folium.Choropleth(
        geo_data=geojson_copy,
        name=config["title"],
        data=df_for_choro,
        columns=["barrio_norm", "media_anual"],
        key_on="feature.properties.barrio_norm",
        fill_color=config["color_scale"],
        fill_opacity=0.7,
        line_opacity=0.5,
        legend_name=f"{config['legend']} ({latest_year})",
        nan_fill_color="lightgray",
        nan_fill_opacity=0.3,
    )
    choropleth.add_to(mapa)

    # --- 6. Añadir Tooltips ---
    # Los tooltips se añaden sobre la capa GeoJson del choropleth
    tooltip = folium.GeoJsonTooltip(
        fields=["nombre", "info"],
        aliases=["Distrito:", f"{variable}:"],
        localize=True,
        sticky=False,
        style=(
            "background-color: white; "
            "color: #333; "
            "font-family: Arial; "
            "font-size: 13px; "
            "padding: 10px; "
            "border-radius: 4px; "
            "box-shadow: 2px 2px 6px rgba(0,0,0,0.3);"
        ),
    )

    # Acceder a la capa GeoJson interna del Choropleth
    # y añadirle el tooltip
    choropleth.geojson.add_child(tooltip)

    # Añadir control de capas
    folium.LayerControl().add_to(mapa)

    # --- 7. Guardar HTML ---
    MAPAS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = MAPAS_DIR / config["output_file"]

    try:
        mapa.save(str(output_path))
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"  Guardado: {output_path.name} ({file_size_kb:.0f} KB)")
        return output_path
    except Exception as e:
        logger.error(f"  Error guardando mapa: {e}")
        return None


# ==============================================================================
# CREACIÓN DE MAPA DE TRÁFICO
# ==============================================================================

def create_traffic_map(
    df_trafico_agg: pd.DataFrame,
    geojson: dict,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un mapa coroplético de intensidad de tráfico (incidencias/día).

    Flujo:
    1. Normaliza nombres de distritos
    2. Inyecta datos en las properties del GeoJSON (para tooltips)
    3. Crea Choropleth con escala YlOrRd
    4. Añade tooltips con distrito, media diaria y total
    5. Guarda HTML

    Args:
        df_trafico_agg: DataFrame agregado por distrito
        geojson: Diccionario GeoJSON
        logger: Logger

    Returns:
        Path al archivo HTML guardado, o None si falla
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("Mapa: Intensidad de Tráfico por distrito")
    logger.info("─" * 40)

    # --- 1. Normalizar nombres ---
    df_agg = df_trafico_agg.copy()
    df_agg["distrito_norm"] = df_agg["distrito"].apply(_normalize_name)

    geojson_copy = json.loads(json.dumps(geojson))  # Deep copy
    for feature in geojson_copy["features"]:
        nombre_original = feature["properties"].get("nombre", "")
        feature["properties"]["distrito_norm"] = _normalize_name(
            nombre_original)
        feature["properties"]["info_trafico"] = "Sin datos"

    # --- 2. Inyectar datos ---
    datos_por_distrito = {
        row["distrito_norm"]: {
            "media_diaria": row["media_diaria"],
            "n_incidencias": int(row["n_incidencias"]),
            "n_dias": int(row["n_dias"]),
        }
        for _, row in df_agg.iterrows()
    }

    matched = 0
    for feature in geojson_copy["features"]:
        distrito_norm = feature["properties"]["distrito_norm"]
        if distrito_norm in datos_por_distrito:
            datos = datos_por_distrito[distrito_norm]
            feature["properties"]["info_trafico"] = (
                f"{datos['media_diaria']:.1f} inc/d\u00eda "
                f"(total: {datos['n_incidencias']:,}, "
                f"{datos['n_dias']} d\u00edas)"
            )
            matched += 1

    logger.info(f"  Distritos con join exitoso: {matched}/{len(df_agg)}")

    if matched == 0:
        logger.warning("  No se pudo hacer match tráfico-GeoJSON")
        return None

    # --- 3. Crear mapa ---
    mapa = folium.Map(
        location=VALENCIA_CENTER,
        zoom_start=ZOOM_START,
        tiles="OpenStreetMap",
    )

    df_for_choro = df_agg[["distrito_norm", "media_diaria"]].copy()

    choropleth = folium.Choropleth(
        geo_data=geojson_copy,
        name="Tráfico — Incidencias diarias por distrito",
        data=df_for_choro,
        columns=["distrito_norm", "media_diaria"],
        key_on="feature.properties.distrito_norm",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.5,
        legend_name="Incidencias/d\u00eda",
        nan_fill_color="lightgray",
        nan_fill_opacity=0.3,
    )
    choropleth.add_to(mapa)

    # --- 4. Tooltips ---
    tooltip = folium.GeoJsonTooltip(
        fields=["nombre", "info_trafico"],
        aliases=["Distrito:", "Tr\u00e1fico:"],
        localize=True,
        sticky=False,
        style=(
            "background-color: white; "
            "color: #333; "
            "font-family: Arial; "
            "font-size: 13px; "
            "padding: 10px; "
            "border-radius: 4px; "
            "box-shadow: 2px 2px 6px rgba(0,0,0,0.3);"
        ),
    )
    choropleth.geojson.add_child(tooltip)

    folium.LayerControl().add_to(mapa)

    # --- 5. Guardar ---
    MAPAS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = MAPAS_DIR / "mapa_trafico.html"

    try:
        mapa.save(str(output_path))
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"  Guardado: {output_path.name} ({file_size_kb:.0f} KB)")
        return output_path
    except Exception as e:
        logger.error(f"  Error guardando mapa: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la generación de los 3 mapas de calor de Valencia.

    Flujo:
        1. Cargar datos (contaminación stats, tráfico, GeoJSON)
        2. Generar mapa de NO₂
        3. Generar mapa de PM2.5
        4. Preparar datos de tráfico por distrito
        5. Generar mapa de tráfico
        6. Resumen final
    """
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("FASE 6.1: GENERACIÓN DE MAPAS DE CALOR (FOLIUM)")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")

    # ------------------------------------------------------------------
    # 1. Cargar datos
    # ------------------------------------------------------------------
    df_contam, df_trafico, geojson = load_data(logger)

    if geojson is None:
        logger.error("GeoJSON no disponible. No se pueden generar mapas.")
        print("\n\u274c ERROR: Sin GeoJSON. Verifica las rutas.")
        return

    # ------------------------------------------------------------------
    # 2-3. Mapas de contaminación (NO₂ y PM2.5)
    # ------------------------------------------------------------------
    mapas_generados = []
    mapas_fallidos = []

    if df_contam is not None and not df_contam.empty:
        for variable, config in POLLUTION_VARIABLES.items():
            result = create_pollution_map(
                df_contam, geojson, variable, config, logger
            )
            if result:
                mapas_generados.append(result.name)
            else:
                mapas_fallidos.append(config["output_file"])
    else:
        logger.warning("Sin datos de contaminación → mapas NO₂/PM2.5 omitidos")
        mapas_fallidos.extend([c["output_file"]
                              for c in POLLUTION_VARIABLES.values()])

    # ------------------------------------------------------------------
    # 4-5. Mapa de tráfico
    # ------------------------------------------------------------------
    if df_trafico is not None and not df_trafico.empty:
        trafico_agg = prepare_traffic_by_distrito(df_trafico, logger)
        if trafico_agg is not None:
            result = create_traffic_map(trafico_agg, geojson, logger)
            if result:
                mapas_generados.append(result.name)
            else:
                mapas_fallidos.append("mapa_trafico.html")
        else:
            mapas_fallidos.append("mapa_trafico.html")
    else:
        logger.warning("Sin datos de tráfico → mapa de tráfico omitido")
        mapas_fallidos.append("mapa_trafico.html")

    # ------------------------------------------------------------------
    # 6. Resumen final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN FINAL — MAPAS GENERADOS")
    logger.info("=" * 60)
    logger.info(f"  Directorio de salida: {MAPAS_DIR}")
    logger.info(f"  Mapas generados: {len(mapas_generados)}")
    for nombre in mapas_generados:
        logger.info(f"    \u2713 {nombre}")
    if mapas_fallidos:
        logger.info(f"  Mapas fallidos/omitidos: {len(mapas_fallidos)}")
        for nombre in mapas_fallidos:
            logger.info(f"    \u2717 {nombre}")
    logger.info("=" * 60)

    # Mensaje para consola
    if mapas_generados:
        print(
            f"\n\u2705 {len(mapas_generados)} mapas generados en: {MAPAS_DIR}")
        for nombre in mapas_generados:
            print(f"   \u2192 {nombre}")
    else:
        print("\n\u26a0\ufe0f  No se generó ningún mapa. Revisa los logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
