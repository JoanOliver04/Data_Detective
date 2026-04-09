# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Carga Centralizada de Datos del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/data_loader.py
Autor: Joan | Fecha: 2026
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st

from config import (
    PROJECT_ROOT,
    CONTAMINACION_PARQUET, METEOROLOGIA_CSV, TRAFICO_CSV,
    IMPACTO_EVENTOS_CSV, CONTAM_ANUAL_BARRIO_CSV,
    PRECIP_MENSUAL_CSV, TENDENCIAS_CSV,
    METEO_DINAMICA_DIR, FORECAST_GLOB_PATTERN,
    CONTAM_DINAMICA_DIR, AQICN_GLOB_PATTERN,
    TRAFICO_DINAMICO_DIR, DGT_GLOB_PATTERN,
    ESTACION_BARRIO_MAP,
    ESQUEMA_CONTAMINACION, ESQUEMA_METEOROLOGIA,
    ESQUEMA_TRAFICO, ESQUEMA_IMPACTO_EVENTOS,
    ESQUEMA_CONTAM_ANUAL,
)

logger = logging.getLogger("DataLoader")


def _validar_columnas(df, columnas_requeridas, nombre_dataset):
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        logger.error(
            f"[{nombre_dataset}] Columnas faltantes: {faltantes}. "
            f"Disponibles: {list(df.columns)}"
        )
        return False
    return True


def _archivo_existe_y_no_vacio(path, nombre):
    if not path.exists():
        logger.warning(f"[{nombre}] Archivo no encontrado: {path}")
        return False
    if path.stat().st_size == 0:
        logger.warning(f"[{nombre}] Archivo vacio: {path}")
        return False
    return True


def _normalizar_col_anio(df):
    """Renombra columna 'ano'/'a\\xf1o' a 'anio' si existe."""
    if "anio" in df.columns:
        return df
    for col in df.columns:
        if col in ("ano", "a\u00f1o", "year"):
            return df.rename(columns={col: "anio"})
    return df


def _safe_to_datetime(df, col):
    """
    Convierte una columna a datetime de forma robusta.
    Maneja strings con timezone (ej: '2024-01-15 12:00:00+00:00'),
    formatos mixtos, y timestamps Unix.
    Si parse_dates de read_csv falla silenciosamente, esto lo fuerza.
    """
    if col not in df.columns:
        return df
    if pd.api.types.is_datetime64_any_dtype(df[col]):
        return df  # Ya es datetime, no hacer nada
    try:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    except Exception:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception as e:
            logger.warning(f"No se pudo parsear columna '{col}': {e}")
    return df


# ==============================================================================
# CARGA DE DATOS PRINCIPALES
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner="Cargando datos de contaminación...")
def cargar_contaminacion():
    nombre = "Contaminacion"
    if not _archivo_existe_y_no_vacio(CONTAMINACION_PARQUET, nombre):
        return None
    try:
        df = pd.read_parquet(CONTAMINACION_PARQUET)
    except Exception as e:
        logger.error(f"[{nombre}] Error Parquet: {e}")
        return None
    if not _validar_columnas(df, ESQUEMA_CONTAMINACION, nombre):
        return None
    df["barrio"] = df["estacion_id"].map(ESTACION_BARRIO_MAP)
    df["anio"] = df["fecha_utc"].dt.year
    logger.info(
        f"[{nombre}] {len(df):,} registros | {df['anio'].min()}-{df['anio'].max()}")
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando datos meteorológicos...")
def cargar_meteorologia():
    nombre = "Meteorologia"
    if not _archivo_existe_y_no_vacio(METEOROLOGIA_CSV, nombre):
        return None
    try:
        df = pd.read_csv(METEOROLOGIA_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error CSV: {e}")
        return None
    if not _validar_columnas(df, ESQUEMA_METEOROLOGIA, nombre):
        return None
    df = _safe_to_datetime(df, "fecha")
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    logger.info(f"[{nombre}] {len(df):,} registros")
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando datos de tráfico...")
def cargar_trafico():
    nombre = "Trafico"
    if not _archivo_existe_y_no_vacio(TRAFICO_CSV, nombre):
        return None
    try:
        df = pd.read_csv(TRAFICO_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error CSV: {e}")
        return None
    if not _validar_columnas(df, ESQUEMA_TRAFICO, nombre):
        return None
    df = _safe_to_datetime(df, "fecha")
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    df["dia_semana"] = df["fecha"].dt.day_name()
    logger.info(f"[{nombre}] {len(df):,} registros")
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando impacto de eventos...")
def cargar_impacto_eventos():
    nombre = "Impacto Eventos"
    if not _archivo_existe_y_no_vacio(IMPACTO_EVENTOS_CSV, nombre):
        return None
    try:
        df = pd.read_csv(IMPACTO_EVENTOS_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error CSV: {e}")
        return None
    if not _validar_columnas(df, ESQUEMA_IMPACTO_EVENTOS, nombre):
        return None
    for col in ["fecha_inicio", "fecha_fin"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    logger.info(
        f"[{nombre}] {len(df):,} filas | {df['evento_id'].nunique()} eventos")
    return df


# ==============================================================================
# CARGA DE ESTADISTICAS AGREGADAS (Fase 5.4)
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner="Cargando estadísticas anuales...")
def cargar_contam_anual_barrio():
    nombre = "Contam. Anual/Barrio"
    if not _archivo_existe_y_no_vacio(CONTAM_ANUAL_BARRIO_CSV, nombre):
        return None
    try:
        df = pd.read_csv(CONTAM_ANUAL_BARRIO_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error: {e}")
        return None
    df = _normalizar_col_anio(df)
    if not _validar_columnas(df, ESQUEMA_CONTAM_ANUAL, nombre):
        return None
    logger.info(f"[{nombre}] {len(df):,} filas")
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando precipitaciones mensuales...")
def cargar_precip_mensual():
    nombre = "Precip. Mensual"
    if not _archivo_existe_y_no_vacio(PRECIP_MENSUAL_CSV, nombre):
        return None
    try:
        df = pd.read_csv(PRECIP_MENSUAL_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error: {e}")
        return None
    df = _normalizar_col_anio(df)
    logger.info(f"[{nombre}] {len(df):,} filas")
    return df


@st.cache_data(ttl=3600, show_spinner="Cargando tendencias históricas...")
def cargar_tendencias():
    nombre = "Tendencias"
    if not _archivo_existe_y_no_vacio(TENDENCIAS_CSV, nombre):
        return None
    try:
        df = pd.read_csv(TENDENCIAS_CSV)
    except Exception as e:
        logger.error(f"[{nombre}] Error: {e}")
        return None
    df = _normalizar_col_anio(df)
    logger.info(f"[{nombre}] {len(df):,} filas")
    return df


# ==============================================================================
# CARGA DE PRONOSTICO (dato dinamico mas reciente)
# ==============================================================================

@st.cache_data(ttl=300, show_spinner="Cargando pronóstico 72h...")
def cargar_pronostico_72h():
    nombre = "Pronostico 72h"
    if not METEO_DINAMICA_DIR.exists():
        logger.warning(f"[{nombre}] Dir no encontrado: {METEO_DINAMICA_DIR}")
        return None
    archivos = sorted(METEO_DINAMICA_DIR.glob(FORECAST_GLOB_PATTERN))
    if not archivos:
        logger.warning(f"[{nombre}] Sin archivos {FORECAST_GLOB_PATTERN}")
        return None
    latest_file = archivos[-1]
    try:
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"[{nombre}] Error: {e}")
        return None

    forecast_list = None
    for key in ["forecast", "pronostico"]:
        container = data.get(key, {})
        if isinstance(container, dict) and "list" in container:
            forecast_list = container["list"]
            break
    if forecast_list is None:
        forecast_list = data.get("list")
    if not forecast_list or not isinstance(forecast_list, list):
        logger.error(f"[{nombre}] Sin lista de pronosticos en JSON")
        return None

    records = []
    for entry in forecast_list[:24]:
        dt_unix = entry.get("dt")
        if dt_unix is None:
            continue
        try:
            fecha = datetime.fromtimestamp(dt_unix, tz=timezone.utc)
        except (OSError, ValueError):
            continue
        main = entry.get("main", {})
        rain = entry.get("rain", {})
        records.append({
            "datetime": fecha,
            "temp_c": main.get("temp"),
            "humidity_pct": main.get("humidity"),
            "rain_mm": rain.get("3h", 0.0),
            "precip_probability_pct": round(entry.get("pop", 0) * 100, 1),
        })
    if not records:
        return None

    df = pd.DataFrame(records)
    metadata = data.get("_metadata", {})
    df.attrs["archivo_fuente"] = latest_file.name
    df.attrs["timestamp_captura"] = metadata.get(
        "timestamp_captura", metadata.get("timestamp_utc", latest_file.stem)
    )
    logger.info(f"[{nombre}] {len(df)} puntos desde {latest_file.name}")
    return df


# ==============================================================================
# CARGA DE DATOS EN TIEMPO REAL (JSONs de streaming)
# ==============================================================================

def _cargar_ultimo_json(directorio: Path, patron: str, nombre: str) -> Optional[dict]:
    """Lee el JSON mas reciente que coincida con el patron glob."""
    if not directorio.exists():
        logger.warning(f"[{nombre}] Dir no encontrado: {directorio}")
        return None
    archivos = sorted(directorio.glob(patron))
    if not archivos:
        logger.warning(f"[{nombre}] Sin archivos {patron}")
        return None
    latest = archivos[-1]
    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_archivo_fuente"] = latest.name
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"[{nombre}] Error leyendo {latest.name}: {e}")
        return None


@st.cache_data(ttl=300, show_spinner=False)
def cargar_contaminacion_realtime() -> Optional[dict]:
    """
    Carga el ultimo JSON de AQICN con datos de calidad del aire en tiempo real.

    Returns:
        Diccionario con claves: timestamp, archivo, estaciones (lista de dicts
        con estacion_id, nombre, barrio, aqi, contaminantes).
        None si no hay datos disponibles.
    """
    nombre = "Contaminación RT"
    data = _cargar_ultimo_json(CONTAM_DINAMICA_DIR, AQICN_GLOB_PATTERN, nombre)
    if data is None:
        return None

    metadata = data.get("_metadata", {})
    estaciones_raw = data.get("estaciones", {})
    if not estaciones_raw:
        logger.warning(f"[{nombre}] Sin estaciones en JSON")
        return None

    estaciones = []
    for est_id, est_data in estaciones_raw.items():
        datos = est_data.get("datos", {})
        iaqi = datos.get("iaqi", {})
        estaciones.append({
            "estacion_id": est_id,
            "nombre": est_data.get("nombre", est_id),
            "barrio": ESTACION_BARRIO_MAP.get(est_id, "Desconocido"),
            "aqi": datos.get("aqi"),
            "dominante": datos.get("dominentpol", ""),
            "no2": iaqi.get("no2", {}).get("v"),
            "o3": iaqi.get("o3", {}).get("v"),
            "pm10": iaqi.get("pm10", {}).get("v"),
            "pm25": iaqi.get("pm25", {}).get("v"),
            "so2": iaqi.get("so2", {}).get("v"),
            "co": iaqi.get("co", {}).get("v"),
            "temp": iaqi.get("t", {}).get("v"),
            "humedad": iaqi.get("h", {}).get("v"),
        })

    result = {
        "timestamp": metadata.get("timestamp_captura", ""),
        "archivo": data.get("_archivo_fuente", ""),
        "estaciones": estaciones,
    }
    logger.info(f"[{nombre}] {len(estaciones)} estaciones desde {result['archivo']}")
    return result


@st.cache_data(ttl=300, show_spinner=False)
def cargar_meteo_realtime() -> Optional[dict]:
    """
    Carga el ultimo JSON de OpenWeatherMap con datos meteorologicos actuales.

    Returns:
        Diccionario con: timestamp, archivo, temp, sensacion, humedad,
        presion, viento_ms, descripcion, icono.
        None si no hay datos disponibles.
    """
    nombre = "Meteorología RT"
    data = _cargar_ultimo_json(METEO_DINAMICA_DIR, FORECAST_GLOB_PATTERN, nombre)
    if data is None:
        return None

    metadata = data.get("_metadata", {})
    # El JSON puede tener "actual" o "weather" como clave para datos actuales
    actual = data.get("actual") or data.get("weather")
    if actual is None:
        logger.warning(f"[{nombre}] Sin datos actuales en JSON")
        return None

    main = actual.get("main", {})
    wind = actual.get("wind", {})
    weather_list = actual.get("weather", [])
    weather_info = weather_list[0] if weather_list else {}

    result = {
        "timestamp": metadata.get("timestamp_captura", ""),
        "archivo": data.get("_archivo_fuente", ""),
        "temp": main.get("temp"),
        "sensacion": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "humedad": main.get("humidity"),
        "presion": main.get("pressure"),
        "viento_ms": wind.get("speed"),
        "viento_racha": wind.get("gust"),
        "descripcion": weather_info.get("description", ""),
        "icono": weather_info.get("icon", ""),
    }
    logger.info(f"[{nombre}] {result['temp']}°C desde {result['archivo']}")
    return result


@st.cache_data(ttl=300, show_spinner=False)
def cargar_trafico_realtime() -> Optional[dict]:
    """
    Carga el ultimo JSON de DGT con incidencias de trafico en tiempo real.
    Filtra solo las incidencias de la Comunitat Valenciana / provincia Valencia.

    Returns:
        Diccionario con: timestamp, archivo, total_espana, incidencias_valencia
        (lista de dicts con id, tipo, severidad, carretera, municipio, causa).
        None si no hay datos disponibles.
    """
    nombre = "Tráfico RT"
    data = _cargar_ultimo_json(TRAFICO_DINAMICO_DIR, DGT_GLOB_PATTERN, nombre)
    if data is None:
        return None

    metadata = data.get("_metadata", {})
    incidencias_raw = data.get("incidencias", [])

    # Filtrar por Comunitat Valenciana o provincia Valencia
    valencia_incs = []
    for inc in incidencias_raw:
        loc = inc.get("localizacion", {})
        punto_from = loc.get("punto_from", {})
        punto_to = loc.get("punto_to", {})
        es_valencia = any(
            p.get("comunidad_autonoma") == "Comunitat Valenciana"
            or p.get("provincia") == "Valencia"
            for p in [punto_from, punto_to]
        )
        if es_valencia:
            valencia_incs.append({
                "id": inc.get("id"),
                "tipo": inc.get("tipo_datex", "").replace("sit:", ""),
                "severidad": inc.get("severidad", "unknown"),
                "causa": inc.get("causa_tipo", ""),
                "carretera": loc.get("carretera", ""),
                "municipio": punto_from.get("municipio", ""),
                "provincia": punto_from.get("provincia", ""),
                "lat": punto_from.get("latitud"),
                "lon": punto_from.get("longitud"),
            })

    result = {
        "timestamp": metadata.get("timestamp_captura", ""),
        "archivo": data.get("_archivo_fuente", ""),
        "total_espana": len(incidencias_raw),
        "incidencias_valencia": valencia_incs,
    }
    logger.info(
        f"[{nombre}] {len(valencia_incs)} incidencias Valencia "
        f"(de {len(incidencias_raw)} totales) desde {result['archivo']}"
    )
    return result


# ==============================================================================
# LECTURA DE HTML PRE-GENERADOS
# ==============================================================================

@st.cache_data(ttl=7200, show_spinner=False)
def leer_html_visualizacion(ruta_str):
    path = Path(ruta_str)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.error(f"[HTML] Error leyendo {path.name}: {e}")
        return None


# ==============================================================================
# DIAGNOSTICO
# ==============================================================================

def diagnostico_datos():
    datasets = {
        "Contaminacion (Parquet)": CONTAMINACION_PARQUET,
        "Meteorologia (CSV)": METEOROLOGIA_CSV,
        "Trafico (CSV)": TRAFICO_CSV,
        "Impacto Eventos (CSV)": IMPACTO_EVENTOS_CSV,
        "Estadisticas: Contam. Anual": CONTAM_ANUAL_BARRIO_CSV,
        "Estadisticas: Precip. Mensual": PRECIP_MENSUAL_CSV,
        "Estadisticas: Tendencias": TENDENCIAS_CSV,
    }
    informe = {}
    for nombre, path in datasets.items():
        existe = path.exists()
        try:
            ruta_rel = str(path.relative_to(PROJECT_ROOT))
        except ValueError:
            ruta_rel = str(path)
        informe[nombre] = {
            "existe": existe,
            "tamanio_kb": round(path.stat().st_size / 1024, 1) if existe else 0,
            "ruta": ruta_rel,
        }
    return informe
