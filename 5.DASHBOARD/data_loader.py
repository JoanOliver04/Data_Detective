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

@st.cache_data(ttl=3600, show_spinner="Cargando datos de contaminacion...")
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


@st.cache_data(ttl=3600, show_spinner="Cargando datos meteorologicos...")
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


@st.cache_data(ttl=3600, show_spinner="Cargando datos de trafico...")
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

@st.cache_data(ttl=3600, show_spinner="Cargando estadisticas anuales...")
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


@st.cache_data(ttl=3600, show_spinner="Cargando tendencias historicas...")
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

@st.cache_data(ttl=300, show_spinner="Cargando pronostico 72h...")
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
