# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Sistema de Alertas de Contaminacion
==============================================================================
Ruta: 5.DASHBOARD/components/alertas.py
Autor: Joan | Fecha: 2026

Evalua si algun barrio supera los umbrales OMS en el periodo mas reciente
disponible (ultimos 30 dias) y muestra alertas ordenadas por gravedad.

Niveles:
  - Critica  (ratio > 1.5x OMS): st.error  con mensaje 🚨
  - Aviso    (ratio > 1.0x OMS): st.warning con mensaje ⚠️
  - OK       (todo dentro):       st.success con mensaje ✅
"""

import logging
from datetime import timedelta
from typing import List, Dict, Optional

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, VARIABLE_COLORS

logger = logging.getLogger(__name__)

# Maximo de alertas visibles directamente (el resto van al expander)
MAX_ALERTAS_VISIBLES = 5

# Ventana temporal preferida en dias
VENTANA_DIAS = 30

# Ratio a partir del cual la alerta es critica
RATIO_CRITICO = 1.5


# ==============================================================================
# LOGICA DE CALCULO
# ==============================================================================

def _filtrar_periodo_reciente(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Filtra el DataFrame a los ultimos VENTANA_DIAS dias de datos disponibles.

    Parte de la fecha maxima presente en el df (no de hoy), para que el
    dashboard funcione tanto con datos en tiempo real como con historicos.

    Args:
        df: DataFrame de contaminacion con columna 'fecha_utc'.

    Returns:
        Tupla (df_filtrado, descripcion_periodo).
    """
    if "fecha_utc" not in df.columns:
        return df, "periodo completo (sin columna fecha_utc)"

    # Asegurar que fecha_utc es datetime
    col = df["fecha_utc"]
    if not pd.api.types.is_datetime64_any_dtype(col):
        try:
            col = pd.to_datetime(col, utc=True, errors="coerce")
        except Exception:
            return df, "periodo completo (error al parsear fechas)"

    fecha_max = col.max()
    if pd.isna(fecha_max):
        return df, "periodo completo (sin fechas validas)"

    fecha_min_ventana = fecha_max - timedelta(days=VENTANA_DIAS)
    df_filtrado = df[col >= fecha_min_ventana]

    if df_filtrado.empty:
        # Fallback: usar todos los datos disponibles
        logger.warning("[Alertas] Sin datos en los ultimos %d dias; usando todos.", VENTANA_DIAS)
        return df, f"todos los datos disponibles (sin datos recientes)"

    fecha_min_real = col[col >= fecha_min_ventana].min()
    descripcion = (
        f"{fecha_min_real.strftime('%d/%m/%Y')} → "
        f"{fecha_max.strftime('%d/%m/%Y')}"
    )
    return df_filtrado, descripcion


def _calcular_alertas(df_reciente: pd.DataFrame) -> List[Dict]:
    """
    Genera la lista de alertas comparando medias por barrio/variable vs OMS.

    Args:
        df_reciente: DataFrame ya filtrado al periodo de evaluacion.

    Returns:
        Lista de dicts ordenada de mayor a menor ratio (mas grave primero).
        Cada dict contiene: barrio, variable, media, umbral, ratio, critica.
    """
    df_valido = (
        df_reciente[df_reciente["calidad_dato"] == "ok"]
        if "calidad_dato" in df_reciente.columns
        else df_reciente
    )

    if df_valido.empty:
        return []

    # Agrupar por barrio + variable, calcular media
    grupo = (
        df_valido
        .groupby(["barrio", "variable"], as_index=False)["valor"]
        .mean()
        .rename(columns={"valor": "media"})
    )

    alertas = []
    for _, fila in grupo.iterrows():
        barrio   = fila["barrio"]
        variable = fila["variable"]
        media    = float(fila["media"])
        umbral   = UMBRALES_OMS.get(variable)

        if umbral is None or pd.isna(media):
            continue

        ratio = media / umbral
        if ratio <= 1.0:
            continue  # Dentro del umbral, sin alerta

        alertas.append({
            "barrio":   barrio,
            "variable": variable,
            "media":    round(media, 2),
            "umbral":   umbral,
            "ratio":    round(ratio, 3),
            "critica":  ratio >= RATIO_CRITICO,
        })

    # Ordenar: criticas primero, luego por ratio descendente
    alertas.sort(key=lambda a: (not a["critica"], -a["ratio"]))

    logger.info(
        "[Alertas] %d alertas generadas (%d criticas).",
        len(alertas),
        sum(1 for a in alertas if a["critica"]),
    )
    for a in alertas:
        nivel = "CRITICA" if a["critica"] else "AVISO"
        logger.warning(
            "[Alertas] %s | %s | %s: %.1f µg/m³ (OMS: %.1f, ratio: %.2fx)",
            nivel, a["barrio"], a["variable"], a["media"], a["umbral"], a["ratio"],
        )

    return alertas


def _texto_alerta(alerta: Dict) -> str:
    """
    Genera el texto de una alerta individual.

    Args:
        alerta: Dict con barrio, variable, media, umbral, ratio, critica.

    Returns:
        String formateado para mostrar en st.warning / st.error.
    """
    barrio   = alerta["barrio"]
    variable = alerta["variable"]
    media    = alerta["media"]
    umbral   = alerta["umbral"]
    ratio    = alerta["ratio"]

    if alerta["critica"]:
        return (
            f"🚨 ALERTA CRÍTICA: {barrio} — {variable} a {media:.1f} µg/m³ "
            f"(umbral OMS: {umbral:.0f} µg/m³ | {ratio:.1f}x el límite)"
        )
    return (
        f"⚠️ {barrio}: {variable} a {media:.1f} µg/m³ "
        f"(umbral OMS: {umbral:.0f} µg/m³ | {ratio:.2f}x el límite)"
    )


# ==============================================================================
# RENDERIZADO
# ==============================================================================

def render_alertas(df_contaminacion: Optional[pd.DataFrame]) -> None:
    """
    Renderiza el panel de alertas de contaminacion en el dashboard.

    Muestra hasta MAX_ALERTAS_VISIBLES alertas directamente; el resto
    quedan ocultas en un st.expander. Si todo esta dentro de OMS,
    muestra un mensaje de confirmacion verde.

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion
                          (con filtros globales ya aplicados).
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.info("Sin datos de contaminacion para evaluar alertas.")
        return

    if "barrio" not in df_contaminacion.columns:
        st.info("Sin columna 'barrio' en los datos; no se pueden evaluar alertas.")
        return

    # --- Filtrar al periodo reciente ---
    df_reciente, descripcion_periodo = _filtrar_periodo_reciente(df_contaminacion)

    if df_reciente.empty:
        st.warning("Sin datos recientes para evaluar alertas.")
        return

    # --- Calcular alertas ---
    alertas = _calcular_alertas(df_reciente)

    # --- Cabecera del panel ---
    n_barrios = df_reciente["barrio"].nunique() if "barrio" in df_reciente.columns else "?"
    st.caption(
        f"Evaluacion sobre {n_barrios} barrios | Periodo: {descripcion_periodo}"
    )

    # --- Caso: sin alertas ---
    if not alertas:
        st.success(
            "✅ Todos los barrios dentro de los límites OMS "
            f"en el periodo evaluado ({descripcion_periodo})."
        )
        return

    # --- Separar alertas visibles del resto ---
    visibles  = alertas[:MAX_ALERTAS_VISIBLES]
    restantes = alertas[MAX_ALERTAS_VISIBLES:]

    _render_lista_alertas(visibles)

    if restantes:
        with st.expander(
            f"Ver todas las alertas ({len(alertas)} en total — "
            f"{len(restantes)} adicionales)"
        ):
            _render_lista_alertas(restantes)


def _render_lista_alertas(alertas: List[Dict]) -> None:
    """
    Renderiza una lista de alertas usando st.error / st.warning segun gravedad.

    Args:
        alertas: Lista de dicts de alerta, ya ordenada.
    """
    for alerta in alertas:
        texto = _texto_alerta(alerta)
        if alerta["critica"]:
            st.error(texto)
        else:
            st.warning(texto)
