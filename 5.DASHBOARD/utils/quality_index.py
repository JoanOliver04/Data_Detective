# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utils: Indice de Calidad del Aire (0-10)
==============================================================================
Ruta: 5.DASHBOARD/utils/quality_index.py
Autor: Joan | Fecha: 2026

Calcula un indice sintetico de calidad del aire combinando todos los
contaminantes disponibles mediante media ponderada normalizada con los
umbrales OMS.

Escala: 0 (pesimo) → 10 (excelente)
"""

import logging
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Pesos de cada variable en el indice global (deben sumar 1.0)
PESOS_DEFAULT: Dict[str, float] = {
    "NO2": 0.30,
    "PM2.5": 0.30,
    "PM10": 0.15,
    "O3": 0.15,
    "SO2": 0.05,
    "CO": 0.05,
}

# Niveles por rango de score
_NIVELES = [
    (9.0, "Excelente", "#2ca02c", "🟢"),
    (7.0, "Bueno", "#27ae60", "🟢"),
    (5.0, "Aceptable", "#f39c12", "🟡"),
    (3.0, "Deficiente", "#e67e22", "🟠"),
    (1.0, "Malo", "#e74c3c", "🔴"),
    (0.0, "Peligroso", "#8b0000", "⛔"),
]


def nivel_desde_score(score: float) -> Tuple[str, str, str]:
    """
    Convierte un score numerico en nivel, color y emoji.

    Args:
        score: Valor entre 0.0 y 10.0.

    Returns:
        Tupla (nivel, color_hex, emoji).
    """
    for umbral, nivel, color, emoji in _NIVELES:
        if score >= umbral:
            return nivel, color, emoji
    return "Peligroso", "#8b0000", "⛔"


def calcular_indice_calidad(
    df: Optional[pd.DataFrame],
    umbrales_oms: Dict[str, float],
    pesos: Optional[Dict[str, float]] = None,
) -> Optional[Dict]:
    """
    Calcula el indice sintetico de calidad del aire (0-10).

    Algoritmo:
      1. Para cada variable, filtra registros validos (calidad_dato == 'ok')
         y calcula la media.
      2. ratio = media_variable / umbral_OMS
      3. score_var = max(0, 10 - ratio * 5)
         (ratio=0 → 10, ratio=1 → 5, ratio=2 → 0)
      4. Score global = media ponderada de las variables con datos.
         Los pesos de variables sin datos se redistribuyen proporcionalmente.

    Args:
        df: DataFrame filtrado de contaminacion con columnas
            [variable, valor, calidad_dato].
        umbrales_oms: Diccionario {variable: umbral_oms}.
        pesos: Pesos personalizados. Si None, usa PESOS_DEFAULT.

    Returns:
        Diccionario con claves:
          - "score": float 0-10
          - "nivel": str
          - "color": str (hex)
          - "emoji": str
          - "detalle_variables": dict {variable: {"media": float, "score": float, "ratio": float}}
        Retorna None si no hay datos suficientes.
    """
    if df is None or df.empty:
        logger.warning("[IndiceCalidad] DataFrame vacio o None.")
        return None

    pesos_activos = pesos if pesos is not None else PESOS_DEFAULT.copy()

    # --- Calcular media y score por variable ---
    detalle: Dict[str, Dict] = {}
    df_valido = df[df["calidad_dato"] == "ok"] if "calidad_dato" in df.columns else df

    for variable, umbral in umbrales_oms.items():
        if variable not in pesos_activos:
            continue
        df_var = df_valido[df_valido["variable"] == variable]["valor"].dropna()
        if df_var.empty:
            continue
        media = float(df_var.mean())
        ratio = media / umbral if umbral else 0.0
        score_var = max(0.0, 10.0 - ratio * 5.0)
        detalle[variable] = {
            "media": round(media, 2),
            "ratio": round(ratio, 3),
            "score": round(score_var, 2),
            "peso": pesos_activos[variable],
        }

    if not detalle:
        logger.warning("[IndiceCalidad] Sin variables con datos validos.")
        return None

    # --- Redistribuir pesos de variables ausentes ---
    peso_total = sum(detalle[v]["peso"] for v in detalle)
    if peso_total == 0:
        return None

    score_global = (
        sum(detalle[v]["score"] * detalle[v]["peso"] for v in detalle) / peso_total
    )

    score_global = round(min(10.0, max(0.0, score_global)), 2)
    nivel, color, emoji = nivel_desde_score(score_global)

    logger.info(
        f"[IndiceCalidad] score={score_global} nivel={nivel} "
        f"variables={list(detalle.keys())}"
    )

    return {
        "score": score_global,
        "nivel": nivel,
        "color": color,
        "emoji": emoji,
        "detalle_variables": detalle,
    }
