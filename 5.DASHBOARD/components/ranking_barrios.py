# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Ranking de Barrios por Calidad del Aire
==============================================================================
Ruta: 5.DASHBOARD/components/ranking_barrios.py
Autor: Joan | Fecha: 2026

Renderiza una tabla ordenada de barrios de Valencia segun su indice de
calidad del aire (0-10), identificando el contaminante mas problematico
en cada barrio.
"""

import logging
from typing import Optional

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, VARIABLE_COLORS
from utils.quality_index import calcular_indice_calidad, nivel_desde_score

logger = logging.getLogger(__name__)

# Registros minimos para considerar un barrio con datos fiables
MIN_REGISTROS = 100

# Medallas para el podio
_MEDALLAS = {1: "🥇", 2: "🥈", 3: "🥉"}


# ==============================================================================
# FUNCION AUXILIAR: PEOR VARIABLE
# ==============================================================================

def _peor_variable(df_barrio: pd.DataFrame) -> Optional[str]:
    """
    Identifica el contaminante con el ratio mas alto respecto a su umbral OMS.

    Args:
        df_barrio: DataFrame filtrado para un barrio concreto.

    Returns:
        Nombre de la variable mas problematica, o None si sin datos.
    """
    df_valido = (
        df_barrio[df_barrio["calidad_dato"] == "ok"]
        if "calidad_dato" in df_barrio.columns
        else df_barrio
    )

    peor_var: Optional[str] = None
    peor_ratio: float = -1.0

    for variable, umbral in UMBRALES_OMS.items():
        if not umbral:
            continue
        valores = df_valido[df_valido["variable"] == variable]["valor"].dropna()
        if valores.empty:
            continue
        ratio = float(valores.mean()) / umbral
        if ratio > peor_ratio:
            peor_ratio = ratio
            peor_var = variable

    return peor_var


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def render_ranking_barrios(df_contaminacion: pd.DataFrame) -> None:
    """
    Renderiza el ranking de barrios ordenados de mejor a peor calidad del aire.

    Para cada barrio:
      - Calcula el indice 0-10 con calcular_indice_calidad()
      - Identifica el contaminante mas problematico con _peor_variable()
      - Avisa si el barrio tiene menos de MIN_REGISTROS mediciones validas

    Muestra la tabla con st.dataframe y column_config.ProgressColumn para
    el score, mas un encabezado visual con la linea de verificacion rapida.

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion (filtros globales ya aplicados).
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.warning("Sin datos de contaminacion para generar el ranking.")
        return

    if "barrio" not in df_contaminacion.columns:
        st.warning("El DataFrame no contiene la columna 'barrio'.")
        return

    barrios_disponibles = sorted(
        df_contaminacion["barrio"].dropna().unique().tolist()
    )

    if not barrios_disponibles:
        st.info("No hay barrios identificados en los datos filtrados.")
        return

    # --- Calcular indice por barrio ---
    filas = []
    for barrio in barrios_disponibles:
        df_b = df_contaminacion[df_contaminacion["barrio"] == barrio]

        # Contar registros validos
        if "calidad_dato" in df_b.columns:
            n_ok = int((df_b["calidad_dato"] == "ok").sum())
        else:
            n_ok = len(df_b)

        indice = calcular_indice_calidad(df_b, UMBRALES_OMS)
        if indice is None:
            logger.warning(f"[Ranking] Sin indice para barrio '{barrio}'")
            continue

        peor_var = _peor_variable(df_b)

        filas.append({
            "barrio":           barrio,
            "score":            indice["score"],
            "nivel":            indice["nivel"],
            "color":            indice["color"],
            "emoji":            indice["emoji"],
            "peor_variable":    peor_var or "-",
            "n_registros":      n_ok,
            "pocos_datos":      n_ok < MIN_REGISTROS,
        })

    if not filas:
        st.warning("No se pudo calcular el indice para ningun barrio.")
        return

    # Ordenar de mejor (score alto) a peor (score bajo)
    filas.sort(key=lambda x: x["score"], reverse=True)

    logger.info(
        f"[Ranking] {len(filas)} barrios calculados. "
        f"Mejor: {filas[0]['barrio']} ({filas[0]['score']}), "
        f"Peor: {filas[-1]['barrio']} ({filas[-1]['score']})"
    )

    # --- Lineas de verificacion rapida (formato del enunciado) ---
    _render_resumen_rapido(filas)

    st.markdown("")

    # --- Tabla completa con column_config ---
    _render_tabla(filas)

    # --- Aviso barrios con pocos datos ---
    barrios_aviso = [f["barrio"] for f in filas if f["pocos_datos"]]
    if barrios_aviso:
        st.caption(
            f"⚠️ Datos limitados (<{MIN_REGISTROS} registros validos): "
            + ", ".join(barrios_aviso)
            + ". Los scores pueden no ser representativos."
        )


# ==============================================================================
# HELPERS DE RENDERIZADO
# ==============================================================================

def _render_resumen_rapido(filas: list) -> None:
    """
    Muestra una linea compacta por barrio en el formato:
      '🥇 Benimaclet — 7.8/10 🟢 Bueno | Peor: PM2.5'

    Args:
        filas: Lista de dicts de barrios, ordenada de mejor a peor.
    """
    lineas_html = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, f"{i}.")
        color   = fila["color"]
        aviso   = " ⚠️" if fila["pocos_datos"] else ""
        peor_color = VARIABLE_COLORS.get(fila["peor_variable"], "#888")

        lineas_html.append(
            f'<div style="padding:6px 0;border-bottom:1px solid #2a2a3a;">'
            f'<span style="font-size:1.05rem;font-weight:600;">'
            f'{pos_str} {fila["barrio"]}{aviso}</span>'
            f'<span style="color:#888;"> — </span>'
            f'<span style="color:{color};font-weight:700;">'
            f'{fila["score"]:.1f}/10 {fila["emoji"]} {fila["nivel"]}</span>'
            f'<span style="color:#888;"> | Peor: </span>'
            f'<span style="color:{peor_color};font-weight:600;">'
            f'{fila["peor_variable"]}</span>'
            f'</div>'
        )

    st.markdown(
        '<div style="font-family:monospace;padding:4px 0;">'
        + "".join(lineas_html)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_tabla(filas: list) -> None:
    """
    Muestra la tabla completa con st.dataframe y ProgressColumn para el score.

    Args:
        filas: Lista de dicts de barrios, ordenada de mejor a peor.
    """
    registros_tabla = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, str(i))
        aviso   = " ⚠️" if fila["pocos_datos"] else ""
        registros_tabla.append({
            "Pos":             pos_str,
            "Barrio":          fila["barrio"] + aviso,
            "Score":           fila["score"],
            "Nivel":           f'{fila["emoji"]} {fila["nivel"]}',
            "Peor variable":   fila["peor_variable"],
            "Registros validos": fila["n_registros"],
        })

    df_tabla = pd.DataFrame(registros_tabla)

    st.dataframe(
        df_tabla,
        column_config={
            "Pos": st.column_config.TextColumn(
                "Pos",
                width="small",
            ),
            "Barrio": st.column_config.TextColumn(
                "Barrio",
                width="medium",
            ),
            "Score": st.column_config.ProgressColumn(
                "Score (0-10)",
                min_value=0,
                max_value=10,
                format="%.2f",
                width="large",
            ),
            "Nivel": st.column_config.TextColumn(
                "Nivel",
                width="medium",
            ),
            "Peor variable": st.column_config.TextColumn(
                "Peor contaminante",
                width="small",
            ),
            "Registros validos": st.column_config.NumberColumn(
                "Registros",
                format="%d",
                width="small",
            ),
        },
        hide_index=True,
        use_container_width=True,
        height=min(36 + len(filas) * 35 + 36, 400),
    )
