# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.2: KPIs de Contaminacion
==============================================================================
Componente modular que renderiza las metricas principales de calidad
del aire: registros validos, media, comparativa OMS/UE, estaciones.

Ruta: 5.DASHBOARD/components/kpis.py
Autor: Joan | Fecha: 2026
"""

import logging

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, UMBRALES_UE, VARIABLE_COLORS
from utils.quality_index import calcular_indice_calidad, nivel_desde_score

logger = logging.getLogger("KPIs")


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def render_kpis_contaminacion(df: pd.DataFrame, variable: str) -> None:
    """
    Renderiza 4 KPIs para la variable de contaminacion seleccionada.

    Metricas:
        1. Registros validos (calidad_dato == 'ok')
        2. Media de la variable (ug/m3) con delta vs OMS
        3. % por encima del umbral OMS
        4. Numero de estaciones activas

    Args:
        df: DataFrame filtrado de contaminacion (ya con filtros globales).
        variable: Variable seleccionada (NO2, O3, PM10, PM2.5...).
    """
    if df is None or df.empty:
        st.warning(
            "No hay datos de contaminación para los filtros seleccionados.")
        return

    # Filtrar por variable y registros validos
    mask = (df["variable"] == variable)
    if "calidad_dato" in df.columns:
        mask = mask & (df["calidad_dato"] == "ok")
    df_var = df[mask].copy()

    if df_var.empty:
        st.warning(
            f"No hay registros válidos de {variable} en el periodo seleccionado.")
        return

    # --- Calculos ---
    n_registros = len(df_var)
    media = df_var["valor"].mean()
    umbral_oms = UMBRALES_OMS.get(variable)
    umbral_ue = UMBRALES_UE.get(variable)
    n_estaciones = df_var["estacion_id"].nunique()

    # Porcentaje de registros por encima del umbral OMS
    if umbral_oms and not df_var.empty:
        pct_sobre_oms = (df_var["valor"] > umbral_oms).mean() * 100
    else:
        pct_sobre_oms = None

    # Delta: cuanto supera la media al umbral OMS (para el indicador)
    if umbral_oms and pd.notna(media):
        delta_oms = media - umbral_oms
        delta_str = f"{delta_oms:+.1f} vs OMS ({umbral_oms})"
        # En contaminacion: superar OMS es MALO -> delta_color inverso
        delta_color = "inverse"
    else:
        delta_str = None
        delta_color = "off"

    # Color de acento segun variable
    color = VARIABLE_COLORS.get(variable, "#1f77b4")

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {color};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Indicadores de {variable}</h4>'
        f'<small style="color:#888;">Registros con calidad validada</small>'
        f'</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    # KPI 1: Registros validos
    c1.metric(
        label="Registros válidos",
        value=f"{n_registros:,}",
        help=f"Total de mediciones de {variable} con calidad \u2018ok\u2019.",
    )

    # KPI 2: Media con delta OMS
    c2.metric(
        label=f"Media {variable}",
        value=f"{media:.1f} µg/m³" if pd.notna(media) else "-",
        delta=delta_str,
        delta_color=delta_color,
        help=(
            f"Media aritmética de {variable}. "
            f"Umbral OMS: {umbral_oms} \u00b5g/m\u00b3. "
            f"Umbral UE: {umbral_ue} \u00b5g/m\u00b3."
            if umbral_oms else f"Media aritmética de {variable}."
        ),
    )

    # KPI 3: % por encima de OMS
    if pct_sobre_oms is not None:
        # Icono visual de riesgo
        if pct_sobre_oms > 50:
            icono = "ALTO"
        elif pct_sobre_oms > 25:
            icono = "MODERADO"
        else:
            icono = "BAJO"
        c3.metric(
            label=f"% > OMS ({umbral_oms})",
            value=f"{pct_sobre_oms:.1f}%",
            delta=f"Riesgo {icono}",
            delta_color="inverse" if pct_sobre_oms > 25 else "normal",
            help=(
                f"Porcentaje de mediciones que superan el umbral OMS "
                f"de {umbral_oms} \u00b5g/m\u00b3 para {variable}."
            ),
        )
    else:
        c3.metric(
            label="% > OMS",
            value="-",
            help="Sin umbral OMS definido para esta variable.",
        )

    # KPI 4: Estaciones activas
    c4.metric(
        label="Estaciones activas",
        value=str(n_estaciones),
        help="Número de estaciones de medición con datos en el periodo.",
    )

    logger.info(
        f"[KPIs] {variable}: {n_registros:,} registros, "
        f"media={media:.2f}, estaciones={n_estaciones}"
    )


# ==============================================================================
# INDICE SINTETICO DE CALIDAD DEL AIRE
# ==============================================================================

def render_indice_calidad(df: pd.DataFrame) -> None:
    """
    Renderiza el indice sintetico de calidad del aire (0-10) como KPI principal.

    Muestra:
      - Score global con nivel y emoji
      - Barra de progreso visual
      - Expander con desglose por variable

    Args:
        df: DataFrame filtrado de contaminacion (con filtros globales aplicados).
    """
    if df is None or df.empty:
        st.warning("Sin datos para calcular el índice de calidad del aire.")
        return

    indice = calcular_indice_calidad(df, UMBRALES_OMS)

    if indice is None:
        st.warning("Sin registros válidos para calcular el índice de calidad.")
        return

    score  = indice["score"]
    nivel  = indice["nivel"]
    color  = indice["color"]
    emoji  = indice["emoji"]
    detalle = indice["detalle_variables"]

    # --- Cabecera ---
    st.markdown(
        '<div class="section-header">'
        '<h4>Índice de Calidad del Aire</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Score sintético 0-10 basado en umbrales OMS (10 = óptimo)'
        '</p></div>',
        unsafe_allow_html=True,
    )

    # --- Fila principal: score grande + nivel + barra ---
    col_score, col_nivel, col_barra = st.columns([1, 2, 3])

    with col_score:
        st.metric(
            label="Score global",
            value=f"{score:.1f} / 10",
            help=(
                "Media ponderada de los sub-scores por contaminante. "
                "Pesos: NO2 30%, PM2.5 30%, PM10 15%, O3 15%, SO2 5%, CO 5%."
            ),
        )

    with col_nivel:
        st.markdown(
            f'<div style="padding-top:0.6rem;">'
            f'<span style="font-size:2rem;">{emoji}</span> '
            f'<span style="font-size:1.4rem;font-weight:700;color:{color};">'
            f'{nivel}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col_barra:
        st.markdown(
            '<p style="margin-bottom:0.3rem;color:#888;font-size:0.8rem;">'
            'Nivel de calidad</p>',
            unsafe_allow_html=True,
        )
        # Barra de progreso: score/10
        st.progress(
            value=score / 10.0,
            text=f"{score:.1f} / 10",
        )

    # --- Expander: desglose por variable ---
    with st.expander("Desglose por contaminante"):
        if not detalle:
            st.info("Sin datos de desglose disponibles.")
            return

        cols = st.columns(len(detalle))
        for col, (variable, datos) in zip(cols, sorted(detalle.items())):
            var_color = VARIABLE_COLORS.get(variable, "#888")
            var_score = datos["score"]
            var_ratio = datos["ratio"]
            var_media = datos["media"]
            umbral    = UMBRALES_OMS.get(variable, "-")
            var_nivel, var_nivel_color, var_emoji = _nivel_variable(var_score)

            with col:
                st.markdown(
                    f'<div style="border-left:3px solid {var_color};'
                    f'padding-left:8px;margin-bottom:0.5rem;">'
                    f'<b style="color:{var_color};">{variable}</b><br>'
                    f'<span style="font-size:1.3rem;font-weight:700;">'
                    f'{var_score:.1f}</span>'
                    f'<span style="color:#888;font-size:0.75rem;"> / 10</span><br>'
                    f'<span style="color:{var_nivel_color};font-size:0.8rem;">'
                    f'{var_emoji} {var_nivel}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"Media: {var_media:.1f} µg/m³  \n"
                    f"Umbral OMS: {umbral} µg/m³  \n"
                    f"Ratio: {var_ratio:.2f}x"
                )

    logger.info(f"[IndiceCalidad] Renderizado: score={score} nivel={nivel}")


def _nivel_variable(score: float):
    """
    Retorna (nivel, color, emoji) para el score de una variable individual.

    Args:
        score: Score 0-10 de la variable.

    Returns:
        Tupla (nivel, color_hex, emoji).
    """
    return nivel_desde_score(score)
