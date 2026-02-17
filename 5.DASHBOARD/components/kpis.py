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
from typing import Optional

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, UMBRALES_UE, VARIABLE_COLORS

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
        st.warning("No hay datos de contaminacion para los filtros seleccionados.")
        return

    # Filtrar por variable y registros validos
    mask = (df["variable"] == variable)
    if "calidad_dato" in df.columns:
        mask = mask & (df["calidad_dato"] == "ok")
    df_var = df[mask].copy()

    if df_var.empty:
        st.warning(f"No hay registros validos de {variable} en el periodo seleccionado.")
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
        label="Registros validos",
        value=f"{n_registros:,}",
        help=f"Total de mediciones de {variable} con calidad 'ok'.",
    )

    # KPI 2: Media con delta OMS
    c2.metric(
        label=f"Media {variable}",
        value=f"{media:.1f} ug/m3" if pd.notna(media) else "-",
        delta=delta_str,
        delta_color=delta_color,
        help=(
            f"Media aritmetica de {variable}. "
            f"Umbral OMS: {umbral_oms} ug/m3. "
            f"Umbral UE: {umbral_ue} ug/m3."
            if umbral_oms else f"Media aritmetica de {variable}."
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
                f"de {umbral_oms} ug/m3 para {variable}."
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
        help="Numero de estaciones de medicion con datos en el periodo.",
    )

    logger.info(
        f"[KPIs] {variable}: {n_registros:,} registros, "
        f"media={media:.2f}, estaciones={n_estaciones}"
    )
