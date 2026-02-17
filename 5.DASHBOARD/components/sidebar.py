# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Sidebar - Filtros Globales del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/components/sidebar.py
Autor: Joan | Fecha: 2026
"""

from typing import Dict, Any, Optional, List
import pandas as pd
import streamlit as st
from config import VARIABLES_PRINCIPALES, ESTACION_BARRIO_MAP

FiltrosGlobales = Dict[str, Any]


def render_sidebar(
    df_contam: Optional[pd.DataFrame],
    df_eventos: Optional[pd.DataFrame],
) -> FiltrosGlobales:
    """
    Renderiza el sidebar y retorna filtros seleccionados.
    Retorna dict con: anio_min, anio_max, variable, barrios, tipos_evento.
    """
    with st.sidebar:
        st.markdown(
            '<div style="text-align:center;padding:0.5rem 0 1rem 0;">'
            '<h1 style="font-size:1.6rem;margin-bottom:0.2rem;">Data Detective</h1>'
            '<p style="font-size:0.85rem;color:#aaa;margin:0;">Valencia - Analisis Urbano</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.divider()
        st.subheader("Filtros Globales")

        filtros = {}
        filtros["anio_min"], filtros["anio_max"] = _filtro_rango_anios(df_contam)
        filtros["variable"] = _filtro_variable(df_contam)
        filtros["barrios"] = _filtro_barrios(df_contam)
        filtros["tipos_evento"] = _filtro_tipos_evento(df_eventos)

        st.divider()
        _render_info_datos(df_contam, df_eventos)
        return filtros


def _filtro_rango_anios(df_contam):
    if df_contam is not None and "anio" in df_contam.columns:
        anio_min = int(df_contam["anio"].min())
        anio_max = int(df_contam["anio"].max())
    else:
        anio_min, anio_max = 2020, 2026
    rango = st.slider(
        "Rango de anios",
        min_value=anio_min, max_value=anio_max,
        value=(anio_min, anio_max),
        help="Filtra datos por periodo temporal.",
    )
    return rango[0], rango[1]


def _filtro_variable(df_contam):
    if df_contam is not None and "variable" in df_contam.columns:
        disponibles = sorted(df_contam["variable"].unique().tolist())
        opciones = [v for v in VARIABLES_PRINCIPALES if v in disponibles]
        if not opciones:
            opciones = disponibles
    else:
        opciones = VARIABLES_PRINCIPALES
    return st.selectbox(
        "Variable contaminante", options=opciones, index=0,
        help="NO2 y PM2.5 son los mas relevantes para salud urbana.",
    )


def _filtro_barrios(df_contam):
    if df_contam is not None and "barrio" in df_contam.columns:
        disponibles = sorted(df_contam["barrio"].dropna().unique().tolist())
    else:
        disponibles = sorted(set(ESTACION_BARRIO_MAP.values()))
    return st.multiselect(
        "Distritos", options=disponibles, default=[],
        help="Si no seleccionas ninguno, se muestran todos.",
    )


def _filtro_tipos_evento(df_eventos):
    if df_eventos is not None and "tipo_evento" in df_eventos.columns:
        disponibles = sorted(df_eventos["tipo_evento"].dropna().unique().tolist())
    else:
        disponibles = []
    if not disponibles:
        st.caption("Tipos de evento: sin datos disponibles")
        return []
    return st.multiselect(
        "Tipo de evento", options=disponibles, default=[],
        help="Si no seleccionas ninguno, se muestran todos.",
    )


def _render_info_datos(df_contam, df_eventos):
    st.caption("Estado de los datos")
    for nombre, ok in [("Contaminacion", df_contam is not None), ("Eventos", df_eventos is not None)]:
        icono = "[OK]" if ok else "[--]"
        st.markdown(f"<small>{icono} {nombre}</small>", unsafe_allow_html=True)
    if df_contam is not None and "anio" in df_contam.columns:
        st.markdown(
            f"<small>{len(df_contam):,} registros ({df_contam['anio'].min()}-{df_contam['anio'].max()})</small>",
            unsafe_allow_html=True,
        )
    st.divider()
    st.markdown(
        '<div style="text-align:center;padding:0.3rem 0;">'
        '<small style="color:#666;">Data Detective Valencia<br>Proyecto Big Data - 2026 - Joan</small>'
        '</div>',
        unsafe_allow_html=True,
    )
