# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Sidebar - Filtros Globales del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/components/sidebar.py
Autor: Joan | Fecha: 2026
"""

from datetime import datetime
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
            '<p style="font-size:0.85rem;color:#aaa;margin:0;">Valencia - Análisis Urbano</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.divider()
        st.subheader("Filtros Globales")

        filtros = {}
        filtros["anio_min"], filtros["anio_max"] = _filtro_rango_anios(
            df_contam)
        filtros["variable"] = _filtro_variable(df_contam)
        filtros["barrios"] = _filtro_barrios(df_contam)
        filtros["tipos_evento"] = _filtro_tipos_evento(df_eventos)

        st.divider()
        _render_info_datos(df_contam, df_eventos)

        hora_actual = datetime.now().strftime("%H:%M:%S")
        st.caption(f"🔄 Última actualización: {hora_actual}")

        return filtros


def _filtro_rango_anios(df_contam):
    if df_contam is not None and "anio" in df_contam.columns:
        anio_min = int(df_contam["anio"].min())
        anio_max = int(df_contam["anio"].max())
    else:
        anio_min, anio_max = 2020, 2026
    rango = st.slider(
        "Rango de años",
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
        help="NO2 y PM2.5 son los más relevantes para salud urbana.",
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
        disponibles = sorted(
            df_eventos["tipo_evento"].dropna().unique().tolist())
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
    datasets = [
        ("Contaminación", df_contam is not None),
        ("Eventos", df_eventos is not None),
    ]
    for nombre, ok in datasets:
        color = "#2ca02c" if ok else "#d62728"
        icono = "●" if ok else "○"
        estado = "Cargado" if ok else "No disponible"
        st.markdown(
            f'<small><span style="color:{color};">{icono}</span> '
            f'{nombre}: {estado}</small>',
            unsafe_allow_html=True,
        )
    if df_contam is not None and "anio" in df_contam.columns:
        n = len(df_contam)
        rango = f"{df_contam['anio'].min()}-{df_contam['anio'].max()}"
        st.markdown(
            f'<small style="color:#888;">📊 {n:,} registros · {rango}</small>',
            unsafe_allow_html=True,
        )
    st.divider()
    st.markdown(
        '<div style="text-align:center;padding:0.3rem 0;">'
        '<small style="color:#555;">'
        '🔍 Data Detective Valencia<br>'
        'Proyecto Big Data · Universitat de València<br>'
        '2026 · Joan Oliver'
        '</small></div>',
        unsafe_allow_html=True,
    )
