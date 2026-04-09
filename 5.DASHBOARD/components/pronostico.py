# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.6: Tab Pronostico 72h
==============================================================================
Componente modular que renderiza la seccion de pronostico meteorologico:
  1. KPIs: temp maxima, lluvia total, prob. max. lluvia, nivel de riesgo
  2. Grafico interactivo (HTML pre-generado o mensaje informativo)
  3. Funcion orquestadora render_tab_pronostico()

Heuristica de riesgo de calidad del aire (replica generar_pronostico.py):
  - Lluvia total > 10 mm  -> Riesgo BAJO  (washout atmosferico)
  - Prob. max. precip > 60% -> Riesgo MODERADO (posible limpieza)
  - Caso contrario          -> Riesgo ALTO  (acumulacion probable)

Columnas esperadas del DataFrame (data_loader.cargar_pronostico_72h):
  datetime, temp_c, humidity_pct, rain_mm, precip_probability_pct

Ruta: 5.DASHBOARD/components/pronostico.py
Autor: Joan | Fecha: 2026
"""

import logging

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import (
    TAB_NAMES, DESCRIPCION_TABS, VIS_PRONOSTICO_72H_HTML,
)
from data_loader import leer_html_visualizacion

logger = logging.getLogger("Pronostico")

# Colores de la seccion
COLOR_PRONOSTICO = "#17becf"     # Azul agua (coherente con meteo)

# Umbrales de riesgo (replica generar_pronostico.py)
RAIN_THRESHOLD_LOW = 10.0        # mm totales -> riesgo bajo
POP_THRESHOLD_MOD = 60.0         # % prob. max. -> riesgo moderado

# Colores de riesgo
RIESGO_CONFIG = {
    "BAJO": {"color": "#2ca02c", "emoji": "\U0001f7e2", "desc": "Washout atmosférico probable"},
    "MODERADO": {"color": "#ff7f0e", "emoji": "\U0001f7e1", "desc": "Posible limpieza parcial"},
    "ALTO": {"color": "#d62728", "emoji": "\U0001f534", "desc": "Acumulación de contaminantes probable"},
}

# Altura del grafico embebido (px)
CHART_HEIGHT = 600


# ==============================================================================
# 1. KPIs
# ==============================================================================

def render_kpis_pronostico(df: pd.DataFrame) -> None:
    """
    Renderiza 4 KPIs del pronostico 72h:
      - Temperatura maxima prevista (C)
      - Lluvia total acumulada (mm)
      - Probabilidad maxima de lluvia (%)
      - Nivel de riesgo de calidad del aire

    La heuristica de riesgo replica la logica de generar_pronostico.py:
      lluvia > 10mm -> BAJO | prob > 60% -> MODERADO | else -> ALTO

    Args:
        df: DataFrame de pronostico con columnas:
            temp_c, rain_mm, precip_probability_pct.
    """
    if df is None or df.empty:
        st.warning("No hay datos de pronóstico disponibles.")
        return

    # --- Calculos ---
    temp_max = df["temp_c"].max() if "temp_c" in df.columns else None
    temp_min = df["temp_c"].min() if "temp_c" in df.columns else None
    lluvia_total = df["rain_mm"].sum() if "rain_mm" in df.columns else 0
    prob_max = (
        df["precip_probability_pct"].max()
        if "precip_probability_pct" in df.columns else 0
    )

    # Heuristica de riesgo (misma logica que generar_pronostico.py)
    if lluvia_total > RAIN_THRESHOLD_LOW:
        riesgo = "BAJO"
        razon = (
            f"Lluvia prevista ({lluvia_total:.1f} mm) > "
            f"{RAIN_THRESHOLD_LOW} mm: efecto washout"
        )
    elif prob_max > POP_THRESHOLD_MOD:
        riesgo = "MODERADO"
        razon = (
            f"Prob. máx. precipitación ({prob_max:.0f}%) > "
            f"{POP_THRESHOLD_MOD:.0f}%: posible limpieza parcial"
        )
    else:
        riesgo = "ALTO"
        razon = (
            "Sin lluvia significativa prevista: "
            "acumulación de contaminantes probable"
        )

    cfg = RIESGO_CONFIG[riesgo]

    # --- Metadata ---
    archivo = df.attrs.get("archivo_fuente", "?")
    captura = df.attrs.get("timestamp_captura", "?")

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {COLOR_PRONOSTICO};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Pronóstico próximas 72 horas</h4>'
        f'<small style="color:#888;">'
        f'Fuente: OpenWeatherMap | Archivo: {archivo} | Captura: {captura}'
        f'</small></div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    # KPI 1: Temperatura maxima
    c1.metric(
        label="Temp. máxima",
        value=f"{temp_max:.1f} °C" if pd.notna(temp_max) else "-",
        delta=(
            f"Mín: {temp_min:.1f} °C" if pd.notna(temp_min) else None
        ),
        delta_color="off",
        help="Temperatura máxima y mínima previstas en las próximas 72 horas.",
    )

    # KPI 2: Lluvia total
    c2.metric(
        label="Lluvia total 72h",
        value=f"{lluvia_total:.1f} mm",
        help="Precipitación acumulada prevista para las próximas 72 horas.",
    )

    # KPI 3: Probabilidad maxima
    c3.metric(
        label="Prob. max. lluvia",
        value=f"{prob_max:.0f}%",
        help="Probabilidad máxima de precipitación en cualquier franja de 3h.",
    )

    # KPI 4: Nivel de riesgo
    c4.metric(
        label="Riesgo calidad aire",
        value=f"{cfg['emoji']} {riesgo}",
        delta=razon,
        delta_color="off",
        help=(
            "Indicador heurístico basado en el efecto washout de la lluvia. "
            "BAJO = lluvia limpia el aire. ALTO = sin lluvia, contaminantes se acumulan."
        ),
    )

    logger.info(
        f"[KPIs Pronostico] Tmax={temp_max}, "
        f"Lluvia={lluvia_total:.1f}mm, "
        f"ProbMax={prob_max:.0f}%, Riesgo={riesgo}"
    )


# ==============================================================================
# 2. GRAFICO DE PRONOSTICO
# ==============================================================================

def render_grafico_pronostico(df: pd.DataFrame) -> None:
    """
    Renderiza el grafico de pronostico 72h.

    Estrategia:
      1. Intenta cargar HTML pre-generado (Fase 6.4)
      2. Si no existe, muestra mensaje informativo

    Args:
        df: DataFrame de pronostico (usado para metadata).
    """
    st.markdown(
        '<div class="section-header">'
        '<h4>Evolución prevista 72h</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Temperatura, precipitación y probabilidad de lluvia'
        '</p></div>',
        unsafe_allow_html=True,
    )

    # Estrategia 1: HTML pre-generado
    if VIS_PRONOSTICO_72H_HTML.exists():
        html_content = leer_html_visualizacion(str(VIS_PRONOSTICO_72H_HTML))
        if html_content:
            st.caption(
                f"Gráfico pre-generado: `{VIS_PRONOSTICO_72H_HTML.name}` "
                f"(regenerar con generar_pronostico.py)"
            )
            components.html(html_content, height=CHART_HEIGHT, scrolling=False)
            return

    # Estrategia 2: Mensaje informativo
    st.info(
        "Gráfico de pronóstico no disponible. "
        "Genera la visualización ejecutando:\n\n"
        "`python 2.SCRIPTS/procesamiento/generar_pronostico.py`\n\n"
        "Esto requiere datos de OpenWeatherMap en "
        "`1.DATOS_EN_CRUDO/dinamicos/meteorologia/`."
    )
    logger.warning("[Grafico] pronostico_72h.html no encontrado")


# ==============================================================================
# 3. FUNCION ORQUESTADORA
# ==============================================================================

def render_tab_pronostico(datos: dict) -> None:
    """
    Orquesta la tab completa de Pronostico 72h.

    Estructura:
      1. Header con descripcion
      2. KPIs (4 metricas incluyendo riesgo)
      3. Grafico interactivo pre-generado
      4. Explicacion de la heuristica de riesgo

    Args:
        datos: Diccionario con datos filtrados.
              Claves usadas: 'pronostico'.
    """
    # Header
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["pronostico"]}</h3>'
        f'<p>{DESCRIPCION_TABS["pronostico"]}</p></div>',
        unsafe_allow_html=True,
    )

    df = datos.get("pronostico")

    # Guard clause
    if df is None:
        st.error(
            "Sin datos de pronóstico. "
            "Ejecuta streaming_openweather.py para capturar datos "
            "y generar_pronostico.py para generar la visualización."
        )
        return

    # 1. KPIs
    render_kpis_pronostico(df)

    st.divider()

    # 2. Grafico
    render_grafico_pronostico(df)

    st.divider()

    # 3. Explicacion de la heuristica
    with st.expander("\u00bfCómo se calcula el indicador de riesgo?", expanded=False):
        st.markdown(
            "El indicador de riesgo de calidad del aire se basa en el "
            "**efecto washout** (lavado atmosférico): la lluvia arrastra "
            "partículas contaminantes, limpiando el aire.\n\n"
            f"- **Lluvia total > {RAIN_THRESHOLD_LOW} mm** \u2192 "
            f"Riesgo **BAJO** {RIESGO_CONFIG['BAJO']['emoji']} "
            f"(washout efectivo)\n"
            f"- **Prob. max. > {POP_THRESHOLD_MOD:.0f}%** \u2192 "
            f"Riesgo **MODERADO** {RIESGO_CONFIG['MODERADO']['emoji']} "
            f"(posible limpieza parcial)\n"
            f"- **Sin lluvia significativa** \u2192 "
            f"Riesgo **ALTO** {RIESGO_CONFIG['ALTO']['emoji']} "
            f"(acumulación de contaminantes)\n\n"
            "*Fuente: heurística implementada en generar_pronostico.py, "
            "basada en literatura sobre deposición húmeda de contaminantes.*"
        )
