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
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

from theme import get_theme
from config import (
    TAB_NAMES, DESCRIPCION_TABS, VIS_PRONOSTICO_72H_HTML,
    UMBRALES_OMS, VARIABLE_COLORS,
)
from data_loader import leer_html_visualizacion
from utils.pronostico_estadistico import (
    pronosticar_contaminacion_manana,
    obtener_distribucion_mensual,
)

logger = logging.getLogger("Pronostico")

# Colores de la seccion
COLOR_PRONOSTICO = "#17becf"     # Azul agua (coherente con meteo)

# Umbrales de riesgo (replica generar_pronostico.py)
RAIN_THRESHOLD_LOW = 10.0        # mm totales -> riesgo bajo
POP_THRESHOLD_MOD = 60.0         # % prob. max. -> riesgo moderado

# Colores de riesgo
RIESGO_CONFIG = {
    "BAJO": {"color": "#2ca02c", "emoji": "🟢", "desc": "Washout atmosférico probable"},
    "MODERADO": {"color": "#ff7f0e", "emoji": "🟡", "desc": "Posible limpieza parcial"},
    "ALTO": {"color": "#d62728", "emoji": "🔴", "desc": "Acumulación de contaminantes probable"},
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

    # Estrategia 2: Grafico dinamico desde el DataFrame
    if df is not None and not df.empty and "datetime" in df.columns:
        _generar_grafico_pronostico_dinamico(df)
        return

    # Estrategia 3: Mensaje informativo
    st.info(
        "Gráfico de pronóstico no disponible. "
        "Genera la visualización ejecutando:\n\n"
        "`python 2.SCRIPTS/procesamiento/generar_pronostico.py`\n\n"
        "Esto requiere datos de OpenWeatherMap en "
        "`1.DATOS_EN_CRUDO/dinamicos/meteorologia/`."
    )
    logger.warning("[Grafico] pronostico_72h.html no encontrado")


# ==============================================================================
# 2b. GRAFICO DINAMICO (fallback si no hay HTML pre-generado)
# ==============================================================================

def _generar_grafico_pronostico_dinamico(df: pd.DataFrame) -> None:
    """
    Genera un grafico Plotly con temperatura, precipitacion y probabilidad
    de lluvia a partir del DataFrame de pronostico.

    Se usa como fallback cuando el HTML pre-generado no esta disponible.

    Args:
        df: DataFrame con columnas datetime, temp_c, rain_mm,
            precip_probability_pct, humidity_pct.
    """
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.6, 0.4],
        subplot_titles=("Temperatura (°C)", "Precipitación (mm) y probabilidad (%)"),
    )

    # --- Temperatura ---
    fig.add_trace(go.Scatter(
        x=df["datetime"],
        y=df["temp_c"],
        name="Temperatura",
        mode="lines+markers",
        line=dict(color="#ff7f0e", width=2.5),
        marker=dict(size=4),
        hovertemplate="<b>%{x|%d/%m %H:%M}</b><br>Temp: %{y:.1f}°C<extra></extra>",
    ), row=1, col=1)

    # --- Humedad (eje secundario, mas sutil) ---
    if "humidity_pct" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["datetime"],
            y=df["humidity_pct"],
            name="Humedad",
            mode="lines",
            line=dict(color="#17becf", width=1.5, dash="dot"),
            opacity=0.6,
            hovertemplate="Humedad: %{y:.0f}%<extra></extra>",
        ), row=1, col=1)

    # --- Precipitacion (barras) ---
    if "rain_mm" in df.columns:
        fig.add_trace(go.Bar(
            x=df["datetime"],
            y=df["rain_mm"],
            name="Lluvia (mm)",
            marker_color="rgba(23,190,207,0.7)",
            hovertemplate="Lluvia: %{y:.1f} mm<extra></extra>",
        ), row=2, col=1)

    # --- Probabilidad de precipitacion (linea) ---
    if "precip_probability_pct" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["datetime"],
            y=df["precip_probability_pct"],
            name="Prob. lluvia (%)",
            mode="lines",
            line=dict(color="#3498db", width=2),
            hovertemplate="Prob: %{y:.0f}%<extra></extra>",
        ), row=2, col=1)

    fig.update_layout(
        template=get_theme()["plotly_template"],
        height=550,
        margin=dict(l=60, r=30, t=40, b=40),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        showlegend=True,
    )

    fig.update_yaxes(title_text="°C / %", row=1, col=1)
    fig.update_yaxes(title_text="mm / %", row=2, col=1)
    fig.update_xaxes(title_text="Fecha y hora", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)
    st.caption("Gráfico generado dinámicamente desde los datos de pronóstico.")
    logger.info("[Grafico] Pronostico dinamico: %d puntos", len(df))


# ==============================================================================
# 3. FUNCION ORQUESTADORA
# ==============================================================================

# ==============================================================================
# 4. PRONOSTICO DE CONTAMINACION (MANANA)
# ==============================================================================

# Mapping para nombres bonitos con subindices
_VAR_DISPLAY = {
    "NO2": "NO₂", "O3": "O₃", "PM10": "PM₁₀", "PM2.5": "PM₂.₅",
}

# Emojis de confianza
_CONFIANZA_EMOJI = {
    "Alta": "🟢", "Media": "🟡", "Baja": "🟠",
}


def render_pronostico_contaminacion(
    df_contaminacion: Optional[pd.DataFrame],
    contam_rt: Optional[dict],
) -> None:
    """
    Renderiza la seccion de pronostico de contaminacion para manana.

    Muestra:
      - 4 KPIs (NO2, O3, PM10, PM2.5) con prediccion y confianza
      - Boxplot de contexto historico con punto de prediccion

    Args:
        df_contaminacion: DataFrame de contaminacion historica.
        contam_rt: Datos RT de AQICN (o None).
    """
    manana = datetime.now() + timedelta(days=1)
    dias_es = {
        0: "lunes", 1: "martes", 2: "miércoles", 3: "jueves",
        4: "viernes", 5: "sábado", 6: "domingo",
    }
    meses_es = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }
    dia_str = dias_es.get(manana.weekday(), "")
    mes_str = meses_es.get(manana.month, "")

    st.markdown(
        f'<div style="border-left:4px solid {COLOR_PRONOSTICO};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">📊 Pronóstico de Contaminación (mañana)</h4>'
        f'<small style="color:#888;">'
        f'{dia_str.capitalize()} {manana.day} de {mes_str} — '
        f'Basado en tendencias estadísticas históricas'
        f'</small></div>',
        unsafe_allow_html=True,
    )

    pronostico = pronosticar_contaminacion_manana(
        df_contaminacion, contam_rt, manana,
    )

    if pronostico is None:
        st.info(
            "Sin datos históricos suficientes para generar el pronóstico de "
            "contaminación. Se necesitan datos en contaminacion_normalizada.parquet."
        )
        return

    # --- 4 KPIs ---
    variables_kpi = ["NO2", "O3", "PM10", "PM2.5"]
    vars_con_datos = [v for v in variables_kpi if v in pronostico]

    if not vars_con_datos:
        st.info("Sin variables con datos suficientes para el pronóstico.")
        return

    cols = st.columns(len(vars_con_datos))
    for col, variable in zip(cols, vars_con_datos):
        datos = pronostico[variable]
        pred = datos["prediccion"]
        rango_min = datos["rango_min"]
        rango_max = datos["rango_max"]
        confianza = datos["confianza"]
        tendencia = datos["tendencia"]
        n_reg = datos["n_registros"]
        emoji_conf = _CONFIANZA_EMOJI.get(confianza, "⚪")
        var_display = _VAR_DISPLAY.get(variable, variable)
        color_var = VARIABLE_COLORS.get(variable, "#888")
        umbral_oms = UMBRALES_OMS.get(variable)

        # Delta: comparar con umbral OMS
        if umbral_oms and pred > umbral_oms:
            delta_str = f"Supera OMS ({umbral_oms})"
            delta_color = "inverse"
        elif umbral_oms:
            delta_str = f"Bajo OMS ({umbral_oms})"
            delta_color = "normal"
        else:
            delta_str = None
            delta_color = "off"

        with col:
            st.metric(
                label=f"{var_display} previsto",
                value=f"{pred:.1f} µg/m³",
                delta=delta_str,
                delta_color=delta_color,
                help=(
                    f"Rango esperado: {rango_min:.1f} – {rango_max:.1f} µg/m³\n\n"
                    f"Confianza: {emoji_conf} {confianza} ({n_reg} registros históricos)\n\n"
                    f"Tendencia: {tendencia}\n\n"
                    f"Método: {datos['metodo']}"
                ),
            )
            st.caption(
                f"{emoji_conf} {confianza} · {tendencia}"
            )

    # --- Boxplot de contexto historico ---
    _render_boxplot_contexto(df_contaminacion, pronostico, manana.month, vars_con_datos)

    # --- Explicacion del metodo ---
    with st.expander("¿Cómo se calcula este pronóstico?", expanded=False):
        st.markdown(
            "El pronóstico de contaminación es **heurístico-estadístico** "
            "(no usa modelos de Machine Learning):\n\n"
            "1. **Base histórica**: media de mediciones para el mismo mes y día "
            "de la semana (ej: todos los miércoles de abril históricos).\n"
            "2. **Corrección por tendencia**: si los últimos 3 años muestran "
            "tendencia al alza o baja, se ajusta proporcionalmente.\n"
            "3. **Datos en tiempo real**: si hay datos RT recientes de AQICN, "
            "se ponderan 60% histórico + 40% RT.\n\n"
            "**Escala de confianza:**\n"
            "- 🟢 **Alta**: ≥50 registros históricos para ese mes+día\n"
            "- 🟡 **Media**: ≥10 registros\n"
            "- 🟠 **Baja**: <10 registros (usa media mensual general)\n\n"
            "*Este pronóstico es orientativo. No sustituye a modelos "
            "atmosféricos profesionales.*"
        )


def _render_boxplot_contexto(
    df_contaminacion: Optional[pd.DataFrame],
    pronostico: dict,
    mes: int,
    variables: list,
) -> None:
    """
    Renderiza boxplots de distribucion historica mensual con la prediccion marcada.

    Args:
        df_contaminacion: DataFrame de contaminacion.
        pronostico: Resultados del pronostico por variable.
        mes: Mes objetivo (1-12).
        variables: Variables a mostrar.
    """
    meses_es = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }

    fig = go.Figure()
    has_data = False

    for variable in variables:
        serie = obtener_distribucion_mensual(df_contaminacion, variable, mes)
        if serie is None or serie.empty:
            continue

        has_data = True
        color = VARIABLE_COLORS.get(variable, "#888")
        var_display = _VAR_DISPLAY.get(variable, variable)

        # Boxplot
        fig.add_trace(go.Box(
            y=serie.values,
            name=var_display,
            marker_color=color,
            boxmean=True,
            opacity=0.7,
        ))

        # Punto de prediccion
        if variable in pronostico:
            pred = pronostico[variable]["prediccion"]
            fig.add_trace(go.Scatter(
                x=[var_display],
                y=[pred],
                mode="markers",
                name=f"Predicción {var_display}",
                marker=dict(
                    color="#ff0040",
                    size=14,
                    symbol="diamond",
                    line=dict(width=2, color="white"),
                ),
                showlegend=False,
                hovertemplate=(
                    f"<b>Predicción {var_display}</b><br>"
                    f"{pred:.1f} µg/m³<extra></extra>"
                ),
            ))

    if not has_data:
        return

    mes_nombre = meses_es.get(mes, str(mes))
    fig.update_layout(
        title=f"Contexto histórico — Distribución en {mes_nombre} vs predicción (◆)",
        yaxis_title="µg/m³",
        template=get_theme()["plotly_template"],
        height=420,
        margin=dict(t=60, b=40),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Cajas: distribución histórica del mes (mediana, Q1-Q3, bigotes). "
        "Diamante rojo (◆): predicción para mañana."
    )


# ==============================================================================
# 5. FUNCION ORQUESTADORA
# ==============================================================================

def render_tab_pronostico(datos: dict) -> None:
    """
    Orquesta la tab completa de Pronostico 72h.

    Estructura:
      1. Header con descripcion
      2. KPIs meteo (4 metricas incluyendo riesgo)
      3. Grafico interactivo pre-generado
      4. Explicacion de la heuristica de riesgo
      5. Pronostico de contaminacion para manana

    Args:
        datos: Diccionario con datos filtrados.
              Claves usadas: 'pronostico', 'contaminacion', 'contam_rt'.
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
    else:
        # 1. KPIs meteo
        render_kpis_pronostico(df)

        st.divider()

        # 2. Grafico meteo
        render_grafico_pronostico(df)

        st.divider()

        # 3. Explicacion de la heuristica
        with st.expander("¿Cómo se calcula el indicador de riesgo?", expanded=False):
            st.markdown(
                "El indicador de riesgo de calidad del aire se basa en el "
                "**efecto washout** (lavado atmosférico): la lluvia arrastra "
                "partículas contaminantes, limpiando el aire.\n\n"
                f"- **Lluvia total > {RAIN_THRESHOLD_LOW} mm** → "
                f"Riesgo **BAJO** {RIESGO_CONFIG['BAJO']['emoji']} "
                f"(washout efectivo)\n"
                f"- **Prob. max. > {POP_THRESHOLD_MOD:.0f}%** → "
                f"Riesgo **MODERADO** {RIESGO_CONFIG['MODERADO']['emoji']} "
                f"(posible limpieza parcial)\n"
                f"- **Sin lluvia significativa** → "
                f"Riesgo **ALTO** {RIESGO_CONFIG['ALTO']['emoji']} "
                f"(acumulación de contaminantes)\n\n"
                "*Fuente: heurística implementada en generar_pronostico.py, "
                "basada en literatura sobre deposición húmeda de contaminantes.*"
            )

    st.divider()

    # 4. Pronostico de contaminacion para manana
    df_contaminacion = datos.get("contaminacion")
    contam_rt = datos.get("contam_rt")
    render_pronostico_contaminacion(df_contaminacion, contam_rt)
