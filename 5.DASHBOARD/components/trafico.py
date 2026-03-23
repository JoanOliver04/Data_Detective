# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.4: Tab Trafico
==============================================================================
Componente modular que renderiza la seccion completa de trafico:
  1. KPIs: total incidencias, media diaria, dia pico, anio mas congestionado
  2. Grafico temporal adaptativo (barras anual / linea mensual)
  3. Distribucion semanal (barras horizontales lun-dom)
  4. Mapa de trafico (HTML pre-generado o mensaje informativo)
  5. Funcion orquestadora render_tab_trafico()

Columnas esperadas del DataFrame (data_loader.cargar_trafico):
  fecha (datetime), anio, mes, dia_semana, ubicacion, calidad_dato

Ruta: 5.DASHBOARD/components/trafico.py
Autor: Joan | Fecha: 2026
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from config import (
    TAB_NAMES, DESCRIPCION_TABS, MAPA_TRAFICO_HTML,
)
from data_loader import leer_html_visualizacion

logger = logging.getLogger("Trafico")

# Colores de la seccion
COLOR_TRAFICO = "#ff7f0e"           # Naranja principal
COLOR_TRAFICO_LIGHT = "#ffbb78"     # Naranja claro (barras)

# Orden correcto de dias (lunes a domingo) con nombres en ingles
# tal como genera pandas con dt.day_name()
DIAS_ORDEN = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]
DIAS_NOMBRE_ES = {
    "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
    "Thursday": "Jueves", "Friday": "Viernes",
    "Saturday": "Sábado", "Sunday": "Domingo",
}

# Altura del mapa embebido (px)
MAP_HEIGHT = 500


# ==============================================================================
# 1. KPIs
# ==============================================================================

def render_kpis_trafico(df: pd.DataFrame) -> None:
    """
    Renderiza 4 KPIs de trafico:
      - Total incidencias
      - Media diaria de incidencias
      - Dia de la semana con mas incidencias
      - Anio mas congestionado

    Args:
        df: DataFrame de trafico filtrado.
    """
    if df is None or df.empty:
        st.warning("No hay datos de trafico para los filtros seleccionados.")
        return

    # --- Calculos ---
    total = len(df)

    # Media diaria: dias unicos con datos
    if "fecha" in df.columns:
        try:
            dias_unicos = df["fecha"].dt.date.nunique()
        except AttributeError:
            dias_unicos = df["fecha"].nunique()
    else:
        dias_unicos = 1
    media_diaria = total / max(dias_unicos, 1)

    # Dia de la semana con mas incidencias
    if "dia_semana" in df.columns and not df["dia_semana"].dropna().empty:
        conteo_dia = df["dia_semana"].value_counts()
        dia_pico_en = conteo_dia.idxmax()
        dia_pico = DIAS_NOMBRE_ES.get(dia_pico_en, dia_pico_en)
        val_dia_pico = int(conteo_dia.max())
    else:
        dia_pico = "-"
        val_dia_pico = None

    # Anio mas congestionado
    if "anio" in df.columns and not df["anio"].dropna().empty:
        conteo_anio = df.groupby("anio").size()
        anio_pico = int(conteo_anio.idxmax())
        val_anio_pico = int(conteo_anio.max())
    else:
        anio_pico = "-"
        val_anio_pico = None

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {COLOR_TRAFICO};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Indicadores de tráfico</h4>'
        f'<small style="color:#888;">Incidencias en la red viaria</small>'
        f'</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Total incidencias",
        value=f"{total:,}",
        help="Número total de incidencias de tráfico registradas.",
    )

    c2.metric(
        label="Media diaria",
        value=f"{media_diaria:.1f}",
        help=f"Incidencias por día ({dias_unicos:,} días con datos).",
    )

    c3.metric(
        label="Día con mas tráfico",
        value=dia_pico,
        delta=f"{val_dia_pico:,} incidencias" if val_dia_pico else None,
        delta_color="off",
        help="Día de la semana con mayor número de incidencias.",
    )

    c4.metric(
        label="Año mas congestionado",
        value=str(anio_pico),
        delta=f"{val_anio_pico:,} incidencias" if val_anio_pico else None,
        delta_color="off",
        help="Año con mayor volumen de incidencias registradas.",
    )

    logger.info(
        f"[KPIs Trafico] Total={total:,}, Media diaria={media_diaria:.1f}, "
        f"Dia pico={dia_pico}, Anio pico={anio_pico}"
    )


# ==============================================================================
# 2. GRAFICO TEMPORAL ADAPTATIVO
# ==============================================================================

def render_grafico_trafico(df: pd.DataFrame) -> None:
    """
    Grafico temporal de incidencias con granularidad adaptativa:
      - Rango > 5 anios -> barras con total anual
      - Rango <= 5 anios -> linea con total mensual

    Args:
        df: DataFrame de trafico filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para generar el grafico de trafico.")
        return

    if "anio" not in df.columns:
        st.info("Columna 'anio' no disponible para graficar.")
        return

    anio_min = int(df["anio"].min())
    anio_max = int(df["anio"].max())
    rango = anio_max - anio_min

    if rango > 5:
        _grafico_trafico_anual(df, anio_min, anio_max)
    else:
        _grafico_trafico_mensual(df, anio_min, anio_max)


def _grafico_trafico_anual(
    df: pd.DataFrame, anio_min: int, anio_max: int,
) -> None:
    """Barras de incidencias totales por anio."""
    serie = (
        df.groupby("anio", as_index=False)
        .size()
        .rename(columns={"size": "incidencias"})
    )
    serie = serie.sort_values("anio")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=serie["anio"],
        y=serie["incidencias"],
        name="Incidencias",
        marker_color=COLOR_TRAFICO_LIGHT,
        opacity=0.85,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Incidencias: %{y:,}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text=f"Incidencias de tráfico anuales ({anio_min}-{anio_max})",
            font=dict(size=16),
        ),
        xaxis_title="Año",
        yaxis_title="Incidencias",
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
        bargap=0.15,
    )

    st.plotly_chart(fig, width="stretch")
    logger.info(f"[Grafico] Trafico anual: {len(serie)} puntos")


def _grafico_trafico_mensual(
    df: pd.DataFrame, anio_min: int, anio_max: int,
) -> None:
    """Linea de incidencias mensuales."""
    df = df.copy()

    # Construir periodo YYYY-MM-01 (asegurar int para evitar floats)
    df["periodo"] = pd.to_datetime(
        df["anio"].astype(int).astype(str) + "-" +
        df["mes"].astype(int).astype(str).str.zfill(2) + "-01"
    )

    serie = (
        df.groupby("periodo", as_index=False)
        .size()
        .rename(columns={"size": "incidencias"})
    )
    serie = serie.sort_values("periodo")

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=serie["periodo"],
        y=serie["incidencias"],
        name="Incidencias mensuales",
        mode="lines+markers",
        line=dict(color=COLOR_TRAFICO, width=2.5),
        marker=dict(size=5, color=COLOR_TRAFICO),
        fill="tozeroy",
        fillcolor="rgba(255,127,14,0.12)",
        hovertemplate=(
            "<b>%{x|%b %Y}</b><br>"
            "Incidencias: %{y:,}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text=f"Incidencias de tráfico mensuales ({anio_min}-{anio_max})",
            font=dict(size=16),
        ),
        xaxis_title="Fecha",
        yaxis_title="Incidencias",
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")
    logger.info(f"[Grafico] Trafico mensual: {len(serie)} puntos")


# ==============================================================================
# 3. DISTRIBUCION SEMANAL
# ==============================================================================

def render_distribucion_semana(df: pd.DataFrame) -> None:
    """
    Media de incidencias por dia de la semana (lunes a domingo).
    Barras horizontales para mejor lectura.

    Args:
        df: DataFrame de trafico filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para la distribucion semanal.")
        return

    if "dia_semana" not in df.columns:
        st.info("Columna 'dia_semana' no disponible.")
        return

    # Contar incidencias por dia_semana
    conteo = df["dia_semana"].value_counts()

    # Calcular semanas totales en el dataset para obtener media
    if "fecha" in df.columns:
        try:
            n_semanas = max(
                (df["fecha"].max() - df["fecha"].min()).days / 7, 1
            )
        except Exception:
            n_semanas = 1
    else:
        n_semanas = 1

    # Construir DataFrame ordenado lun-dom
    datos_semana = []
    for dia_en in DIAS_ORDEN:
        total_dia = conteo.get(dia_en, 0)
        datos_semana.append({
            "dia_en": dia_en,
            "dia": DIAS_NOMBRE_ES.get(dia_en, dia_en),
            "total": int(total_dia),
            "media_semanal": round(total_dia / n_semanas, 1),
        })

    df_semana = pd.DataFrame(datos_semana)

    # Invertir orden para que lunes quede arriba en barras horizontales
    df_semana = df_semana.iloc[::-1]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df_semana["dia"],
        x=df_semana["media_semanal"],
        orientation="h",
        marker_color=[
            COLOR_TRAFICO if dia in ("Viernes", "Sabado", "Domingo")
            else COLOR_TRAFICO_LIGHT
            for dia in df_semana["dia"]
        ],
        opacity=0.85,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Media semanal: %{x:.1f} incidencias<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text="Distribución de incidencias por día de la semana",
            font=dict(size=15),
        ),
        xaxis_title="Media de incidencias por semana",
        yaxis_title="",
        template="plotly_dark",
        height=350,
        margin=dict(l=100, r=30, t=50, b=40),
        showlegend=False,
    )

    st.plotly_chart(fig, width="stretch")

    # Insight automatico
    if not df_semana.empty:
        idx_max = df_semana["media_semanal"].idxmax()
        dia_max = df_semana.loc[idx_max, "dia"]
        val_max = df_semana.loc[idx_max, "media_semanal"]
        st.caption(
            f"Día con más tráfico: **{dia_max}** "
            f"({val_max:.1f} incidencias/semana de media)."
        )

    logger.info(f"[Distribucion] {len(df_semana)} dias procesados")


# ==============================================================================
# 4. MAPA DE TRAFICO
# ==============================================================================

def render_mapa_trafico() -> None:
    """
    Renderiza el mapa de trafico embebido en Streamlit.
    Prioridad: carga HTML pre-generado de Fase 6.1.
    Si no existe, muestra mensaje informativo.
    """
    st.markdown(
        '<div class="section-header">'
        '<h4>Mapa de incidencias de trafico</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Distribución espacial de incidencias por distritos'
        '</p></div>',
        unsafe_allow_html=True,
    )

    if MAPA_TRAFICO_HTML.exists():
        html_content = leer_html_visualizacion(str(MAPA_TRAFICO_HTML))
        if html_content:
            st.caption(
                f"Mapa pre-generado: `{MAPA_TRAFICO_HTML.name}` "
                f"(regenerar con generar_mapas.py)"
            )
            components.html(html_content, height=MAP_HEIGHT, scrolling=False)
            return

    # Fallback: sin mapa
    st.info(
        "Mapa de trafico no disponible. "
        "Genera el mapa ejecutando:\n\n"
        "`python 2.SCRIPTS/procesamiento/generar_mapas.py`"
    )
    logger.warning("[Mapa] mapa_trafico.html no encontrado")


# ==============================================================================
# 5. FUNCION ORQUESTADORA
# ==============================================================================

def render_tab_trafico(datos: dict) -> None:
    """
    Orquesta la tab completa de Trafico.

    Estructura:
      1. Header con descripcion
      2. KPIs (4 metricas)
      3. Grafico temporal adaptativo
      4. Distribucion semanal + Mapa en dos columnas
      5. Resumen de filtros activos

    Args:
        datos: Diccionario con datos filtrados.
              Claves usadas: 'trafico', '_filtros'.
    """
    # Header
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["trafico"]}</h3>'
        f'<p>{DESCRIPCION_TABS["trafico"]}</p></div>',
        unsafe_allow_html=True,
    )

    df = datos.get("trafico")

    # Guard clause
    if df is None:
        st.error(
            "Sin datos de trafico. "
            "Ejecuta pipeline_etl.py para generar los datos limpios."
        )
        return

    # 1. KPIs
    render_kpis_trafico(df)

    st.divider()

    # 2. Grafico temporal
    render_grafico_trafico(df)

    st.divider()

    # 3. Distribucion semanal + Mapa en columnas
    col_semana, col_mapa = st.columns([2, 3])

    with col_semana:
        render_distribucion_semana(df)

    with col_mapa:
        render_mapa_trafico()

    # 4. Resumen de filtros
    filtros = datos.get("_filtros", {})
    anio_min = filtros.get("anio_min", "?")
    anio_max = filtros.get("anio_max", "?")

    st.markdown("---")
    st.caption(
        f"Filtros activos \u2014 Periodo: {anio_min}-{anio_max}"
    )
