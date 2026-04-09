# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.3: Tab Precipitaciones / Meteorologia
==============================================================================
Componente modular que renderiza la seccion completa de precipitaciones:
  1. KPIs: acumulado, media anual, anio mas lluvioso/seco
  2. Grafico temporal adaptativo (barras anual / linea mensual)
  3. Climatologia mensual historica (media por mes ene-dic)
  4. Funcion orquestadora render_tab_meteorologia()

Columnas esperadas del DataFrame (data_loader.cargar_meteorologia):
  fecha (datetime), precipitacion_mm, temp_c, humedad_pct,
  calidad_dato, anio, mes

Ruta: 5.DASHBOARD/components/meteorologia.py
Autor: Joan | Fecha: 2026
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import TAB_NAMES, DESCRIPCION_TABS, COLORS

logger = logging.getLogger("Meteorologia")

# Nombres de meses en espanol (index 1-12)
MESES_NOMBRES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

# Color principal para precipitaciones
COLOR_PRECIP = "#17becf"          # Azul agua (config.COLORS["info"])
COLOR_PRECIP_LIGHT = "#7fdbea"    # Azul agua claro (barras)
COLOR_TEMP = "#ff7f0e"            # Naranja para temperatura


# ==============================================================================
# 1. KPIs
# ==============================================================================

def render_kpis_meteorologia(df: pd.DataFrame) -> None:
    """
    Renderiza 4 KPIs de precipitaciones:
      - Precipitacion acumulada total (mm)
      - Media anual (mm/anio)
      - Anio mas lluvioso
      - Anio mas seco

    Args:
        df: DataFrame de meteorologia filtrado (con columnas anio, precipitacion_mm).
    """
    if df is None or df.empty:
        st.warning("No hay datos meteorológicos para los filtros seleccionados.")
        return

    # Filtrar registros validos
    df_ok = df.copy()
    if "calidad_dato" in df_ok.columns:
        df_ok = df_ok[df_ok["calidad_dato"] == "ok"]

    if df_ok.empty:
        st.warning("No hay registros meteorológicos válidos en el periodo.")
        return

    # --- Calculos ---
    precip_total = df_ok["precipitacion_mm"].sum()

    # Media anual: agrupar por anio, sumar precipitacion, luego media
    precip_anual = df_ok.groupby("anio", as_index=False).agg(
        acumulado=("precipitacion_mm", "sum"),
    )
    media_anual = precip_anual["acumulado"].mean(
    ) if not precip_anual.empty else 0

    # Anio mas lluvioso y mas seco
    if not precip_anual.empty:
        idx_max = precip_anual["acumulado"].idxmax()
        idx_min = precip_anual["acumulado"].idxmin()
        anio_lluvioso = int(precip_anual.loc[idx_max, "anio"])
        val_lluvioso = precip_anual.loc[idx_max, "acumulado"]
        anio_seco = int(precip_anual.loc[idx_min, "anio"])
        val_seco = precip_anual.loc[idx_min, "acumulado"]
    else:
        anio_lluvioso = anio_seco = "-"
        val_lluvioso = val_seco = 0

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {COLOR_PRECIP};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Indicadores de precipitación</h4>'
        f'<small style="color:#888;">Datos con calidad validada</small>'
        f'</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Precip. acumulada",
        value=f"{precip_total:,.1f} mm",
        help="Suma total de precipitación en el periodo seleccionado.",
    )

    c2.metric(
        label="Media anual",
        value=f"{media_anual:,.1f} mm/año",
        help="Media de la precipitación acumulada por año.",
    )

    c3.metric(
        label="Año más lluvioso",
        value=str(anio_lluvioso),
        delta=f"{val_lluvioso:,.1f} mm" if isinstance(
            anio_lluvioso, int) else None,
        delta_color="off",
        help="Año con mayor precipitación acumulada en el periodo.",
    )

    c4.metric(
        label="Año más seco",
        value=str(anio_seco),
        delta=f"{val_seco:,.1f} mm" if isinstance(anio_seco, int) else None,
        delta_color="off",
        help="Año con menor precipitación acumulada en el periodo.",
    )

    logger.info(
        f"[KPIs Meteo] Acumulado={precip_total:.1f}mm, "
        f"Media anual={media_anual:.1f}mm, "
        f"Lluvioso={anio_lluvioso}, Seco={anio_seco}"
    )


# ==============================================================================
# 2. GRAFICO TEMPORAL ADAPTATIVO
# ==============================================================================

def render_grafico_precipitacion(df: pd.DataFrame) -> None:
    """
    Grafico temporal de precipitaciones con granularidad adaptativa:
      - Rango > 5 anios -> barras con acumulado anual
      - Rango <= 5 anios -> linea con acumulado mensual

    Args:
        df: DataFrame de meteorologia filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para generar el gráfico de precipitaciones.")
        return

    df_ok = df.copy()
    if "calidad_dato" in df_ok.columns:
        df_ok = df_ok[df_ok["calidad_dato"] == "ok"]

    if df_ok.empty:
        st.info("Sin registros válidos para graficar.")
        return

    anio_min = int(df_ok["anio"].min())
    anio_max = int(df_ok["anio"].max())
    rango = anio_max - anio_min

    if rango > 5:
        _grafico_anual(df_ok, anio_min, anio_max)
    else:
        _grafico_mensual(df_ok, anio_min, anio_max)


def _grafico_anual(df: pd.DataFrame, anio_min: int, anio_max: int) -> None:
    """Barras de precipitacion acumulada por anio."""
    serie = df.groupby("anio", as_index=False).agg(
        acumulado=("precipitacion_mm", "sum"),
        temp_media=("temp_c", "mean"),
    )
    serie = serie.sort_values("anio")

    fig = go.Figure()

    # Barras de precipitacion
    fig.add_trace(go.Bar(
        x=serie["anio"],
        y=serie["acumulado"],
        name="Precipitacion (mm)",
        marker_color=COLOR_PRECIP_LIGHT,
        opacity=0.85,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Precipitación: %{y:,.1f} mm<br>"
            "<extra></extra>"
        ),
    ))

    # Linea de temperatura media (eje secundario)
    fig.add_trace(go.Scatter(
        x=serie["anio"],
        y=serie["temp_media"],
        name="Temp. media (C)",
        mode="lines+markers",
        line=dict(color=COLOR_TEMP, width=2),
        marker=dict(size=5),
        yaxis="y2",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Temp. media: %{y:.1f} °C<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text=f"Precipitación anual acumulada ({anio_min}-{anio_max})",
            font=dict(size=16),
        ),
        xaxis_title="Año",
        yaxis=dict(
            title="Precipitación (mm)",
            side="left",
            showgrid=True,
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis2=dict(
            title="Temperatura (°C)",
            side="right",
            overlaying="y",
            showgrid=False,
        ),
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=60, t=60, b=50),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        bargap=0.15,
    )

    st.plotly_chart(fig, width="stretch")

    logger.info(
        f"[Grafico] Precipitación anual: {len(serie)} puntos"
    )


def _grafico_mensual(df: pd.DataFrame, anio_min: int, anio_max: int) -> None:
    """Linea de precipitación acumulada mensual."""
    # Crear columna periodo YYYY-MM para ordenar correctamente
    df = df.copy()
    df["periodo"] = pd.to_datetime(
        df["anio"].astype(int).astype(str) + "-" +
        df["mes"].astype(int).astype(str).str.zfill(2) + "-01"
    )

    serie = df.groupby("periodo", as_index=False).agg(
        acumulado=("precipitacion_mm", "sum"),
    )
    serie = serie.sort_values("periodo")

    fig = go.Figure()

    # Area + linea de precipitacion mensual
    fig.add_trace(go.Scatter(
        x=serie["periodo"],
        y=serie["acumulado"],
        name="Precipitacion mensual",
        mode="lines",
        line=dict(color=COLOR_PRECIP, width=2),
        fill="tozeroy",
        fillcolor="rgba(23,190,207,0.15)",
        hovertemplate=(
            "<b>%{x|%b %Y}</b><br>"
            "Precipitación: %{y:,.1f} mm<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text=f"Precipitación mensual ({anio_min}-{anio_max})",
            font=dict(size=16),
        ),
        xaxis_title="Fecha",
        yaxis_title="Precipitación (mm)",
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
    )

    st.plotly_chart(fig, width="stretch")

    logger.info(
        f"[Grafico] Precipitacion mensual: {len(serie)} puntos"
    )


# ==============================================================================
# 3. CLIMATOLOGIA MENSUAL HISTORICA
# ==============================================================================

def render_climatologia_mensual(df: pd.DataFrame) -> None:
    """
    Media historica de precipitacion por mes (enero-diciembre).
    Muestra el patron climatologico tipico de Valencia.

    Args:
        df: DataFrame de meteorologia (puede ser con o sin filtro de anio).
    """
    if df is None or df.empty:
        st.info("Sin datos para calcular la climatología mensual.")
        return

    df_ok = df.copy()
    if "calidad_dato" in df_ok.columns:
        df_ok = df_ok[df_ok["calidad_dato"] == "ok"]

    if df_ok.empty or "mes" not in df_ok.columns:
        st.info("Sin datos mensuales para la climatología.")
        return

    # Paso 1: acumulado mensual por anio-mes
    mensual = df_ok.groupby(["anio", "mes"], as_index=False).agg(
        precip_mes=("precipitacion_mm", "sum"),
    )

    # Paso 2: media de esos acumulados mensuales agrupando solo por mes
    clima = mensual.groupby("mes", as_index=False).agg(
        media=("precip_mes", "mean"),
        desviacion=("precip_mes", "std"),
        n_anios=("precip_mes", "count"),
    )
    clima = clima.sort_values("mes")

    # Nombres de mes
    clima["mes_nombre"] = clima["mes"].map(MESES_NOMBRES)

    # Rellenar std NaN (si solo hay 1 anio)
    clima["desviacion"] = clima["desviacion"].fillna(0)

    anio_min = int(df_ok["anio"].min())
    anio_max = int(df_ok["anio"].max())

    fig = go.Figure()

    # Barras de media mensual
    fig.add_trace(go.Bar(
        x=clima["mes_nombre"],
        y=clima["media"],
        name="Media mensual",
        marker_color=COLOR_PRECIP_LIGHT,
        opacity=0.85,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Media: %{y:.1f} mm<br>"
            "<extra></extra>"
        ),
    ))

    # Barras de error (desviacion estandar)
    fig.add_trace(go.Scatter(
        x=clima["mes_nombre"],
        y=clima["media"],
        error_y=dict(
            type="data",
            array=clima["desviacion"],
            visible=True,
            color="rgba(255,255,255,0.3)",
            thickness=1.5,
        ),
        mode="markers",
        marker=dict(color=COLOR_PRECIP, size=6),
        name="Desviacion std.",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Media: %{y:.1f} mm<br>"
            "Std: %{error_y.array:.1f} mm<br>"
            "Anios con datos: " +
            clima["n_anios"].astype(str).tolist().__repr__() +
            "<extra></extra>"
        ),
        showlegend=True,
    ))

    fig.update_layout(
        title=dict(
            text=f"Climatología mensual histórica ({anio_min}-{anio_max})",
            font=dict(size=16),
        ),
        xaxis_title="Mes",
        yaxis_title="Precipitación media (mm)",
        template="plotly_dark",
        height=400,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        bargap=0.2,
    )

    st.plotly_chart(fig, width="stretch")

    # Insight automatico: mes mas lluvioso
    if not clima.empty:
        idx_max = clima["media"].idxmax()
        mes_max = clima.loc[idx_max, "mes_nombre"]
        val_max = clima.loc[idx_max, "media"]
        st.caption(
            f"Mes más lluvioso históricamente: **{mes_max}** "
            f"con {val_max:.1f} mm de media."
        )

    logger.info(
        f"[Climatologia] {len(clima)} meses, "
        f"periodo {anio_min}-{anio_max}"
    )


# ==============================================================================
# 4. FUNCION ORQUESTADORA
# ==============================================================================

def render_tab_meteorologia(datos: dict) -> None:
    """
    Orquesta la tab completa de Precipitaciones/Meteorologia.

    Estructura:
      1. Header con descripcion
      2. KPIs (4 metricas)
      3. Grafico temporal adaptativo
      4. Climatologia mensual historica
      5. Resumen de filtros activos

    Args:
        datos: Diccionario con todos los datos filtrados.
              Claves usadas: 'meteorologia', '_filtros'.
    """
    # Header
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["precipitaciones"]}</h3>'
        f'<p>{DESCRIPCION_TABS["precipitaciones"]}</p></div>',
        unsafe_allow_html=True,
    )

    df = datos.get("meteorologia")

    # Guard clause
    if df is None:
        st.error(
            "Sin datos meteorológicos. "
            "Ejecuta pipeline_etl.py para generar los datos limpios."
        )
        return

    # 1. KPIs
    render_kpis_meteorologia(df)

    st.divider()

    # 2. Grafico temporal adaptativo
    render_grafico_precipitacion(df)

    st.divider()

    # 3. Climatologia mensual
    render_climatologia_mensual(df)

    # 4. Resumen de filtros
    filtros = datos.get("_filtros", {})
    anio_min = filtros.get("anio_min", "?")
    anio_max = filtros.get("anio_max", "?")

    st.markdown("---")
    st.caption(
        f"Filtros activos \u2014 Periodo: {anio_min}-{anio_max}"
    )
