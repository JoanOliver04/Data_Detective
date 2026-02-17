# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.2: Graficos Temporales de Contaminacion
==============================================================================
Componente modular que renderiza:
  1. Serie temporal interactiva (Plotly) con limites OMS/UE
  2. Tabla de tendencia anual con cambio porcentual

Ruta: 5.DASHBOARD/components/trends.py
Autor: Joan | Fecha: 2026
"""

import logging
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import (
    UMBRALES_OMS, UMBRALES_UE,
    VARIABLE_COLORS, COLORS,
)

logger = logging.getLogger("Trends")


# ==============================================================================
# 1. SERIE TEMPORAL INTERACTIVA
# ==============================================================================

def render_grafico_contaminacion(df: pd.DataFrame, variable: str) -> None:
    """
    Renderiza un grafico Plotly de serie temporal para la variable dada.

    Comportamiento adaptativo:
      - Si el rango de anios es > 5: muestra medias ANUALES
      - Si el rango es <= 5: muestra medias MENSUALES

    Incluye lineas horizontales para umbrales OMS y UE.

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable seleccionada (NO2, O3, PM10, PM2.5...).
    """
    if df is None or df.empty:
        st.info("Sin datos para generar el grafico temporal.")
        return

    # Filtrar por variable y calidad
    mask = (df["variable"] == variable)
    if "calidad_dato" in df.columns:
        mask = mask & (df["calidad_dato"] == "ok")
    df_var = df[mask].copy()

    if df_var.empty:
        st.info(f"Sin registros validos de {variable} para graficar.")
        return

    # --- Decidir granularidad automaticamente ---
    anio_min = df_var["anio"].min()
    anio_max = df_var["anio"].max()
    rango_anios = anio_max - anio_min

    if rango_anios > 5:
        # Granularidad ANUAL
        serie = (
            df_var
            .groupby("anio", as_index=False)["valor"]
            .mean()
            .rename(columns={"anio": "periodo", "valor": "media"})
        )
        x_label = "Anio"
        titulo_granularidad = "Media anual"
        hover_template = "<b>%{x}</b><br>Media: %{y:.1f} ug/m3<extra></extra>"
    else:
        # Granularidad MENSUAL
        if "fecha_utc" in df_var.columns:
            df_var["mes"] = df_var["fecha_utc"].dt.to_period("M")
            serie = (
                df_var
                .groupby("mes", as_index=False)["valor"]
                .mean()
            )
            serie["periodo"] = serie["mes"].dt.to_timestamp()
            serie = serie.rename(columns={"valor": "media"})
            serie = serie.drop(columns=["mes"])
        else:
            # Fallback: por anio aunque rango sea corto
            serie = (
                df_var
                .groupby("anio", as_index=False)["valor"]
                .mean()
                .rename(columns={"anio": "periodo", "valor": "media"})
            )
        x_label = "Fecha"
        titulo_granularidad = "Media mensual"
        hover_template = "<b>%{x}</b><br>Media: %{y:.1f} ug/m3<extra></extra>"

    if serie.empty:
        st.info("Sin datos agregados para el grafico.")
        return

    # --- Construir grafico Plotly ---
    color_var = VARIABLE_COLORS.get(variable, COLORS["primary"])
    umbral_oms = UMBRALES_OMS.get(variable)
    umbral_ue = UMBRALES_UE.get(variable)

    fig = go.Figure()

    # Linea principal
    fig.add_trace(go.Scatter(
        x=serie["periodo"],
        y=serie["media"],
        mode="lines+markers",
        name=f"{variable} ({titulo_granularidad})",
        line=dict(color=color_var, width=2.5),
        marker=dict(size=5, color=color_var),
        hovertemplate=hover_template,
    ))

    # Linea umbral OMS
    if umbral_oms is not None:
        fig.add_hline(
            y=umbral_oms,
            line_dash="dash",
            line_color="#e74c3c",
            line_width=1.5,
            annotation_text=f"OMS ({umbral_oms})",
            annotation_position="top left",
            annotation_font=dict(color="#e74c3c", size=11),
        )

    # Linea umbral UE
    if umbral_ue is not None:
        fig.add_hline(
            y=umbral_ue,
            line_dash="dot",
            line_color="#f39c12",
            line_width=1.5,
            annotation_text=f"UE ({umbral_ue})",
            annotation_position="bottom left",
            annotation_font=dict(color="#f39c12", size=11),
        )

    # Layout profesional
    fig.update_layout(
        title=dict(
            text=f"Evolucion de {variable} en Valencia ({titulo_granularidad})",
            font=dict(size=16),
        ),
        xaxis_title=x_label,
        yaxis_title=f"{variable} (ug/m3)",
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    st.plotly_chart(fig, width="stretch")

    logger.info(
        f"[Grafico] {variable}: {len(serie)} puntos, "
        f"granularidad={'anual' if rango_anios > 5 else 'mensual'}"
    )


# ==============================================================================
# 2. TENDENCIA ANUAL CON CAMBIO PORCENTUAL
# ==============================================================================

def render_tendencia_anual(df: pd.DataFrame, variable: str) -> None:
    """
    Muestra la media anual de la variable en toda la ciudad,
    con el cambio porcentual respecto al anio anterior.

    Incluye indicadores visuales (flechas y colores):
      - Rojo + flecha arriba: empeoramiento (sube contaminacion)
      - Verde + flecha abajo: mejora (baja contaminacion)

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable seleccionada.
    """
    if df is None or df.empty:
        st.info("Sin datos para calcular tendencia anual.")
        return

    mask = (df["variable"] == variable)
    if "calidad_dato" in df.columns:
        mask = mask & (df["calidad_dato"] == "ok")
    df_var = df[mask].copy()

    if df_var.empty:
        return

    # Agrupar por anio (sintaxis compatible con todas las versiones de pandas)
    tendencia = (
        df_var
        .groupby("anio", as_index=False)
        .agg(media=("valor", "mean"), registros=("valor", "count"))
    )

    tendencia = tendencia.sort_values("anio")

    # Calcular cambio porcentual
    tendencia["cambio_pct"] = tendencia["media"].pct_change() * 100

    if len(tendencia) < 2:
        st.caption("Se necesitan al menos 2 anios para calcular tendencia.")
        return

    # --- Renderizar ---
    st.markdown(
        '<div class="section-header">'
        '<h4>Tendencia anual</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Media anual de toda la ciudad con cambio respecto al anio anterior'
        '</p></div>',
        unsafe_allow_html=True,
    )

    # Mostrar ultimos 6 anios como maximo para no saturar
    ultimos = tendencia.tail(6)
    cols = st.columns(len(ultimos))

    for i, (_, row) in enumerate(ultimos.iterrows()):
        anio = int(row["anio"])
        media = row["media"]
        cambio = row["cambio_pct"]

        if pd.notna(cambio):
            # En contaminacion: subir es malo, bajar es bueno
            if cambio > 0:
                flecha = "^"
                delta_color = "inverse"
            else:
                flecha = "v"
                delta_color = "inverse"
            delta_str = f"{cambio:+.1f}%"
        else:
            delta_str = None
            delta_color = "off"

        cols[i].metric(
            label=str(anio),
            value=f"{media:.1f}",
            delta=delta_str,
            delta_color=delta_color,
        )

    logger.info(f"[Tendencia] {variable}: {len(tendencia)} anios calculados")
