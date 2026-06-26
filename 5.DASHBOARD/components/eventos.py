# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.5: Tab Eventos Masivos
==============================================================================
Componente modular que renderiza la seccion completa de impacto de eventos:
  1. KPIs: total eventos, impacto medio NO2/trafico, tipo con mayor impacto
  2. Grafico de impacto en contaminacion por tipo de evento
  3. Grafico de impacto en trafico por tipo de evento
  4. Timeline de eventos (scatter tipo Gantt)
  5. Funcion orquestadora render_tab_eventos()

Columnas esperadas del DataFrame (correlacion_eventos.py):
  evento_id, nombre_evento, tipo_evento, impacto_esperado,
  fecha_inicio, fecha_fin, variable,
  media_evento, media_baseline, impacto_pct,
  n_dias_evento, n_dias_baseline,
  media_temp_evento, media_temp_baseline,
  media_precip_evento, media_precip_baseline,
  impacto_trafico_pct

Nota: cada fila es un par (evento x variable_contaminante),
      por lo que un mismo evento aparece varias veces (una por NO2, O3...).

Ruta: 5.DASHBOARD/components/eventos.py
Autor: Joan | Fecha: 2026
"""

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from theme import get_theme
from config import TAB_NAMES, DESCRIPCION_TABS

logger = logging.getLogger("Eventos")

# Colores de la seccion
COLOR_EVENTO = "#9b59b6"  # Purpura principal
COLOR_POSITIVO = "#d62728"  # Rojo = sube contaminacion (malo)
COLOR_NEGATIVO = "#2ca02c"  # Verde = baja contaminacion (bueno)
COLOR_NEUTRO = "#7f7f7f"  # Gris

# Paleta para categorias de evento (taxonomia multidimensional v2)
# Mapea categoria_evento -> color (tipo_evento = categoria_evento)
PALETA_TIPOS = {
    "festivo": "#e74c3c",  # Rojo (Fallas, Navidad, fiestas)
    "deportivo": "#3498db",  # Azul (Valencia CF, maratones)
    "musical": "#9b59b6",  # Purpura (conciertos, festivales)
    "cultural": "#e67e22",  # Naranja (exposiciones, teatro)
    "institucional": "#1abc9c",  # Turquesa (conferencias, talleres)
    "comercial": "#f39c12",  # Amarillo (ferias, mercados)
    "religioso": "#8e44ad",  # Violeta (procesiones)
    "otro": "#95a5a6",  # Gris (sin clasificar)
}


# ==============================================================================
# UTILIDADES
# ==============================================================================


def _color_impacto(valor: float) -> str:
    """Retorna color segun si el impacto es positivo (malo) o negativo (bueno)."""
    if pd.isna(valor):
        return COLOR_NEUTRO
    return COLOR_POSITIVO if valor > 0 else COLOR_NEGATIVO


def _color_tipo_evento(tipo: str) -> str:
    """Retorna color para un tipo de evento."""
    tipo_lower = str(tipo).lower()
    for key, color in PALETA_TIPOS.items():
        if key in tipo_lower:
            return color
    return PALETA_TIPOS["otro"]


# ==============================================================================
# 1. KPIs
# ==============================================================================


def render_kpis_eventos(df: pd.DataFrame) -> None:
    """
    Renderiza 4 KPIs de eventos masivos:
      - Numero total de eventos unicos
      - Impacto medio en NO2 (%)
      - Impacto medio en trafico (%)
      - Tipo de evento con mayor impacto

    Args:
        df: DataFrame de impacto_eventos filtrado.
    """
    if df is None or df.empty:
        st.warning("No hay datos de eventos para los filtros seleccionados.")
        return

    # --- Calculos ---
    n_eventos = df["evento_id"].nunique()

    # Impacto medio NO2 (filtrar filas donde variable == NO2)
    df_no2 = df[df["variable"] == "NO2"] if "variable" in df.columns else df
    media_no2 = df_no2["impacto_pct"].dropna().mean() if not df_no2.empty else np.nan

    # Impacto medio trafico (impacto_trafico_pct existe una vez por fila,
    # pero se repite por variable; tomar media por evento unico)
    if "impacto_trafico_pct" in df.columns:
        trafico_por_evento = df.drop_duplicates(subset=["evento_id"])[
            "impacto_trafico_pct"
        ].dropna()
        media_trafico = (
            trafico_por_evento.mean() if not trafico_por_evento.empty else np.nan
        )
    else:
        media_trafico = np.nan

    # Tipo con mayor impacto medio absoluto (sobre contaminacion)
    if "tipo_evento" in df.columns and "impacto_pct" in df.columns:
        impacto_por_tipo = df.groupby("tipo_evento", as_index=False)[
            "impacto_pct"
        ].mean()
        impacto_por_tipo["abs_impacto"] = impacto_por_tipo["impacto_pct"].abs()
        if not impacto_por_tipo.empty:
            idx_max = impacto_por_tipo["abs_impacto"].idxmax()
            tipo_max = impacto_por_tipo.loc[idx_max, "tipo_evento"]
            val_tipo_max = impacto_por_tipo.loc[idx_max, "impacto_pct"]
        else:
            tipo_max = "-"
            val_tipo_max = None
    else:
        tipo_max = "-"
        val_tipo_max = None

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {COLOR_EVENTO};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Indicadores de impacto de eventos</h4>'
        f'<small style="color:#888;">'
        f"Análisis quasi-experimental vs. baseline de días comparables"
        f"</small></div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Eventos analizados",
        value=str(n_eventos),
        help="Número de eventos únicos con datos de impacto calculados.",
    )

    c2.metric(
        label="Impacto medio NO2",
        value=f"{media_no2:+.1f}%" if pd.notna(media_no2) else "-",
        delta=(
            "Sube"
            if pd.notna(media_no2) and media_no2 > 0
            else ("Baja" if pd.notna(media_no2) else None)
        ),
        delta_color="inverse",
        help=(
            "Cambio porcentual medio de NO2 durante eventos "
            "respecto al baseline de días comparables."
        ),
    )

    c3.metric(
        label="Impacto medio tráfico",
        value=f"{media_trafico:+.1f}%" if pd.notna(media_trafico) else "-",
        delta=(
            "Sube"
            if pd.notna(media_trafico) and media_trafico > 0
            else ("Baja" if pd.notna(media_trafico) else None)
        ),
        delta_color="inverse",
        help=(
            "Cambio porcentual medio de incidencias de tráfico "
            "durante eventos respecto al baseline."
        ),
    )

    c4.metric(
        label="Tipo mayor impacto",
        value=str(tipo_max),
        delta=f"{val_tipo_max:+.1f}%" if val_tipo_max is not None else None,
        delta_color="off",
        help="Tipo de evento con mayor impacto absoluto medio en contaminación.",
    )

    logger.info(
        f"[KPIs Eventos] {n_eventos} eventos, "
        f"NO2={media_no2:.1f}%, trafico={media_trafico:.1f}%"
        if pd.notna(media_no2) and pd.notna(media_trafico)
        else f"[KPIs Eventos] {n_eventos} eventos"
    )


# ==============================================================================
# 2. IMPACTO EN CONTAMINACION POR TIPO DE EVENTO
# ==============================================================================


def render_grafico_impacto_contaminacion(df: pd.DataFrame) -> None:
    """
    Barras agrupadas de impacto medio (%) en contaminacion por tipo de evento.
    Colores: rojo si sube (malo), verde si baja (bueno).

    Si hay multiples variables, muestra las principales (NO2, PM10, PM2.5).

    Args:
        df: DataFrame de impacto_eventos filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para el gráfico de impacto en contaminación.")
        return

    if "variable" not in df.columns or "tipo_evento" not in df.columns:
        st.info("Columnas necesarias no disponibles.")
        return

    # Variables principales a mostrar
    vars_principales = ["NO2", "PM10", "PM2.5", "O3"]
    vars_disponibles = [v for v in vars_principales if v in df["variable"].unique()]

    if not vars_disponibles:
        vars_disponibles = df["variable"].unique().tolist()[:4]

    # Agregar: media de impacto_pct por tipo_evento y variable
    agg = (
        df[df["variable"].isin(vars_disponibles)]
        .groupby(["tipo_evento", "variable"], as_index=False)["impacto_pct"]
        .mean()
    )

    if agg.empty:
        st.info("Sin datos agregados de impacto.")
        return

    fig = go.Figure()

    from config import VARIABLE_COLORS

    for var in vars_disponibles:
        datos_var = agg[agg["variable"] == var].sort_values("tipo_evento")
        if datos_var.empty:
            continue

        color_var = VARIABLE_COLORS.get(var, COLOR_EVENTO)

        fig.add_trace(
            go.Bar(
                x=datos_var["tipo_evento"],
                y=datos_var["impacto_pct"],
                name=var,
                marker_color=color_var,
                opacity=0.85,
                hovertemplate=(
                    "<b>%{x}</b><br>" f"{var}: " + "%{y:+.1f}%<br>" "<extra></extra>"
                ),
            )
        )

    # Linea de referencia en 0%
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="rgba(255,255,255,0.4)",
        line_width=1,
    )

    fig.update_layout(
        title=dict(
            text="Impacto en contaminación por tipo de evento",
            font=dict(size=16),
        ),
        xaxis_title="Tipo de evento",
        yaxis_title="Impacto medio (%)",
        template=get_theme()["plotly_template"],
        height=420,
        margin=dict(l=60, r=30, t=60, b=50),
        barmode="group",
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

    # Insight
    st.caption(
        "Valores positivos (+) indican aumento de contaminación durante el evento. "
        "Valores negativos (-) indican mejora respecto al baseline."
    )

    logger.info(f"[Grafico contam] {len(agg)} combinaciones tipo x variable")


# ==============================================================================
# 3. IMPACTO EN TRAFICO POR TIPO DE EVENTO
# ==============================================================================


def render_grafico_impacto_trafico(df: pd.DataFrame) -> None:
    """
    Barras de impacto medio (%) en tráfico por tipo de evento.
    Color según positivo (rojo) o negativo (verde).

    Args:
        df: DataFrame de impacto_eventos filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para el gráfico de impacto en tráfico.")
        return

    if "impacto_trafico_pct" not in df.columns or "tipo_evento" not in df.columns:
        st.info("Columnas de impacto de tráfico no disponibles.")
        return

    # Tomar una fila por evento (impacto_trafico_pct es igual en todas las
    # variables del mismo evento)
    df_unico = df.drop_duplicates(subset=["evento_id"])

    agg = (
        df_unico.groupby("tipo_evento", as_index=False)["impacto_trafico_pct"]
        .mean()
        .dropna(subset=["impacto_trafico_pct"])
    )

    if agg.empty:
        st.info("Sin datos agregados de impacto en tráfico.")
        return

    agg = agg.sort_values("impacto_trafico_pct", ascending=True)

    # Colores: rojo si sube, verde si baja
    colores = [_color_impacto(v) for v in agg["impacto_trafico_pct"]]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=agg["tipo_evento"],
            y=agg["impacto_trafico_pct"],
            marker_color=colores,
            opacity=0.85,
            hovertemplate=(
                "<b>%{x}</b><br>" "Impacto tráfico: %{y:+.1f}%<br>" "<extra></extra>"
            ),
        )
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="rgba(255,255,255,0.4)",
        line_width=1,
    )

    fig.update_layout(
        title=dict(
            text="Impacto en tráfico por tipo de evento",
            font=dict(size=16),
        ),
        xaxis_title="Tipo de evento",
        yaxis_title="Impacto medio (%)",
        template=get_theme()["plotly_template"],
        height=400,
        margin=dict(l=60, r=30, t=60, b=50),
        showlegend=False,
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")

    st.caption(
        "Rojo = más incidencias de tráfico durante el evento. "
        "Verde = menos incidencias que el baseline."
    )

    logger.info(f"[Grafico trafico] {len(agg)} tipos de evento")


# ==============================================================================
# 4. TIMELINE DE EVENTOS
# ==============================================================================


def render_timeline_eventos(df: pd.DataFrame) -> None:
    """
    Scatter timeline tipo Gantt simplificado:
      - Eje X: fecha_inicio a fecha_fin (linea horizontal por evento)
      - Eje Y: nombre del evento
      - Color por tipo_evento
      - Hover con detalles

    Args:
        df: DataFrame de impacto_eventos filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para la línea de tiempo de eventos.")
        return

    for col in ["fecha_inicio", "fecha_fin", "nombre_evento"]:
        if col not in df.columns:
            st.info(f"Columna '{col}' no disponible para timeline.")
            return

    # Tomar una fila por evento (evitar duplicados por variable)
    df_ev = df.drop_duplicates(subset=["evento_id"]).copy()

    # Asegurar datetime
    for col in ["fecha_inicio", "fecha_fin"]:
        if not pd.api.types.is_datetime64_any_dtype(df_ev[col]):
            df_ev[col] = pd.to_datetime(df_ev[col], errors="coerce")

    df_ev = df_ev.dropna(subset=["fecha_inicio", "fecha_fin"])

    if df_ev.empty:
        st.info("Sin eventos con fechas válidas para la línea de tiempo.")
        return

    df_ev = df_ev.sort_values("fecha_inicio")

    fig = go.Figure()

    # Cada evento es una linea horizontal (scatter con inicio y fin)
    for _, row in df_ev.iterrows():
        tipo = row.get("tipo_evento", "otro")
        color = _color_tipo_evento(tipo)
        nombre = row["nombre_evento"]
        impacto_str = ""
        if "impacto_trafico_pct" in row and pd.notna(row["impacto_trafico_pct"]):
            impacto_str = f"<br>Impacto tráfico: {row['impacto_trafico_pct']:+.1f}%"

        fig.add_trace(
            go.Scatter(
                x=[row["fecha_inicio"], row["fecha_fin"]],
                y=[nombre, nombre],
                mode="lines+markers",
                line=dict(color=color, width=8),
                marker=dict(size=8, color=color),
                name=tipo,
                showlegend=False,
                hovertemplate=(
                    f"<b>{nombre}</b><br>"
                    f"Tipo: {tipo}<br>"
                    f"Inicio: {row['fecha_inicio'].strftime('%d/%m/%Y')}<br>"
                    f"Fin: {row['fecha_fin'].strftime('%d/%m/%Y')}"
                    f"{impacto_str}"
                    f"<extra></extra>"
                ),
            )
        )

    # Leyenda manual para tipos unicos
    tipos_unicos = df_ev["tipo_evento"].unique()
    for tipo in tipos_unicos:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=_color_tipo_evento(tipo)),
                name=str(tipo),
                showlegend=True,
            )
        )

    fig.update_layout(
        title=dict(
            text="Timeline de eventos masivos",
            font=dict(size=16),
        ),
        xaxis_title="Fecha",
        yaxis_title="",
        template=get_theme()["plotly_template"],
        height=max(300, len(df_ev) * 40 + 100),
        margin=dict(l=200, r=30, t=60, b=50),
        hovermode="closest",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title="Tipo de evento",
        ),
    )

    st.plotly_chart(fig, width="stretch")

    logger.info(f"[Timeline] {len(df_ev)} eventos renderizados")


# ==============================================================================
# 5. FUNCION ORQUESTADORA
# ==============================================================================


def render_tab_eventos(datos: dict) -> None:
    """
    Orquesta la tab completa de Eventos Masivos.

    Estructura:
      1. Header con descripcion
      2. KPIs (4 metricas)
      3. [Impacto contaminacion | Impacto trafico] en columnas
      4. Timeline de eventos
      5. Resumen de filtros

    Args:
        datos: Diccionario con datos filtrados.
              Claves usadas: 'eventos', '_filtros'.
    """
    # Header
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["eventos"]}</h3>'
        f'<p>{DESCRIPCION_TABS["eventos"]}</p></div>',
        unsafe_allow_html=True,
    )

    df = datos.get("eventos")

    # Guard clause
    if df is None:
        st.error(
            "Sin datos de impacto de eventos. "
            "Ejecuta correlacion_eventos.py para generar el análisis."
        )
        return

    # 1. KPIs
    render_kpis_eventos(df)

    st.divider()

    # 2. Graficos de impacto en dos columnas
    col_contam, col_trafico = st.columns(2)

    with col_contam:
        render_grafico_impacto_contaminacion(df)

    with col_trafico:
        render_grafico_impacto_trafico(df)

    st.divider()

    # 3. Timeline
    render_timeline_eventos(df)

    # 4. Tabla resumen rapida
    st.divider()
    with st.expander("📋 Tabla de eventos analizados"):
        df_resumen = (
            df.drop_duplicates(subset=["evento_id"])[
                [
                    "nombre_evento",
                    "tipo_evento",
                    "impacto_esperado",
                    "impacto_trafico_pct",
                ]
            ].copy()
            if all(
                c in df.columns
                for c in ["nombre_evento", "tipo_evento", "impacto_esperado"]
            )
            else None
        )
        if df_resumen is not None and not df_resumen.empty:
            df_resumen = df_resumen.rename(
                columns={
                    "nombre_evento": "Evento",
                    "tipo_evento": "Tipo",
                    "impacto_esperado": "Impacto esperado",
                    "impacto_trafico_pct": "Impacto tráfico (%)",
                }
            )
            st.dataframe(
                df_resumen,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Impacto tráfico (%)": st.column_config.NumberColumn(
                        format="%.1f%%",
                    ),
                },
            )
        else:
            st.info("Sin datos de resumen disponibles.")

    # 5. Resumen de filtros
    filtros = datos.get("_filtros", {})
    tipos_ev = filtros.get("tipos_evento", [])

    st.markdown("---")
    st.caption(
        f"Filtros activos — "
        f"Tipos de evento: {', '.join(tipos_ev) if tipos_ev else 'Todos'}"
    )
