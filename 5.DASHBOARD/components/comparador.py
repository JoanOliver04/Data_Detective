# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Comparador Historico de Contaminacion
==============================================================================
Ruta: 5.DASHBOARD/components/comparador.py
Autor: Joan | Fecha: 2026

Permite comparar datos de contaminacion en dos modos:
  - "Evento vs Baseline": usa impacto_eventos.csv con medias pre-calculadas
  - "Periodo vs Periodo": calcula medias ad-hoc sobre el df de contaminacion

Ambos modos incluyen graficos interactivos Plotly y tabla de diferencias.
"""

import logging
from datetime import date, timedelta
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from theme import get_theme
from config import UMBRALES_OMS, VARIABLE_COLORS
from components.exportar import render_panel_exportacion

logger = logging.getLogger(__name__)

# Etiquetas de modo
_MODO_EVENTO = "Evento vs Baseline"
_MODO_PERIODO = "Periodo vs Periodo"
_MODO_DETECTIVE = "🔍 Detective de Patrones"

# Orden preferido de variables en graficos
_ORDEN_VARS = ["NO2", "PM2.5", "PM10", "O3", "SO2", "CO"]

# Colores fijos para los dos conjuntos de barras / trazos radar
_COLOR_EVENTO = "#e74c3c"  # rojo
_COLOR_BASE = "#3498db"  # azul
_COLOR_A = "#1f77b4"  # azul oscuro
_COLOR_B = "#ff7f0e"  # naranja


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================


def render_comparador(
    df_contaminacion: Optional[pd.DataFrame],
    df_eventos: Optional[pd.DataFrame],
) -> None:
    """
    Renderiza el comparador historico de contaminacion.

    Modos disponibles:
      - Evento vs Baseline: metricas + grafico de barras por variable.
      - Periodo vs Periodo: grafico radar normalizado + tabla de cambio %.

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion (filtros globales).
        df_eventos: DataFrame de impacto de eventos masivos.
    """
    modo = st.radio(
        "Modo de comparación",
        [_MODO_EVENTO, _MODO_PERIODO, _MODO_DETECTIVE],
        horizontal=True,
        help=(
            "**Evento vs Baseline**: compara un evento concreto con dias normales. "
            "**Periodo vs Periodo**: compara dos rangos de fechas libremente. "
            "**Detective de Patrones**: detecta anomalías, correlaciones y patrones temporales."
        ),
    )

    st.divider()

    df_exportar: Optional[pd.DataFrame] = None

    if modo == _MODO_EVENTO:
        df_exportar = _render_modo_evento(df_eventos)
    elif modo == _MODO_PERIODO:
        df_exportar = _render_modo_periodo(df_contaminacion)
    else:
        df_exportar = _render_modo_detective(df_contaminacion)

    st.divider()
    render_panel_exportacion(df_exportar, nombre_dataset="comparador")


# ==============================================================================
# MODO A: EVENTO VS BASELINE
# ==============================================================================


def _render_modo_evento(df_eventos: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Modo Evento vs Baseline.

    Muestra metricas clave del evento seleccionado (impacto en contaminacion,
    trafico y condiciones meteorologicas) y un grafico de barras comparativo
    variable a variable.

    Args:
        df_eventos: DataFrame de impacto_eventos.csv.

    Returns:
        DataFrame del evento seleccionado (para exportacion).
    """
    if df_eventos is None or df_eventos.empty:
        st.info(
            "Sin datos de eventos disponibles. "
            "Ejecuta correlacion_eventos.py para generar impacto_eventos.csv."
        )
        return None

    # --- Selector de evento ---
    eventos_disponibles = sorted(df_eventos["nombre_evento"].dropna().unique().tolist())
    if not eventos_disponibles:
        st.warning("No hay eventos en el dataset.")
        return None

    nombre_sel = st.selectbox(
        "Selecciona el evento",
        options=eventos_disponibles,
        help="Eventos procesados en la fase de correlacion ETL.",
    )

    df_ev = df_eventos[df_eventos["nombre_evento"] == nombre_sel].copy()
    if df_ev.empty:
        st.warning(f"Sin datos para el evento '{nombre_sel}'.")
        return None

    logger.info(
        "[Comparador] Evento seleccionado: %s (%d variables)", nombre_sel, len(df_ev)
    )

    # Metadatos del evento (primera fila; campos event-level)
    primera = df_ev.iloc[0]
    fecha_ini = primera.get("fecha_inicio", "")
    fecha_fin = primera.get("fecha_fin", "")
    tipo = primera.get("tipo_evento", primera.get("categoria_evento", ""))

    # Cabecera del evento
    st.markdown(
        f'<div style="border-left:4px solid {_COLOR_EVENTO};padding-left:12px;margin-bottom:1rem;">'
        f'<h4 style="margin:0;">{nombre_sel}</h4>'
        f'<small style="color:#888;">{tipo} &nbsp;|&nbsp; '
        f"{_fmt_fecha(fecha_ini)} → {_fmt_fecha(fecha_fin)}</small>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Elegir variable para los 4 KPIs (la de mayor impacto absoluto)
    variables_ev = [v for v in _ORDEN_VARS if v in df_ev["variable"].values]
    if not variables_ev:
        variables_ev = df_ev["variable"].dropna().unique().tolist()

    var_kpi_default = _variable_mayor_impacto(df_ev, variables_ev)
    var_kpi = st.selectbox(
        "Variable para KPIs",
        options=variables_ev,
        index=(
            variables_ev.index(var_kpi_default)
            if var_kpi_default in variables_ev
            else 0
        ),
        help="Elige la variable cuyos KPIs quieres ver en detalle.",
    )

    fila_kpi = df_ev[df_ev["variable"] == var_kpi].iloc[0]

    # --- 4 KPIs ---
    _render_kpis_evento(fila_kpi, var_kpi)

    st.divider()

    # --- Grafico de barras multivariable ---
    fig = _grafico_comparativo_barras(df_ev, nombre_sel, variables_ev)
    st.plotly_chart(fig, use_container_width=True)

    return df_ev


def _render_kpis_evento(fila: pd.Series, variable: str) -> None:
    """
    Renderiza 4 st.metric para la variable seleccionada del evento.

    KPIs: contaminacion (media evento vs baseline), impacto %, trafico, meteo.

    Args:
        fila: Serie con datos de una variable del evento.
        variable: Nombre de la variable seleccionada.
    """
    media_ev = _safe_float(fila.get("media_evento"))
    media_bl = _safe_float(fila.get("media_baseline"))
    imp_pct = _safe_float(fila.get("impacto_pct"))
    traf_pct = _safe_float(fila.get("impacto_trafico_pct"))
    temp_ev = _safe_float(fila.get("media_temp_evento"))
    temp_bl = _safe_float(fila.get("media_temp_baseline"))
    _safe_int(fila.get("n_dias_evento"))
    n_dias_bl = _safe_int(fila.get("n_dias_baseline"))

    c1, c2, c3, c4 = st.columns(4)

    # KPI 1: Contaminacion media
    if media_ev is not None and media_bl is not None:
        delta_c = f"{media_ev - media_bl:+.1f} µg/m³ vs baseline"
        c1.metric(
            label=f"{variable} durante evento",
            value=f"{media_ev:.1f} µg/m³",
            delta=delta_c,
            delta_color="inverse",
            help=f"Media baseline: {media_bl:.1f} µg/m³ ({n_dias_bl} días)",
        )
    else:
        c1.metric(label=f"{variable} durante evento", value="-")

    # KPI 2: Impacto %
    if imp_pct is not None:
        c2.metric(
            label="Impacto en contaminación",
            value=f"{imp_pct:+.1f}%",
            delta="Aumento" if imp_pct > 0 else "Reducción",
            delta_color="inverse" if imp_pct > 0 else "normal",
            help="Cambio porcentual de la media durante el evento vs baseline.",
        )
    else:
        c2.metric(label="Impacto en contaminación", value="-")

    # KPI 3: Trafico
    if traf_pct is not None:
        c3.metric(
            label="Impacto en tráfico",
            value=f"{traf_pct:+.1f}%",
            delta="Más tráfico" if traf_pct > 0 else "Menos tráfico",
            delta_color="inverse" if traf_pct > 0 else "normal",
            help="Cambio porcentual de incidencias de tráfico durante el evento.",
        )
    else:
        c3.metric(label="Impacto en tráfico", value="-")

    # KPI 4: Temperatura
    if temp_ev is not None and temp_bl is not None:
        delta_t = f"{temp_ev - temp_bl:+.1f}°C vs baseline"
        c4.metric(
            label="Temperatura media",
            value=f"{temp_ev:.1f}°C",
            delta=delta_t,
            help=f"Temp. media baseline: {temp_bl:.1f}°C",
        )
    else:
        c4.metric(label="Temperatura media", value="-")


def _variable_mayor_impacto(df_ev: pd.DataFrame, variables: list) -> Optional[str]:
    """
    Retorna la variable con mayor impacto_pct absoluto en el evento.

    Args:
        df_ev: DataFrame del evento.
        variables: Lista de variables disponibles.

    Returns:
        Nombre de la variable mas impactada, o None.
    """
    if "impacto_pct" not in df_ev.columns or not variables:
        return variables[0] if variables else None
    sub = df_ev[df_ev["variable"].isin(variables)].copy()
    sub["_abs"] = sub["impacto_pct"].abs()
    idx = sub["_abs"].idxmax()
    return sub.loc[idx, "variable"] if pd.notna(idx) else variables[0]


def _grafico_comparativo_barras(
    df_ev: pd.DataFrame,
    nombre_evento: str,
    variables: list,
) -> go.Figure:
    """
    Grafico de barras agrupadas: media durante evento vs media baseline.

    Cada par de barras representa una variable de contaminacion.
    Las barras se colorean uniformemente (rojo = evento, azul = baseline).

    Args:
        df_ev: DataFrame del evento (todas las variables).
        nombre_evento: Nombre del evento para el titulo.
        variables: Lista de variables en el orden de visualizacion.

    Returns:
        Figura Plotly lista para st.plotly_chart.
    """
    df_plot = df_ev[df_ev["variable"].isin(variables)].copy()
    df_plot["variable"] = pd.Categorical(
        df_plot["variable"], categories=variables, ordered=True
    )
    df_plot = df_plot.sort_values("variable")

    medias_ev = []
    medias_bl = []
    labels_x = []

    for _, fila in df_plot.iterrows():
        me = _safe_float(fila.get("media_evento"))
        mb = _safe_float(fila.get("media_baseline"))
        if me is None and mb is None:
            continue
        labels_x.append(fila["variable"])
        medias_ev.append(me if me is not None else 0.0)
        medias_bl.append(mb if mb is not None else 0.0)

    if not labels_x:
        return go.Figure().update_layout(title="Sin datos para el gráfico")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Durante evento",
            x=labels_x,
            y=medias_ev,
            marker_color=_COLOR_EVENTO,
            opacity=0.85,
            text=[f"{v:.1f}" for v in medias_ev],
            textposition="outside",
        )
    )
    fig.add_trace(
        go.Bar(
            name="Baseline (días normales)",
            x=labels_x,
            y=medias_bl,
            marker_color=_COLOR_BASE,
            opacity=0.85,
            text=[f"{v:.1f}" for v in medias_bl],
            textposition="outside",
        )
    )

    fig.update_layout(
        title=f"{nombre_evento} — Contaminación: Evento vs Baseline",
        xaxis_title="Variable",
        yaxis_title="Media (µg/m³)",
        barmode="group",
        template=get_theme()["plotly_template"],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=80, b=40),
        height=420,
    )

    logger.info(
        "[Comparador] Grafico barras generado: %s, %d variables.",
        nombre_evento,
        len(labels_x),
    )
    return fig


# ==============================================================================
# MODO B: PERIODO VS PERIODO
# ==============================================================================


def _render_modo_periodo(
    df_contaminacion: Optional[pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """
    Modo Periodo vs Periodo.

    Calcula medias por variable en dos rangos de fechas y muestra:
      - Grafico radar (valores normalizados al umbral OMS)
      - Tabla con Variable | Periodo A | Periodo B | Cambio %

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion.

    Returns:
        DataFrame con la tabla comparativa (para exportacion).
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.info("Sin datos de contaminacion para el modo Periodo vs Periodo.")
        return None

    if "fecha_utc" not in df_contaminacion.columns:
        st.warning(
            "El dataset no contiene columna 'fecha_utc'; no es posible filtrar por periodo."
        )
        return None

    # Derivar limites de fechas del propio dataset
    fechas = pd.to_datetime(
        df_contaminacion["fecha_utc"], utc=True, errors="coerce"
    ).dropna()
    fecha_max = fechas.max().date()
    fecha_min = fechas.min().date()

    # Defaults: Periodo A = ultimos 30 dias, Periodo B = mismo periodo 1 año antes
    default_a_fin = fecha_max
    default_a_ini = max(fecha_min, fecha_max - timedelta(days=30))
    default_b_fin = default_a_fin - timedelta(days=365)
    default_b_ini = default_a_ini - timedelta(days=365)
    default_b_ini = max(fecha_min, default_b_ini)
    default_b_fin = max(fecha_min, default_b_fin)

    # --- Selector de periodos ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(
            f'<b style="color:{_COLOR_A};">Periodo A</b>', unsafe_allow_html=True
        )
        periodo_a = st.date_input(
            "Rango Periodo A",
            value=(default_a_ini, default_a_fin),
            min_value=fecha_min,
            max_value=fecha_max,
            key="comparador_periodo_a",
        )
    with col_b:
        st.markdown(
            f'<b style="color:{_COLOR_B};">Periodo B</b>', unsafe_allow_html=True
        )
        periodo_b = st.date_input(
            "Rango Periodo B",
            value=(default_b_ini, default_b_fin),
            min_value=fecha_min,
            max_value=fecha_max,
            key="comparador_periodo_b",
        )

    # Validar que ambos periodos son rangos completos (tupla de 2 fechas)
    if not (isinstance(periodo_a, (tuple, list)) and len(periodo_a) == 2):
        st.info("Selecciona fecha de inicio Y fin para el Periodo A.")
        return None
    if not (isinstance(periodo_b, (tuple, list)) and len(periodo_b) == 2):
        st.info("Selecciona fecha de inicio Y fin para el Periodo B.")
        return None

    ini_a, fin_a = periodo_a
    ini_b, fin_b = periodo_b

    if ini_a > fin_a or ini_b > fin_b:
        st.error("La fecha de inicio debe ser anterior a la fecha de fin.")
        return None

    # --- Filtrar y calcular medias ---
    medias_a, label_a, n_a = _calcular_medias_periodo(df_contaminacion, ini_a, fin_a)
    medias_b, label_b, n_b = _calcular_medias_periodo(df_contaminacion, ini_b, fin_b)

    if not medias_a and not medias_b:
        st.warning("Sin datos en ninguno de los dos periodos seleccionados.")
        return None

    col_info_a, col_info_b = st.columns(2)
    with col_info_a:
        st.caption(f"Periodo A: {label_a} — {n_a:,} registros")
    with col_info_b:
        st.caption(f"Periodo B: {label_b} — {n_b:,} registros")

    if not medias_a:
        st.warning(f"Sin datos válidos en el Periodo A ({label_a}).")
    if not medias_b:
        st.warning(f"Sin datos válidos en el Periodo B ({label_b}).")

    if not medias_a or not medias_b:
        return None

    logger.info(
        "[Comparador] Periodo A: %s (%d registros) | Periodo B: %s (%d registros)",
        label_a,
        n_a,
        label_b,
        n_b,
    )

    # --- Grafico radar ---
    fig_radar = _grafico_radar_periodos(medias_a, medias_b, label_a, label_b)
    st.plotly_chart(fig_radar, use_container_width=True)

    st.divider()

    # --- Tabla comparativa ---
    df_tabla = _tabla_comparativa(medias_a, medias_b, label_a, label_b)
    st.dataframe(
        df_tabla,
        column_config={
            "Periodo A (µg/m³)": st.column_config.NumberColumn(format="%.2f"),
            "Periodo B (µg/m³)": st.column_config.NumberColumn(format="%.2f"),
            "Cambio %": st.column_config.NumberColumn(format="%.1f%%"),
            "Umbral OMS": st.column_config.NumberColumn(format="%.0f"),
        },
        hide_index=True,
        use_container_width=True,
    )

    return df_tabla


def _calcular_medias_periodo(
    df: pd.DataFrame,
    fecha_ini: date,
    fecha_fin: date,
) -> Tuple[Dict[str, float], str, int]:
    """
    Calcula la media de cada variable en un rango de fechas.

    Args:
        df: DataFrame de contaminacion con columna 'fecha_utc'.
        fecha_ini: Fecha de inicio del periodo.
        fecha_fin: Fecha de fin del periodo (inclusive).

    Returns:
        Tupla (medias_dict, label_str, n_registros).
        medias_dict: {variable: media_µg_m3}
    """
    label = f"{fecha_ini.strftime('%d/%m/%Y')} → {fecha_fin.strftime('%d/%m/%Y')}"

    # Filtrar por fecha usando la parte de fecha de fecha_utc
    col_fecha = pd.to_datetime(df["fecha_utc"], utc=True, errors="coerce")
    mascara = (col_fecha.dt.date >= fecha_ini) & (col_fecha.dt.date <= fecha_fin)
    df_periodo = df[mascara].copy()

    if df_periodo.empty:
        return {}, label, 0

    if "calidad_dato" in df_periodo.columns:
        df_periodo = df_periodo[df_periodo["calidad_dato"] == "ok"]

    n = len(df_periodo)
    medias: Dict[str, float] = {}
    for var in _ORDEN_VARS:
        sub = df_periodo[df_periodo["variable"] == var]["valor"].dropna()
        if not sub.empty:
            medias[var] = round(float(sub.mean()), 3)

    return medias, label, n


def _grafico_radar_periodos(
    medias_a: Dict[str, float],
    medias_b: Dict[str, float],
    label_a: str,
    label_b: str,
) -> go.Figure:
    """
    Grafico radar (spider) que compara dos periodos.

    Los valores se normalizan dividiendo por el umbral OMS de cada variable,
    de modo que 1.0 = limite OMS (escala comparable entre contaminantes).

    Args:
        medias_a: {variable: media} del Periodo A.
        medias_b: {variable: media} del Periodo B.
        label_a: Etiqueta del Periodo A.
        label_b: Etiqueta del Periodo B.

    Returns:
        Figura Plotly con el radar chart.
    """
    # Variables presentes en al menos uno de los dos periodos
    variables_radar = [v for v in _ORDEN_VARS if v in medias_a or v in medias_b]

    if not variables_radar:
        return go.Figure().update_layout(title="Sin datos para el gráfico radar")

    # Normalizar: valor / umbral_OMS  (1.0 = limite OMS)
    def _normalizar(medias: Dict[str, float], variables: list) -> list:
        vals = []
        for v in variables:
            umbral = UMBRALES_OMS.get(v, 1.0)
            vals.append(round(medias.get(v, 0.0) / umbral, 4))
        return vals

    vals_a = _normalizar(medias_a, variables_radar)
    vals_b = _normalizar(medias_b, variables_radar)

    # Cerrar el poligono repitiendo el primer punto
    theta = variables_radar + [variables_radar[0]]
    vals_a_closed = vals_a + [vals_a[0]]
    vals_b_closed = vals_b + [vals_b[0]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=vals_a_closed,
            theta=theta,
            fill="toself",
            name=f"Periodo A: {label_a}",
            line_color=_COLOR_A,
            fillcolor=_COLOR_A,
            opacity=0.4,
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=vals_b_closed,
            theta=theta,
            fill="toself",
            name=f"Periodo B: {label_b}",
            line_color=_COLOR_B,
            fillcolor=_COLOR_B,
            opacity=0.4,
        )
    )

    fig.update_layout(
        title="Comparación de periodos (valores normalizados al umbral OMS)",
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max(max(vals_a + vals_b, default=0) * 1.2, 1.5)],
                tickfont=dict(size=10),
            ),
            angularaxis=dict(tickfont=dict(size=11)),
        ),
        template=get_theme()["plotly_template"],
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        height=480,
        margin=dict(t=80, b=80),
        annotations=[
            dict(
                text="1.0 = umbral OMS",
                xref="paper",
                yref="paper",
                x=1.0,
                y=0.0,
                showarrow=False,
                font=dict(size=10, color="#888"),
            )
        ],
    )

    logger.info("[Comparador] Radar generado: %d variables.", len(variables_radar))
    return fig


def _tabla_comparativa(
    medias_a: Dict[str, float],
    medias_b: Dict[str, float],
    label_a: str,
    label_b: str,
) -> pd.DataFrame:
    """
    Construye la tabla comparativa Variable | Periodo A | Periodo B | Cambio %.

    Args:
        medias_a: Medias del Periodo A.
        medias_b: Medias del Periodo B.
        label_a: Etiqueta del Periodo A (para nombre de columna).
        label_b: Etiqueta del Periodo B (para nombre de columna).

    Returns:
        DataFrame con la comparacion.
    """
    variables = [v for v in _ORDEN_VARS if v in medias_a or v in medias_b]
    filas = []
    for var in variables:
        val_a = medias_a.get(var)
        val_b = medias_b.get(var)
        umbral = UMBRALES_OMS.get(var)

        if val_a is not None and val_b is not None and val_b != 0:
            cambio = round((val_a - val_b) / val_b * 100, 1)
        else:
            cambio = None

        filas.append(
            {
                "Variable": var,
                "Periodo A (µg/m³)": val_a,
                "Periodo B (µg/m³)": val_b,
                "Cambio %": cambio,
                "Umbral OMS": umbral,
            }
        )

    return pd.DataFrame(filas)


# ==============================================================================
# MODO C: DETECTIVE DE PATRONES
# ==============================================================================

_MIN_REGISTROS_DETECTIVE = 30


def _render_modo_detective(
    df_contaminacion: Optional[pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """
    Modo Detective de Patrones.

    Analiza dos periodos de contaminacion buscando:
      - Anomalias (z-score)
      - Correlaciones cruzadas entre variables
      - Patrones temporales (dia semana, hora, trimestre)
      - Resumen automatico tipo informe

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion.

    Returns:
        DataFrame resumen para exportacion.
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.info("Sin datos de contaminación para el Detective de Patrones.")
        return None

    if "fecha_utc" not in df_contaminacion.columns:
        st.warning("El dataset no contiene columna 'fecha_utc'.")
        return None

    # --- Derivar limites de fechas ---
    fechas = pd.to_datetime(
        df_contaminacion["fecha_utc"], utc=True, errors="coerce"
    ).dropna()
    fecha_max = fechas.max().date()
    fecha_min = fechas.min().date()

    default_a_fin = fecha_max
    default_a_ini = max(fecha_min, fecha_max - timedelta(days=30))
    default_b_fin = max(fecha_min, default_a_fin - timedelta(days=365))
    default_b_ini = max(fecha_min, default_a_ini - timedelta(days=365))

    # --- Selectores de periodo ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(
            f'<b style="color:{_COLOR_A};">Periodo A (reciente)</b>',
            unsafe_allow_html=True,
        )
        periodo_a = st.date_input(
            "Rango Periodo A",
            value=(default_a_ini, default_a_fin),
            min_value=fecha_min,
            max_value=fecha_max,
            key="detective_periodo_a",
        )
    with col_b:
        st.markdown(
            f'<b style="color:{_COLOR_B};">Periodo B (referencia)</b>',
            unsafe_allow_html=True,
        )
        periodo_b = st.date_input(
            "Rango Periodo B",
            value=(default_b_ini, default_b_fin),
            min_value=fecha_min,
            max_value=fecha_max,
            key="detective_periodo_b",
        )

    if not (isinstance(periodo_a, (tuple, list)) and len(periodo_a) == 2):
        st.info("Selecciona fecha de inicio Y fin para el Periodo A.")
        return None
    if not (isinstance(periodo_b, (tuple, list)) and len(periodo_b) == 2):
        st.info("Selecciona fecha de inicio Y fin para el Periodo B.")
        return None

    ini_a, fin_a = periodo_a
    ini_b, fin_b = periodo_b
    if ini_a > fin_a or ini_b > fin_b:
        st.error("La fecha de inicio debe ser anterior a la fecha de fin.")
        return None

    # --- Filtrar datos ---
    df_a = _filtrar_periodo_df(df_contaminacion, ini_a, fin_a)
    df_b = _filtrar_periodo_df(df_contaminacion, ini_b, fin_b)

    label_a = f"{ini_a.strftime('%d/%m/%Y')} → {fin_a.strftime('%d/%m/%Y')}"
    label_b = f"{ini_b.strftime('%d/%m/%Y')} → {fin_b.strftime('%d/%m/%Y')}"

    col_info_a, col_info_b = st.columns(2)
    with col_info_a:
        st.caption(f"Periodo A: {label_a} — {len(df_a):,} registros")
    with col_info_b:
        st.caption(f"Periodo B: {label_b} — {len(df_b):,} registros")

    if len(df_a) < _MIN_REGISTROS_DETECTIVE and len(df_b) < _MIN_REGISTROS_DETECTIVE:
        st.warning(
            f"Se necesitan al menos {_MIN_REGISTROS_DETECTIVE} registros en "
            f"alguno de los periodos para el análisis. Prueba con rangos más amplios."
        )
        return None

    # Combinar para analisis temporal (usa Periodo A como principal)
    df_principal = df_a if len(df_a) >= len(df_b) else df_b

    # Variables disponibles
    variables_disponibles = [
        v
        for v in _ORDEN_VARS
        if v in df_a["variable"].values or v in df_b["variable"].values
    ]

    # --- Barras comparativas (como Periodo vs Periodo) ---
    medias_a = _medias_por_variable(df_a, variables_disponibles)
    medias_b = _medias_por_variable(df_b, variables_disponibles)

    if medias_a and medias_b:
        fig_barras = _grafico_comparativo_detective(
            medias_a,
            medias_b,
            label_a,
            label_b,
            variables_disponibles,
        )
        st.plotly_chart(fig_barras, use_container_width=True)

    st.divider()

    # --- a) Deteccion de anomalias ---
    st.markdown("### ⚠️ Detección de anomalías")
    df_anomalias = _detectar_anomalias(df_a, df_b, variables_disponibles)
    if df_anomalias is not None:
        _render_anomalias(df_anomalias)
    else:
        st.info("Sin datos suficientes para detectar anomalías entre periodos.")

    st.divider()

    # --- b) Correlaciones cruzadas ---
    st.markdown("### 🔗 Correlaciones cruzadas entre variables")
    if len(df_principal) >= _MIN_REGISTROS_DETECTIVE:
        _render_correlaciones(df_principal, variables_disponibles)
    else:
        st.info("Sin registros suficientes para calcular correlaciones.")

    st.divider()

    # --- c) Patrones temporales ---
    st.markdown("### 📅 Patrones temporales")
    if len(df_principal) >= _MIN_REGISTROS_DETECTIVE:
        _render_patrones_temporales(df_principal, variables_disponibles)
    else:
        st.info("Sin registros suficientes para detectar patrones temporales.")

    st.divider()

    # --- d) Resumen automatico ---
    st.markdown("### 🕵️ Informe del Detective")
    _render_resumen_detective(
        df_a,
        df_b,
        df_anomalias,
        df_principal,
        label_a,
        label_b,
        variables_disponibles,
    )

    logger.info(
        "[Comparador] Detective: A=%d reg, B=%d reg, %d variables",
        len(df_a),
        len(df_b),
        len(variables_disponibles),
    )

    return df_anomalias


def _filtrar_periodo_df(
    df: pd.DataFrame,
    fecha_ini: date,
    fecha_fin: date,
) -> pd.DataFrame:
    """
    Filtra el DataFrame de contaminacion a un rango de fechas, solo registros 'ok'.

    Args:
        df: DataFrame con columna 'fecha_utc'.
        fecha_ini: Inicio del rango (inclusive).
        fecha_fin: Fin del rango (inclusive).

    Returns:
        DataFrame filtrado.
    """
    col_fecha = pd.to_datetime(df["fecha_utc"], utc=True, errors="coerce")
    mascara = (col_fecha.dt.date >= fecha_ini) & (col_fecha.dt.date <= fecha_fin)
    df_filtrado = df[mascara].copy()
    if "calidad_dato" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["calidad_dato"] == "ok"]
    return df_filtrado


def _medias_por_variable(
    df: pd.DataFrame,
    variables: List[str],
) -> Dict[str, float]:
    """Calcula la media de cada variable en el DataFrame."""
    medias: Dict[str, float] = {}
    for var in variables:
        sub = df[df["variable"] == var]["valor"].dropna()
        if not sub.empty:
            medias[var] = round(float(sub.mean()), 3)
    return medias


def _grafico_comparativo_detective(
    medias_a: Dict[str, float],
    medias_b: Dict[str, float],
    label_a: str,
    label_b: str,
    variables: List[str],
) -> go.Figure:
    """Grafico de barras agrupadas para los dos periodos del detective."""
    vars_plot = [v for v in variables if v in medias_a or v in medias_b]
    vals_a = [medias_a.get(v, 0.0) for v in vars_plot]
    vals_b = [medias_b.get(v, 0.0) for v in vars_plot]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name=f"A: {label_a}",
            x=vars_plot,
            y=vals_a,
            marker_color=_COLOR_A,
            opacity=0.85,
            text=[f"{v:.1f}" for v in vals_a],
            textposition="outside",
        )
    )
    fig.add_trace(
        go.Bar(
            name=f"B: {label_b}",
            x=vars_plot,
            y=vals_b,
            marker_color=_COLOR_B,
            opacity=0.85,
            text=[f"{v:.1f}" for v in vals_b],
            textposition="outside",
        )
    )
    fig.update_layout(
        title="Comparación de medias por variable",
        xaxis_title="Variable",
        yaxis_title="Media (µg/m³)",
        barmode="group",
        template=get_theme()["plotly_template"],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=80, b=40),
        height=400,
    )
    return fig


# --- a) ANOMALIAS ---


def _detectar_anomalias(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    variables: List[str],
) -> Optional[pd.DataFrame]:
    """
    Detecta cambios significativos entre periodos usando z-score.

    Para cada variable, calcula z = (media_A - media_B) / std_B.
    |z| > 2 → cambio significativo, |z| > 3 → anomalia.

    Args:
        df_a: DataFrame del Periodo A.
        df_b: DataFrame del Periodo B (referencia para std).
        variables: Lista de variables a analizar.

    Returns:
        DataFrame con columnas [Variable, Media_A, Media_B, Std_B, Z_score,
        Cambio_pct, Diagnostico], o None si no hay datos.
    """
    filas = []
    for var in variables:
        vals_a = df_a[df_a["variable"] == var]["valor"].dropna()
        vals_b = df_b[df_b["variable"] == var]["valor"].dropna()
        if vals_a.empty or vals_b.empty:
            continue
        media_a = float(vals_a.mean())
        media_b = float(vals_b.mean())
        std_b = float(vals_b.std())

        if std_b > 0:
            z = (media_a - media_b) / std_b
        else:
            z = 0.0

        cambio_pct = ((media_a - media_b) / media_b * 100) if media_b != 0 else 0.0

        if abs(z) > 3:
            diag = "🚨 Anomalía detectada"
        elif abs(z) > 2:
            diag = "⚠️ Cambio significativo"
        else:
            diag = "✅ Normal"

        filas.append(
            {
                "Variable": var,
                "Media A": round(media_a, 2),
                "Media B": round(media_b, 2),
                "Std B": round(std_b, 2),
                "Z-score": round(z, 2),
                "Cambio %": round(cambio_pct, 1),
                "Diagnóstico": diag,
            }
        )

    if not filas:
        return None
    return pd.DataFrame(filas)


def _render_anomalias(df_anomalias: pd.DataFrame) -> None:
    """Renderiza la tabla de anomalias con colores segun diagnostico."""
    # Destacar anomalias con metricas
    anomalias = df_anomalias[df_anomalias["Z-score"].abs() > 2]
    if not anomalias.empty:
        cols = st.columns(min(len(anomalias), 4))
        for i, (_, fila) in enumerate(anomalias.iterrows()):
            with cols[i % len(cols)]:
                st.metric(
                    label=f"{fila['Variable']} {fila['Diagnóstico'].split()[0]}",
                    value=f"{fila['Cambio %']:+.1f}%",
                    delta=f"z={fila['Z-score']:+.2f}",
                    delta_color="inverse" if fila["Cambio %"] > 0 else "normal",
                )
    else:
        st.success("Sin anomalías significativas entre los periodos seleccionados.")

    st.dataframe(
        df_anomalias,
        column_config={
            "Media A": st.column_config.NumberColumn(format="%.2f"),
            "Media B": st.column_config.NumberColumn(format="%.2f"),
            "Std B": st.column_config.NumberColumn(format="%.2f"),
            "Z-score": st.column_config.NumberColumn(format="%.2f"),
            "Cambio %": st.column_config.NumberColumn(format="%.1f%%"),
        },
        hide_index=True,
        use_container_width=True,
    )


# --- b) CORRELACIONES ---


def _render_correlaciones(
    df: pd.DataFrame,
    variables: List[str],
) -> None:
    """
    Calcula y muestra la matriz de correlacion de Pearson entre variables.

    Pivotea los datos a formato ancho (fecha x variable) y calcula .corr().
    Resalta correlaciones fuertes (|r| > 0.7).

    Args:
        df: DataFrame de contaminacion.
        variables: Variables a incluir.
    """
    # Necesitamos una columna temporal para agrupar
    df_work = df[df["variable"].isin(variables)].copy()
    df_work["fecha_dia"] = pd.to_datetime(
        df_work["fecha_utc"], utc=True, errors="coerce"
    ).dt.date

    # Pivotar: media diaria por variable
    pivot = df_work.pivot_table(
        index="fecha_dia",
        columns="variable",
        values="valor",
        aggfunc="mean",
    )

    # Necesitamos al menos 2 variables con datos superpuestos
    pivot = pivot.dropna(axis=1, how="all")
    if pivot.shape[1] < 2:
        st.info(
            "Se necesitan al menos 2 variables con datos en el periodo para correlaciones."
        )
        return

    corr = pivot.corr()

    # Heatmap Plotly
    vars_corr = [v for v in _ORDEN_VARS if v in corr.columns]
    corr_ordered = corr.loc[vars_corr, vars_corr]

    # Texto con valores formateados
    text_matrix = [
        [f"{corr_ordered.iloc[i, j]:.2f}" for j in range(len(vars_corr))]
        for i in range(len(vars_corr))
    ]

    fig = go.Figure(
        data=go.Heatmap(
            z=corr_ordered.values,
            x=vars_corr,
            y=vars_corr,
            text=text_matrix,
            texttemplate="%{text}",
            colorscale="RdBu_r",
            zmin=-1,
            zmax=1,
            colorbar=dict(title="r"),
        )
    )
    fig.update_layout(
        title="Correlación de Pearson entre contaminantes (medias diarias)",
        template=get_theme()["plotly_template"],
        height=400,
        margin=dict(t=60, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Destacar pares con correlacion fuerte
    pares_fuertes = []
    for i, v1 in enumerate(vars_corr):
        for j, v2 in enumerate(vars_corr):
            if i < j:
                r = corr_ordered.loc[v1, v2]
                if abs(r) > 0.7:
                    tipo = "positiva" if r > 0 else "negativa"
                    pares_fuertes.append(f"**{v1} ↔ {v2}**: r={r:.2f} ({tipo})")

    if pares_fuertes:
        st.markdown("**Correlaciones fuertes (|r| > 0.7):**")
        for p in pares_fuertes:
            st.markdown(f"- {p}")
    else:
        st.caption("Sin correlaciones fuertes (|r| > 0.7) entre variables.")


# --- c) PATRONES TEMPORALES ---


@st.cache_data(ttl=3600, show_spinner=False)
def _preparar_datos_temporales(
    df: pd.DataFrame,
    variables: tuple,
) -> pd.DataFrame:
    """
    Prepara el DataFrame para analisis temporal: parsea fechas y aniade
    columnas dia_semana, hora y trimestre.  Cacheado 1h para evitar
    repetir pd.to_datetime sobre datos que no cambian.

    Args:
        df: DataFrame de contaminacion filtrado.
        variables: Tupla de variables a incluir (hashable para cache).

    Returns:
        DataFrame listo con columnas dia_semana, hora, trimestre.
    """
    df_work = df[df["variable"].isin(variables)].copy()
    df_work["fecha_dt"] = pd.to_datetime(
        df_work["fecha_utc"], utc=True, errors="coerce"
    )
    df_work = df_work.dropna(subset=["fecha_dt"])
    if df_work.empty:
        return df_work
    df_work["dia_semana"] = df_work["fecha_dt"].dt.day_name()
    df_work["hora"] = df_work["fecha_dt"].dt.hour
    df_work["trimestre"] = df_work["fecha_dt"].dt.quarter
    return df_work


def _render_patrones_temporales(
    df: pd.DataFrame,
    variables: List[str],
) -> None:
    """
    Identifica patrones temporales: dia de semana, hora, trimestre.

    Args:
        df: DataFrame de contaminacion.
        variables: Variables disponibles.
    """
    df_work = _preparar_datos_temporales(df, tuple(variables))

    if df_work.empty:
        st.info("Sin datos temporales para analizar.")
        return

    # Variable principal para analisis (la de mas registros)
    var_counts = df_work["variable"].value_counts()
    var_counts.index[0]

    var_selector = st.selectbox(
        "Variable para patrones temporales",
        options=[v for v in _ORDEN_VARS if v in var_counts.index],
        index=0,
        key="detective_var_temporal",
    )
    df_var = df_work[df_work["variable"] == var_selector].copy()

    col_dia, col_hora = st.columns(2)

    # --- Heatmap: dia de semana ---
    with col_dia:
        dias_orden = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        dias_es = {
            "Monday": "Lunes",
            "Tuesday": "Martes",
            "Wednesday": "Miércoles",
            "Thursday": "Jueves",
            "Friday": "Viernes",
            "Saturday": "Sábado",
            "Sunday": "Domingo",
        }

        media_dia = df_var.groupby("dia_semana")["valor"].mean()
        dias_con_datos = [d for d in dias_orden if d in media_dia.index]
        if dias_con_datos:
            vals_dia = [round(media_dia.get(d, 0), 2) for d in dias_con_datos]
            labels_dia = [dias_es.get(d, d) for d in dias_con_datos]
            color_var = VARIABLE_COLORS.get(var_selector, "#1f77b4")

            fig_dia = go.Figure(
                go.Bar(
                    x=labels_dia,
                    y=vals_dia,
                    marker_color=color_var,
                    opacity=0.8,
                    text=[f"{v:.1f}" for v in vals_dia],
                    textposition="outside",
                )
            )
            fig_dia.update_layout(
                title=f"{var_selector} — Media por día de la semana",
                yaxis_title="µg/m³",
                template=get_theme()["plotly_template"],
                height=350,
                margin=dict(t=50, b=30),
            )
            st.plotly_chart(fig_dia, use_container_width=True)

            peor_dia_idx = int(np.argmax(vals_dia))
            st.caption(
                f"Peor día: **{labels_dia[peor_dia_idx]}** ({vals_dia[peor_dia_idx]:.1f} µg/m³)"
            )

    # --- Hora del dia ---
    with col_hora:
        horas_con_datos = sorted(df_var["hora"].unique())
        if len(horas_con_datos) > 1:
            media_hora = df_var.groupby("hora")["valor"].mean()
            vals_hora = [round(media_hora.get(h, 0), 2) for h in horas_con_datos]

            fig_hora = go.Figure(
                go.Scatter(
                    x=horas_con_datos,
                    y=vals_hora,
                    mode="lines+markers",
                    line=dict(
                        color=VARIABLE_COLORS.get(var_selector, "#1f77b4"), width=2
                    ),
                    marker=dict(size=6),
                    fill="tozeroy",
                    fillcolor=(
                        VARIABLE_COLORS.get(var_selector, "#1f77b4")
                        .replace(")", ",0.15)")
                        .replace("rgb", "rgba")
                        if "rgb" in VARIABLE_COLORS.get(var_selector, "")
                        else None
                    ),
                )
            )
            fig_hora.update_layout(
                title=f"{var_selector} — Media por hora del día",
                xaxis_title="Hora",
                yaxis_title="µg/m³",
                template=get_theme()["plotly_template"],
                height=350,
                margin=dict(t=50, b=30),
                xaxis=dict(dtick=2),
            )
            st.plotly_chart(fig_hora, use_container_width=True)

            hora_pico = horas_con_datos[int(np.argmax(vals_hora))]
            st.caption(f"Hora pico: **{hora_pico}:00** ({max(vals_hora):.1f} µg/m³)")
        else:
            st.info("Sin variación horaria suficiente (datos sin detalle de hora).")

    # --- Estacionalidad (trimestre) ---
    trimestres_con_datos = sorted(df_var["trimestre"].unique())
    if len(trimestres_con_datos) > 1:
        media_trim = df_var.groupby("trimestre")["valor"].mean()
        trim_labels = {
            1: "Q1 (Ene-Mar)",
            2: "Q2 (Abr-Jun)",
            3: "Q3 (Jul-Sep)",
            4: "Q4 (Oct-Dic)",
        }
        labels_t = [trim_labels.get(t, f"Q{t}") for t in trimestres_con_datos]
        vals_t = [round(media_trim.get(t, 0), 2) for t in trimestres_con_datos]

        fig_trim = go.Figure(
            go.Bar(
                x=labels_t,
                y=vals_t,
                marker_color=[VARIABLE_COLORS.get(var_selector, "#1f77b4")]
                * len(vals_t),
                opacity=0.8,
                text=[f"{v:.1f}" for v in vals_t],
                textposition="outside",
            )
        )
        fig_trim.update_layout(
            title=f"{var_selector} — Media por trimestre (estacionalidad)",
            yaxis_title="µg/m³",
            template=get_theme()["plotly_template"],
            height=320,
            margin=dict(t=50, b=30),
        )
        st.plotly_chart(fig_trim, use_container_width=True)

        peor_trim_idx = int(np.argmax(vals_t))
        st.caption(
            f"Trimestre más contaminado: **{labels_t[peor_trim_idx]}** ({vals_t[peor_trim_idx]:.1f} µg/m³)"
        )


# --- d) RESUMEN AUTOMATICO ---


@st.cache_data(ttl=3600, show_spinner=False)
def _calcular_patron_temporal(
    df: pd.DataFrame,
) -> tuple:
    """
    Calcula el peor dia de la semana y la hora pico de contaminacion.
    Cacheado 1h para no repetir pd.to_datetime + groupby en cada rerun.

    Args:
        df: DataFrame de contaminacion de un periodo.

    Returns:
        Tupla (peor_dia_es, media_peor_dia, hora_pico, media_hora_pico).
        Devuelve (None, 0, None, 0) si no hay datos suficientes.
    """
    dias_es = {
        "Monday": "lunes",
        "Tuesday": "martes",
        "Wednesday": "miércoles",
        "Thursday": "jueves",
        "Friday": "viernes",
        "Saturday": "sábado",
        "Sunday": "domingo",
    }
    df_temp = df.copy()
    df_temp["fecha_dt"] = pd.to_datetime(
        df_temp["fecha_utc"], utc=True, errors="coerce"
    )
    df_temp = df_temp.dropna(subset=["fecha_dt"])
    if df_temp.empty:
        return None, 0.0, None, 0.0

    df_temp["dia_semana"] = df_temp["fecha_dt"].dt.day_name()
    df_temp["hora"] = df_temp["fecha_dt"].dt.hour

    peor_dia_es, media_peor = None, 0.0
    media_dia = df_temp.groupby("dia_semana")["valor"].mean()
    if not media_dia.empty:
        peor_dia = media_dia.idxmax()
        peor_dia_es = dias_es.get(peor_dia, peor_dia)
        media_peor = float(media_dia[peor_dia])

    hora_pico, media_hora_pico = None, 0.0
    if df_temp["hora"].nunique() > 1:
        media_hora = df_temp.groupby("hora")["valor"].mean()
        hora_pico = media_hora.idxmax()
        media_hora_pico = float(media_hora[hora_pico])

    return peor_dia_es, media_peor, hora_pico, media_hora_pico


def _render_resumen_detective(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    df_anomalias: Optional[pd.DataFrame],
    df_principal: pd.DataFrame,
    label_a: str,
    label_b: str,
    variables: List[str],
) -> None:
    """
    Genera un resumen automatico tipo 'informe detective'.

    Combina hallazgos de anomalias, correlaciones y patrones en texto legible.

    Args:
        df_a: DataFrame Periodo A.
        df_b: DataFrame Periodo B.
        df_anomalias: Tabla de anomalias (puede ser None).
        df_principal: DataFrame principal (el mayor de los dos).
        label_a: Etiqueta del Periodo A.
        label_b: Etiqueta del Periodo B.
        variables: Variables analizadas.
    """
    parrafos: List[str] = []

    # Hallazgos de anomalias
    if df_anomalias is not None and not df_anomalias.empty:
        anomalias_graves = df_anomalias[df_anomalias["Z-score"].abs() > 2]
        if not anomalias_graves.empty:
            for _, fila in anomalias_graves.iterrows():
                var = fila["Variable"]
                cambio = fila["Cambio %"]
                z = fila["Z-score"]
                direccion = "subió" if cambio > 0 else "bajó"
                var_display = (
                    var.replace("2.5", "₂.₅")
                    .replace("NO2", "NO₂")
                    .replace("O3", "O₃")
                    .replace("PM10", "PM₁₀")
                    .replace("SO2", "SO₂")
                )
                severidad = (
                    "anomalía significativa" if abs(z) > 3 else "cambio significativo"
                )
                parrafos.append(
                    f"El **{var_display}** {direccion} un **{abs(cambio):.1f}%** "
                    f"entre los periodos analizados ({severidad}, z={z:+.2f})."
                )

            # Buscar coincidencias (multiples variables subiendo/bajando)
            subiendo = anomalias_graves[anomalias_graves["Cambio %"] > 0]
            if len(subiendo) >= 2:
                vars_subiendo = ", ".join(subiendo["Variable"].tolist())
                parrafos.append(
                    f"Las variables **{vars_subiendo}** aumentaron simultáneamente, "
                    f"lo que sugiere una fuente común de emisión o condiciones "
                    f"meteorológicas desfavorables."
                )
        else:
            parrafos.append(
                "No se detectaron cambios significativos entre los dos periodos. "
                "Los niveles de contaminación se mantuvieron estables."
            )
    else:
        parrafos.append(
            "Sin datos suficientes en ambos periodos para análisis comparativo."
        )

    # Patron temporal (dia y hora del periodo principal)
    if len(df_principal) >= _MIN_REGISTROS_DETECTIVE:
        peor_dia_es, media_peor_dia, hora_pico, media_hora_pico = (
            _calcular_patron_temporal(df_principal)
        )
        if peor_dia_es:
            parrafos.append(
                f"El peor día de la semana es el **{peor_dia_es}** "
                f"(media: {media_peor_dia:.1f} µg/m³)."
            )
        if hora_pico is not None:
            parrafos.append(
                f"La hora pico de contaminación es las **{int(hora_pico)}:00** "
                f"({media_hora_pico:.1f} µg/m³)."
            )

    # Componer informe
    if not parrafos:
        st.info("Sin hallazgos suficientes para generar un informe.")
        return

    informe = f"**Periodo analizado:** {label_a} vs {label_b}\n\n" + "\n\n".join(
        parrafos
    )

    st.markdown(
        f'<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.1);'
        f'border-radius:10px;padding:20px;margin:8px 0;">'
        f'<div style="font-size:0.95rem;line-height:1.7;">'
        f"{_markdown_to_html(informe)}"
        f"</div></div>",
        unsafe_allow_html=True,
    )


def _markdown_to_html(texto: str) -> str:
    """Conversion minimalista de Markdown bold a HTML bold y saltos de linea."""
    import re

    texto = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", texto)
    texto = texto.replace("\n\n", "<br><br>")
    return texto


# ==============================================================================
# UTILIDADES
# ==============================================================================


def _safe_float(val) -> Optional[float]:
    """Convierte un valor a float, retorna None si no es posible."""
    try:
        return float(val) if pd.notna(val) else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    """Convierte un valor a int, retorna None si no es posible."""
    try:
        return int(val) if pd.notna(val) else None
    except (TypeError, ValueError):
        return None


def _fmt_fecha(val) -> str:
    """Formatea una fecha como 'DD/MM/YYYY' o '-' si no disponible."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    try:
        return pd.Timestamp(val).strftime("%d/%m/%Y")
    except Exception:
        return str(val)
