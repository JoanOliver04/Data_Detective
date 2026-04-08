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
from typing import Optional, Dict, Tuple

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import UMBRALES_OMS, VARIABLE_COLORS, VARIABLES_CONTAMINACION
from components.exportar import render_panel_exportacion

logger = logging.getLogger(__name__)

# Etiquetas de modo
_MODO_EVENTO  = "Evento vs Baseline"
_MODO_PERIODO = "Periodo vs Periodo"

# Orden preferido de variables en graficos
_ORDEN_VARS = ["NO2", "PM2.5", "PM10", "O3", "SO2", "CO"]

# Colores fijos para los dos conjuntos de barras / trazos radar
_COLOR_EVENTO  = "#e74c3c"   # rojo
_COLOR_BASE    = "#3498db"   # azul
_COLOR_A       = "#1f77b4"   # azul oscuro
_COLOR_B       = "#ff7f0e"   # naranja


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
        "Modo de comparacion",
        [_MODO_EVENTO, _MODO_PERIODO],
        horizontal=True,
        help=(
            "**Evento vs Baseline**: compara un evento concreto con dias normales. "
            "**Periodo vs Periodo**: compara dos rangos de fechas libremente."
        ),
    )

    st.divider()

    df_exportar: Optional[pd.DataFrame] = None

    if modo == _MODO_EVENTO:
        df_exportar = _render_modo_evento(df_eventos)
    else:
        df_exportar = _render_modo_periodo(df_contaminacion)

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

    logger.info("[Comparador] Evento seleccionado: %s (%d variables)", nombre_sel, len(df_ev))

    # Metadatos del evento (primera fila; campos event-level)
    primera = df_ev.iloc[0]
    fecha_ini = primera.get("fecha_inicio", "")
    fecha_fin = primera.get("fecha_fin", "")
    tipo      = primera.get("tipo_evento", primera.get("categoria_evento", ""))

    # Cabecera del evento
    st.markdown(
        f'<div style="border-left:4px solid {_COLOR_EVENTO};padding-left:12px;margin-bottom:1rem;">'
        f'<h4 style="margin:0;">{nombre_sel}</h4>'
        f'<small style="color:#888;">{tipo} &nbsp;|&nbsp; '
        f'{_fmt_fecha(fecha_ini)} → {_fmt_fecha(fecha_fin)}</small>'
        f'</div>',
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
        index=variables_ev.index(var_kpi_default) if var_kpi_default in variables_ev else 0,
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
    media_ev  = _safe_float(fila.get("media_evento"))
    media_bl  = _safe_float(fila.get("media_baseline"))
    imp_pct   = _safe_float(fila.get("impacto_pct"))
    traf_pct  = _safe_float(fila.get("impacto_trafico_pct"))
    temp_ev   = _safe_float(fila.get("media_temp_evento"))
    temp_bl   = _safe_float(fila.get("media_temp_baseline"))
    n_dias_ev = _safe_int(fila.get("n_dias_evento"))
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
            help=f"Media baseline: {media_bl:.1f} µg/m³ ({n_dias_bl} dias)",
        )
    else:
        c1.metric(label=f"{variable} durante evento", value="-")

    # KPI 2: Impacto %
    if imp_pct is not None:
        c2.metric(
            label="Impacto en contaminacion",
            value=f"{imp_pct:+.1f}%",
            delta="Aumento" if imp_pct > 0 else "Reduccion",
            delta_color="inverse" if imp_pct > 0 else "normal",
            help="Cambio porcentual de la media durante el evento vs baseline.",
        )
    else:
        c2.metric(label="Impacto en contaminacion", value="-")

    # KPI 3: Trafico
    if traf_pct is not None:
        c3.metric(
            label="Impacto en trafico",
            value=f"{traf_pct:+.1f}%",
            delta="Mas trafico" if traf_pct > 0 else "Menos trafico",
            delta_color="inverse" if traf_pct > 0 else "normal",
            help="Cambio porcentual de incidencias de trafico durante el evento.",
        )
    else:
        c3.metric(label="Impacto en trafico", value="-")

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
    df_plot["variable"] = pd.Categorical(df_plot["variable"], categories=variables, ordered=True)
    df_plot = df_plot.sort_values("variable")

    medias_ev = []
    medias_bl = []
    labels_x  = []

    for _, fila in df_plot.iterrows():
        me = _safe_float(fila.get("media_evento"))
        mb = _safe_float(fila.get("media_baseline"))
        if me is None and mb is None:
            continue
        labels_x.append(fila["variable"])
        medias_ev.append(me if me is not None else 0.0)
        medias_bl.append(mb if mb is not None else 0.0)

    if not labels_x:
        return go.Figure().update_layout(title="Sin datos para el grafico")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Durante evento",
        x=labels_x,
        y=medias_ev,
        marker_color=_COLOR_EVENTO,
        opacity=0.85,
        text=[f"{v:.1f}" for v in medias_ev],
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        name="Baseline (dias normales)",
        x=labels_x,
        y=medias_bl,
        marker_color=_COLOR_BASE,
        opacity=0.85,
        text=[f"{v:.1f}" for v in medias_bl],
        textposition="outside",
    ))

    fig.update_layout(
        title=f"{nombre_evento} — Contaminacion: Evento vs Baseline",
        xaxis_title="Variable",
        yaxis_title="Media (µg/m³)",
        barmode="group",
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=80, b=40),
        height=420,
    )

    logger.info(
        "[Comparador] Grafico barras generado: %s, %d variables.", nombre_evento, len(labels_x)
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
        st.info(
            "Sin datos de contaminacion para el modo Periodo vs Periodo."
        )
        return None

    if "fecha_utc" not in df_contaminacion.columns:
        st.warning("El dataset no contiene columna 'fecha_utc'; no es posible filtrar por periodo.")
        return None

    # Derivar limites de fechas del propio dataset
    fechas = pd.to_datetime(df_contaminacion["fecha_utc"], utc=True, errors="coerce").dropna()
    fecha_max = fechas.max().date()
    fecha_min = fechas.min().date()

    # Defaults: Periodo A = ultimos 30 dias, Periodo B = mismo periodo 1 año antes
    default_a_fin   = fecha_max
    default_a_ini   = max(fecha_min, fecha_max - timedelta(days=30))
    default_b_fin   = default_a_fin   - timedelta(days=365)
    default_b_ini   = default_a_ini   - timedelta(days=365)
    default_b_ini   = max(fecha_min, default_b_ini)
    default_b_fin   = max(fecha_min, default_b_fin)

    # --- Selector de periodos ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(f'<b style="color:{_COLOR_A};">Periodo A</b>', unsafe_allow_html=True)
        periodo_a = st.date_input(
            "Rango Periodo A",
            value=(default_a_ini, default_a_fin),
            min_value=fecha_min,
            max_value=fecha_max,
            key="comparador_periodo_a",
        )
    with col_b:
        st.markdown(f'<b style="color:{_COLOR_B};">Periodo B</b>', unsafe_allow_html=True)
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
        st.warning(f"Sin datos validos en el Periodo A ({label_a}).")
    if not medias_b:
        st.warning(f"Sin datos validos en el Periodo B ({label_b}).")

    if not medias_a or not medias_b:
        return None

    logger.info(
        "[Comparador] Periodo A: %s (%d registros) | Periodo B: %s (%d registros)",
        label_a, n_a, label_b, n_b,
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
            "Cambio %":          st.column_config.NumberColumn(format="%.1f%%"),
            "Umbral OMS":        st.column_config.NumberColumn(format="%.0f"),
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
    mascara = (
        (col_fecha.dt.date >= fecha_ini) &
        (col_fecha.dt.date <= fecha_fin)
    )
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
    variables_radar = [
        v for v in _ORDEN_VARS
        if v in medias_a or v in medias_b
    ]

    if not variables_radar:
        return go.Figure().update_layout(title="Sin datos para el radar")

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
    fig.add_trace(go.Scatterpolar(
        r=vals_a_closed,
        theta=theta,
        fill="toself",
        name=f"Periodo A: {label_a}",
        line_color=_COLOR_A,
        fillcolor=_COLOR_A,
        opacity=0.4,
    ))
    fig.add_trace(go.Scatterpolar(
        r=vals_b_closed,
        theta=theta,
        fill="toself",
        name=f"Periodo B: {label_b}",
        line_color=_COLOR_B,
        fillcolor=_COLOR_B,
        opacity=0.4,
    ))

    fig.update_layout(
        title="Comparacion de periodos (valores normalizados al umbral OMS)",
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max(max(vals_a + vals_b, default=0) * 1.2, 1.5)],
                tickfont=dict(size=10),
            ),
            angularaxis=dict(tickfont=dict(size=11)),
        ),
        template="plotly_dark",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        height=480,
        margin=dict(t=80, b=80),
        annotations=[dict(
            text="1.0 = umbral OMS",
            xref="paper", yref="paper",
            x=1.0, y=0.0, showarrow=False,
            font=dict(size=10, color="#888"),
        )],
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

        filas.append({
            "Variable":          var,
            "Periodo A (µg/m³)": val_a,
            "Periodo B (µg/m³)": val_b,
            "Cambio %":          cambio,
            "Umbral OMS":        umbral,
        })

    return pd.DataFrame(filas)


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
