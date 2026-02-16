# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 6.3: Visualización de Impacto de Eventos (Refactorizado)
==============================================================================

Descripción:
    Genera 3 visualizaciones interactivas que muestran cómo los eventos
    masivos de Valencia impactan sobre la contaminación y el tráfico.

    DISEÑO DINÁMICO: No se hardcodea ningún tipo de evento concreto
    (ni "fallas", ni "valenciacf"). El script descubre automáticamente
    todos los valores de tipo_evento que existan en impacto_eventos.csv
    y genera las visualizaciones agrupando por esos tipos.

    Esto lo hace compatible con cualquier clasificación de eventos que
    venga de clasificar_eventos.py (ej: "puntual", "dilatado", o
    cualquier otra categoría futura).

    Visualizaciones:
    ────────────────
    1) Impacto medio en NO₂ por tipo de evento (%)
       → Barras verticales: media de impacto_pct agrupada por tipo_evento
       → Solo filas con variable == "NO2" e impacto_pct válido

    2) Impacto medio en tráfico por tipo de evento (%)
       → Barras verticales: media de impacto_trafico_pct por tipo_evento
       → Deduplicado por evento_id (la columna se repite por variable)

    3) Timeline de NO₂ con eventos superpuestos
       → Línea de NO₂ diario (media ciudad, calidad OK)
       → Regiones sombreadas por cada evento, coloreadas por tipo
       → Rango dinámico centrado en los eventos

Datos de entrada:
    1. 3.DATOS_LIMPIOS/impacto_eventos.csv  (de correlacion_eventos.py)
       Columnas: evento_id, nombre_evento, tipo_evento, impacto_esperado,
                 fecha_inicio, fecha_fin, variable, media_evento,
                 media_baseline, impacto_pct, n_dias_evento, n_dias_baseline,
                 media_temp_evento, media_temp_baseline, media_precip_evento,
                 media_precip_baseline, impacto_trafico_pct

    2. 3.DATOS_LIMPIOS/contaminacion_normalizada.parquet
       Columnas: fecha_utc, variable, valor, calidad_dato

Datos de salida:
    4.VISUALIZACIONES/eventos/impacto_no2_por_tipo.html
    4.VISUALIZACIONES/eventos/impacto_trafico_por_tipo.html
    4.VISUALIZACIONES/eventos/timeline_eventos.html

Ruta esperada del script:
    2.SCRIPTS/procesamiento/generar_visualizaciones_eventos.py

Uso:
    python generar_visualizaciones_eventos.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Entrada ---
IMPACTO_PATH = PROJECT_ROOT / "3.DATOS_LIMPIOS" / "impacto_eventos.csv"
CONTAMINACION_PATH = (
    PROJECT_ROOT / "3.DATOS_LIMPIOS" / "contaminacion_normalizada.parquet"
)

# --- Salida ---
EVENTOS_VIS_DIR = PROJECT_ROOT / "4.VISUALIZACIONES" / "eventos"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# --- Paleta accesible (colorblind-friendly, máx 8 tipos) ---
# Se asigna dinámicamente a los tipos de evento encontrados
ACCESSIBLE_PALETTE = [
    "#1F77B4",   # Azul
    "#FF7F0E",   # Naranja
    "#2CA02C",   # Verde
    "#D62728",   # Rojo
    "#9467BD",   # Morado
    "#8C564B",   # Marrón
    "#E377C2",   # Rosa
    "#7F7F7F",   # Gris
]

# Color para la línea diaria de NO₂
COLOR_NO2_LINE = "#7F7F7F"
COLOR_ROLLING = "#FF7F0E"


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola).
    Mismo patrón que todas las fases del proyecto.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "generar_visualizaciones_eventos.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Vis_Eventos")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

def load_data(
    logger: logging.Logger,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Carga los datasets necesarios para las visualizaciones de eventos.

    Returns:
        Tupla: (df_impacto, df_contam)
        Cualquiera puede ser None si falla la carga.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 1: Carga de datos")
    logger.info("=" * 60)

    df_impacto = None
    df_contam = None

    # --- 1A: Impacto de eventos ---
    logger.info(f"  1A: Impacto eventos \u2192 {IMPACTO_PATH.name}")
    if IMPACTO_PATH.exists():
        try:
            df_impacto = pd.read_csv(IMPACTO_PATH)

            # Parsear fechas
            df_impacto["fecha_inicio"] = pd.to_datetime(
                df_impacto["fecha_inicio"], errors="coerce"
            )
            df_impacto["fecha_fin"] = pd.to_datetime(
                df_impacto["fecha_fin"], errors="coerce"
            )

            logger.info(f"      {len(df_impacto):,} filas cargadas")

            # Descubrimiento dinámico de tipos de evento
            tipos = sorted(df_impacto["tipo_evento"].dropna().unique())
            logger.info(
                f"      Tipos de evento detectados ({len(tipos)}): {tipos}")

            variables = sorted(df_impacto["variable"].dropna().unique())
            logger.info(f"      Variables: {variables}")

            n_events = df_impacto["evento_id"].nunique()
            logger.info(f"      Eventos \u00fanicos: {n_events}")

        except Exception as e:
            logger.error(f"      Error leyendo CSV: {e}")
    else:
        logger.warning(
            f"      No encontrado: {IMPACTO_PATH}\n"
            f"      Ejecuta correlacion_eventos.py (Fase 5.5)"
        )

    # --- 1B: Contaminación normalizada (para timeline) ---
    logger.info(f"  1B: Contaminaci\u00f3n \u2192 {CONTAMINACION_PATH.name}")
    if CONTAMINACION_PATH.exists():
        try:
            df_contam = pd.read_parquet(CONTAMINACION_PATH)
            df_contam["fecha_utc"] = pd.to_datetime(
                df_contam["fecha_utc"], utc=True
            )
            logger.info(f"      {len(df_contam):,} registros cargados")
        except Exception as e:
            logger.error(f"      Error leyendo Parquet: {e}")
    else:
        logger.warning(
            f"      No encontrado: {CONTAMINACION_PATH}\n"
            f"      Ejecuta normalizar_contaminacion.py (Fase 5.1)"
        )

    return df_impacto, df_contam


# ==============================================================================
# UTILIDADES
# ==============================================================================

def _save_figure(
    fig: go.Figure,
    filename: str,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Guarda una figura Plotly como HTML autocontenido.
    """
    EVENTOS_VIS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = EVENTOS_VIS_DIR / filename

    try:
        fig.write_html(
            str(output_path),
            include_plotlyjs=True,
            full_html=True,
        )
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"      Guardado: {filename} ({file_size_kb:.0f} KB)")
        return output_path
    except Exception as e:
        logger.error(f"      Error guardando {filename}: {e}")
        return None


def _build_color_map(tipos: list) -> dict:
    """
    Construye un mapeo dinámico tipo_evento → color accesible.

    Si hay más tipos que colores en la paleta, se reciclan.
    El mapeo es determinista (mismo orden siempre) porque
    los tipos se ordenan alfabéticamente.

    Args:
        tipos: Lista de tipos de evento únicos

    Returns:
        Dict {tipo_evento: color_hex}
    """
    sorted_tipos = sorted(tipos)
    return {
        tipo: ACCESSIBLE_PALETTE[i % len(ACCESSIBLE_PALETTE)]
        for i, tipo in enumerate(sorted_tipos)
    }


# ==============================================================================
# VIS 1: IMPACTO MEDIO EN NO₂ POR TIPO DE EVENTO
# ==============================================================================

def generate_pollution_by_type(
    df_impacto: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un gráfico de barras verticales con el impacto medio en NO₂
    agrupado por tipo_evento.

    Flujo:
    1. Filtrar filas con variable == "NO2" e impacto_pct no nulo
    2. Agrupar por tipo_evento → calcular media de impacto_pct
    3. Crear barras verticales coloreadas por signo (+/-)
    4. Hover con nº eventos y rango de impacto

    NO se hardcodea ningún tipo de evento concreto.

    Args:
        df_impacto: DataFrame de impacto_eventos.csv
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("\u2500" * 40)
    logger.info("VIS 1: Impacto medio en NO\u2082 por tipo de evento")
    logger.info("\u2500" * 40)

    # --- 1. Filtrar NO₂ con impacto válido ---
    df_no2 = df_impacto[
        (df_impacto["variable"] == "NO2") &
        (df_impacto["impacto_pct"].notna())
    ].copy()

    if df_no2.empty:
        # Fallback: probar con cualquier variable
        logger.warning(
            "      Sin datos de NO\u2082, intentando con todas las variables")
        df_no2 = df_impacto[df_impacto["impacto_pct"].notna()].copy()
        if df_no2.empty:
            logger.warning("      Sin datos de impacto v\u00e1lido")
            return None

    logger.info(f"      Filas con impacto v\u00e1lido: {len(df_no2)}")

    # --- 2. Agrupar por tipo_evento ---
    agg = (
        df_no2
        .groupby("tipo_evento", as_index=False)
        .agg(
            impacto_medio=("impacto_pct", "mean"),
            n_eventos=("evento_id", "nunique"),
            impacto_min=("impacto_pct", "min"),
            impacto_max=("impacto_pct", "max"),
        )
    )
    agg["impacto_medio"] = agg["impacto_medio"].round(1)
    agg["impacto_min"] = agg["impacto_min"].round(1)
    agg["impacto_max"] = agg["impacto_max"].round(1)

    n_tipos = len(agg)
    logger.info(f"      Tipos de evento agrupados: {n_tipos}")
    for _, row in agg.iterrows():
        logger.info(
            f"        {row['tipo_evento']:>15}: "
            f"impacto medio = {row['impacto_medio']:+.1f}%, "
            f"{row['n_eventos']} eventos"
        )

    # --- 3. Color según signo del impacto ---
    agg["color"] = agg["impacto_medio"].apply(
        lambda x: "#D62728" if x > 0 else "#2CA02C"
    )

    # --- 4. Crear gráfico ---
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=agg["tipo_evento"],
        y=agg["impacto_medio"],
        marker_color=agg["color"],
        customdata=agg[["n_eventos", "impacto_min", "impacto_max"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Impacto medio: %{y:+.1f}%<br>"
            "Eventos: %{customdata[0]}<br>"
            "Rango: [%{customdata[1]:+.1f}%, %{customdata[2]:+.1f}%]"
            "<extra></extra>"
        ),
    ))

    fig.add_hline(
        y=0,
        line_dash="solid",
        line_color="#333333",
        line_width=1,
    )

    fig.update_layout(
        title=dict(
            text="Impacto medio en NO\u2082 por tipo de evento (%)",
            font=dict(size=18),
            x=0.5,
            xanchor="center",
        ),
        xaxis=dict(
            title="Tipo de evento",
            title_font=dict(size=14),
        ),
        yaxis=dict(
            title="Impacto medio (%)",
            title_font=dict(size=14),
            zeroline=True,
        ),
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        margin=dict(l=60, r=40, t=80, b=80),
        showlegend=False,
        annotations=[
            dict(
                text=(
                    "<span style='color:#D62728'>"
                    "\u25cf M\u00e1s contaminaci\u00f3n</span>"
                    " &nbsp; "
                    "<span style='color:#2CA02C'>"
                    "\u25cf Menos contaminaci\u00f3n</span>"
                ),
                xref="paper", yref="paper",
                x=0.5, y=-0.18,
                showarrow=False,
                font=dict(size=12),
            )
        ],
    )

    return _save_figure(fig, "impacto_no2_por_tipo.html", logger)


# ==============================================================================
# VIS 2: IMPACTO MEDIO EN TRÁFICO POR TIPO DE EVENTO
# ==============================================================================

def generate_traffic_by_type(
    df_impacto: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un gráfico de barras verticales con el impacto medio en
    tráfico agrupado por tipo_evento.

    Usa impacto_trafico_pct porque es la única métrica de tráfico
    disponible en impacto_eventos.csv (media_evento y media_baseline
    son de contaminación en µg/m³, no de tráfico).

    Flujo:
    1. Filtrar filas con impacto_trafico_pct no nulo
    2. Deduplicar por evento_id (la columna se repite en cada fila
       del mismo evento, una por variable de contaminación)
    3. Agrupar por tipo_evento → media de impacto_trafico_pct
    4. Barras verticales coloreadas por signo

    Args:
        df_impacto: DataFrame de impacto_eventos.csv
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("\u2500" * 40)
    logger.info("VIS 2: Impacto medio en tr\u00e1fico por tipo de evento")
    logger.info("\u2500" * 40)

    # --- 1. Filtrar con impacto de tráfico válido ---
    df_traf = df_impacto[df_impacto["impacto_trafico_pct"].notna()].copy()

    if df_traf.empty:
        logger.warning("      Sin datos de impacto de tr\u00e1fico")
        return None

    # --- 2. Deduplicar por evento_id ---
    df_unique = df_traf.drop_duplicates(subset=["evento_id"]).copy()

    logger.info(
        f"      Eventos con tr\u00e1fico v\u00e1lido: {len(df_unique)}"
    )

    # --- 3. Agrupar por tipo_evento ---
    agg = (
        df_unique
        .groupby("tipo_evento", as_index=False)
        .agg(
            impacto_trafico_medio=("impacto_trafico_pct", "mean"),
            n_eventos=("evento_id", "nunique"),
            impacto_min=("impacto_trafico_pct", "min"),
            impacto_max=("impacto_trafico_pct", "max"),
        )
    )
    agg["impacto_trafico_medio"] = agg["impacto_trafico_medio"].round(1)
    agg["impacto_min"] = agg["impacto_min"].round(1)
    agg["impacto_max"] = agg["impacto_max"].round(1)

    n_tipos = len(agg)
    logger.info(f"      Tipos de evento agrupados: {n_tipos}")
    for _, row in agg.iterrows():
        logger.info(
            f"        {row['tipo_evento']:>15}: "
            f"impacto tr\u00e1fico medio = {row['impacto_trafico_medio']:+.1f}%, "
            f"{row['n_eventos']} eventos"
        )

    # --- 4. Color según signo ---
    agg["color"] = agg["impacto_trafico_medio"].apply(
        lambda x: "#D62728" if x > 0 else "#1F77B4"
    )

    # --- 5. Crear gráfico ---
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=agg["tipo_evento"],
        y=agg["impacto_trafico_medio"],
        marker_color=agg["color"],
        customdata=agg[["n_eventos", "impacto_min", "impacto_max"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Impacto tr\u00e1fico: %{y:+.1f}%<br>"
            "Eventos: %{customdata[0]}<br>"
            "Rango: [%{customdata[1]:+.1f}%, %{customdata[2]:+.1f}%]"
            "<extra></extra>"
        ),
    ))

    fig.add_hline(
        y=0,
        line_dash="solid",
        line_color="#333333",
        line_width=1,
    )

    fig.update_layout(
        title=dict(
            text="Impacto medio en tr\u00e1fico por tipo de evento (%)",
            font=dict(size=18),
            x=0.5,
            xanchor="center",
        ),
        xaxis=dict(
            title="Tipo de evento",
            title_font=dict(size=14),
        ),
        yaxis=dict(
            title="Impacto en incidencias de tr\u00e1fico (%)",
            title_font=dict(size=14),
            zeroline=True,
        ),
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        margin=dict(l=60, r=40, t=80, b=80),
        showlegend=False,
        annotations=[
            dict(
                text=(
                    "<span style='color:#D62728'>"
                    "\u25cf M\u00e1s incidencias</span>"
                    " &nbsp; "
                    "<span style='color:#1F77B4'>"
                    "\u25cf Menos incidencias</span>"
                ),
                xref="paper", yref="paper",
                x=0.5, y=-0.18,
                showarrow=False,
                font=dict(size=12),
            )
        ],
    )

    return _save_figure(fig, "impacto_trafico_por_tipo.html", logger)


# ==============================================================================
# VIS 3: TIMELINE DE NO₂ CON EVENTOS SUPERPUESTOS
# ==============================================================================

def generate_timeline(
    df_impacto: pd.DataFrame,
    df_contam: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un timeline interactivo: línea de NO₂ diario con eventos
    superpuestos como regiones sombreadas verticales.

    El color de cada región se asigna dinámicamente según tipo_evento
    usando la paleta accesible. No se hardcodea ningún tipo.

    Flujo:
    1. Agregar NO₂ diario (media todas estaciones, calidad OK)
    2. Media móvil 7 días para suavizar ruido
    3. Deduplicar eventos por evento_id
    4. Recortar rango temporal al de los eventos (±30 días)
    5. Superponer vrect() por evento, coloreado por tipo
    6. Marcadores hover en el centro de cada evento

    Args:
        df_impacto: DataFrame de impacto_eventos.csv
        df_contam: DataFrame de contaminación normalizada
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("\u2500" * 40)
    logger.info("VIS 3: Timeline de NO\u2082 con eventos superpuestos")
    logger.info("\u2500" * 40)

    # === PARTE A: Serie diaria de NO₂ ===

    df_no2 = df_contam[
        (df_contam["variable"] == "NO2") &
        (df_contam["calidad_dato"] == "ok")
    ].copy()

    if df_no2.empty:
        logger.warning(
            "      Sin datos de NO\u2082 v\u00e1lidos para el timeline")
        return None

    df_no2["fecha_dia"] = df_no2["fecha_utc"].dt.date
    no2_diario = (
        df_no2
        .groupby("fecha_dia", as_index=False)
        .agg(
            media_no2=("valor", "mean"),
            n_registros=("valor", "count"),
        )
    )
    no2_diario["fecha_dia"] = pd.to_datetime(no2_diario["fecha_dia"])
    no2_diario["media_no2"] = no2_diario["media_no2"].round(1)
    no2_diario = no2_diario.sort_values("fecha_dia").reset_index(drop=True)

    logger.info(
        f"      NO\u2082 diario: {len(no2_diario):,} d\u00edas "
        f"({no2_diario['fecha_dia'].min().date()} \u2192 "
        f"{no2_diario['fecha_dia'].max().date()})"
    )

    # === PARTE B: Eventos únicos ===

    eventos_unique = (
        df_impacto
        .drop_duplicates(subset=["evento_id"])
        [["evento_id", "nombre_evento", "tipo_evento",
          "impacto_esperado", "fecha_inicio", "fecha_fin"]]
        .copy()
    )
    eventos_unique = eventos_unique.dropna(
        subset=["fecha_inicio", "fecha_fin"])

    logger.info(
        f"      Eventos \u00fanicos para timeline: {len(eventos_unique)}")

    if eventos_unique.empty:
        logger.warning("      Sin eventos con fechas v\u00e1lidas")
        return None

    # Tipos encontrados (dinámico)
    tipos_encontrados = sorted(eventos_unique["tipo_evento"].dropna().unique())
    color_map = _build_color_map(tipos_encontrados)
    logger.info(f"      Tipos en timeline: {tipos_encontrados}")

    # === PARTE C: Recortar rango temporal ===

    evento_min = eventos_unique["fecha_inicio"].min()
    evento_max = eventos_unique["fecha_fin"].max()
    margin_days = pd.Timedelta(days=30)
    view_start = evento_min - margin_days
    view_end = evento_max + margin_days

    no2_view = no2_diario[
        (no2_diario["fecha_dia"] >= view_start) &
        (no2_diario["fecha_dia"] <= view_end)
    ].copy()

    if no2_view.empty:
        logger.warning(
            "      No hay datos de NO\u2082 en el rango de eventos. "
            "Usando todo el rango disponible."
        )
        no2_view = no2_diario.copy()

    logger.info(
        f"      Rango visualizado: "
        f"{no2_view['fecha_dia'].min().date()} \u2192 "
        f"{no2_view['fecha_dia'].max().date()} "
        f"({len(no2_view)} d\u00edas)"
    )

    # === PARTE D: Crear gráfico ===

    fig = go.Figure()

    # Línea de NO₂ diario
    fig.add_trace(go.Scatter(
        x=no2_view["fecha_dia"],
        y=no2_view["media_no2"],
        mode="lines",
        name="NO\u2082 diario",
        line=dict(color=COLOR_NO2_LINE, width=1.2),
        hovertemplate=(
            "<b>%{x|%d/%m/%Y}</b><br>"
            "NO\u2082: %{y:.1f} \u00b5g/m\u00b3"
            "<extra></extra>"
        ),
    ))

    # Media móvil 7 días
    if len(no2_view) >= 7:
        rolling_7 = no2_view["media_no2"].rolling(7, center=True).mean()

        fig.add_trace(go.Scatter(
            x=no2_view["fecha_dia"],
            y=rolling_7,
            mode="lines",
            name="Media m\u00f3vil (7d)",
            line=dict(color=COLOR_ROLLING, width=2, dash="dot"),
            hovertemplate=(
                "<b>%{x|%d/%m/%Y}</b><br>"
                "Media 7d: %{y:.1f} \u00b5g/m\u00b3"
                "<extra></extra>"
            ),
        ))

    # === PARTE E: Superponer eventos ===

    eventos_to_plot = eventos_unique.sort_values("fecha_inicio").head(50)
    show_labels = len(eventos_to_plot) <= 15

    for _, ev in eventos_to_plot.iterrows():
        tipo = str(ev["tipo_evento"]) if pd.notna(
            ev["tipo_evento"]) else "otro"
        nombre = (
            str(ev["nombre_evento"])[:35]
            if pd.notna(ev["nombre_evento"])
            else "Evento"
        )

        # Color dinámico por tipo → rgba semi-transparente
        base_color = color_map.get(tipo, "#7F7F7F")
        r = int(base_color[1:3], 16)
        g = int(base_color[3:5], 16)
        b = int(base_color[5:7], 16)
        fill_color = f"rgba({r},{g},{b},0.18)"

        fig.add_vrect(
            x0=ev["fecha_inicio"],
            x1=ev["fecha_fin"],
            fillcolor=fill_color,
            opacity=1.0,
            line_width=0.5,
            line_color=f"rgba({r},{g},{b},0.4)",
            annotation_text=nombre if show_labels else "",
            annotation_position="top left",
            annotation_font=dict(size=9, color="#555"),
            annotation_textangle=-45,
        )

    # Marcadores hover en centro de cada evento
    event_centers_x = []
    event_centers_y = []
    event_labels = []

    for _, ev in eventos_to_plot.iterrows():
        if pd.notna(ev["fecha_inicio"]) and pd.notna(ev["fecha_fin"]):
            center_date = ev["fecha_inicio"] + (
                ev["fecha_fin"] - ev["fecha_inicio"]
            ) / 2

            # NO₂ más cercano a la fecha central
            if not no2_view.empty:
                idx = (no2_view["fecha_dia"] - center_date).abs().idxmin()
                y_val = no2_view.loc[idx, "media_no2"]
            else:
                y_val = 0

            event_centers_x.append(center_date)
            event_centers_y.append(y_val)

            nombre = (
                str(ev["nombre_evento"])[:35]
                if pd.notna(ev["nombre_evento"])
                else "?"
            )
            tipo = str(ev["tipo_evento"]) if pd.notna(
                ev["tipo_evento"]) else "?"
            impacto = (
                str(ev["impacto_esperado"])
                if pd.notna(ev["impacto_esperado"])
                else "?"
            )

            event_labels.append(
                f"<b>{nombre}</b><br>"
                f"Tipo: {tipo}<br>"
                f"Impacto esperado: {impacto}<br>"
                f"Fechas: {ev['fecha_inicio'].strftime('%d/%m/%Y')} "
                f"\u2192 {ev['fecha_fin'].strftime('%d/%m/%Y')}"
            )

    if event_centers_x:
        fig.add_trace(go.Scatter(
            x=event_centers_x,
            y=event_centers_y,
            mode="markers",
            name="Eventos",
            marker=dict(
                symbol="diamond",
                size=10,
                color=COLOR_ROLLING,
                line=dict(width=1.5, color="#333"),
            ),
            hovertemplate="%{text}<extra></extra>",
            text=event_labels,
        ))

    # Layout
    date_min_str = no2_view["fecha_dia"].min().strftime("%Y")
    date_max_str = no2_view["fecha_dia"].max().strftime("%Y")

    fig.update_layout(
        title=dict(
            text=(
                f"Timeline de NO\u2082 con eventos superpuestos "
                f"({date_min_str}\u2013{date_max_str})"
            ),
            font=dict(size=17),
            x=0.5,
            xanchor="center",
        ),
        xaxis=dict(
            title="Fecha",
            title_font=dict(size=14),
            rangeslider=dict(visible=True, thickness=0.05),
        ),
        yaxis=dict(
            title="NO\u2082 (\u00b5g/m\u00b3)",
            title_font=dict(size=14),
            rangemode="tozero",
        ),
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        margin=dict(l=60, r=40, t=80, b=100),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        hovermode="x unified",
    )

    return _save_figure(fig, "timeline_eventos.html", logger)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la generación de las 3 visualizaciones de impacto de eventos.

    Flujo:
        1. Cargar datos (impacto_eventos + contaminación)
        2. Generar impacto de NO₂ por tipo de evento
        3. Generar impacto de tráfico por tipo de evento
        4. Generar timeline de eventos con picos de contaminación
        5. Resumen final
    """
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("FASE 6.3: VISUALIZACI\u00d3N DE IMPACTO DE EVENTOS")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Proyecto ra\u00edz: {PROJECT_ROOT}")

    # ------------------------------------------------------------------
    # 1. Cargar datos
    # ------------------------------------------------------------------
    df_impacto, df_contam = load_data(logger)

    if df_impacto is None:
        logger.error(
            "Sin datos de impacto de eventos. "
            "Ejecuta correlacion_eventos.py (Fase 5.5) primero."
        )
        print("\n\u274c ERROR: Sin impacto_eventos.csv")
        return

    # ------------------------------------------------------------------
    # 2-4. Generar visualizaciones
    # ------------------------------------------------------------------
    vis_generadas = []
    vis_fallidas = []

    # VIS 1: NO₂ por tipo
    result = generate_pollution_by_type(df_impacto, logger)
    if result:
        vis_generadas.append(result.name)
    else:
        vis_fallidas.append("impacto_no2_por_tipo.html")

    # VIS 2: Tráfico por tipo
    result = generate_traffic_by_type(df_impacto, logger)
    if result:
        vis_generadas.append(result.name)
    else:
        vis_fallidas.append("impacto_trafico_por_tipo.html")

    # VIS 3: Timeline
    if df_contam is not None:
        result = generate_timeline(df_impacto, df_contam, logger)
        if result:
            vis_generadas.append(result.name)
        else:
            vis_fallidas.append("timeline_eventos.html")
    else:
        logger.warning(
            "Sin contaminaci\u00f3n normalizada \u2192 timeline omitido"
        )
        vis_fallidas.append("timeline_eventos.html")

    # ------------------------------------------------------------------
    # 5. Resumen final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN FINAL \u2014 VISUALIZACIONES DE EVENTOS")
    logger.info("=" * 60)
    logger.info(f"  Directorio de salida: {EVENTOS_VIS_DIR}")
    logger.info(f"  Visualizaciones generadas: {len(vis_generadas)}")
    for nombre in vis_generadas:
        logger.info(f"    \u2713 {nombre}")
    if vis_fallidas:
        logger.info(f"  Fallidas/omitidas: {len(vis_fallidas)}")
        for nombre in vis_fallidas:
            logger.info(f"    \u2717 {nombre}")
    logger.info("=" * 60)

    if vis_generadas:
        print(
            f"\n\u2705 {len(vis_generadas)} visualizaciones generadas en: "
            f"{EVENTOS_VIS_DIR}"
        )
        for nombre in vis_generadas:
            print(f"   \u2192 {nombre}")
    else:
        print(
            "\n\u26a0\ufe0f  No se gener\u00f3 ninguna visualizaci\u00f3n. Revisa los logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
