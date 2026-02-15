# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 6.2: Gráficos de Tendencias Temporales (Plotly)
==============================================================================

Descripción:
    Genera 3 gráficos interactivos de tendencias temporales para Valencia
    usando Plotly Express:

      1) Evolución anual de NO₂ (media ciudad, todas las estaciones)
         → px.line con marcadores + línea de tendencia OMS
      2) Precipitaciones anuales acumuladas (mm)
         → px.bar con colores dinámicos
      3) Comparativa estacional NO₂: Verano vs Invierno
         → px.bar agrupado (grouped bar)

    Todos los gráficos:
      - Calculan el rango de años dinámicamente (sin hardcodear)
      - Usan paleta de colores accesible
      - Son interactivos (hover, zoom, pan)
      - Se exportan como HTML autocontenido

Datos de entrada:
    1. 3.DATOS_LIMPIOS/contaminacion_normalizada.parquet
       Columnas: fecha_utc, estacion_id, variable, valor, calidad_dato
    2. 3.DATOS_LIMPIOS/meteorologia_limpio.csv
       Columnas: fecha, precipitacion_mm, temp_c, calidad_dato

Datos de salida:
    4.VISUALIZACIONES/graficos/evolucion_no2.html
    4.VISUALIZACIONES/graficos/precipitaciones_anuales.html
    4.VISUALIZACIONES/graficos/comparativa_estacional.html

Ruta esperada del script:
    2.SCRIPTS/procesamiento/generar_graficos.py

Uso:
    python generar_graficos.py

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
CONTAMINACION_PATH = (
    PROJECT_ROOT / "3.DATOS_LIMPIOS" / "contaminacion_normalizada.parquet"
)
METEOROLOGIA_PATH = (
    PROJECT_ROOT / "3.DATOS_LIMPIOS" / "meteorologia_limpio.csv"
)

# --- Salida ---
GRAFICOS_DIR = PROJECT_ROOT / "4.VISUALIZACIONES" / "graficos"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# --- Umbrales de referencia ---
# Límite anual OMS para NO₂ (actualizado directrices 2021): 10 µg/m³
# Límite anual UE (Directiva 2008/50/CE): 40 µg/m³
OMS_NO2_ANUAL = 10.0
UE_NO2_ANUAL = 40.0

# --- Definición de estaciones ---
MESES_VERANO = [6, 7, 8]       # Junio, Julio, Agosto
MESES_INVIERNO = [12, 1, 2]    # Diciembre, Enero, Febrero

# --- Paleta accesible (colorblind-friendly) ---
COLOR_NO2_LINE = "#D62728"       # Rojo ladrillo (línea principal)
COLOR_OMS = "#2CA02C"            # Verde OMS
COLOR_UE = "#FF7F0E"             # Naranja UE
COLOR_VERANO = "#FF7F0E"         # Naranja cálido
COLOR_INVIERNO = "#1F77B4"       # Azul frío
COLOR_PRECIP = "#1F77B4"         # Azul agua


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola).
    Mismo patrón que todas las fases del proyecto.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "generar_graficos.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Generar_Graficos")
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

def load_contaminacion(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Carga el Parquet normalizado de contaminación (Fase 5.1).

    Esquema esperado:
        fecha_utc (datetime64[ns, UTC])
        estacion_id (str)
        estacion_nombre (str)
        fuente (str)
        variable (str)  → NO2, O3, PM10, PM2.5, SO2, CO
        valor (float64)
        unidad (str)    → µg/m³
        calidad_dato (str) → ok, invalid, missing

    Returns:
        DataFrame o None si el fichero no existe o está vacío.
    """
    logger.info(f"  Contaminación → {CONTAMINACION_PATH.name}")

    if not CONTAMINACION_PATH.exists():
        logger.error(
            f"      No encontrado: {CONTAMINACION_PATH}\n"
            f"      Ejecuta normalizar_contaminacion.py (Fase 5.1)"
        )
        return None

    try:
        df = pd.read_parquet(CONTAMINACION_PATH)
    except Exception as e:
        logger.error(f"      Error leyendo Parquet: {e}")
        return None

    if df.empty:
        logger.warning("      Fichero vacío")
        return None

    # Asegurar que fecha_utc es datetime UTC
    df["fecha_utc"] = pd.to_datetime(df["fecha_utc"], utc=True)

    logger.info(f"      {len(df):,} registros cargados")
    logger.info(
        f"      Rango: {df['fecha_utc'].min().year} → "
        f"{df['fecha_utc'].max().year}"
    )
    logger.info(f"      Variables: {sorted(df['variable'].unique())}")

    return df


def load_meteorologia(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Carga el CSV normalizado de meteorología (Fase 5.2).

    Esquema esperado:
        fecha (datetime)
        hora (int)
        precipitacion_mm (float)
        temp_c (float)
        humedad_pct (float)
        fuente (str)
        calidad_dato (str) → ok, missing

    Returns:
        DataFrame o None si el fichero no existe o está vacío.
    """
    logger.info(f"  Meteorología → {METEOROLOGIA_PATH.name}")

    if not METEOROLOGIA_PATH.exists():
        logger.error(
            f"      No encontrado: {METEOROLOGIA_PATH}\n"
            f"      Ejecuta limpiar_meteorologia.py (Fase 5.2)"
        )
        return None

    try:
        df = pd.read_csv(METEOROLOGIA_PATH)
        # Parseo robusto de fecha (puede ser tz-aware o naïve)
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=True)
    except Exception as e:
        logger.error(f"      Error leyendo CSV: {e}")
        return None

    if df.empty:
        logger.warning("      Fichero vacío")
        return None

    logger.info(f"      {len(df):,} registros cargados")
    logger.info(
        f"      Rango: {df['fecha'].min()} → {df['fecha'].max()}"
    )

    return df


# ==============================================================================
# UTILIDADES
# ==============================================================================

def _save_figure(fig: go.Figure, filename: str, logger: logging.Logger) -> Optional[Path]:
    """
    Guarda una figura Plotly como HTML autocontenido.

    Args:
        fig: Figura Plotly
        filename: Nombre del archivo (e.g., "evolucion_no2.html")
        logger: Logger

    Returns:
        Path al archivo guardado, o None si falla
    """
    GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = GRAFICOS_DIR / filename

    try:
        fig.write_html(
            str(output_path),
            include_plotlyjs=True,  # HTML autocontenido (sin CDN)
            full_html=True,
        )
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"      Guardado: {filename} ({file_size_kb:.0f} KB)")
        return output_path
    except Exception as e:
        logger.error(f"      Error guardando {filename}: {e}")
        return None


# ==============================================================================
# GRÁFICO 1: EVOLUCIÓN ANUAL DE NO₂
# ==============================================================================

def generate_no2_evolution(
    df_contam: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un gráfico de línea con la evolución anual del NO₂ en Valencia.

    Proceso:
    1. Filtra variable == "NO2" y calidad_dato == "ok"
    2. Extrae año desde fecha_utc
    3. Calcula media anual a nivel ciudad (promedio de todas las estaciones)
    4. Incluye el nº de registros válidos por año como hover info
    5. Añade líneas de referencia OMS (10 µg/m³) y UE (40 µg/m³)
    6. Título dinámico con rango de años

    Args:
        df_contam: DataFrame de contaminación normalizada
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("GRÁFICO 1: Evolución anual NO₂")
    logger.info("─" * 40)

    # --- 1. Filtrar NO₂ con calidad OK ---
    df_no2 = df_contam[
        (df_contam["variable"] == "NO2") &
        (df_contam["calidad_dato"] == "ok")
    ].copy()

    if df_no2.empty:
        logger.warning("      Sin datos de NO₂ con calidad OK")
        return None

    # --- 2. Extraer año ---
    df_no2["year"] = df_no2["fecha_utc"].dt.year

    # --- 3. Media anual (ciudad) ---
    no2_anual = (
        df_no2
        .groupby("year", as_index=False)
        .agg(
            media_no2=("valor", "mean"),
            n_registros=("valor", "count"),
            n_estaciones=("estacion_id", "nunique"),
        )
    )
    no2_anual["media_no2"] = no2_anual["media_no2"].round(2)

    year_min = no2_anual["year"].min()
    year_max = no2_anual["year"].max()
    n_years = len(no2_anual)

    logger.info(f"      Rango: {year_min} → {year_max} ({n_years} años)")
    logger.info(
        f"      NO₂ medio global: "
        f"{no2_anual['media_no2'].mean():.1f} µg/m³"
    )

    # --- 4. Crear gráfico con Plotly ---
    fig = px.line(
        no2_anual,
        x="year",
        y="media_no2",
        markers=True,
        labels={
            "year": "Año",
            "media_no2": "NO₂ (µg/m³)",
        },
        title=(
            f"Evolución anual de NO₂ en Valencia "
            f"({year_min}–{year_max})"
        ),
        hover_data={
            "n_registros": ":,",
            "n_estaciones": True,
        },
    )

    # Estilo de la línea principal
    fig.update_traces(
        line=dict(color=COLOR_NO2_LINE, width=2.5),
        marker=dict(size=7),
        hovertemplate=(
            "<b>Año %{x}</b><br>"
            "NO₂: %{y:.1f} µg/m³<br>"
            "Registros: %{customdata[0]:,}<br>"
            "Estaciones: %{customdata[1]}"
            "<extra></extra>"
        ),
    )

    # --- 5. Líneas de referencia OMS y UE ---
    fig.add_hline(
        y=OMS_NO2_ANUAL,
        line_dash="dash",
        line_color=COLOR_OMS,
        line_width=1.5,
        annotation_text=f"Límite OMS ({OMS_NO2_ANUAL} µg/m³)",
        annotation_position="top left",
        annotation_font=dict(color=COLOR_OMS, size=11),
    )

    fig.add_hline(
        y=UE_NO2_ANUAL,
        line_dash="dot",
        line_color=COLOR_UE,
        line_width=1.5,
        annotation_text=f"Límite UE ({UE_NO2_ANUAL} µg/m³)",
        annotation_position="bottom left",
        annotation_font=dict(color=COLOR_UE, size=11),
    )

    # --- Layout ---
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        xaxis=dict(
            dtick=5,                         # Marca cada 5 años
            tickformat="d",                   # Sin separador de miles
            title_font=dict(size=14),
        ),
        yaxis=dict(
            title_font=dict(size=14),
            rangemode="tozero",               # Eje Y empieza en 0
        ),
        title=dict(
            font=dict(size=18),
            x=0.5,                            # Centrado
            xanchor="center",
        ),
        margin=dict(l=60, r=40, t=80, b=60),
        hovermode="x unified",
    )

    # --- 6. Guardar ---
    return _save_figure(fig, "evolucion_no2.html", logger)


# ==============================================================================
# GRÁFICO 2: PRECIPITACIONES ANUALES
# ==============================================================================

def generate_precipitation_annual(
    df_meteo: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un gráfico de barras con la precipitación anual acumulada.

    Proceso:
    1. Filtra calidad_dato == "ok" y precipitacion_mm no nula
    2. Extrae año desde fecha
    3. Suma la precipitación por año
    4. Calcula media histórica como línea de referencia
    5. Colores dinámicos: barras por encima/debajo de la media

    Args:
        df_meteo: DataFrame de meteorología limpia
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("GRÁFICO 2: Precipitaciones anuales")
    logger.info("─" * 40)

    # --- 1. Filtrar datos válidos ---
    df = df_meteo.copy()

    # Filtrar solo calidad OK si la columna existe
    if "calidad_dato" in df.columns:
        df = df[df["calidad_dato"] == "ok"].copy()

    # Descartar NaN en precipitación
    df = df.dropna(subset=["precipitacion_mm"])

    if df.empty:
        logger.warning("      Sin datos de precipitación válidos")
        return None

    # --- 2. Extraer año ---
    df["year"] = df["fecha"].dt.year

    # --- 3. Agregar por año ---
    # Precipitación se ACUMULA (suma), no se promedia
    precip_anual = (
        df
        .groupby("year", as_index=False)
        .agg(
            precipitacion_total=("precipitacion_mm", "sum"),
            n_registros=("precipitacion_mm", "count"),
            n_dias_lluvia=("precipitacion_mm", lambda x: (x > 0).sum()),
        )
    )
    precip_anual["precipitacion_total"] = precip_anual["precipitacion_total"].round(1)

    year_min = precip_anual["year"].min()
    year_max = precip_anual["year"].max()

    # Media histórica para línea de referencia
    media_historica = precip_anual["precipitacion_total"].mean()

    logger.info(f"      Rango: {year_min} → {year_max} ({len(precip_anual)} años)")
    logger.info(f"      Media histórica: {media_historica:.1f} mm/año")

    # --- 4. Clasificar barras (encima/debajo de media) ---
    precip_anual["categoria"] = precip_anual["precipitacion_total"].apply(
        lambda x: "Por encima de la media" if x >= media_historica
        else "Por debajo de la media"
    )

    # --- 5. Crear gráfico ---
    fig = px.bar(
        precip_anual,
        x="year",
        y="precipitacion_total",
        color="categoria",
        color_discrete_map={
            "Por encima de la media": "#1F77B4",     # Azul
            "Por debajo de la media": "#AEC7E8",     # Azul claro
        },
        labels={
            "year": "Año",
            "precipitacion_total": "Precipitación (mm)",
            "categoria": "Respecto a media",
        },
        title=(
            f"Precipitación anual acumulada en Valencia "
            f"({year_min}–{year_max})"
        ),
        hover_data={
            "n_registros": ":,",
            "n_dias_lluvia": True,
            "categoria": False,
        },
    )

    # Hover personalizado
    fig.update_traces(
        hovertemplate=(
            "<b>Año %{x}</b><br>"
            "Total: %{y:.1f} mm<br>"
            "Registros: %{customdata[0]:,}<br>"
            "Días con lluvia: %{customdata[1]}"
            "<extra></extra>"
        ),
    )

    # Línea de media histórica
    fig.add_hline(
        y=media_historica,
        line_dash="dash",
        line_color="#333333",
        line_width=1.5,
        annotation_text=f"Media histórica ({media_historica:.0f} mm)",
        annotation_position="top right",
        annotation_font=dict(color="#333333", size=11),
    )

    # Layout
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        xaxis=dict(
            dtick=5,
            tickformat="d",
            title_font=dict(size=14),
        ),
        yaxis=dict(
            title_font=dict(size=14),
            rangemode="tozero",
        ),
        title=dict(font=dict(size=18), x=0.5, xanchor="center"),
        margin=dict(l=60, r=40, t=80, b=60),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        bargap=0.15,
    )

    return _save_figure(fig, "precipitaciones_anuales.html", logger)


# ==============================================================================
# GRÁFICO 3: COMPARATIVA ESTACIONAL (VERANO vs INVIERNO)
# ==============================================================================

def generate_seasonal_comparison(
    df_contam: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera un gráfico de barras agrupadas comparando el NO₂ medio
    en verano (Jun-Ago) vs invierno (Dic-Feb) por año.

    ¿Por qué esta comparativa es relevante?
    ─────────────────────────────────────────
    El NO₂ en Valencia muestra un patrón estacional marcado:
      - INVIERNO: más calefacción + inversión térmica + menos viento
                  → concentraciones más altas
      - VERANO:   más fotoquímica (O₃) pero menos NO₂ directo,
                  más brisa marina → concentraciones más bajas

    Este gráfico permite visualizar si esa brecha se cierra con los años
    (indicando mejoras en movilidad/calefacción) o se mantiene.

    Nota sobre diciembre: Para el invierno de un año N, se usan
    diciembre de N-1, enero de N y febrero de N. Esto sigue la
    convención meteorológica estándar.

    Args:
        df_contam: DataFrame de contaminación normalizada
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("GRÁFICO 3: Comparativa estacional NO₂")
    logger.info("─" * 40)

    # --- 1. Filtrar NO₂ con calidad OK ---
    df_no2 = df_contam[
        (df_contam["variable"] == "NO2") &
        (df_contam["calidad_dato"] == "ok")
    ].copy()

    if df_no2.empty:
        logger.warning("      Sin datos de NO₂ con calidad OK")
        return None

    # --- 2. Extraer año y mes ---
    df_no2["year"] = df_no2["fecha_utc"].dt.year
    df_no2["month"] = df_no2["fecha_utc"].dt.month

    # --- 3. Asignar estación del año ---
    # Verano: meses 6, 7, 8 → pertenece al año calendario
    # Invierno: dic del año anterior + ene, feb del año actual
    #   → Convención meteorológica: dic 2024 + ene 2025 + feb 2025 = "Invierno 2025"
    def _assign_season(row):
        """Asigna estación y año estacional a una fila."""
        m = row["month"]
        y = row["year"]

        if m in MESES_VERANO:
            return pd.Series({"estacion": "Verano", "year_season": y})
        elif m in [1, 2]:
            # Enero y febrero → invierno del mismo año calendario
            return pd.Series({"estacion": "Invierno", "year_season": y})
        elif m == 12:
            # Diciembre → invierno del año siguiente
            return pd.Series({"estacion": "Invierno", "year_season": y + 1})
        else:
            # Meses de primavera/otoño → excluir
            return pd.Series({"estacion": None, "year_season": None})

    seasons = df_no2.apply(_assign_season, axis=1)
    df_no2["estacion"] = seasons["estacion"]
    df_no2["year_season"] = seasons["year_season"]

    # Filtrar solo verano e invierno
    df_seasonal = df_no2[df_no2["estacion"].notna()].copy()
    df_seasonal["year_season"] = df_seasonal["year_season"].astype(int)

    if df_seasonal.empty:
        logger.warning("      Sin datos estacionales tras filtrar")
        return None

    # --- 4. Media estacional por año ---
    seasonal_means = (
        df_seasonal
        .groupby(["year_season", "estacion"], as_index=False)
        .agg(
            media_no2=("valor", "mean"),
            n_registros=("valor", "count"),
        )
    )
    seasonal_means["media_no2"] = seasonal_means["media_no2"].round(2)

    # Solo años con datos en AMBAS estaciones
    years_both = (
        seasonal_means
        .groupby("year_season")["estacion"]
        .nunique()
    )
    years_complete = years_both[years_both == 2].index
    seasonal_means = seasonal_means[
        seasonal_means["year_season"].isin(years_complete)
    ].copy()

    if seasonal_means.empty:
        logger.warning("      Sin años con datos en ambas estaciones")
        return None

    year_min = seasonal_means["year_season"].min()
    year_max = seasonal_means["year_season"].max()

    logger.info(
        f"      Rango: {year_min} → {year_max} "
        f"({len(years_complete)} años completos)"
    )

    # Log de medias globales
    for est in ["Verano", "Invierno"]:
        media = seasonal_means[
            seasonal_means["estacion"] == est
        ]["media_no2"].mean()
        logger.info(f"      {est}: media global = {media:.1f} µg/m³")

    # --- 5. Crear gráfico ---
    fig = px.bar(
        seasonal_means,
        x="year_season",
        y="media_no2",
        color="estacion",
        barmode="group",
        color_discrete_map={
            "Verano": COLOR_VERANO,
            "Invierno": COLOR_INVIERNO,
        },
        labels={
            "year_season": "Año",
            "media_no2": "NO₂ (µg/m³)",
            "estacion": "Estación",
        },
        title=(
            f"NO₂ en Valencia: Verano vs Invierno "
            f"({year_min}–{year_max})"
        ),
        hover_data={"n_registros": ":,"},
    )

    # Hover personalizado
    fig.update_traces(
        hovertemplate=(
            "<b>%{x} — %{data.name}</b><br>"
            "NO₂: %{y:.1f} µg/m³<br>"
            "Registros: %{customdata[0]:,}"
            "<extra></extra>"
        ),
    )

    # Línea OMS
    fig.add_hline(
        y=OMS_NO2_ANUAL,
        line_dash="dash",
        line_color=COLOR_OMS,
        line_width=1.5,
        annotation_text=f"OMS ({OMS_NO2_ANUAL} µg/m³)",
        annotation_position="top left",
        annotation_font=dict(color=COLOR_OMS, size=11),
    )

    # Layout
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13),
        xaxis=dict(
            dtick=5,
            tickformat="d",
            title_font=dict(size=14),
        ),
        yaxis=dict(
            title_font=dict(size=14),
            rangemode="tozero",
        ),
        title=dict(font=dict(size=18), x=0.5, xanchor="center"),
        margin=dict(l=60, r=40, t=80, b=60),
        legend=dict(
            title="Estación del año",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        bargap=0.15,
        bargroupgap=0.05,
    )

    return _save_figure(fig, "comparativa_estacional.html", logger)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la generación de los 3 gráficos de tendencias temporales.

    Flujo:
        1. Cargar datos de contaminación y meteorología
        2. Generar gráfico de evolución NO₂
        3. Generar gráfico de precipitaciones anuales
        4. Generar gráfico de comparativa estacional
        5. Resumen final
    """
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("FASE 6.2: GRÁFICOS DE TENDENCIAS TEMPORALES (PLOTLY)")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")

    # ------------------------------------------------------------------
    # 1. Cargar datos
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 1: Carga de datos")
    logger.info("=" * 60)

    df_contam = load_contaminacion(logger)
    df_meteo = load_meteorologia(logger)

    # Verificar que tenemos al menos algo
    if df_contam is None and df_meteo is None:
        logger.error("Sin datos de entrada. Abortando.")
        print("\n\u274c ERROR: Sin datos. Ejecuta primero el pipeline ETL (Fase 5).")
        return

    # ------------------------------------------------------------------
    # 2-4. Generar gráficos
    # ------------------------------------------------------------------
    graficos_generados = []
    graficos_fallidos = []

    # Gráfico 1: NO₂
    if df_contam is not None:
        result = generate_no2_evolution(df_contam, logger)
        if result:
            graficos_generados.append(result.name)
        else:
            graficos_fallidos.append("evolucion_no2.html")
    else:
        logger.warning("Sin contaminación → gráfico NO₂ omitido")
        graficos_fallidos.append("evolucion_no2.html")

    # Gráfico 2: Precipitaciones
    if df_meteo is not None:
        result = generate_precipitation_annual(df_meteo, logger)
        if result:
            graficos_generados.append(result.name)
        else:
            graficos_fallidos.append("precipitaciones_anuales.html")
    else:
        logger.warning("Sin meteorología → gráfico precipitaciones omitido")
        graficos_fallidos.append("precipitaciones_anuales.html")

    # Gráfico 3: Estacional
    if df_contam is not None:
        result = generate_seasonal_comparison(df_contam, logger)
        if result:
            graficos_generados.append(result.name)
        else:
            graficos_fallidos.append("comparativa_estacional.html")
    else:
        logger.warning("Sin contaminación → gráfico estacional omitido")
        graficos_fallidos.append("comparativa_estacional.html")

    # ------------------------------------------------------------------
    # 5. Resumen final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN FINAL — GRÁFICOS GENERADOS")
    logger.info("=" * 60)
    logger.info(f"  Directorio de salida: {GRAFICOS_DIR}")
    logger.info(f"  Gráficos generados: {len(graficos_generados)}")
    for nombre in graficos_generados:
        logger.info(f"    \u2713 {nombre}")
    if graficos_fallidos:
        logger.info(f"  Gráficos fallidos/omitidos: {len(graficos_fallidos)}")
        for nombre in graficos_fallidos:
            logger.info(f"    \u2717 {nombre}")
    logger.info("=" * 60)

    # Mensaje para consola
    if graficos_generados:
        print(f"\n\u2705 {len(graficos_generados)} gráficos generados en: {GRAFICOS_DIR}")
        for nombre in graficos_generados:
            print(f"   \u2192 {nombre}")
    else:
        print("\n\u26a0\ufe0f  No se generó ningún gráfico. Revisa los logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
