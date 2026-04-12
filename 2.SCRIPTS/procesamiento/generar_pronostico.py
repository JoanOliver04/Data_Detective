# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 6.4: Pronóstico 72h con Indicadores de Riesgo
==============================================================================

Descripción:
    Genera una visualización interactiva del pronóstico meteorológico
    de 72 horas para Valencia, con indicadores heurísticos de riesgo
    de calidad del aire.

    Panel superior:
      - Línea: Temperatura (°C) — eje Y izquierdo
      - Línea: Humedad (%) — eje Y derecho

    Panel inferior:
      - Barras: Precipitación acumulada cada 3h (mm)
      - Línea: Probabilidad de precipitación (%)

    Indicador de riesgo:
      La lluvia tiene un efecto de "lavado atmosférico" (washout)
      que reduce PM10, PM2.5 y NO₂.  En tiempo seco con alta presión,
      los contaminantes se acumulan en la capa de mezcla urbana.

      Reglas heurísticas:
        - Lluvia total > 10 mm  → Riesgo de contaminación BAJO
        - Prob. máx. precip > 60%  → Riesgo MODERADO
        - Caso contrario  → Riesgo ALTO

Datos de entrada:
    Último archivo de:
    1.DATOS_EN_CRUDO/dinamicos/meteorologia/openweather_*.json

    Estructura JSON (generada por streaming_openweather.py, Fase 3.2):
    {
      "_metadata": { "timestamp_captura": "..." },
      "weather": { ... },
      "forecast": {
        "list": [
          {
            "dt": 1707510000,
            "main": { "temp": 20.1, "humidity": 70 },
            "rain": { "3h": 1.2 },
            "pop": 0.65
          }, ...
        ]
      }
    }

Datos de salida:
    4.VISUALIZACIONES/pronostico/pronostico_72h.html

Ruta esperada del script:
    2.SCRIPTS/procesamiento/generar_pronostico.py

Uso:
    python generar_pronostico.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Entrada ---
METEO_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "meteorologia"
FORECAST_GLOB = "openweather_*.json"

# --- Salida ---
PRONOSTICO_DIR = PROJECT_ROOT / "4.VISUALIZACIONES" / "pronostico"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# --- Constantes ---
FORECAST_HOURS = 72
FORECAST_INTERVAL_H = 3     # OWM /forecast da puntos cada 3h
MAX_POINTS = FORECAST_HOURS // FORECAST_INTERVAL_H  # 24 puntos

# --- Colores accesibles (colorblind-friendly) ---
COLOR_TEMP = "#D62728"       # Rojo
COLOR_HUMIDITY = "#1F77B4"   # Azul
COLOR_RAIN = "#2CA02C"       # Verde
COLOR_POP = "#FF7F0E"        # Naranja

# --- Umbrales de riesgo ---
RAIN_THRESHOLD_LOW = 10.0    # mm totales → riesgo bajo
POP_THRESHOLD_MOD = 60.0     # % prob. máx. → riesgo moderado


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola).
    Mismo patrón que todas las fases del proyecto.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "generar_pronostico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Generar_Pronostico")
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

def load_latest_forecast(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Selecciona el archivo de pronóstico más reciente y extrae los datos
    de las próximas 72 horas.

    Flujo:
    1. Buscar todos los openweather_*.json
    2. Seleccionar el más reciente (por nombre — contiene timestamp
       YYYYMMDD_HHMMSS por diseño de streaming_openweather.py)
    3. Parsear forecast → list
    4. Para cada entrada: extraer dt, temp, humidity, rain.3h, pop
    5. Limitar a las primeras 72h (24 puntos)

    Manejo de campos opcionales:
    - rain puede no existir si no hay precipitación prevista → 0
    - pop puede faltar en alguna entrada → 0

    Args:
        logger: Logger configurado

    Returns:
        DataFrame con columnas:
        [datetime, temp_c, humidity_pct, rain_mm, precip_probability_pct]
        o None si no hay datos.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 1: Carga del pron\u00f3stico m\u00e1s reciente")
    logger.info("=" * 60)

    # --- 1. Buscar archivos ---
    if not METEO_DIR.exists():
        logger.error(f"  Directorio no encontrado: {METEO_DIR}")
        return None

    archivos = sorted(METEO_DIR.glob(FORECAST_GLOB))

    if not archivos:
        logger.error(
            f"  Sin archivos {FORECAST_GLOB} en {METEO_DIR}\n"
            f"  Ejecuta streaming_openweather.py (Fase 3.2)"
        )
        return None

    logger.info(f"  Archivos encontrados: {len(archivos)}")

    # --- 2. Seleccionar el más reciente ---
    # sorted() ordena por nombre → el último es el más reciente
    latest_file = archivos[-1]
    file_size_kb = latest_file.stat().st_size / 1024
    logger.info(f"  Seleccionado: {latest_file.name} ({file_size_kb:.1f} KB)")

    # --- 3. Parsear JSON ---
    try:
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"  JSON inv\u00e1lido: {e}")
        return None
    except Exception as e:
        logger.error(f"  Error leyendo archivo: {e}")
        return None

    # Log de metadatos si existen
    metadata = data.get("_metadata", {})
    if metadata:
        ts_captura = metadata.get(
            "timestamp_captura", metadata.get("timestamp_utc", "?")
        )
        logger.info(f"  Capturado: {ts_captura}")

    # --- 4. Localizar lista de pronósticos ---
    # Soportar dos estructuras:
    #   A) {"forecast": {"list": [...]}} ← streaming_openweather.py
    #   B) {"list": [...]}               ← JSON directo de OWM /forecast

    forecast_list = None

    # Intentar estructura A: {"forecast": {"list": [...]}}
    forecast_data = data.get("forecast", {})
    if isinstance(forecast_data, dict):
        forecast_list = forecast_data.get("list")

    # Intentar estructura B: {"pronostico": {"list": [...]}} ← FIX
    if forecast_list is None:
        pronostico_data = data.get("pronostico", {})
        if isinstance(pronostico_data, dict):
            forecast_list = pronostico_data.get("list")

    # Intentar estructura C: {"list": [...]} (JSON directo de OWM)
    if forecast_list is None:
        forecast_list = data.get("list")

    if not forecast_list or not isinstance(forecast_list, list):
        logger.error(
            "  No se encontr\u00f3 la lista de pron\u00f3sticos. "
            "Verifica la estructura del JSON."
        )
        logger.debug(f"  Claves ra\u00edz: {list(data.keys())}")
        return None

    logger.info(f"  Entradas de pron\u00f3stico: {len(forecast_list)}")

    # --- 5. Parsear cada entrada ---
    records = []

    for entry in forecast_list:
        dt_unix = entry.get("dt")
        if dt_unix is None:
            continue

        try:
            fecha = datetime.fromtimestamp(dt_unix, tz=timezone.utc)
        except (OSError, ValueError):
            continue

        # Temperatura y humedad
        main = entry.get("main", {})
        temp = main.get("temp")
        humidity = main.get("humidity")

        # Precipitación (campo opcional — 0 si no llueve)
        # OWM /forecast usa rain.3h; /weather usa rain.1h
        rain_data = entry.get("rain", {})
        rain_mm = 0.0
        if isinstance(rain_data, dict):
            rain_mm = rain_data.get("3h", rain_data.get("1h", 0.0))
        elif isinstance(rain_data, (int, float)):
            rain_mm = float(rain_data)

        # Probabilidad de precipitación (0–1 → convertir a %)
        pop = entry.get("pop", 0.0)
        if pop is None:
            pop = 0.0
        pop_pct = float(pop) * 100

        records.append({
            "datetime": fecha,
            "temp_c": float(temp) if temp is not None else None,
            "humidity_pct": float(humidity) if humidity is not None else None,
            "rain_mm": float(rain_mm),
            "precip_probability_pct": round(pop_pct, 1),
        })

    if not records:
        logger.error("  Sin registros v\u00e1lidos tras parsear")
        return None

    df = pd.DataFrame(records)
    df = df.sort_values("datetime").reset_index(drop=True)

    # --- 6. Limitar a 72h ---
    if len(df) > MAX_POINTS:
        df = df.head(MAX_POINTS)
        logger.info(f"  Recortado a {MAX_POINTS} puntos ({FORECAST_HOURS}h)")

    # Resumen
    dt_min = df["datetime"].min()
    dt_max = df["datetime"].max()
    hours_span = (dt_max - dt_min).total_seconds() / 3600

    logger.info(f"  Rango: {dt_min.isoformat()} \u2192 {dt_max.isoformat()}")
    logger.info(f"  Cobertura: {hours_span:.0f}h ({len(df)} puntos)")
    logger.info(
        f"  Temp: {df['temp_c'].min():.1f}\u00b0C \u2192 "
        f"{df['temp_c'].max():.1f}\u00b0C"
    )
    logger.info(f"  Lluvia total: {df['rain_mm'].sum():.1f} mm")

    return df


# ==============================================================================
# INDICADORES DE RIESGO
# ==============================================================================

def compute_risk_indicators(
    df: pd.DataFrame,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Calcula indicadores de riesgo de calidad del aire a partir del
    pronóstico meteorológico de 72h.

    Heurística:
    ───────────
    La precipitación reduce contaminantes por lavado atmosférico (washout).
    El tiempo seco con alta presión favorece la acumulación de NO₂ y PM.

    Reglas:
      1. Lluvia total > 10 mm  → Riesgo BAJO  (washout efectivo)
      2. Prob. máx. > 60%      → Riesgo MODERADO (posible limpieza)
      3. Caso contrario         → Riesgo ALTO (acumulación probable)

    Args:
        df: DataFrame con pronóstico de 72h
        logger: Logger

    Returns:
        Dict con mean_temp, mean_humidity, total_rain, max_precip_prob,
        risk_level, risk_color, risk_reason
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 2: Indicadores de riesgo")
    logger.info("=" * 60)

    mean_temp = df["temp_c"].mean()
    mean_humidity = df["humidity_pct"].mean()
    total_rain = df["rain_mm"].sum()
    max_precip_prob = df["precip_probability_pct"].max()

    # Aplicar heurística
    if total_rain > RAIN_THRESHOLD_LOW:
        risk_level = "BAJO"
        risk_color = "#2CA02C"
        risk_reason = (
            f"Lluvia prevista ({total_rain:.1f} mm) supera "
            f"{RAIN_THRESHOLD_LOW} mm \u2192 lavado atmosf\u00e9rico esperado"
        )
    elif max_precip_prob > POP_THRESHOLD_MOD:
        risk_level = "MODERADO"
        risk_color = "#FF7F0E"
        risk_reason = (
            f"Prob. m\u00e1x. precipitaci\u00f3n ({max_precip_prob:.0f}%) > "
            f"{POP_THRESHOLD_MOD:.0f}% \u2192 posible lavado parcial"
        )
    else:
        risk_level = "ALTO"
        risk_color = "#D62728"
        risk_reason = (
            f"Poca lluvia ({total_rain:.1f} mm) y prob. m\u00e1x. baja "
            f"({max_precip_prob:.0f}%) \u2192 acumulaci\u00f3n probable"
        )

    indicators = {
        "mean_temp": round(mean_temp, 1) if pd.notna(mean_temp) else None,
        "mean_humidity": round(mean_humidity, 1) if pd.notna(mean_humidity) else None,
        "total_rain": round(total_rain, 1),
        "max_precip_prob": round(max_precip_prob, 1),
        "risk_level": risk_level,
        "risk_color": risk_color,
        "risk_reason": risk_reason,
    }

    logger.info(f"  Temp media: {indicators['mean_temp']}\u00b0C")
    logger.info(f"  Humedad media: {indicators['mean_humidity']}%")
    logger.info(f"  Lluvia total: {indicators['total_rain']} mm")
    logger.info(
        f"  Max prob. precipitaci\u00f3n: {indicators['max_precip_prob']}%")
    logger.info(f"  \u2192 Riesgo contaminaci\u00f3n: {risk_level}")
    logger.info(f"  Raz\u00f3n: {risk_reason}")

    return indicators


# ==============================================================================
# GENERACIÓN DE VISUALIZACIÓN
# ==============================================================================

def generate_visualization(
    df: pd.DataFrame,
    indicators: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Genera una visualización multi-panel del pronóstico 72h con el
    indicador de riesgo de calidad del aire.

    Panel superior (row=1):
      - Línea roja: Temperatura (°C) — eje Y izquierdo
      - Línea azul punteada: Humedad (%) — eje Y derecho (0–100)

    Panel inferior (row=2):
      - Barras verdes: Precipitación acumulada 3h (mm) — eje Y izquierdo
      - Línea naranja: Probabilidad de precipitación (%) — eje Y derecho

    Anotación:
      - Banner con "Riesgo de contaminación: NIVEL"
      - Color según nivel (verde / naranja / rojo)
      - Métricas resumidas debajo

    Args:
        df: DataFrame con pronóstico de 72h
        indicators: Diccionario de indicadores de riesgo
        logger: Logger

    Returns:
        Path al HTML generado, o None si falla
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("PASO 3: Generaci\u00f3n de visualizaci\u00f3n")
    logger.info("=" * 60)

    # Crear figura con 2 filas, cada una con eje Y secundario
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=(
            "Temperatura y Humedad",
            "Precipitaci\u00f3n y Probabilidad de lluvia",
        ),
        specs=[
            [{"secondary_y": True}],
            [{"secondary_y": True}],
        ],
    )

    # ══════════════════════════════════════════════════════════════
    # PANEL SUPERIOR: Temperatura + Humedad
    # ══════════════════════════════════════════════════════════════

    # Temperatura (eje Y izquierdo)
    fig.add_trace(
        go.Scatter(
            x=df["datetime"],
            y=df["temp_c"],
            mode="lines+markers",
            name="Temperatura (\u00b0C)",
            legendgroup="meteo",
            legendgrouptitle_text="Meteorolog\u00eda",
            line=dict(color=COLOR_TEMP, width=2.5),
            marker=dict(size=5),
            hovertemplate=(
                "<b>%{x|%d/%m %H:%M}</b><br>"
                "Temp: %{y:.1f}\u00b0C"
                "<extra></extra>"
            ),
        ),
        row=1, col=1,
        secondary_y=False,
    )

    # Humedad (eje Y derecho)
    fig.add_trace(
        go.Scatter(
            x=df["datetime"],
            y=df["humidity_pct"],
            mode="lines",
            name="Humedad (%)",
            legendgroup="meteo",
            line=dict(color=COLOR_HUMIDITY, width=1.5, dash="dot"),
            hovertemplate=(
                "<b>%{x|%d/%m %H:%M}</b><br>"
                "Humedad: %{y:.0f}%"
                "<extra></extra>"
            ),
        ),
        row=1, col=1,
        secondary_y=True,
    )

    # Ejes panel superior
    fig.update_yaxes(
        title_text="Temperatura (\u00b0C)",
        color=COLOR_TEMP,
        row=1, col=1,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="Humedad (%)",
        color=COLOR_HUMIDITY,
        range=[0, 100],
        row=1, col=1,
        secondary_y=True,
    )

    # ══════════════════════════════════════════════════════════════
    # PANEL INFERIOR: Precipitación + Probabilidad
    # ══════════════════════════════════════════════════════════════

    # Barras de precipitación (eje Y izquierdo)
    fig.add_trace(
        go.Bar(
            x=df["datetime"],
            y=df["rain_mm"],
            name="Lluvia (mm/3h)",
            legendgroup="precip",
            legendgrouptitle_text="Precipitaci\u00f3n",
            marker_color=COLOR_RAIN,
            opacity=0.7,
            hovertemplate=(
                "<b>%{x|%d/%m %H:%M}</b><br>"
                "Lluvia: %{y:.1f} mm"
                "<extra></extra>"
            ),
        ),
        row=2, col=1,
        secondary_y=False,
    )

    # Línea de probabilidad de precipitación (eje Y derecho)
    fig.add_trace(
        go.Scatter(
            x=df["datetime"],
            y=df["precip_probability_pct"],
            mode="lines+markers",
            name="Prob. lluvia (%)",
            legendgroup="precip",
            line=dict(color=COLOR_POP, width=2),
            marker=dict(size=4),
            hovertemplate=(
                "<b>%{x|%d/%m %H:%M}</b><br>"
                "Prob. lluvia: %{y:.0f}%"
                "<extra></extra>"
            ),
        ),
        row=2, col=1,
        secondary_y=True,
    )

    # Ejes panel inferior
    fig.update_yaxes(
        title_text="Precipitaci\u00f3n (mm/3h)",
        color=COLOR_RAIN,
        rangemode="tozero",
        row=2, col=1,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="Probabilidad (%)",
        color=COLOR_POP,
        range=[0, 100],
        row=2, col=1,
        secondary_y=True,
    )

    # ══════════════════════════════════════════════════════════════
    # ANOTACIÓN: Indicador de riesgo
    # ══════════════════════════════════════════════════════════════

    risk_level = indicators["risk_level"]
    risk_color = indicators["risk_color"]

    risk_text = (
        f"Riesgo de contaminaci\u00f3n: <b>{risk_level}</b><br>"
        f"<span style='font-size:11px'>"
        f"Temp media: {indicators['mean_temp']}\u00b0C"
        f" | Lluvia total: {indicators['total_rain']} mm"
        f" | Prob. m\u00e1x: {indicators['max_precip_prob']}%"
        f"</span>"
    )

    fig.add_annotation(
        text=risk_text,
        xref="paper", yref="paper",
        x=0.98, y=0.98,
        xanchor="right",
        yanchor="top",
        showarrow=False,
        font=dict(size=15, color="white"),
        bgcolor=risk_color,
        bordercolor=risk_color,
        borderwidth=2,
        borderpad=10,
        opacity=0.9,
    )

    # ══════════════════════════════════════════════════════════════
    # LAYOUT GENERAL
    # ══════════════════════════════════════════════════════════════

    dt_start = df["datetime"].min().strftime("%d/%m/%Y %H:%M")
    dt_end = df["datetime"].max().strftime("%d/%m/%Y %H:%M")

    fig.update_layout(
        title=dict(
            text=(
                "Pron\u00f3stico 72h & Riesgo de Calidad del Aire "
                "\u2014 Valencia<br>"
                f"<span style='font-size:13px; color:#666'>"
                f"{dt_start} \u2192 {dt_end} UTC</span>"
            ),
            font=dict(size=18),
            x=0.5,
            xanchor="center",
        ),
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=12),
        height=700,
        margin=dict(l=70, r=70, t=120, b=160),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
            bgcolor="rgba(0,0,0,0)",
            tracegroupgap=30,
            traceorder="grouped",
            itemwidth=40,
        ),
        hovermode="x unified",
    )

    # Formato del eje X
    fig.update_xaxes(
        tickformat="%d/%m\n%H:%M",
        dtick=6 * 3600 * 1000,    # Marca cada 6 horas
        row=2, col=1,
    )

    # ══════════════════════════════════════════════════════════════
    # GUARDAR
    # ══════════════════════════════════════════════════════════════

    PRONOSTICO_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PRONOSTICO_DIR / "pronostico_72h.html"

    try:
        fig.write_html(
            str(output_path),
            include_plotlyjs=True,
            full_html=True,
        )
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"  Guardado: {output_path.name} ({file_size_kb:.0f} KB)")
        return output_path
    except Exception as e:
        logger.error(f"  Error guardando: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la generación del pronóstico 72h con indicadores de riesgo.

    Flujo:
        1. Cargar último archivo de pronóstico OpenWeatherMap
        2. Calcular indicadores de riesgo de calidad del aire
        3. Generar visualización multi-panel
        4. Resumen final
    """
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("FASE 6.4: PRON\u00d3STICO 72h CON INDICADORES DE RIESGO")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Proyecto ra\u00edz: {PROJECT_ROOT}")

    # ------------------------------------------------------------------
    # 1. Cargar pronóstico
    # ------------------------------------------------------------------
    df = load_latest_forecast(logger)

    if df is None or df.empty:
        logger.error("Sin datos de pron\u00f3stico. Abortando.")
        print(
            "\n\u274c ERROR: Sin datos de pron\u00f3stico.\n"
            "   Ejecuta streaming_openweather.py (Fase 3.2) primero."
        )
        return

    # ------------------------------------------------------------------
    # 2. Calcular indicadores
    # ------------------------------------------------------------------
    indicators = compute_risk_indicators(df, logger)

    # ------------------------------------------------------------------
    # 3. Generar visualización
    # ------------------------------------------------------------------
    result = generate_visualization(df, indicators, logger)

    # ------------------------------------------------------------------
    # 4. Resumen final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN FINAL")
    logger.info("=" * 60)

    if result:
        logger.info(f"  Archivo: {result}")
        logger.info(f"  Riesgo: {indicators['risk_level']}")
        logger.info(f"  Puntos de datos: {len(df)}")
        logger.info("=" * 60)

        print(
            f"\n\u2705 Pron\u00f3stico generado: {result.name}\n"
            f"   Riesgo de contaminaci\u00f3n: {indicators['risk_level']}\n"
            f"   Raz\u00f3n: {indicators['risk_reason']}"
        )
    else:
        logger.error("  No se pudo generar la visualizaci\u00f3n")
        logger.info("=" * 60)
        print("\n\u274c ERROR generando visualizaci\u00f3n. Revisa los logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
