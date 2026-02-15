# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.2: Normalización de Datos Meteorológicos
==============================================================================

Descripción:
    Unifica los datos meteorológicos de las tres fuentes del proyecto
    (AEMET histórico, AVAMET scraping y OpenWeatherMap streaming) en un
    único dataset canónico, listo para análisis y visualización.

Fuentes de entrada:
    1. AEMET → 1.DATOS_EN_CRUDO/estaticos/meteorologia/aemet_*.csv
    2. AVAMET→ 1.DATOS_EN_CRUDO/dinamicos/precipitaciones/avamet_*.json
    3. OWM  → 1.DATOS_EN_CRUDO/dinamicos/meteorologia/openweather_*.json

Esquema canónico de salida:
    fecha           → datetime64[ns, UTC]  (timestamp con zona horaria)
    hora            → int                  (0–23, extraída de fecha UTC)
    precipitacion_mm→ float64              (mm; NaN si no disponible)
    temp_c          → float64              (°C; NaN si no disponible)
    humedad_pct     → float64              (%; NaN si no disponible)
    fuente          → str                  (aemet | avamet | openweather)
    calidad_dato    → str                  (ok | invalid | missing)

Decisiones de diseño:
    - Los datos AEMET son medias DIARIAS → se anclan a las 12:00 local
      (centro del período de muestreo) y se convierten a UTC.
    - Los datos AVAMET son capturas puntuales con timestamp del scraping.
      Se tratan como instantáneos y se convierten de local → UTC.
    - Los datos OpenWeatherMap incluyen endpoint /weather (instantáneo)
      y /forecast (cada 3h). Ambos traen timestamps Unix en UTC.
    - AEMET usa coma como decimal en algunos campos → se normaliza.
    - Validación de rangos físicos:
        precipitación: 0–500 mm (máx histórico España ~400 mm/día)
        temperatura:  -20–50 °C (rango peninsular amplio)
        humedad:       0–100 %
    - Valores fuera de rango → NaN + log warning (NO se eliminan filas).
    - Las filas con los 3 valores meteorológicos NaN se marcan como
      "missing"; si algún valor es NaN pero otros no → se mantiene "ok".

Uso:
    python 2.SCRIPTS/procesamiento/limpiar_meteorologia.py

Salida:
    3.DATOS_LIMPIOS/meteorologia_limpio.csv

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia

Commit sugerido:
    feat: add Phase 5.2 meteorological data normalization pipeline
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import pandas as pd

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas relativas al directorio raíz del proyecto
# El script vive en: 2.SCRIPTS/procesamiento/
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Entradas ---
AEMET_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "meteorologia"
AVAMET_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "precipitaciones"
OWM_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "meteorologia"

# --- Salida ---
OUTPUT_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
OUTPUT_CSV = OUTPUT_DIR / "meteorologia_limpio.csv"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# Zona horaria de referencia para datos locales (AEMET/AVAMET)
TIMEZONE_LOCAL = "Europe/Madrid"

# ==============================================================================
# CONSTANTES DEL DOMINIO
# ==============================================================================

# Variables canónicas del esquema de salida
VARIABLES_CANONICAS = ["precipitacion_mm", "temp_c", "humedad_pct"]

# Rangos físicos razonables para validación
# Basados en extremos históricos documentados para la Península Ibérica.
# Se usa un rango generoso para no descartar picos reales (p.ej. DANA).
RANGOS_FISICOS = {
    "precipitacion_mm": {"min": 0.0, "max": 500.0},
    "temp_c":           {"min": -20.0, "max": 50.0},
    "humedad_pct":      {"min": 0.0, "max": 100.0},
}

# Mapeo de variables AEMET (descargar_aemet_historico.py) → canónicas
# AEMET guarda en formato largo: [fecha, estacion, variable, valor]
# donde 'variable' toma estos nombres (definidos en VARIABLES_MAPPING)
AEMET_VARIABLE_MAP = {
    "precipitacion":      "precipitacion_mm",
    "temperatura_media":  "temp_c",
    "humedad_media":      "humedad_pct",
    # Aliases alternativos por si el CSV tiene los nombres originales AEMET
    "prec":               "precipitacion_mm",
    "tmed":               "temp_c",
    "hrMedia":            "humedad_pct",
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola) siguiendo el patrón del proyecto.
    Mismo estilo que normalizar_contaminacion.py (Fase 5.1).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "limpiar_meteorologia.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Limpiar_Meteorologia")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Archivo (todo)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    # Consola (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# CARGADORES POR FUENTE
# ==============================================================================

def cargar_aemet(logger: logging.Logger) -> pd.DataFrame:
    """
    Carga todos los CSV generados por descargar_aemet_historico.py (Fase 2.3).

    Formato esperado (largo): [fecha, estacion, variable, valor]
    Donde 'variable' puede ser: precipitacion, temperatura_media, humedad_media, etc.

    Este cargador:
    1. Lee todos los CSV que coinciden con aemet_*.csv
    2. Filtra solo las 3 variables de interés (precip, temp, humedad)
    3. Pivota de formato largo a ancho: una fila por (fecha, estación)
       con columnas precipitacion_mm, temp_c, humedad_pct

    Returns:
        DataFrame con columnas: [fecha, precipitacion_mm, temp_c, humedad_pct, fuente]
        Fechas son naïve (Europe/Madrid implícito, sin tz_info).
    """
    patron = "aemet_*.csv"
    archivos = sorted(AEMET_DIR.glob(patron))

    if not archivos:
        logger.warning(f"AEMET: sin archivos en {AEMET_DIR} con patrón '{patron}'")
        return pd.DataFrame()

    logger.info(f"AEMET: encontrados {len(archivos)} archivos")

    frames = []
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo, parse_dates=["fecha"])
            frames.append(df)
            logger.debug(f"  → {archivo.name}: {len(df)} registros")
        except Exception as e:
            logger.error(f"  ✘ Error leyendo {archivo.name}: {e}")

    if not frames:
        return pd.DataFrame()

    df_largo = pd.concat(frames, ignore_index=True)
    logger.info(f"AEMET: {len(df_largo):,} registros totales cargados (formato largo)")

    # --- Mapear nombres de variables a canónicos ---
    df_largo["variable"] = df_largo["variable"].map(
        lambda v: AEMET_VARIABLE_MAP.get(v, AEMET_VARIABLE_MAP.get(v.strip(), None))
    )

    # Filtrar solo variables que mapearon correctamente
    antes = len(df_largo)
    df_largo = df_largo.dropna(subset=["variable"]).copy()
    descartadas = antes - len(df_largo)
    if descartadas > 0:
        logger.info(
            f"AEMET: {descartadas} registros descartados "
            f"(variables no relevantes para meteorología)"
        )

    # Filtrar solo variables canónicas
    df_largo = df_largo[df_largo["variable"].isin(VARIABLES_CANONICAS)].copy()

    if df_largo.empty:
        logger.warning("AEMET: sin datos tras filtrado de variables")
        return pd.DataFrame()

    # --- Pivotar: largo → ancho ---
    # Agrupar por (fecha, estacion) y pivotar variables como columnas.
    # Si hay duplicados (misma fecha+estacion+variable), tomar la media.
    df_ancho = df_largo.pivot_table(
        index=["fecha"],
        columns="variable",
        values="valor",
        aggfunc="mean"
    ).reset_index()

    # Asegurar que las 3 columnas canónicas existen (rellenar con NaN si no)
    for col in VARIABLES_CANONICAS:
        if col not in df_ancho.columns:
            df_ancho[col] = float("nan")

    df_ancho["fuente"] = "aemet"

    logger.info(
        f"AEMET: {len(df_ancho):,} filas tras pivotar "
        f"(fechas únicas con al menos 1 variable)"
    )

    return df_ancho


def cargar_avamet(logger: logging.Logger) -> pd.DataFrame:
    """
    Carga los JSON capturados por scraping_avamet.py (Fase 3.3).

    Estructura esperada del JSON (simplificada):
    {
      "_metadata": { "timestamp_captura": "...", ... },
      "datos": [
        {
          "estacion": "Valencia - Campanar",
          "precipitacion_raw": "2.5",
          "celdas_raw": [...],
          ...
        }
      ]
    }

    AVAMET es primariamente una fuente de PRECIPITACIÓN.
    No siempre incluye temperatura ni humedad.

    Returns:
        DataFrame con columnas: [fecha, precipitacion_mm, temp_c, humedad_pct, fuente]
    """
    patron_json = "avamet_*.json"
    patron_csv = "avamet_*.csv"

    archivos_json = sorted(AVAMET_DIR.glob(patron_json))
    archivos_csv = sorted(AVAMET_DIR.glob(patron_csv))

    if not archivos_json and not archivos_csv:
        logger.warning(f"AVAMET: sin archivos en {AVAMET_DIR}")
        return pd.DataFrame()

    records = []
    archivos_ok = 0
    archivos_error = 0

    # --- Procesar JSON (salida de scraping_avamet.py) ---
    for archivo in archivos_json:
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                captura = json.load(f)

            # Extraer timestamp de captura como fecha de referencia
            metadata = captura.get("_metadata", {})
            timestamp_str = metadata.get(
                "timestamp_captura",
                metadata.get("timestamp", None)
            )

            if timestamp_str:
                try:
                    fecha_captura = pd.to_datetime(timestamp_str)
                except Exception:
                    # Fallback: usar la fecha del nombre del archivo
                    # avamet_YYYYMMDD_HHMMSS.json
                    fecha_captura = _extraer_fecha_de_nombre(archivo.name)
            else:
                fecha_captura = _extraer_fecha_de_nombre(archivo.name)

            if fecha_captura is None:
                logger.warning(f"  {archivo.name}: sin timestamp válido, saltando")
                archivos_error += 1
                continue

            # Extraer datos meteorológicos
            datos = captura.get("datos", [])
            if not datos:
                # Intentar estructura alternativa (lista plana en raíz)
                datos = captura if isinstance(captura, list) else []

            for registro in datos:
                if not isinstance(registro, dict):
                    continue

                precip = _parsear_numero(
                    registro.get("precipitacion_raw",
                                 registro.get("precipitacion",
                                              registro.get("lluvia", None)))
                )

                # AVAMET puede incluir temperatura y humedad
                temp = _parsear_numero(
                    registro.get("temperatura",
                                 registro.get("temp",
                                              registro.get("temperatura_c", None)))
                )
                humedad = _parsear_numero(
                    registro.get("humedad",
                                 registro.get("humedad_relativa",
                                              registro.get("hr", None)))
                )

                records.append({
                    "fecha": fecha_captura,
                    "precipitacion_mm": precip,
                    "temp_c": temp,
                    "humedad_pct": humedad,
                    "fuente": "avamet",
                })

            archivos_ok += 1

        except json.JSONDecodeError as e:
            logger.error(f"  ✘ JSON inválido: {archivo.name} → {e}")
            archivos_error += 1
        except Exception as e:
            logger.error(f"  ✘ Error procesando {archivo.name}: {e}")
            archivos_error += 1

    # --- Procesar CSV (si existe como formato alternativo) ---
    for archivo in archivos_csv:
        try:
            df_csv = pd.read_csv(archivo)

            # Detectar columna de fecha
            fecha_col = None
            for col_name in ["fecha", "date", "timestamp", "Fecha"]:
                if col_name in df_csv.columns:
                    fecha_col = col_name
                    break

            if fecha_col is None:
                # Extraer fecha del nombre del archivo
                fecha_ref = _extraer_fecha_de_nombre(archivo.name)
                if fecha_ref is None:
                    logger.warning(f"  {archivo.name}: sin columna de fecha, saltando")
                    archivos_error += 1
                    continue
                df_csv["_fecha_ref"] = fecha_ref
                fecha_col = "_fecha_ref"

            df_csv["fecha"] = pd.to_datetime(df_csv[fecha_col], errors="coerce")

            # Buscar columnas relevantes (tolerancia a variantes de nombre)
            precip_col = _buscar_columna(
                df_csv, ["precipitacion", "precip", "lluvia", "rain", "mm", "prec"]
            )
            temp_col = _buscar_columna(
                df_csv, ["temperatura", "temp", "temp_c", "tmed"]
            )
            hum_col = _buscar_columna(
                df_csv, ["humedad", "humidity", "hr", "humedad_relativa"]
            )

            for _, row in df_csv.iterrows():
                records.append({
                    "fecha": row["fecha"],
                    "precipitacion_mm": _parsear_numero(
                        row.get(precip_col) if precip_col else None
                    ),
                    "temp_c": _parsear_numero(
                        row.get(temp_col) if temp_col else None
                    ),
                    "humedad_pct": _parsear_numero(
                        row.get(hum_col) if hum_col else None
                    ),
                    "fuente": "avamet",
                })

            archivos_ok += 1

        except Exception as e:
            logger.error(f"  ✘ Error leyendo CSV {archivo.name}: {e}")
            archivos_error += 1

    logger.info(
        f"AVAMET: {archivos_ok} archivos procesados OK, "
        f"{archivos_error} con errores, "
        f"{len(records)} registros extraídos"
    )

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def cargar_openweather(logger: logging.Logger) -> pd.DataFrame:
    """
    Carga los JSON capturados por streaming_openweather.py (Fase 3.2).

    Estructura esperada (por streaming_openweather.py):
    {
      "_metadata": { "timestamp_captura": "...", ... },
      "weather": {                    ← datos actuales (/weather)
        "main": { "temp": 22.5, "humidity": 65, ... },
        "rain": { "1h": 0.5 },       ← precipitación última hora (mm)
        "dt": 1707500000,             ← timestamp Unix UTC
        ...
      },
      "forecast": {                   ← pronóstico (/forecast)
        "list": [
          {
            "dt": 1707510000,
            "main": { "temp": 20.1, "humidity": 70, ... },
            "rain": { "3h": 1.2 },   ← precipitación en 3h (mm)
            ...
          },
          ...
        ]
      }
    }

    Extrae:
    - Datos /weather → 1 registro por archivo (condiciones actuales)
    - Datos /forecast → N registros por archivo (pronósticos cada 3h)
      NOTA: Solo extraemos /weather para evitar duplicar pronósticos como
      datos reales. Los pronósticos se podrán integrar en Fase 6/7.

    Returns:
        DataFrame con columnas: [fecha, precipitacion_mm, temp_c, humedad_pct, fuente]
        Las fechas ya son UTC (convertidas desde Unix timestamp).
    """
    archivos = sorted(OWM_DIR.glob("openweather_*.json"))

    if not archivos:
        logger.warning(f"OpenWeather: sin archivos JSON en {OWM_DIR}")
        return pd.DataFrame()

    logger.info(f"OpenWeather: encontrados {len(archivos)} archivos JSON")

    records = []
    archivos_ok = 0
    archivos_error = 0

    for archivo in archivos:
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                captura = json.load(f)

            # --- Extraer datos de /weather (actual) ---
            weather = captura.get("weather") or captura.get("actual")
            if weather and isinstance(weather, dict):
                record = _extraer_weather_record(weather, logger)
                if record is not None:
                    records.append(record)

            # --- Opcionalmente extraer /forecast ---
            # Descomenta si quieres incluir pronósticos como datos.
            # NOTA: Esto puede inflar el dataset con datos no observados.
            # forecast = captura.get("forecast") or captura.get("pronostico")
            # if forecast and isinstance(forecast, dict):
            #     for entry in forecast.get("list", []):
            #         record = _extraer_weather_record(entry, logger)
            #         if record is not None:
            #             records.append(record)

            archivos_ok += 1

        except json.JSONDecodeError as e:
            logger.error(f"  ✘ JSON inválido: {archivo.name} → {e}")
            archivos_error += 1
        except Exception as e:
            logger.error(f"  ✘ Error procesando {archivo.name}: {e}")
            archivos_error += 1

    logger.info(
        f"OpenWeather: {archivos_ok} archivos procesados OK, "
        f"{archivos_error} con errores, "
        f"{len(records)} registros extraídos"
    )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Las fechas ya son UTC (desde Unix timestamp)
    # Marcar como tz-aware
    df["fecha"] = pd.to_datetime(df["fecha"], utc=True)

    return df


# ==============================================================================
# FUNCIONES AUXILIARES DE CARGA
# ==============================================================================

def _extraer_weather_record(
    entry: dict,
    logger: logging.Logger
) -> Optional[dict]:
    """
    Extrae un registro meteorológico de un bloque de respuesta OWM.

    Funciona tanto para /weather como para entradas individuales de /forecast.
    OWM con units=metric devuelve:
    - temp en °C
    - humidity en %
    - rain.1h o rain.3h en mm

    Args:
        entry: Diccionario con datos de un punto temporal OWM
        logger: Logger

    Returns:
        Dict con {fecha, precipitacion_mm, temp_c, humedad_pct, fuente} o None
    """
    # Extraer timestamp (Unix epoch en UTC)
    dt_unix = entry.get("dt")
    if dt_unix is None:
        return None

    try:
        fecha = pd.Timestamp(dt_unix, unit="s", tz="UTC")
    except Exception:
        return None

    # Extraer variables meteorológicas
    main = entry.get("main", {})
    rain = entry.get("rain", {})

    temp = main.get("temp")  # °C (con units=metric)
    humidity = main.get("humidity")  # %

    # Precipitación: OWM usa rain.1h o rain.3h dependiendo del endpoint
    precip = rain.get("1h", rain.get("3h", 0.0))

    # Si rain.3h existe (pronóstico), normalizar a mm/h para consistencia
    # DECISIÓN: Mantener el valor acumulado tal cual, ya que AEMET también
    # reporta acumulados diarios. La normalización temporal se hará en Fase 6.

    return {
        "fecha": fecha,
        "precipitacion_mm": float(precip) if precip is not None else 0.0,
        "temp_c": float(temp) if temp is not None else float("nan"),
        "humedad_pct": float(humidity) if humidity is not None else float("nan"),
        "fuente": "openweather",
    }


def _extraer_fecha_de_nombre(nombre_archivo: str) -> Optional[pd.Timestamp]:
    """
    Extrae timestamp del nombre de archivo tipo: fuente_YYYYMMDD_HHMMSS.ext

    Ejemplo: avamet_20260215_143000.json → 2026-02-15 14:30:00

    Returns:
        pd.Timestamp o None si no se puede parsear
    """
    import re
    match = re.search(r"(\d{8})_(\d{6})", nombre_archivo)
    if match:
        try:
            return pd.to_datetime(match.group(1) + match.group(2), format="%Y%m%d%H%M%S")
        except Exception:
            pass

    # Fallback: solo fecha
    match = re.search(r"(\d{8})", nombre_archivo)
    if match:
        try:
            return pd.to_datetime(match.group(1), format="%Y%m%d")
        except Exception:
            pass

    return None


def _parsear_numero(valor) -> Optional[float]:
    """
    Convierte un valor a float, manejando:
    - None → NaN
    - Strings con coma como decimal ("2,5" → 2.5)
    - Strings vacíos → NaN
    - "Ip" / "Acum" (AEMET: precipitación inapreciable) → 0.0
    - Valores ya numéricos → float directo

    Returns:
        float o NaN
    """
    if valor is None:
        return float("nan")

    if isinstance(valor, (int, float)):
        return float(valor)

    if isinstance(valor, str):
        valor = valor.strip()
        if valor == "" or valor == "-" or valor.lower() == "nan":
            return float("nan")
        if valor.lower() in ("ip", "acum", "inapreciable", "varias"):
            return 0.0
        try:
            return float(valor.replace(",", "."))
        except ValueError:
            return float("nan")

    return float("nan")


def _buscar_columna(df: pd.DataFrame, keywords: list) -> Optional[str]:
    """
    Busca la primera columna del DataFrame cuyo nombre contenga
    alguna de las keywords (case-insensitive).

    Returns:
        Nombre de la columna encontrada o None
    """
    for col in df.columns:
        col_lower = col.lower().strip()
        for kw in keywords:
            if kw.lower() in col_lower:
                return col
    return None


# ==============================================================================
# TRANSFORMACIONES
# ==============================================================================

def convertir_a_utc(
    df: pd.DataFrame,
    logger: logging.Logger,
    nombre_fuente: str = ""
) -> pd.DataFrame:
    """
    Convierte la columna 'fecha' a UTC timezone-aware.

    - Fechas naïve (sin tz) → se interpretan como Europe/Madrid → UTC
    - Fechas ya tz-aware → se convierten a UTC

    Misma lógica que normalizar_contaminacion.py pero adaptada para
    meteorología (sin columna fecha_utc separada, sobreescribe 'fecha').

    Args:
        df: DataFrame con columna 'fecha'
        logger: Logger
        nombre_fuente: Nombre para logging ("AEMET", "AVAMET", etc.)

    Returns:
        DataFrame con 'fecha' convertida a UTC
    """
    if df.empty or "fecha" not in df.columns:
        return df

    # Detectar si ya tiene timezone info
    try:
        tiene_tz = isinstance(df["fecha"].dtype, pd.DatetimeTZDtype)
    except AttributeError:
        tiene_tz = pd.api.types.is_datetime64tz_dtype(df["fecha"])

    logger.debug(f"{nombre_fuente}: tiene_tz = {tiene_tz}")

    if tiene_tz:
        # Ya tiene timezone → convertir a UTC
        df["fecha"] = df["fecha"].dt.tz_convert("UTC")
    else:
        # Asegurar que es datetime
        if not pd.api.types.is_datetime64_any_dtype(df["fecha"]):
            logger.warning(f"{nombre_fuente}: 'fecha' no es datetime. Convirtiendo...")
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

        # Localizar a Europe/Madrid → UTC
        try:
            df["fecha"] = (
                df["fecha"]
                .dt.tz_localize(
                    TIMEZONE_LOCAL,
                    ambiguous="NaT",
                    nonexistent="shift_forward"
                )
                .dt.tz_convert("UTC")
            )
        except Exception as e:
            logger.warning(
                f"{nombre_fuente}: error localizando timezone: {e}. "
                f"Asignando UTC directo."
            )
            df["fecha"] = df["fecha"].dt.tz_localize("UTC")

    # Contar NaT generados
    nat_count = df["fecha"].isna().sum()
    if nat_count > 0:
        logger.warning(f"{nombre_fuente}: {nat_count} fechas ambiguas → NaT")

    return df


def validar_rangos(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida rangos físicos de las 3 variables meteorológicas.

    Valores fuera de rango:
    - Se reemplazan por NaN (para no perder la fila)
    - Se loguean como WARNING con detalle
    - Se marcan en la columna 'calidad_dato'

    Args:
        df: DataFrame con columnas canónicas
        logger: Logger

    Returns:
        DataFrame con 'calidad_dato' añadida/actualizada y valores
        fuera de rango reemplazados por NaN.
    """
    if df.empty:
        return df

    total_invalidos = 0

    for variable, rango in RANGOS_FISICOS.items():
        if variable not in df.columns:
            continue

        vmin = rango["min"]
        vmax = rango["max"]

        # Encontrar valores fuera de rango (excluyendo NaN)
        mask_fuera = df[variable].notna() & (
            (df[variable] < vmin) | (df[variable] > vmax)
        )

        n_fuera = mask_fuera.sum()
        if n_fuera > 0:
            # Log algunos ejemplos para debug
            ejemplos = df.loc[mask_fuera, ["fecha", variable]].head(5)
            logger.warning(
                f"  {variable}: {n_fuera} valores fuera de rango "
                f"[{vmin}, {vmax}]. Ejemplos:\n{ejemplos.to_string()}"
            )

            # Reemplazar por NaN (NO eliminar la fila)
            df.loc[mask_fuera, variable] = float("nan")
            total_invalidos += n_fuera

    logger.info(f"Validación: {total_invalidos} valores fuera de rango → NaN")

    # --- Asignar calidad_dato ---
    # "missing" si las 3 variables son NaN; "ok" en caso contrario.
    # Si tenía valores fuera de rango (ahora NaN), se marcan como "invalid"
    # solo si TODOS los campos son NaN tras la corrección.
    cols_valor = [c for c in VARIABLES_CANONICAS if c in df.columns]

    all_nan = df[cols_valor].isna().all(axis=1)
    df["calidad_dato"] = "ok"
    df.loc[all_nan, "calidad_dato"] = "missing"

    # Estadísticas de calidad
    calidad_counts = df["calidad_dato"].value_counts()
    for cat, n in calidad_counts.items():
        pct = n / len(df) * 100
        logger.info(f"  Calidad '{cat}': {n:,} filas ({pct:.1f}%)")

    return df


def extraer_hora(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Extrae la hora (0–23) de la columna 'fecha' UTC.

    Args:
        df: DataFrame con columna 'fecha' timezone-aware (UTC)
        logger: Logger

    Returns:
        DataFrame con columna 'hora' añadida (int 0–23)
    """
    if df.empty or "fecha" not in df.columns:
        return df

    df["hora"] = df["fecha"].dt.hour
    logger.debug(f"Columna 'hora' extraída. Distribución: {df['hora'].describe()}")

    return df


# ==============================================================================
# GUARDADO Y RESUMEN
# ==============================================================================

def guardar_resultados(
    df: pd.DataFrame,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Guarda el dataset limpio en CSV.

    Para meteorología usamos CSV (no Parquet) porque:
    - El dataset es más pequeño que contaminación
    - CSV es más accesible para inspección manual
    - Compatible con Excel para revisión rápida

    Args:
        df: DataFrame final a guardar
        logger: Logger

    Returns:
        Path al archivo CSV guardado o None si error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        size = OUTPUT_CSV.stat().st_size
        size_str = (
            f"{size / (1024*1024):.1f} MB" if size >= 1024*1024
            else f"{size / 1024:.1f} KB"
        )
        logger.info(f"✔ CSV guardado: {OUTPUT_CSV.name} ({size_str})")
        logger.info(f"  Ruta: {OUTPUT_CSV}")
        return OUTPUT_CSV

    except Exception as e:
        logger.error(f"Error guardando CSV: {e}")
        return None


def imprimir_resumen(df: pd.DataFrame, logger: logging.Logger) -> None:
    """
    Imprime un resumen estadístico detallado del dataset final.
    Sigue el mismo estilo que normalizar_contaminacion.py.
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("RESUMEN FINAL - METEOROLOGÍA NORMALIZADA")
    logger.info("=" * 70)

    if df.empty:
        logger.warning("Dataset vacío, sin estadísticas que mostrar.")
        return

    logger.info(f"  Total registros: {len(df):,}")
    logger.info(f"  Rango temporal:  {df['fecha'].min()} → {df['fecha'].max()}")
    logger.info(f"  Fuentes:         {sorted(df['fuente'].unique())}")

    # Desglose por fuente
    logger.info("")
    logger.info("  Por fuente:")
    for fuente, grupo in df.groupby("fuente"):
        logger.info(
            f"    {fuente:>12}: {len(grupo):>10,} registros | "
            f"{grupo['fecha'].min().date()} → {grupo['fecha'].max().date()}"
        )

    # Desglose por calidad
    logger.info("")
    logger.info("  Calidad de datos:")
    calidad = df["calidad_dato"].value_counts()
    for cat, n in calidad.items():
        pct = n / len(df) * 100
        logger.info(f"    {cat:>8}: {n:>10,} ({pct:.1f}%)")

    # Estadísticas por variable (solo datos ok)
    logger.info("")
    logger.info("  Estadísticas por variable (calidad_dato = ok):")
    df_ok = df[df["calidad_dato"] == "ok"]

    stats_map = {
        "precipitacion_mm": "mm",
        "temp_c": "°C",
        "humedad_pct": "%",
    }

    for var, unidad in stats_map.items():
        if var in df_ok.columns:
            vals = df_ok[var].dropna()
            if len(vals) > 0:
                logger.info(
                    f"    {var:>18}: n={len(vals):>8,} | "
                    f"media={vals.mean():>8.1f} | "
                    f"mediana={vals.median():>8.1f} | "
                    f"min={vals.min():>8.1f} | "
                    f"max={vals.max():>8.1f} {unidad}"
                )
            else:
                logger.info(f"    {var:>18}: sin datos válidos")

    logger.info("=" * 70)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta el pipeline completo de normalización meteorológica.

    Flujo:
        1. Cargar datos de las 3 fuentes
        2. Convertir timestamps a UTC
        3. Concatenar todo
        4. Validar rangos físicos
        5. Extraer columna hora
        6. Aplicar esquema canónico
        7. Guardar CSV
        8. Imprimir resumen
    """
    logger = setup_logging()

    logger.info("=" * 70)
    logger.info("FASE 5.2: NORMALIZACIÓN DE DATOS METEOROLÓGICOS")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")
    logger.info("")

    # ------------------------------------------------------------------
    # PASO 1: Cargar datos de las 3 fuentes
    # ------------------------------------------------------------------
    logger.info("─" * 40)
    logger.info("PASO 1: Carga de datos")
    logger.info("─" * 40)

    df_aemet = cargar_aemet(logger)
    df_avamet = cargar_avamet(logger)
    df_owm = cargar_openweather(logger)

    # Verificar que hay al menos una fuente con datos
    fuentes_vacias = sum([df_aemet.empty, df_avamet.empty, df_owm.empty])
    if fuentes_vacias == 3:
        logger.error("No se encontraron datos en ninguna fuente. Abortando.")
        print("\n❌ ERROR: Sin datos de entrada. Verifica las rutas.")
        return

    logger.info(
        f"\nResumen de carga: "
        f"AEMET={len(df_aemet):,} | AVAMET={len(df_avamet):,} | OWM={len(df_owm):,}"
    )

    # ------------------------------------------------------------------
    # PASO 2: Convertir timestamps a UTC (por fuente, antes de concatenar)
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 2: Normalización temporal → UTC")
    logger.info("─" * 40)

    frames_normalizados = []

    # --- AEMET ---
    if not df_aemet.empty:
        logger.info("→ Procesando AEMET...")
        # Anclaje a mediodía: AEMET son medias diarias (00:00-23:59).
        # Mismo razonamiento que GVA en Fase 5.1.
        df_aemet["fecha"] = df_aemet["fecha"] + pd.Timedelta(hours=12)
        logger.debug("AEMET: anclaje +12h (medias diarias → mediodía local)")
        df_aemet = convertir_a_utc(df_aemet, logger, "AEMET")
        frames_normalizados.append(df_aemet)

    # --- AVAMET ---
    if not df_avamet.empty:
        logger.info("→ Procesando AVAMET...")
        # AVAMET timestamps son locales (hora de scraping)
        df_avamet = convertir_a_utc(df_avamet, logger, "AVAMET")
        frames_normalizados.append(df_avamet)

    # --- OpenWeatherMap ---
    if not df_owm.empty:
        logger.info("→ Procesando OpenWeatherMap...")
        # OWM ya viene en UTC (timestamp Unix) → tz-aware desde el cargador
        # Solo verificar
        if not isinstance(df_owm["fecha"].dtype, pd.DatetimeTZDtype):
            logger.warning("OWM: fecha no es tz-aware. Forzando UTC...")
            df_owm["fecha"] = pd.to_datetime(df_owm["fecha"], utc=True)
        else:
            df_owm["fecha"] = df_owm["fecha"].dt.tz_convert("UTC")
        frames_normalizados.append(df_owm)

    if not frames_normalizados:
        logger.error("Ninguna fuente produjo datos tras normalización temporal.")
        return

    # ------------------------------------------------------------------
    # PASO 3: Concatenar
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 3: Concatenación")
    logger.info("─" * 40)

    df_all = pd.concat(frames_normalizados, ignore_index=True)
    logger.info(f"Total tras concatenar: {len(df_all):,} registros")

    # Asegurar que las 3 columnas canónicas existen
    for col in VARIABLES_CANONICAS:
        if col not in df_all.columns:
            df_all[col] = float("nan")
            logger.debug(f"Columna '{col}' añadida con NaN (no presente en datos)")

    # ------------------------------------------------------------------
    # PASO 4: Validar rangos físicos
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 4: Validación de rangos")
    logger.info("─" * 40)

    df_all = validar_rangos(df_all, logger)

    # ------------------------------------------------------------------
    # PASO 5: Extraer hora
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 5: Extracción de hora")
    logger.info("─" * 40)

    df_all = extraer_hora(df_all, logger)

    # ------------------------------------------------------------------
    # PASO 6: Aplicar esquema canónico final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 6: Esquema canónico final")
    logger.info("─" * 40)

    # Seleccionar y ordenar columnas según el esquema definido
    columnas_finales = [
        "fecha", "hora",
        "precipitacion_mm", "temp_c", "humedad_pct",
        "fuente", "calidad_dato"
    ]

    # Verificar que todas las columnas existen
    for col in columnas_finales:
        if col not in df_all.columns:
            logger.error(f"Columna requerida '{col}' no encontrada. Abortando.")
            return

    df_final = df_all[columnas_finales].copy()

    # Ordenar por fecha
    df_final = df_final.sort_values("fecha").reset_index(drop=True)

    # Asegurar tipos
    df_final["hora"] = df_final["hora"].astype(int)
    df_final["precipitacion_mm"] = df_final["precipitacion_mm"].astype(float)
    df_final["temp_c"] = df_final["temp_c"].astype(float)
    df_final["humedad_pct"] = df_final["humedad_pct"].astype(float)
    df_final["fuente"] = df_final["fuente"].astype(str)
    df_final["calidad_dato"] = df_final["calidad_dato"].astype(str)

    logger.info(f"Registros finales: {len(df_final):,}")
    logger.info(f"Columnas: {list(df_final.columns)}")
    logger.info(f"Dtypes:\n{df_final.dtypes}")

    # ------------------------------------------------------------------
    # PASO 7: Guardar
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 7: Guardado")
    logger.info("─" * 40)

    csv_path = guardar_resultados(df_final, logger)

    # ------------------------------------------------------------------
    # PASO 8: Resumen
    # ------------------------------------------------------------------
    imprimir_resumen(df_final, logger)

    # Mensaje final para consola
    if csv_path:
        print(f"\n✅ NORMALIZACIÓN COMPLETA: {len(df_final):,} registros")
        print(f"   → CSV: {csv_path}")
    else:
        print(f"\n⚠️ Normalización completada pero sin CSV. Revisa logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
