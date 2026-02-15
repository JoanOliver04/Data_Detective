# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.1: Normalización de Datos de Contaminación
==============================================================================

Descripción:
    Unifica los datos de contaminación de las tres fuentes del proyecto
    (GVA histórico, EEA histórico y AQICN streaming) en un único dataset
    canónico, listo para análisis y visualización.

Fuentes de entrada:
    1. GVA  → 1.DATOS_EN_CRUDO/estaticos/contaminacion/gva_*_historico.csv
    2. EEA  → 1.DATOS_EN_CRUDO/estaticos/eea/eea_valencia_filtrado.csv
    3. AQICN→ 1.DATOS_EN_CRUDO/dinamicos/contaminacion/aqicn_*.json

Esquema canónico de salida:
    fecha_utc       → datetime64[ns, UTC]  (timestamp con zona horaria)
    estacion_id     → str                  (código numérico de la estación)
    estacion_nombre → str                  (nombre legible de la estación)
    fuente          → str                  (gva | eea | aqicn)
    variable        → str                  (NO2 | O3 | PM10 | PM2.5 | SO2 | CO)
    valor           → float64             (concentración en µg/m³)
    unidad          → str                  (siempre µg/m³)
    calidad_dato    → str                  (ok | invalid | missing)

Decisiones de diseño:
    - Los datos GVA y EEA son medias DIARIAS → se asignan a las 00:00 UTC
      del día correspondiente (CET/CEST → UTC).
    - Los datos AQICN son instantáneos con timestamp propio en local →
      se convierten a UTC usando la zona Europe/Madrid.
    - AQICN reporta AQI, no µg/m³. Se extrae el campo "v" de cada
      contaminante en "iaqi", que sí viene en µg/m³.
    - Se validan rangos físicos por contaminante (basados en umbrales
      de la OMS/UE). Los valores fuera de rango se marcan como "invalid"
      pero NO se eliminan, para preservar trazabilidad.
    - Las filas con valor NaN se marcan como "missing".
    - Salida en Parquet con compresión snappy para rendimiento óptimo.

Uso:
    python 2.SCRIPTS/procesamiento/normalizar_contaminacion.py

Salida:
    3.DATOS_LIMPIOS/contaminacion_normalizada.parquet

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia

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
GVA_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "contaminacion"
EEA_FILE = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / \
    "estaticos" / "eea" / "eea_valencia_filtrado.csv"
AQICN_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "contaminacion"

# --- Salida ---
OUTPUT_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
OUTPUT_FILE = OUTPUT_DIR / "contaminacion_normalizada.parquet"
# Copia CSV para inspección rápida (opcional pero útil para debug)
OUTPUT_CSV = OUTPUT_DIR / "contaminacion_normalizada.csv"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# ==============================================================================
# CONSTANTES DEL DOMINIO
# ==============================================================================

# Variables canónicas del proyecto
VARIABLES_CANONICAS = ["NO2", "O3", "PM10", "PM2.5", "SO2", "CO"]

# Mapeo de nombres alternativos → nombre canónico
# (cubre variantes encontradas en GVA, EEA y AQICN)
VARIABLE_ALIASES = {
    # Canónicos (identidad)
    "NO2": "NO2",
    "O3": "O3",
    "PM10": "PM10",
    "PM2.5": "PM2.5",
    "SO2": "SO2",
    "CO": "CO",
    # Variantes comunes
    "PM25": "PM2.5",
    "pm25": "PM2.5",
    "pm2.5": "PM2.5",
    "pm10": "PM10",
    "no2": "NO2",
    "o3": "O3",
    "so2": "SO2",
    "co": "CO",
    # Variantes AQICN (minúsculas en campo iaqi)
    "pm25": "PM2.5",
}

# Rangos físicos razonables para validación (µg/m³)
# Basados en umbrales OMS y valores extremos históricos documentados.
# valor_max es generoso para no descartar picos reales (p.ej. Fallas).
#
# NOTA SOBRE CO: Los datos de CO llegan ya en µg/m³ desde las fases anteriores:
#   - GVA (Fase 2.1): los archivos .txt de GVA reportan CO en µg/m³
#   - EEA (Fase 2.2): el Parquet E1a usa µg/m³ como unidad estándar
#   - AQICN (Fase 3.1): el campo iaqi.co.v devuelve µg/m³
# Por tanto, NO se aplica conversión mg/m³ → µg/m³ aquí.
# Si en el futuro se detecta que alguna fuente reporta CO en mg/m³,
# se deberá añadir una conversión explícita (* 1000) antes de este paso.
RANGOS_FISICOS = {
    "NO2":   {"min": 0, "max": 600},    # Picos industriales/tráfico ~400
    "O3":    {"min": 0, "max": 500},    # Episodios extremos ~300-400
    "PM10":  {"min": 0, "max": 1000},   # Tormentas de polvo sahariano ~800
    "PM2.5": {"min": 0, "max": 500},    # Episodios Fallas/calima ~300
    "SO2":   {"min": 0, "max": 1000},   # Zonas industriales ~500
    "CO":    {"min": 0, "max": 50000},  # En µg/m³ (50 mg/m³ = 50000 µg/m³)
}

# Registro maestro de estaciones de Valencia
# Fuente: combinación de GVA + AQICN (streaming_aqicn.py)
ESTACIONES_VALENCIA = {
    "46250001": "València - Centro (Avd. Francia)",
    "46250004": "València - Pista de Silla (antigua)",
    "46250030": "València - Pista de Silla",
    "46250047": "València - Politècnic",
    "46250050": "València - Molí del Sol",
    "46250054": "València - Conselleria Meteo (Centre)",
}

# Zona horaria de referencia para datos locales (GVA/EEA)
TIMEZONE_LOCAL = "Europe/Madrid"


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola) siguiendo el patrón del proyecto.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "normalizar_contaminacion.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Normalizar_Contaminacion")
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

def cargar_gva(logger: logging.Logger) -> pd.DataFrame:
    """
    Carga todos los CSV procesados de GVA.

    Los archivos tienen formato: [fecha, estacion, variable, valor]
    generado por descargar_gva_historico.py (Fase 2.1).
    Las fechas son locales (Europe/Madrid) y representan medias diarias.
    """
    patron = "gva_*_historico.csv"
    archivos = sorted(GVA_DIR.glob(patron))

    if not archivos:
        logger.warning(f"GVA: sin archivos en {GVA_DIR} con patrón '{patron}'")
        return pd.DataFrame()

    logger.info(f"GVA: encontrados {len(archivos)} archivos")

    frames = []
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo, parse_dates=["fecha"])
            df["fuente"] = "gva"
            frames.append(df)
            logger.debug(f"  → {archivo.name}: {len(df)} registros")
        except Exception as e:
            logger.error(f"  ✘ Error leyendo {archivo.name}: {e}")

    if not frames:
        return pd.DataFrame()

    resultado = pd.concat(frames, ignore_index=True)
    logger.info(f"GVA: {len(resultado)} registros totales cargados")
    return resultado


def cargar_eea(
    logger: logging.Logger,
    chunksize: Optional[int] = None
) -> pd.DataFrame:
    """
    Carga el CSV consolidado de EEA.

    El archivo tiene formato: [fecha, estacion, variable, valor]
    generado por procesar_eea_historico.py (Fase 2.2).
    Las fechas son locales y representan medias diarias.

    Args:
        logger: Logger para registrar eventos.
        chunksize: Si se especifica (p.ej. 100_000), lee el CSV en chunks
                   para reducir consumo de RAM con archivos grandes (~2.5M filas).
                   Por defecto None = lectura completa en memoria (más rápido
                   si la RAM lo permite, que es el caso actual con ~32 MB).

    Ejemplo de uso con chunks (para el futuro si el dataset crece):
        df_eea = cargar_eea(logger, chunksize=100_000)
    """
    if not EEA_FILE.exists():
        logger.warning(f"EEA: archivo no encontrado → {EEA_FILE}")
        return pd.DataFrame()

    try:
        if chunksize is not None:
            # --- Lectura chunked (Big Data future-proof) ---
            # Útil cuando el CSV supere la RAM disponible.
            # Cada chunk se procesa y acumula en una lista.
            logger.info(f"EEA: lectura chunked (chunksize={chunksize:,})")
            chunks = []
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(EEA_FILE, parse_dates=[
                            "fecha"], chunksize=chunksize)
            ):
                chunk["fuente"] = "eea"
                chunks.append(chunk)
                total_rows += len(chunk)
                logger.debug(
                    f"  Chunk {i+1}: {len(chunk):,} filas (acumulado: {total_rows:,})")

            if not chunks:
                logger.warning("EEA: archivo vacío tras lectura chunked")
                return pd.DataFrame()

            df = pd.concat(chunks, ignore_index=True)
            logger.info(
                f"EEA: {len(df):,} registros cargados (chunked) desde {EEA_FILE.name}")
        else:
            # --- Lectura completa (por defecto, más rápido) ---
            df = pd.read_csv(EEA_FILE, parse_dates=["fecha"])
            df["fuente"] = "eea"
            logger.info(
                f"EEA: {len(df):,} registros cargados desde {EEA_FILE.name}")

        return df

    except Exception as e:
        logger.error(f"EEA: error leyendo {EEA_FILE.name}: {e}")
        return pd.DataFrame()


def cargar_aqicn(logger: logging.Logger) -> pd.DataFrame:
    """
    Carga y extrae datos de todos los JSON capturados por streaming_aqicn.py.

    Estructura esperada del JSON (simplificada):
    {
      "_metadata": { "timestamp_utc": "...", ... },
      "estaciones": {
        "46250030": {
          "nombre": "...",
          "datos": {
            "time": { "iso": "2026-02-09T18:00:00+01:00", ... },
            "iaqi": {
              "no2": {"v": 25.3},
              "pm10": {"v": 42.0},
              ...
            }
          }
        }
      }
    }

    El campo "v" dentro de iaqi contiene la concentración en µg/m³
    (NO el índice AQI, que está en el campo raíz "aqi").
    """
    archivos = sorted(AQICN_DIR.glob("aqicn_*.json"))

    if not archivos:
        logger.warning(f"AQICN: sin archivos JSON en {AQICN_DIR}")
        return pd.DataFrame()

    logger.info(f"AQICN: encontrados {len(archivos)} archivos JSON")

    records = []
    archivos_ok = 0
    archivos_error = 0

    for archivo in archivos:
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                captura = json.load(f)

            estaciones = captura.get("estaciones", {})

            for codigo, info in estaciones.items():
                datos = info.get("datos")
                if datos is None:
                    continue

                # Extraer timestamp de la medición
                time_info = datos.get("time", {})
                iso_str = time_info.get("iso")
                if not iso_str:
                    logger.debug(f"  {archivo.name} / {codigo}: sin timestamp")
                    continue

                # Extraer contaminantes individuales desde iaqi
                iaqi = datos.get("iaqi", {})
                for variable_raw, value_dict in iaqi.items():
                    # Normalizar nombre de variable
                    var_canon = VARIABLE_ALIASES.get(
                        variable_raw.lower(),
                        variable_raw.upper()
                    )

                    # Solo variables de interés
                    if var_canon not in VARIABLES_CANONICAS:
                        continue

                    valor = value_dict.get("v") if isinstance(
                        value_dict, dict) else None

                    records.append({
                        "fecha_iso": iso_str,
                        "estacion": codigo,
                        "variable": var_canon,
                        "valor": valor,
                        "fuente": "aqicn",
                    })

            archivos_ok += 1

        except json.JSONDecodeError as e:
            logger.error(f"  ✘ JSON inválido: {archivo.name} → {e}")
            archivos_error += 1
        except Exception as e:
            logger.error(f"  ✘ Error procesando {archivo.name}: {e}")
            archivos_error += 1

    logger.info(
        f"AQICN: {archivos_ok} archivos procesados OK, "
        f"{archivos_error} con errores, "
        f"{len(records)} registros extraídos"
    )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Parsear timestamps ISO 8601 (incluyen offset como +01:00)
    df["fecha"] = pd.to_datetime(df["fecha_iso"], errors="coerce", utc=True)
    df.drop(columns=["fecha_iso"], inplace=True)

    return df


# ==============================================================================
# TRANSFORMACIONES
# ==============================================================================

def normalizar_variables(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Normaliza los nombres de variables a su forma canónica.
    Elimina filas con variables no reconocidas.
    """
    if df.empty:
        return df

    antes = len(df)

    # Aplicar mapeo de aliases
    df["variable"] = df["variable"].map(
        lambda v: VARIABLE_ALIASES.get(v, VARIABLE_ALIASES.get(v.upper(), v))
    )

    # Filtrar solo variables canónicas
    df = df[df["variable"].isin(VARIABLES_CANONICAS)].copy()

    descartadas = antes - len(df)
    if descartadas > 0:
        logger.info(
            f"Variables: {descartadas} registros con variables no canónicas descartados")

    return df


def convertir_a_utc(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Convierte todas las fechas a UTC timezone-aware.

    - GVA/EEA: fechas naïve (sin zona) → se interpretan como Europe/Madrid
      y se convierten a UTC.
    - AQICN: ya vienen en UTC (convertidas en el cargador).

    NOTA: Se usa pd.api.types.is_datetime64tz_dtype() en lugar de
    df["fecha"].dt.tz is not None, ya que este último puede fallar
    silenciosamente con Series de tipo object o con mixed tz.
    """
    if df.empty:
        return df

    if "fecha" not in df.columns:
        logger.error("Columna 'fecha' no encontrada")
        return df

    # --- FIX: detección robusta de timezone ---
    # pd.api.types.is_datetime64tz_dtype es la forma correcta de comprobar
    # si una columna datetime ya tiene timezone info en pandas.
    # El antiguo `df["fecha"].dt.tz is not None` falla si la columna es
    # object dtype o si contiene mixed timezone-aware/naive values.
    #
    # NOTA pandas >=2.1: is_datetime64tz_dtype está deprecated.
    # Usamos try/except para compatibilidad con pandas 1.x y 2.x+.
    try:
        # Método moderno (pandas >= 2.1)
        tiene_tz = isinstance(df["fecha"].dtype, pd.DatetimeTZDtype)
    except AttributeError:
        # Fallback para pandas < 2.1
        tiene_tz = pd.api.types.is_datetime64tz_dtype(df["fecha"])
    logger.debug(f"Timezone detection: tiene_tz = {tiene_tz}")

    if tiene_tz:
        # Todo el DataFrame ya tiene tz → convertir a UTC por si acaso
        df["fecha_utc"] = df["fecha"].dt.tz_convert("UTC")
    else:
        # Asegurar que la columna es datetime64 naïve (no object)
        # Esto protege contra columnas que no se parsearon bien
        is_datetime = pd.api.types.is_datetime64_any_dtype(df["fecha"])
        if not is_datetime:
            logger.warning(
                "Columna 'fecha' no es datetime. Intentando conversión...")
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

        # Fechas naïve → localizar a Europe/Madrid y luego a UTC
        # NOTA: nonexistent='shift_forward' maneja el cambio de hora CET→CEST
        try:
            df["fecha_utc"] = (
                df["fecha"]
                .dt.tz_localize(TIMEZONE_LOCAL, ambiguous="NaT", nonexistent="shift_forward")
                .dt.tz_convert("UTC")
            )
        except Exception as e:
            logger.warning(
                f"Error localizando timezone: {e}. Asignando UTC directo.")
            df["fecha_utc"] = df["fecha"].dt.tz_localize("UTC")

    # Contar NaT generados por ambigüedad horaria
    nat_count = df["fecha_utc"].isna().sum()
    if nat_count > 0:
        logger.warning(
            f"Timezone: {nat_count} fechas ambiguas convertidas a NaT")

    return df


def enriquecer_estaciones(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Añade el nombre legible de la estación a partir del registro maestro.
    Las estaciones desconocidas reciben nombre "Desconocida ({id})".
    """
    if df.empty:
        return df

    df["estacion_nombre"] = df["estacion"].map(
        lambda code: ESTACIONES_VALENCIA.get(
            str(code), f"Desconocida ({code})")
    )

    desconocidas = df[df["estacion_nombre"].str.startswith("Desconocida")]
    if len(desconocidas) > 0:
        ids_desc = desconocidas["estacion"].unique()
        logger.warning(
            f"Estaciones: {len(ids_desc)} ID(s) sin nombre registrado → {list(ids_desc)}"
        )

    return df


def validar_rangos(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida los valores contra rangos físicos razonables por contaminante.

    NO elimina filas: las marca con calidad_dato = 'invalid' para trazabilidad.
    Las filas sin valor se marcan como 'missing'.
    Las filas dentro de rango se marcan como 'ok'.
    """
    if df.empty:
        return df

    # Inicializar columna de calidad
    df["calidad_dato"] = "ok"

    # Marcar valores nulos como missing
    nulos = df["valor"].isna()
    df.loc[nulos, "calidad_dato"] = "missing"
    n_missing = nulos.sum()

    # Validar rangos por variable
    n_invalid = 0
    for variable, rango in RANGOS_FISICOS.items():
        mascara_var = df["variable"] == variable
        n_var = mascara_var.sum()

        # Log de asunción de unidades para CO (trazabilidad)
        if variable == "CO" and n_var > 0:
            logger.debug(
                f"  CO: {n_var} registros. Asumiendo µg/m³ (sin conversión desde mg/m³). "
                f"Rango validación: [{rango['min']}, {rango['max']}] µg/m³"
            )
        fuera_rango = mascara_var & (
            (df["valor"] < rango["min"]) | (df["valor"] > rango["max"])
        )
        n_fuera = fuera_rango.sum()
        if n_fuera > 0:
            df.loc[fuera_rango, "calidad_dato"] = "invalid"
            n_invalid += n_fuera
            logger.debug(
                f"  {variable}: {n_fuera} valores fuera de rango "
                f"[{rango['min']}, {rango['max']}] µg/m³"
            )

    n_ok = (df["calidad_dato"] == "ok").sum()
    logger.info(
        f"Validación: {n_ok} ok | {n_invalid} invalid | {n_missing} missing "
        f"(total: {len(df)})"
    )

    return df


# ==============================================================================
# CONSOLIDACIÓN FINAL
# ==============================================================================

def consolidar_esquema(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Aplica el esquema canónico final: selecciona, renombra y ordena columnas.
    Elimina duplicados exactos.
    """
    if df.empty:
        logger.warning("DataFrame vacío al consolidar esquema")
        return pd.DataFrame(columns=[
            "fecha_utc", "estacion_id", "estacion_nombre",
            "fuente", "variable", "valor", "unidad", "calidad_dato"
        ])

    # Renombrar columnas al esquema canónico
    df = df.rename(columns={"estacion": "estacion_id"})

    # Añadir columna de unidad (siempre µg/m³ para contaminación atmosférica)
    df["unidad"] = "µg/m³"

    # Asegurar tipo string para estacion_id
    df["estacion_id"] = df["estacion_id"].astype(str)

    # Seleccionar y ordenar columnas finales
    columnas_finales = [
        "fecha_utc",
        "estacion_id",
        "estacion_nombre",
        "fuente",
        "variable",
        "valor",
        "unidad",
        "calidad_dato",
    ]

    # Verificar que existan todas las columnas
    faltantes = [c for c in columnas_finales if c not in df.columns]
    if faltantes:
        logger.error(f"Columnas faltantes en el DataFrame: {faltantes}")
        return pd.DataFrame(columns=columnas_finales)

    df = df[columnas_finales].copy()

    # Eliminar duplicados exactos
    antes = len(df)
    df = df.drop_duplicates()
    duplicados = antes - len(df)
    if duplicados > 0:
        logger.info(f"Duplicados eliminados: {duplicados}")

    # Ordenar por fecha, estación y variable
    df = df.sort_values(
        ["fecha_utc", "estacion_id", "variable"]
    ).reset_index(drop=True)

    return df


# ==============================================================================
# GUARDADO
# ==============================================================================

def guardar_resultados(
    df: pd.DataFrame,
    logger: logging.Logger
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Guarda el dataset normalizado en Parquet (principal) y CSV (debug).
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    parquet_path = None
    csv_path = None

    # --- Parquet (formato principal) ---
    try:
        df.to_parquet(OUTPUT_FILE, engine="pyarrow",
                      index=False, compression="snappy")
        size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        parquet_path = OUTPUT_FILE
        logger.info(
            f"✔ Parquet guardado: {OUTPUT_FILE.name} ({size_mb:.2f} MB)")
    except ImportError:
        # Si pyarrow no está instalado, intentar con fastparquet
        try:
            df.to_parquet(OUTPUT_FILE, engine="fastparquet",
                          index=False, compression="snappy")
            size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
            parquet_path = OUTPUT_FILE
            logger.info(
                f"✔ Parquet guardado (fastparquet): {OUTPUT_FILE.name} ({size_mb:.2f} MB)")
        except ImportError:
            logger.error(
                "No se pudo guardar Parquet: instala pyarrow o fastparquet.\n"
                "  → pip install pyarrow"
            )
    except Exception as e:
        logger.error(f"Error guardando Parquet: {e}")

    # --- CSV (copia de debug, sin timezone info para legibilidad) ---
    try:
        df_csv = df.copy()
        # Convertir fecha UTC a string ISO para que el CSV sea legible
        df_csv["fecha_utc"] = df_csv["fecha_utc"].dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        df_csv.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        size_kb = OUTPUT_CSV.stat().st_size / 1024
        csv_path = OUTPUT_CSV
        logger.info(
            f"✔ CSV debug guardado: {OUTPUT_CSV.name} ({size_kb:.1f} KB)")
    except Exception as e:
        logger.warning(f"No se pudo guardar CSV de debug: {e}")

    return parquet_path, csv_path


# ==============================================================================
# INFORME DE RESUMEN
# ==============================================================================

def imprimir_resumen(df: pd.DataFrame, logger: logging.Logger) -> None:
    """
    Imprime un resumen estadístico del dataset normalizado.
    """
    if df.empty:
        logger.info("Dataset vacío - sin resumen que mostrar")
        return

    logger.info("")
    logger.info("=" * 70)
    logger.info("RESUMEN DEL DATASET NORMALIZADO")
    logger.info("=" * 70)

    # Métricas generales
    logger.info(f"  Total registros: {len(df):,}")
    logger.info(
        f"  Rango temporal:  {df['fecha_utc'].min()} → {df['fecha_utc'].max()}")
    logger.info(f"  Estaciones:      {df['estacion_id'].nunique()}")
    logger.info(f"  Variables:       {sorted(df['variable'].unique())}")

    # Desglose por fuente
    logger.info("")
    logger.info("  Por fuente:")
    for fuente, grupo in df.groupby("fuente"):
        logger.info(
            f"    {fuente:>5}: {len(grupo):>10,} registros | "
            f"{grupo['estacion_id'].nunique()} estaciones | "
            f"{grupo['fecha_utc'].min().date()} → {grupo['fecha_utc'].max().date()}"
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
    for var in sorted(df_ok["variable"].unique()):
        vals = df_ok[df_ok["variable"] == var]["valor"]
        logger.info(
            f"    {var:>5}: n={len(vals):>8,} | "
            f"media={vals.mean():>8.1f} | "
            f"mediana={vals.median():>8.1f} | "
            f"min={vals.min():>8.1f} | "
            f"max={vals.max():>8.1f} µg/m³"
        )

    logger.info("=" * 70)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta el pipeline completo de normalización de contaminación.

    Flujo:
        1. Cargar datos de las 3 fuentes
        2. Normalizar nombres de variables
        3. Convertir timestamps a UTC
        4. Enriquecer con nombres de estación
        5. Validar rangos físicos
        6. Consolidar esquema canónico
        7. Guardar Parquet + CSV
        8. Imprimir resumen
    """
    logger = setup_logging()

    logger.info("=" * 70)
    logger.info("FASE 5.1: NORMALIZACIÓN DE DATOS DE CONTAMINACIÓN")
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

    df_gva = cargar_gva(logger)
    df_eea = cargar_eea(logger)
    df_aqicn = cargar_aqicn(logger)

    # Verificar que hay al menos una fuente con datos
    fuentes_vacias = sum([df_gva.empty, df_eea.empty, df_aqicn.empty])
    if fuentes_vacias == 3:
        logger.error("No se encontraron datos en ninguna fuente. Abortando.")
        print("\n❌ ERROR: Sin datos de entrada. Verifica las rutas.")
        return

    logger.info(
        f"\nResumen de carga: "
        f"GVA={len(df_gva):,} | EEA={len(df_eea):,} | AQICN={len(df_aqicn):,}"
    )

    # ------------------------------------------------------------------
    # PASO 2: Procesar cada fuente por separado (antes de concatenar)
    # Esto es necesario porque AQICN ya tiene tz-aware y GVA/EEA no.
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 2: Normalización por fuente")
    logger.info("─" * 40)

    frames_normalizados = []

    # --- GVA ---
    if not df_gva.empty:
        logger.info("→ Procesando GVA...")
        df_gva = normalizar_variables(df_gva, logger)
        # Anclaje temporal a mediodía para medias diarias:
        # Los datos GVA son promedios del día completo (00:00-23:59).
        # Anclarlos a 00:00 introduce un sesgo temporal (parece que el dato
        # pertenece a la medianoche). Al sumar +12h, el timestamp queda a
        # las 12:00 local → centro del período de muestreo.
        # Esto reduce el error temporal máximo de ±12h a ±12h centrado.
        df_gva["fecha"] = df_gva["fecha"] + pd.Timedelta(hours=12)
        logger.debug(
            "GVA: aplicado anclaje +12h (medias diarias → mediodía local)")
        df_gva = convertir_a_utc(df_gva, logger)
        frames_normalizados.append(df_gva)

    # --- EEA ---
    if not df_eea.empty:
        logger.info("→ Procesando EEA...")
        df_eea = normalizar_variables(df_eea, logger)
        # Mismo anclaje a mediodía que GVA: los datos EEA "Verified E1a"
        # son medias diarias (AggType=day). Ver comentario en bloque GVA.
        df_eea["fecha"] = df_eea["fecha"] + pd.Timedelta(hours=12)
        logger.debug(
            "EEA: aplicado anclaje +12h (medias diarias → mediodía local)")
        df_eea = convertir_a_utc(df_eea, logger)
        frames_normalizados.append(df_eea)

    # --- AQICN (ya tiene fecha UTC del cargador) ---
    if not df_aqicn.empty:
        logger.info("→ Procesando AQICN...")
        df_aqicn = normalizar_variables(df_aqicn, logger)
        # AQICN ya tiene 'fecha' en UTC → renombrar a fecha_utc
        df_aqicn["fecha_utc"] = df_aqicn["fecha"]
        frames_normalizados.append(df_aqicn)

    if not frames_normalizados:
        logger.error("Ninguna fuente produjo datos tras normalización.")
        return

    # ------------------------------------------------------------------
    # PASO 3: Concatenar todo
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 3: Concatenación")
    logger.info("─" * 40)

    df_all = pd.concat(frames_normalizados, ignore_index=True)
    logger.info(f"Total tras concatenar: {len(df_all):,} registros")

    # ------------------------------------------------------------------
    # PASO 4: Enriquecer con nombres de estación
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 4: Enriquecimiento de estaciones")
    logger.info("─" * 40)

    df_all = enriquecer_estaciones(df_all, logger)

    # ------------------------------------------------------------------
    # PASO 5: Validar rangos físicos
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 5: Validación de rangos")
    logger.info("─" * 40)

    df_all = validar_rangos(df_all, logger)

    # ------------------------------------------------------------------
    # PASO 6: Aplicar esquema canónico final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 6: Esquema canónico")
    logger.info("─" * 40)

    df_final = consolidar_esquema(df_all, logger)
    logger.info(f"Registros finales: {len(df_final):,}")

    # ------------------------------------------------------------------
    # PASO 7: Guardar
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 7: Guardado")
    logger.info("─" * 40)

    parquet_path, csv_path = guardar_resultados(df_final, logger)

    # ------------------------------------------------------------------
    # PASO 8: Resumen
    # ------------------------------------------------------------------
    imprimir_resumen(df_final, logger)

    # Mensaje final para consola
    if parquet_path:
        print(f"\n✅ NORMALIZACIÓN COMPLETA: {len(df_final):,} registros")
        print(f"   → Parquet: {parquet_path}")
        if csv_path:
            print(f"   → CSV:     {csv_path}")
    else:
        print(f"\n⚠️ Normalización completada pero sin Parquet. Revisa logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
