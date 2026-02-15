# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.3: Procesamiento de Datos de Tráfico
==============================================================================

Descripción:
    Procesa los archivos JSON capturados por streaming_dgt.py (Fase 3.4)
    y los transforma en un dataset tabular limpio con incidencias de tráfico
    filtradas para la Comunidad Valenciana.

    IMPORTANTE — SOBRE VELOCIDAD E INTENSIDAD:
    ─────────────────────────────────────────────
    El endpoint DGT utilizado (SituationPublication) solo contiene
    INCIDENCIAS (obras, cortes, accidentes, congestión, etc.).
    NO incluye datos de velocidad media ni intensidad de tráfico.
    Esos datos requerirían endpoints diferentes del NAP DGT
    (TrafficStatus, MeasuredDataPublication).

    Por tanto, las columnas 'intensidad' y 'velocidad' se crean en el
    esquema canónico pero se rellenan con NaN, manteniendo la estructura
    definida en el Índice Maestro para compatibilidad futura.

Fuente de entrada:
    1.DATOS_EN_CRUDO/dinamicos/trafico/dgt_*.json

Estructura de cada JSON (generada por streaming_dgt.py):
    {
      "_metadata": {
        "timestamp_captura": "...",
        "timestamp_utc": "...",
        "total_incidencias": N,
        ...
      },
      "incidencias": [
        {
          "id": "SIT_...",
          "tipo_datex": "sit:RoadOrCarriagewayOrLaneManagement",
          "fecha_creacion": "2026-02-15T10:30:00+01:00",
          "fecha_version": "2026-02-15T12:00:00+01:00",
          "probabilidad": "certain",
          "severidad": "low",
          "causa_tipo": "roadMaintenance",
          "tipo_gestion": "laneClosures",
          "localizacion": {
            "carretera": "V-31",
            "punto_from": {
              "latitud": 39.45,
              "longitud": -0.38,
              "comunidad_autonoma": "Comunitat Valenciana",
              "provincia": "Valencia/València",
              "municipio": "València"
            }
          }
        },
        ...
      ],
      "estadisticas": { ... }
    }

Esquema canónico de salida:
    fecha          → datetime64[ns, UTC]  (timestamp tz-aware)
    hora           → int                  (0–23, extraída de fecha UTC)
    ubicacion      → str                  (carretera | municipio | provincia)
    intensidad     → float64              (NaN — no disponible en este endpoint)
    velocidad      → float64              (NaN — no disponible en este endpoint)
    incidencias    → str                  (tipo_datex | severidad | probabilidad)
    fuente         → str                  (siempre "dgt")
    calidad_dato   → str                  (ok | missing)

Filtros aplicados:
    - Solo Comunidad Valenciana (si campo comunidad_autonoma existe)
    - Duplicados exactos eliminados
    - Ordenado por fecha ascendente

Uso:
    python 2.SCRIPTS/procesamiento/limpiar_trafico.py

Salida:
    3.DATOS_LIMPIOS/trafico_limpio.csv

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia

"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

import pandas as pd

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas relativas al directorio raíz del proyecto
# El script vive en: 2.SCRIPTS/procesamiento/
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Entrada ---
DGT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "trafico"

# --- Salida ---
OUTPUT_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
OUTPUT_CSV = OUTPUT_DIR / "trafico_limpio.csv"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# Zona horaria de referencia (DATEX II timestamps son ISO 8601 con offset)
TIMEZONE_LOCAL = "Europe/Madrid"

# ==============================================================================
# CONSTANTES DEL DOMINIO
# ==============================================================================

# Variantes del nombre de la Comunidad Valenciana en datos DGT
# El campo comunidad_autonoma puede venir en castellano o valenciano.
COMUNIDAD_VALENCIANA_ALIASES = {
    "comunitat valenciana",
    "comunidad valenciana",
    "c. valenciana",
    "valencia",
    "valenciana",
    "c.valenciana",
    "com. valenciana",
}

# Provincias de la Comunidad Valenciana (para filtrado alternativo)
PROVINCIAS_CV = {
    "valencia",
    "valència",
    "valencia/valència",
    "alicante",
    "alacant",
    "alicante/alacant",
    "castellón",
    "castelló",
    "castellón/castelló",
    "castellón de la plana",
    "castelló de la plana",
}

# Columnas del esquema canónico final
COLUMNAS_CANONICAS = [
    "fecha",
    "hora",
    "ubicacion",
    "intensidad",
    "velocidad",
    "incidencias",
    "fuente",
    "calidad_dato",
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola) siguiendo el patrón del proyecto.
    Mismo estilo que normalizar_contaminacion.py (Fase 5.1).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "limpiar_trafico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Limpiar_Trafico")
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
# CARGA DE DATOS
# ==============================================================================

def cargar_dgt(logger: logging.Logger) -> List[dict]:
    """
    Carga todos los JSON capturados por streaming_dgt.py (Fase 3.4).

    Lee cada archivo dgt_*.json y extrae la lista de incidencias,
    preservando el timestamp de captura como referencia.

    Returns:
        Lista plana de diccionarios, un dict por situationRecord.
        Cada dict incluye '_timestamp_captura' del archivo padre.
    """
    archivos = sorted(DGT_DIR.glob("dgt_*.json"))

    if not archivos:
        logger.warning(f"DGT: sin archivos JSON en {DGT_DIR}")
        return []

    logger.info(f"DGT: encontrados {len(archivos)} archivos JSON")

    all_records = []
    archivos_ok = 0
    archivos_error = 0
    archivos_vacios = 0

    for archivo in archivos:
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                captura = json.load(f)

            # Extraer metadata para timestamp de referencia
            metadata = captura.get("_metadata", {})
            timestamp_captura = metadata.get(
                "timestamp_utc",
                metadata.get("timestamp_captura", None)
            )

            # Extraer lista de incidencias
            incidencias = captura.get("incidencias", [])

            if not incidencias:
                archivos_vacios += 1
                logger.debug(f"  {archivo.name}: sin incidencias")
                continue

            # Anotar cada record con el timestamp del archivo
            for record in incidencias:
                if isinstance(record, dict):
                    record["_timestamp_captura"] = timestamp_captura
                    record["_archivo_origen"] = archivo.name
                    all_records.append(record)

            archivos_ok += 1
            logger.debug(
                f"  → {archivo.name}: {len(incidencias)} incidencias"
            )

        except json.JSONDecodeError as e:
            logger.error(f"  ✘ JSON inválido: {archivo.name} → {e}")
            archivos_error += 1
        except Exception as e:
            logger.error(f"  ✘ Error leyendo {archivo.name}: {e}")
            archivos_error += 1

    logger.info(
        f"DGT: {archivos_ok} archivos con datos, "
        f"{archivos_vacios} vacíos, "
        f"{archivos_error} con errores"
    )
    logger.info(f"DGT: {len(all_records):,} incidencias totales extraídas")

    return all_records


# ==============================================================================
# TRANSFORMACIONES
# ==============================================================================

def extraer_fecha(record: dict) -> Optional[str]:
    """
    Extrae el timestamp más relevante de una incidencia DGT.

    Prioridad:
    1. fecha_creacion → cuándo se creó la incidencia (más representativa)
    2. fecha_version  → cuándo se actualizó por última vez
    3. fecha_inicio   → inicio de la vigencia
    4. _timestamp_captura → timestamp de captura del archivo (fallback)

    Todos son ISO 8601 con offset (+01:00 / +02:00), excepto
    _timestamp_captura que puede estar en formato libre.

    Args:
        record: Diccionario de una incidencia

    Returns:
        String ISO 8601 con timestamp, o None si no hay ninguno
    """
    for campo in ("fecha_creacion", "fecha_version", "fecha_inicio",
                  "_timestamp_captura"):
        valor = record.get(campo)
        if valor and isinstance(valor, str) and valor.strip():
            return valor.strip()
    return None


def extraer_ubicacion(record: dict) -> str:
    """
    Construye una cadena de ubicación legible a partir de los campos
    de localización de la incidencia.

    Formato: "carretera | municipio | provincia"
    Si algún campo falta, se omite del resultado.

    Args:
        record: Diccionario de una incidencia

    Returns:
        String con ubicación combinada, o "desconocida"
    """
    loc = record.get("localizacion", {})
    partes = []

    # Carretera
    carretera = loc.get("carretera")
    if carretera:
        partes.append(carretera)

    # Municipio y provincia (buscar en punto_from o punto_to)
    for punto_key in ("punto_from", "punto_to"):
        punto = loc.get(punto_key, {})
        if not punto:
            continue

        municipio = punto.get("municipio")
        if municipio and municipio not in partes:
            partes.append(municipio)

        provincia = punto.get("provincia")
        if provincia and provincia not in partes:
            partes.append(provincia)

        # Solo necesitamos un punto con datos
        if municipio or provincia:
            break

    if not partes:
        return "desconocida"

    return " | ".join(partes)


def extraer_incidencias(record: dict) -> str:
    """
    Construye la cadena descriptiva de la incidencia combinando
    tipo_datex, severidad y probabilidad.

    Formato: "tipo_datex | severidad | probabilidad"

    Args:
        record: Diccionario de una incidencia

    Returns:
        String descriptivo, o "sin_tipo" si no hay tipo_datex
    """
    partes = []

    # Tipo DATEX (nombre técnico del tipo de incidencia)
    tipo = record.get("tipo_datex", "")
    if tipo:
        # Limpiar prefijo namespace si existe (e.g. "sit:RoadMaintenance" → "RoadMaintenance")
        if ":" in tipo:
            tipo = tipo.split(":")[-1]
        partes.append(tipo)

    # Severidad
    severidad = record.get("severidad", record.get("severidad_global", ""))
    if severidad:
        partes.append(severidad)

    # Probabilidad
    probabilidad = record.get("probabilidad", "")
    if probabilidad:
        partes.append(probabilidad)

    if not partes:
        return "sin_tipo"

    return " | ".join(partes)


def es_comunidad_valenciana(record: dict) -> bool:
    """
    Determina si una incidencia pertenece a la Comunidad Valenciana.

    Comprueba el campo comunidad_autonoma y/o provincia en los puntos
    de localización (punto_from, punto_to).

    Si no hay información geográfica suficiente, se incluye la incidencia
    por defecto (mejor incluir de más que perder datos).

    Args:
        record: Diccionario de una incidencia

    Returns:
        True si es Comunidad Valenciana o si no se puede determinar
    """
    loc = record.get("localizacion", {})

    if not loc:
        # Sin localización → no podemos filtrar, incluir por seguridad
        return True

    # Buscar en ambos puntos (from y to)
    for punto_key in ("punto_from", "punto_to"):
        punto = loc.get(punto_key, {})

        # Comprobar comunidad autónoma
        comunidad = punto.get("comunidad_autonoma", "")
        if comunidad and comunidad.strip().lower() in COMUNIDAD_VALENCIANA_ALIASES:
            return True

        # Comprobar provincia como fallback
        provincia = punto.get("provincia", "")
        if provincia and provincia.strip().lower() in PROVINCIAS_CV:
            return True

    # Si tenemos localización pero ningún campo coincide → no es CV
    # Solo rechazar si al menos un punto tiene comunidad o provincia definida
    tiene_geo = any(
        loc.get(pk, {}).get("comunidad_autonoma") or
        loc.get(pk, {}).get("provincia")
        for pk in ("punto_from", "punto_to")
    )

    if tiene_geo:
        # Tiene datos geográficos pero no coinciden con CV
        return False

    # Sin datos geográficos suficientes → incluir por precaución
    return True


def records_a_dataframe(
    records: List[dict],
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Transforma la lista de diccionarios de incidencias en un DataFrame
    tabular con el esquema canónico.

    Flujo por cada record:
    1. Extraer fecha (prioridad: creacion > version > inicio > captura)
    2. Construir ubicación (carretera | municipio | provincia)
    3. Construir cadena de incidencias (tipo | severidad | probabilidad)
    4. Asignar intensidad=NaN, velocidad=NaN (no disponibles)
    5. Asignar calidad_dato (ok si tiene tipo_datex, missing si no)

    Args:
        records: Lista de dicts de incidencias
        logger: Logger

    Returns:
        DataFrame con esquema canónico
    """
    if not records:
        logger.warning("Sin registros para transformar")
        return pd.DataFrame(columns=COLUMNAS_CANONICAS)

    rows = []
    fechas_invalidas = 0

    for record in records:
        # --- Fecha ---
        fecha_str = extraer_fecha(record)
        if not fecha_str:
            fechas_invalidas += 1
            continue

        # Parsear ISO 8601 (con offset timezone)
        try:
            fecha = pd.to_datetime(fecha_str, utc=True)
        except Exception:
            try:
                # Fallback: parsear sin UTC forzado, luego localizar
                fecha = pd.to_datetime(fecha_str)
                if fecha.tzinfo is None:
                    fecha = fecha.tz_localize(TIMEZONE_LOCAL).tz_convert("UTC")
                else:
                    fecha = fecha.tz_convert("UTC")
            except Exception:
                fechas_invalidas += 1
                continue

        # --- Ubicación ---
        ubicacion = extraer_ubicacion(record)

        # --- Incidencias ---
        incidencias = extraer_incidencias(record)

        # --- Calidad ---
        tipo_datex = record.get("tipo_datex", "")
        calidad = "ok" if tipo_datex else "missing"

        rows.append({
            "fecha": fecha,
            "ubicacion": ubicacion,
            "intensidad": float("nan"),
            "velocidad": float("nan"),
            "incidencias": incidencias,
            "fuente": "dgt",
            "calidad_dato": calidad,
        })

    if fechas_invalidas > 0:
        logger.warning(
            f"Transformación: {fechas_invalidas} registros sin fecha válida descartados"
        )

    logger.info(f"Transformación: {len(rows):,} registros generados")

    return pd.DataFrame(rows)


def filtrar_comunidad_valenciana(
    records: List[dict],
    logger: logging.Logger
) -> List[dict]:
    """
    Filtra las incidencias para conservar solo las de la Comunidad Valenciana.

    Args:
        records: Lista completa de incidencias (toda España)
        logger: Logger

    Returns:
        Lista filtrada solo con incidencias de la CV
    """
    antes = len(records)

    cv_records = [r for r in records if es_comunidad_valenciana(r)]

    despues = len(cv_records)
    filtradas = antes - despues

    logger.info(
        f"Filtro geográfico: {antes:,} → {despues:,} "
        f"({filtradas:,} fuera de Comunidad Valenciana)"
    )

    return cv_records


def extraer_hora(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Extrae la hora (0–23) de la columna 'fecha' UTC.
    """
    if df.empty or "fecha" not in df.columns:
        return df

    df["hora"] = df["fecha"].dt.hour
    logger.debug(f"Columna 'hora' extraída")

    return df


def eliminar_duplicados(
    df: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Elimina filas duplicadas exactas.

    Una misma incidencia puede aparecer en múltiples capturas
    (múltiples archivos dgt_*.json si la incidencia sigue activa).
    Eliminamos duplicados basándonos en todas las columnas excepto
    las de metadata interna.

    Args:
        df: DataFrame con posibles duplicados
        logger: Logger

    Returns:
        DataFrame sin duplicados exactos
    """
    if df.empty:
        return df

    antes = len(df)

    # Duplicados por combinación de fecha + ubicación + incidencias
    # (una misma incidencia en el mismo momento y lugar)
    subset_cols = ["fecha", "ubicacion", "incidencias"]
    # Verificar que las columnas existen
    subset_cols = [c for c in subset_cols if c in df.columns]

    df = df.drop_duplicates(subset=subset_cols).reset_index(drop=True)

    eliminados = antes - len(df)
    if eliminados > 0:
        logger.info(
            f"Deduplicación: {eliminados:,} duplicados eliminados "
            f"({antes:,} → {len(df):,})"
        )
    else:
        logger.info("Deduplicación: sin duplicados encontrados")

    return df


# ==============================================================================
# GUARDADO Y RESUMEN
# ==============================================================================

def guardar_resultados(
    df: pd.DataFrame,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Guarda el dataset limpio en CSV (UTF-8, sin índice).

    Args:
        df: DataFrame final
        logger: Logger

    Returns:
        Path al CSV guardado o None si error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        size = OUTPUT_CSV.stat().st_size
        size_str = (
            f"{size / (1024*1024):.1f} MB" if size >= 1024 * 1024
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
    Sigue el mismo estilo que Fase 5.1 y 5.2.
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("RESUMEN FINAL - TRÁFICO NORMALIZADO")
    logger.info("=" * 70)

    if df.empty:
        logger.warning("Dataset vacío, sin estadísticas que mostrar.")
        return

    logger.info(f"  Total registros:     {len(df):,}")
    logger.info(
        f"  Rango temporal:      {df['fecha'].min()} → {df['fecha'].max()}")
    logger.info(f"  Ubicaciones únicas:  {df['ubicacion'].nunique()}")

    # Desglose por calidad
    logger.info("")
    logger.info("  Calidad de datos:")
    calidad = df["calidad_dato"].value_counts()
    for cat, n in calidad.items():
        pct = n / len(df) * 100
        logger.info(f"    {cat:>8}: {n:>10,} ({pct:.1f}%)")

    # Desglose por tipo de incidencia (top 10)
    logger.info("")
    logger.info("  Top 10 tipos de incidencia:")
    if "incidencias" in df.columns:
        # Extraer solo el tipo_datex (primera parte antes del primer |)
        tipos = df["incidencias"].str.split(" | ", expand=True)
        if 0 in tipos.columns:
            top_tipos = tipos[0].value_counts().head(10)
            for tipo, count in top_tipos.items():
                pct = count / len(df) * 100
                logger.info(f"    {tipo:>40}: {count:>6,} ({pct:.1f}%)")

    # Desglose por severidad
    logger.info("")
    logger.info("  Desglose por severidad:")
    if "incidencias" in df.columns:
        # Extraer severidad (segunda parte)
        if tipos.shape[1] >= 2 and 1 in tipos.columns:
            sev_counts = tipos[1].value_counts()
            for sev, count in sev_counts.items():
                if pd.notna(sev):
                    pct = count / len(df) * 100
                    logger.info(f"    {sev:>20}: {count:>6,} ({pct:.1f}%)")

    # Top 10 ubicaciones con más incidencias
    logger.info("")
    logger.info("  Top 10 ubicaciones con más incidencias:")
    top_ubic = df["ubicacion"].value_counts().head(10)
    for ubic, count in top_ubic.items():
        logger.info(f"    {ubic[:50]:>50}: {count:>6,}")

    # Distribución horaria
    logger.info("")
    logger.info("  Distribución horaria (top 5 horas con más incidencias):")
    if "hora" in df.columns:
        hora_counts = df["hora"].value_counts().sort_index()
        top_horas = hora_counts.nlargest(5)
        for hora, count in top_horas.items():
            pct = count / len(df) * 100
            logger.info(f"    {hora:02d}:00 UTC: {count:>6,} ({pct:.1f}%)")

    # Nota sobre intensidad/velocidad
    logger.info("")
    logger.info(
        "  NOTA: Las columnas 'intensidad' y 'velocidad' contienen NaN."
    )
    logger.info(
        "  El endpoint SituationPublication de DGT solo reporta incidencias."
    )
    logger.info(
        "  Los datos de flujo requieren TrafficStatus o MeasuredDataPublication."
    )

    logger.info("=" * 70)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta el pipeline completo de procesamiento de tráfico.

    Flujo:
        1. Cargar todos los JSON de DGT
        2. Filtrar solo Comunidad Valenciana
        3. Transformar a DataFrame tabular
        4. Extraer columna hora
        5. Eliminar duplicados
        6. Aplicar esquema canónico final
        7. Guardar CSV
        8. Imprimir resumen
    """
    logger = setup_logging()

    logger.info("=" * 70)
    logger.info("FASE 5.3: PROCESAMIENTO DE DATOS DE TRÁFICO")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")
    logger.info("")

    # ------------------------------------------------------------------
    # PASO 1: Cargar datos de todos los JSON
    # ------------------------------------------------------------------
    logger.info("─" * 40)
    logger.info("PASO 1: Carga de datos DGT")
    logger.info("─" * 40)

    all_records = cargar_dgt(logger)

    if not all_records:
        logger.error(
            "No se encontraron incidencias en ningún archivo. Abortando.")
        print("\n❌ ERROR: Sin datos de entrada. Verifica las rutas.")
        return

    # ------------------------------------------------------------------
    # PASO 2: Filtrar solo Comunidad Valenciana
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 2: Filtro geográfico → Comunidad Valenciana")
    logger.info("─" * 40)

    cv_records = filtrar_comunidad_valenciana(all_records, logger)

    if not cv_records:
        logger.warning(
            "Sin incidencias en la Comunidad Valenciana. "
            "Esto puede ocurrir si las capturas no cubren la región. "
            "Continuando con dataset vacío."
        )
        # No abortamos: generamos un CSV vacío con el esquema correcto
        df = pd.DataFrame(columns=COLUMNAS_CANONICAS)
    else:
        # ------------------------------------------------------------------
        # PASO 3: Transformar a DataFrame
        # ------------------------------------------------------------------
        logger.info("")
        logger.info("─" * 40)
        logger.info("PASO 3: Transformación a DataFrame")
        logger.info("─" * 40)

        df = records_a_dataframe(cv_records, logger)

    if df.empty:
        logger.warning("DataFrame vacío tras transformación.")
        # Continuar para generar CSV con esquema correcto

    # ------------------------------------------------------------------
    # PASO 4: Extraer hora
    # ------------------------------------------------------------------
    if not df.empty:
        logger.info("")
        logger.info("─" * 40)
        logger.info("PASO 4: Extracción de hora")
        logger.info("─" * 40)

        df = extraer_hora(df, logger)

    # ------------------------------------------------------------------
    # PASO 5: Eliminar duplicados
    # ------------------------------------------------------------------
    if not df.empty:
        logger.info("")
        logger.info("─" * 40)
        logger.info("PASO 5: Deduplicación")
        logger.info("─" * 40)

        df = eliminar_duplicados(df, logger)

    # ------------------------------------------------------------------
    # PASO 6: Esquema canónico final
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 6: Esquema canónico final")
    logger.info("─" * 40)

    if not df.empty:
        # Ordenar por fecha
        df = df.sort_values("fecha").reset_index(drop=True)

        # Asegurar que todas las columnas canónicas existen
        for col in COLUMNAS_CANONICAS:
            if col not in df.columns:
                if col in ("intensidad", "velocidad"):
                    df[col] = float("nan")
                elif col == "hora":
                    df[col] = df["fecha"].dt.hour if "fecha" in df.columns else 0
                else:
                    df[col] = ""

        # Seleccionar y ordenar columnas
        df = df[COLUMNAS_CANONICAS].copy()

        # Asegurar tipos
        df["hora"] = pd.to_numeric(
            df["hora"], errors="coerce").fillna(0).astype(int)
        df["intensidad"] = df["intensidad"].astype(float)
        df["velocidad"] = df["velocidad"].astype(float)
        df["fuente"] = df["fuente"].astype(str)
        df["calidad_dato"] = df["calidad_dato"].astype(str)

        logger.info(f"Registros finales: {len(df):,}")
        logger.info(f"Columnas: {list(df.columns)}")
        logger.info(f"Dtypes:\n{df.dtypes}")
    else:
        logger.info("Dataset vacío — se generará CSV con cabeceras solamente")

    # ------------------------------------------------------------------
    # PASO 7: Guardar
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 7: Guardado")
    logger.info("─" * 40)

    csv_path = guardar_resultados(df, logger)

    # ------------------------------------------------------------------
    # PASO 8: Resumen
    # ------------------------------------------------------------------
    if not df.empty:
        imprimir_resumen(df, logger)

    # Mensaje final para consola
    if csv_path:
        if df.empty:
            print(f"\n⚠️ CSV generado vacío (solo cabeceras): {csv_path}")
        else:
            print(f"\n✅ PROCESAMIENTO COMPLETO: {len(df):,} incidencias CV")
            print(f"   → CSV: {csv_path}")
    else:
        print(f"\n⚠️ Procesamiento completado pero sin CSV. Revisa logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
