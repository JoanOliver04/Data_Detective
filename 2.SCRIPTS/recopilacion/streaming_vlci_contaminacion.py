# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.5: Captura de Datos - Contaminación Municipal (VLCi Ayuntamiento)
==============================================================================

Descripción:
    Captura datos diarios de calidad del aire desde las estaciones
    municipales de Valencia publicados en el portal de datos abiertos
    VLCi (opendata.vlci.valencia.es).  Los CSV se sirven desde el
    geoportal del Ayuntamiento, sin autenticación.

    Actualmente se descarga la estación 1A (Universitat Politècnica).
    El CSV contiene datos diarios desde 2014 con 7 variables de
    contaminación (PM2.5, SO2, NO, NO2, PM10, NOx, Ozono).

Fuente de datos:
    URL:       https://mapas.valencia.es/WebsMunicipales/uploads/atmosferica/1A.csv
    Método:    GET (sin autenticación)
    Formato:   CSV separado por ; con cabeceras en primera fila
    Encoding:  UTF-8
    Frecuencia: actualización diaria

Estaciones disponibles:
    - 1A: Universitat Politècnica (Algirós)
    (Se pueden añadir más estaciones cambiando el ID en la URL)

Uso:
    python streaming_vlci_contaminacion.py

Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/contaminacion_vlci/vlci_contam_YYYYMMDD_HHMMSS.json
    - Datos parseados + metadatos de captura

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_vlci_contaminacion.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "contaminacion_vlci"
LOG_DIR = PROJECT_ROOT / "logs"

# Estaciones municipales de calidad del aire
ESTACIONES = {
    "1A": {
        "name": "Universitat Politècnica (1A)",
        "url": "https://mapas.valencia.es/WebsMunicipales/uploads/atmosferica/1A.csv",
    },
}

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": (
        "DataDetective/1.0 "
        "(Proyecto académico universitario; Valencia; "
        "captura datos abiertos calidad del aire municipal)"
    ),
    "Accept": "text/csv, */*;q=0.8",
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================


def setup_logging() -> logging.Logger:
    """Configura logging a archivo y consola."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "streaming_vlci_contaminacion.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_VLCi_Contam")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ==============================================================================
# FUNCIONES DE CAPTURA
# ==============================================================================


def fetch_csv(
    station_id: str,
    station_info: Dict[str, str],
    logger: logging.Logger,
) -> Optional[pd.DataFrame]:
    """
    Descarga y parsea el CSV de una estación municipal.

    Args:
        station_id: Código de la estación (ej. "1A").
        station_info: Dict con 'name' y 'url'.
        logger: Logger del módulo.

    Returns:
        DataFrame con los datos parseados, o None si falla.
    """
    url = station_info["url"]
    logger.info(f"Descargando CSV estación {station_id}: {url}")

    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error HTTP descargando {station_id}: {e}")
        return None

    content_type = response.headers.get("Content-Type", "")
    logger.debug(f"Content-Type: {content_type}, bytes: {len(response.content)}")

    # Detectar separador: el CSV de Valencia usa ;
    try:
        from io import StringIO

        text = response.text
        first_line = text.split("\n", 1)[0]
        sep = ";" if ";" in first_line else ","
        logger.debug(f"Separador detectado: {sep!r}")

        df = pd.read_csv(StringIO(text), sep=sep)
    except Exception as e:
        logger.error(f"Error parseando CSV de {station_id}: {e}")
        return None

    if df.empty:
        logger.warning(f"CSV de {station_id} está vacío")
        return None

    logger.info(
        f"Estación {station_id}: {len(df)} registros, " f"columnas: {list(df.columns)}"
    )
    return df


def build_capture_data(
    station_id: str,
    station_info: Dict[str, str],
    df: pd.DataFrame,
    capture_timestamp: datetime,
) -> Dict[str, Any]:
    """
    Construye el dict de captura con metadatos y registros.

    Args:
        station_id: Código de la estación.
        station_info: Dict con 'name' y 'url'.
        df: DataFrame parseado del CSV.
        capture_timestamp: Momento de la captura.

    Returns:
        Diccionario listo para serializar a JSON.
    """
    # Convertir DataFrame a lista de dicts (NaN → None para JSON)
    registros = df.where(df.notna(), None).to_dict(orient="records")

    # Detectar rango de fechas si hay columna Fecha
    fecha_min = fecha_max = None
    if "Fecha" in df.columns:
        fechas = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
        fechas_validas = fechas.dropna()
        if not fechas_validas.empty:
            fecha_min = fechas_validas.min().strftime("%Y-%m-%d")
            fecha_max = fechas_validas.max().strftime("%Y-%m-%d")

    variables = [c for c in df.columns if c != "Fecha"]

    return {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.5 - Streaming VLCi Contaminación Municipal",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "fuente": "VLCi - Ayuntamiento de Valencia (Open Data)",
            "url_fuente": station_info["url"],
            "estacion_id": station_id,
            "estacion_nombre": station_info["name"],
            "total_registros": len(registros),
            "variables": variables,
            "fecha_min_datos": fecha_min,
            "fecha_max_datos": fecha_max,
        },
        "registros": registros,
    }


def save_capture(
    data: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """Guarda los datos capturados en JSON."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"vlci_contam_{timestamp_str}.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )
        logger.info(f"✔ Archivo guardado: {filename} ({size_str})")
        return output_path
    except Exception as e:
        logger.error(f"Error guardando {filename}: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================


def main():
    """Captura datos de contaminación del portal VLCi de Valencia."""
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA: Contaminación Municipal (VLCi Ayuntamiento)")
    logger.info("=" * 70)

    capture_timestamp = datetime.now()
    exitosas = 0
    fallidas = 0

    for station_id, station_info in ESTACIONES.items():
        logger.info(f"  Procesando estación: {station_id} ({station_info['name']})")

        df = fetch_csv(station_id, station_info, logger)
        if df is None:
            fallidas += 1
            continue

        data = build_capture_data(station_id, station_info, df, capture_timestamp)
        output_path = save_capture(data, logger)

        if output_path is None:
            fallidas += 1
            continue

        exitosas += 1
        meta = data["_metadata"]
        logger.info(
            f"  Registros: {meta['total_registros']} | "
            f"Variables: {meta['variables']} | "
            f"Periodo: {meta['fecha_min_datos']} → {meta['fecha_max_datos']}"
        )

    # Resumen final
    total = len(ESTACIONES)
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estaciones exitosas: {exitosas}/{total}")
    if fallidas > 0:
        logger.info(f"  Estaciones fallidas: {fallidas}/{total}")
    logger.info(f"  Directorio: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {capture_timestamp.isoformat()}")
    logger.info("")


if __name__ == "__main__":
    main()
