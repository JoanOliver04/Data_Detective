# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.6: Captura de Datos en Tiempo Real - Tráfico Espiras (VLCi)
==============================================================================

Descripción:
    Captura datos de intensidad de tráfico en TIEMPO REAL desde los
    detectores electromagnéticos de espira (bucles inductivos) del
    Ayuntamiento de Valencia.  Las espiras cuentan vehículos/hora (ih)
    y están geolocalizadas con coordenadas GPS en las calles de la ciudad.

    A diferencia del feed DGT (streaming_dgt.py), que reporta INCIDENCIAS
    en carreteras nacionales/autopistas, este dataset proporciona FLUJO
    VEHICULAR real medido por ~270 sensores urbanos.

Fuente de datos:
    URL:       https://geoportal.valencia.es/apps/OpenData/Trafico/tra_espiras_p.json
    Método:    GET (sin autenticación)
    Formato:   GeoJSON (FeatureCollection con puntos)
    Frecuencia: actualización cada hora

Campos por feature (propiedades):
    - idpm: ID del punto de medición
    - ih: intensidad horaria (vehículos/hora)
    - angulo: ángulo de orientación del detector
    - fecha_actualizacion: fecha del último dato (DD/MM/YYYY)
    - hora_actualizacion: hora del último dato (HH:MM:SS o similar)

Uso:
    python streaming_vlci_trafico.py

Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/trafico_vlci/vlci_trafico_YYYYMMDD_HHMMSS.json
    - Datos parseados + metadatos de captura

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_vlci_trafico.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "trafico_vlci"
LOG_DIR = PROJECT_ROOT / "logs"

ESPIRAS_URL = "https://geoportal.valencia.es/apps/OpenData/Trafico/tra_espiras_p.json"

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": (
        "DataDetective/1.0 "
        "(Proyecto académico universitario; Valencia; "
        "captura datos abiertos tráfico espiras municipales)"
    ),
    "Accept": "application/json, */*;q=0.8",
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================


def setup_logging() -> logging.Logger:
    """Configura logging a archivo y consola."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "streaming_vlci_trafico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_VLCi_Trafico")
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


def fetch_espiras(logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Descarga el GeoJSON de espiras de tráfico.

    Args:
        logger: Logger del módulo.

    Returns:
        Dict con el GeoJSON parseado, o None si falla.
    """
    logger.info(f"Descargando GeoJSON: {ESPIRAS_URL}")

    try:
        response = requests.get(
            ESPIRAS_URL, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error HTTP descargando espiras: {e}")
        return None

    try:
        geojson = response.json()
    except ValueError as e:
        logger.error(f"Error parseando JSON de espiras: {e}")
        return None

    features = geojson.get("features")
    if features is None:
        logger.error("GeoJSON sin clave 'features'")
        return None

    logger.info(f"GeoJSON recibido: {len(features)} features")
    return geojson


def extract_sensors(
    geojson: Dict[str, Any],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Extrae y filtra los sensores activos del GeoJSON.

    Args:
        geojson: GeoJSON completo de espiras.
        logger: Logger del módulo.

    Returns:
        Lista de dicts con los datos de cada sensor activo (ih > 0).
    """
    sensores = []

    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        # Extraer coordenadas (GeoJSON: [lon, lat])
        coords = geometry.get("coordinates", [])
        if len(coords) >= 2:
            lon = coords[0]
            lat = coords[1]
        else:
            lon = lat = None

        ih = props.get("ih")

        # Convertir ih a numérico para filtrar
        try:
            ih_num = float(ih) if ih is not None else 0
        except (ValueError, TypeError):
            ih_num = 0

        # Solo sensores activos (ih > 0)
        if ih_num <= 0:
            continue

        sensores.append(
            {
                "idpm": props.get("idpm"),
                "ih": ih_num,
                "angulo": props.get("angulo"),
                "fecha_actualizacion": props.get("fecha_actualizacion"),
                "hora_actualizacion": props.get("hora_actualizacion"),
                "lat": lat,
                "lon": lon,
            }
        )

    return sensores


def build_capture_data(
    sensores: List[Dict[str, Any]],
    total_features: int,
    capture_timestamp: datetime,
) -> Dict[str, Any]:
    """
    Construye el dict de captura con metadatos y sensores.

    Args:
        sensores: Lista de sensores activos extraídos.
        total_features: Número total de features en el GeoJSON.
        capture_timestamp: Momento de la captura.

    Returns:
        Diccionario listo para serializar a JSON.
    """
    # Estadísticas de intensidad
    if sensores:
        ihs = [s["ih"] for s in sensores]
        avg_ih = sum(ihs) / len(ihs)
        max_ih = max(ihs)
        max_sensor = next(s for s in sensores if s["ih"] == max_ih)
    else:
        avg_ih = max_ih = 0
        max_sensor = None

    return {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.6 - Streaming VLCi Tráfico Espiras",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "fuente": "VLCi - Geoportal Ayuntamiento de Valencia",
            "url_fuente": ESPIRAS_URL,
            "total_sensores": total_features,
            "sensores_activos": len(sensores),
            "ih_media": round(avg_ih, 1),
            "ih_max": max_ih,
            "sensor_max_id": max_sensor["idpm"] if max_sensor else None,
        },
        "sensores": sensores,
    }


def save_capture(
    data: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """Guarda los datos capturados en JSON."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"vlci_trafico_{timestamp_str}.json"
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
    """Captura datos de tráfico de espiras del portal VLCi de Valencia."""
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: Tráfico Espiras (VLCi Ayuntamiento)")
    logger.info("=" * 70)

    capture_timestamp = datetime.now()

    # 1. Descargar GeoJSON
    geojson = fetch_espiras(logger)
    if geojson is None:
        logger.error("No se pudieron obtener datos de espiras.")
        return

    total_features = len(geojson.get("features", []))

    # 2. Extraer sensores activos
    sensores = extract_sensors(geojson, logger)

    # 3. Construir y guardar
    data = build_capture_data(sensores, total_features, capture_timestamp)
    output_path = save_capture(data, logger)

    if output_path is None:
        logger.error("No se pudo guardar el archivo.")
        return

    # 4. Resumen final
    meta = data["_metadata"]
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Sensores totales:  {meta['total_sensores']}")
    logger.info(f"  Sensores activos:  {meta['sensores_activos']} (ih > 0)")
    logger.info(f"  Intensidad media:  {meta['ih_media']} veh/h")
    logger.info(
        f"  Intensidad máxima: {meta['ih_max']} veh/h (sensor {meta['sensor_max_id']})"
    )
    logger.info(f"  Archivo: {output_path.name}")
    logger.info(f"  Directorio: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {meta['timestamp_captura']}")
    logger.info("")


if __name__ == "__main__":
    main()
