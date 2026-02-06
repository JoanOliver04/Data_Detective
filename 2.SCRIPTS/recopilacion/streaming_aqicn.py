# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.1: Captura de Datos en Tiempo Real - Calidad del Aire (AQICN alternativa)
==============================================================================

Descripción:
    Este script captura datos de contaminación en TIEMPO REAL desde la API de AQICN/WAQI,
    que agrega datos de las estaciones oficiales de GVA.
    
    Razón del cambio: La API original de GVA (agroambient.gva.es) está inactiva.
    AQICN ofrece datos equivalentes en JSON.

Fuente de datos:
    URL base: https://api.waqi.info/feed/@{uid}/?token={token}
    Método:   GET
    Formato:  JSON
    Token:    Requiere registro gratuito en https://aqicn.org/data-platform/token/

Estaciones de Valencia configuradas (con UID de AQICN):
    - 46250001: València - Centro (Avd. Francia) -> UID 6639
    - 46250030: València - Pista de Silla -> UID 6637
    - 46250047: València - Politècnic -> UID 6640
    - 46250050: València - Molí del Sol -> UID 6638
    - 46250054: València - Conselleria Meteo (Centre) -> UID 373816

Uso:
    1. Añade AQI_TOKEN en .env
    2. python streaming_gva.py
    
Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/contaminacion/aqicn_YYYYMMDD_HHMMSS.json
    - Datos RAW sin transformar + metadatos de captura

Autor: Joan (actualizado por Grok)
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import os
from dotenv import load_dotenv
import sys

# Cargar variables de entorno
load_dotenv()

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "contaminacion"
LOG_DIR = PROJECT_ROOT / "logs"

AQI_BASE_URL = "https://api.waqi.info/feed/@{uid}/"
AQI_TOKEN = os.getenv("AQI_TOKEN")

# Estaciones con UID de AQICN
ESTACIONES_VALENCIA = {
    "46250001": {"name": "València - Centro (Avd. Francia)", "uid": 6639},
    "46250030": {"name": "València - Pista de Silla", "uid": 6637},
    "46250047": {"name": "València - Politècnic", "uid": 6640},
    "46250050": {"name": "València - Molí del Sol", "uid": 6638},
    "46250054": {"name": "València - Conselleria Meteo (Centre)", "uid": 373816},
}

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "DataDetective/1.0 (Proyecto académico; Valencia)",
    "Accept": "application/json",
}

# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "streaming_aqicn.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_AQICN")
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


def fetch_station_data(
    station_code: str,
    uid: int,
    logger: logging.Logger
) -> Optional[Any]:
    if not AQI_TOKEN:
        logger.error(
            "AQI_TOKEN no configurada en .env. Regístrate en https://aqicn.org/data-platform/token/")
        return None

    url = AQI_BASE_URL.format(uid=uid) + f"?token={AQI_TOKEN}"
    logger.debug(f"Solicitando datos: {url}")

    try:
        response = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "ok":
                logger.debug(f"Estación {station_code}: respuesta OK")
                return data["data"]  # Solo los datos útiles
            else:
                logger.warning(
                    f"Estación {station_code}: error en datos ({data.get('data')})")
                return None
        else:
            logger.warning(
                f"Estación {station_code}: HTTP {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Estación {station_code}: error {e}")
        return None


def capture_all_stations(
    logger: logging.Logger
) -> Dict[str, Any]:
    capture_timestamp = datetime.now()

    logger.info(
        f"Iniciando captura de {len(ESTACIONES_VALENCIA)} estaciones...")

    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.1 - Streaming (AQICN alternativa)",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "AQICN/WAQI (agrega datos GVA)",
            "url_base": AQI_BASE_URL,
            "estaciones_solicitadas": len(ESTACIONES_VALENCIA),
            "estaciones_exitosas": 0,
            "estaciones_fallidas": 0,
        },
        "estaciones": {}
    }

    exitosas = 0
    fallidas = 0

    for code, info in ESTACIONES_VALENCIA.items():
        name = info["name"]
        uid = info.get("uid")

        logger.info(f"  Capturando: {code} ({name})")

        if uid is None:
            logger.warning(f"  ✘ {code}: UID no disponible en AQICN")
            captured_data["estaciones"][code] = {
                "nombre": name,
                "datos": None,
                "error": "UID no disponible"
            }
            fallidas += 1
            continue

        data = fetch_station_data(code, uid, logger)

        if data is not None:
            captured_data["estaciones"][code] = {
                "nombre": name,
                "datos": data
            }
            exitosas += 1
            logger.info(f"  ✔ {code}: captura exitosa")
        else:
            captured_data["estaciones"][code] = {
                "nombre": name,
                "datos": None,
                "error": "No se pudieron obtener datos"
            }
            fallidas += 1
            logger.warning(f"  ✘ {code}: captura fallida")

    captured_data["_metadata"]["estaciones_exitosas"] = exitosas
    captured_data["_metadata"]["estaciones_fallidas"] = fallidas

    return captured_data

# ==============================================================================
# FUNCIONES DE GUARDADO
# ==============================================================================


def save_capture(
    data: Dict[str, Any],
    logger: logging.Logger
) -> Optional[Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"aqicn_{timestamp_str}.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        size_str = f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"

        logger.info(f"✔ Archivo guardado: {filename} ({size_str})")
        logger.debug(f"  Ruta completa: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Error guardando {filename}: {e}")
        return None

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================


def main():
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: Calidad del Aire (AQICN)")
    logger.info("=" * 70)

    if not AQI_TOKEN:
        logger.error("AQI_TOKEN no encontrada en .env")
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║  CONFIGURACIÓN REQUERIDA                                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  1. Obtén tu token en: https://aqicn.org/data-platform/token/                ║
║  2. Añade a tu archivo .env:                                                 ║
║     AQI_TOKEN=tu_token_aqui                                                  ║
║  3. Vuelve a ejecutar este script                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
        return

    captured_data = capture_all_stations(logger)

    meta = captured_data["_metadata"]
    exitosas = meta["estaciones_exitosas"]
    fallidas = meta["estaciones_fallidas"]
    total = meta["estaciones_solicitadas"]

    if exitosas == 0:
        logger.error(
            "No se pudieron capturar datos. Verifica token o conexión.")
        print("\n❌ CAPTURA FALLIDA: sin datos.")
        return

    output_path = save_capture(captured_data, logger)

    if output_path is None:
        print("\n❌ ERROR: no se pudo guardar el archivo.")
        return

    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estaciones exitosas: {exitosas}/{total}")
    if fallidas > 0:
        logger.info(f"  Estaciones fallidas: {fallidas}/{total}")
    logger.info(f"  Archivo: {output_path.name}")
    logger.info(f"  Ubicación: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {meta['timestamp_captura']}")
    logger.info("")

    if fallidas == 0:
        print(
            f"\n✅ CAPTURA CORRECTA: {exitosas}/{total} estaciones → {output_path.name}")
    else:
        print(
            f"\n⚠️ CAPTURA PARCIAL: {exitosas}/{total} estaciones → {output_path.name}")


if __name__ == "__main__":
    main()
