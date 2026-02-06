# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.1: Captura de Datos en Tiempo Real - GVA (Calidad del Aire)
==============================================================================

Descripción:
    Este script captura datos de contaminación en TIEMPO REAL desde los
    sensores públicos de la Generalitat Valenciana (GVA).
    
    Los datos se guardan en JSON SIN TRANSFORMAR (raw), exactamente como
    los devuelve la API, añadiendo únicamente metadatos de captura.

Fuente de datos:
    URL base: https://agroambient.gva.es/auto/estaciones/datos/
    Ejemplo:  https://agroambient.gva.es/auto/estaciones/datos/46250001_dades.json
    Método:   GET (sin autenticación)
    Formato:  JSON

Estaciones de Valencia configuradas:
    - 46250001: València - Centro (Avd. Francia)
    - 46250030: València - Pista de Silla
    - 46250047: València - Politècnic
    - 46250050: València - Molí del Sol
    - 46250054: València - Conselleria Meteo

Uso:
    python streaming_gva.py
    
    Para automatización con Task Scheduler de Windows:
    - Programa: C:\...\env_data_detective\Scripts\python.exe
    - Argumentos: C:\...\2.SCRIPTS\recopilacion\streaming_gva.py
    - Iniciar en: C:\...\Data_Detective\
    - Disparador: Repetir cada 60 minutos (frecuencia de actualización GVA)

Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/contaminacion/gva_YYYYMMDD_HHMMSS.json
    - Datos RAW sin transformar + metadatos de captura

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_gva.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas base (relativas al directorio raíz del proyecto)
# Estructura: Data_Detective/2.SCRIPTS/recopilacion/streaming_gva.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "contaminacion"
LOG_DIR = PROJECT_ROOT / "logs"

# URL base de la API pública de GVA (sensores en tiempo real)
GVA_BASE_URL = "https://agroambient.gva.es/auto/estaciones/datos"

# Estaciones de Valencia a capturar
# Formato: código -> nombre descriptivo
ESTACIONES_VALENCIA = {
    "46250001": "València - Centro (Avd. Francia)",
    "46250030": "València - Pista de Silla",
    "46250047": "València - Politècnic",
    "46250050": "València - Molí del Sol",
    "46250054": "València - Conselleria Meteo",
}

# Configuración de peticiones HTTP
REQUEST_TIMEOUT = 30        # Segundos máximos de espera por petición
REQUEST_HEADERS = {
    "User-Agent": "DataDetective/1.0 (Proyecto académico; Valencia)",
    "Accept": "application/json",
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging para el script.

    Escribe en:
    - Consola: nivel INFO (mensajes de estado)
    - Archivo: nivel DEBUG (detalle completo para debug)

    Returns:
        logging.Logger: Instancia del logger configurado
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "streaming_gva.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_GVA")
    logger.setLevel(logging.DEBUG)

    # Limpiar handlers existentes (evita duplicados en re-ejecuciones)
    logger.handlers.clear()

    # Handler para archivo (detalle completo)
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Handler para consola (resumen)
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
    logger: logging.Logger
) -> Optional[Any]:
    """
    Realiza una petición GET a la API de GVA para obtener datos de una estación.

    La API devuelve un JSON con los datos actuales de los sensores
    de calidad del aire. No requiere autenticación.

    Args:
        station_code: Código de la estación (ej: "46250001")
        logger: Logger para registrar eventos

    Returns:
        Datos JSON de la estación (dict o list) o None si hay error
    """
    url = f"{GVA_BASE_URL}/{station_code}_dades.json"
    logger.debug(f"Solicitando datos: {url}")

    try:
        response = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT
        )

        # Verificar código HTTP
        if response.status_code == 200:
            # Intentar parsear JSON
            data = response.json()
            logger.debug(f"Estación {station_code}: respuesta OK")
            return data

        elif response.status_code == 404:
            logger.warning(
                f"Estación {station_code}: no encontrada (HTTP 404). "
                f"¿Código de estación correcto?"
            )
            return None

        elif response.status_code >= 500:
            logger.error(
                f"Estación {station_code}: error del servidor "
                f"(HTTP {response.status_code})"
            )
            return None

        else:
            logger.warning(
                f"Estación {station_code}: respuesta inesperada "
                f"(HTTP {response.status_code})"
            )
            return None

    except requests.exceptions.Timeout:
        logger.error(
            f"Estación {station_code}: timeout después de "
            f"{REQUEST_TIMEOUT}s. ¿Problemas de red?"
        )
        return None

    except requests.exceptions.ConnectionError:
        logger.error(
            f"Estación {station_code}: error de conexión. "
            f"Verifica tu conexión a internet."
        )
        return None

    except requests.exceptions.JSONDecodeError:
        logger.error(
            f"Estación {station_code}: la respuesta no es JSON válido. "
            f"Posible mantenimiento del servidor."
        )
        return None

    except requests.exceptions.RequestException as e:
        logger.error(
            f"Estación {station_code}: error inesperado en petición: {e}"
        )
        return None


def capture_all_stations(
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Captura datos de TODAS las estaciones configuradas.

    Construye un diccionario con los datos de cada estación y
    añade metadatos de captura (timestamp, versión, etc.).

    Args:
        logger: Logger para registrar eventos

    Returns:
        Diccionario con estructura:
        {
            "_metadata": { timestamp, estaciones_solicitadas, ... },
            "estaciones": {
                "46250001": { ... datos raw ... },
                "46250030": { ... datos raw ... },
                ...
            }
        }
    """
    # Timestamp de captura (hora local de la máquina)
    capture_timestamp = datetime.now()

    logger.info(
        f"Iniciando captura de {len(ESTACIONES_VALENCIA)} estaciones...")

    # Diccionario para almacenar datos de todas las estaciones
    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.1 - Streaming GVA",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "fuente": "GVA - Generalitat Valenciana",
            "url_base": GVA_BASE_URL,
            "estaciones_solicitadas": len(ESTACIONES_VALENCIA),
            "estaciones_exitosas": 0,
            "estaciones_fallidas": 0,
        },
        "estaciones": {}
    }

    exitosas = 0
    fallidas = 0

    for code, name in ESTACIONES_VALENCIA.items():
        logger.info(f"  Capturando: {code} ({name})")

        data = fetch_station_data(code, logger)

        if data is not None:
            # Guardar datos RAW sin modificar, solo añadimos nombre
            captured_data["estaciones"][code] = {
                "nombre": name,
                "datos": data  # ← JSON completo tal como lo devuelve GVA
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

    # Actualizar metadatos con resultado
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
    """
    Guarda los datos capturados en un archivo JSON.

    El archivo se nombra con el timestamp de captura para mantener
    un histórico incremental de capturas dinámicas.

    Formato nombre: gva_YYYYMMDD_HHMMSS.json

    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos

    Returns:
        Path al archivo guardado o None si hay error
    """
    # Crear directorio de salida si no existe
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generar nombre de archivo con timestamp
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"gva_{timestamp_str}.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Calcular tamaño del archivo
        file_size = output_path.stat().st_size
        size_str = f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"

        logger.info(f"✔ Archivo guardado: {filename} ({size_str})")
        logger.debug(f"  Ruta completa: {output_path}")

        return output_path

    except OSError as e:
        logger.error(f"Error escribiendo archivo {filename}: {e}")
        return None
    except TypeError as e:
        logger.error(f"Error serializando datos a JSON: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal que orquesta la captura de datos en tiempo real.

    Flujo:
    1. Configura logging
    2. Captura datos de todas las estaciones
    3. Guarda el resultado en JSON
    4. Muestra resumen en consola
    """
    # Configurar logging
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: GVA - Calidad del Aire")
    logger.info("=" * 70)

    # Capturar datos de todas las estaciones
    captured_data = capture_all_stations(logger)

    # Extraer resumen de metadatos
    meta = captured_data["_metadata"]
    exitosas = meta["estaciones_exitosas"]
    fallidas = meta["estaciones_fallidas"]
    total = meta["estaciones_solicitadas"]

    # Verificar si se obtuvo al menos algún dato
    if exitosas == 0:
        logger.error(
            "No se pudieron capturar datos de ninguna estación. "
            "Verifica tu conexión a internet o el estado del servidor GVA."
        )
        print("\n❌ CAPTURA FALLIDA: sin datos de ninguna estación.")
        return

    # Guardar datos en JSON
    output_path = save_capture(captured_data, logger)

    if output_path is None:
        logger.error("No se pudo guardar el archivo de captura.")
        print("\n❌ ERROR: no se pudo guardar el archivo.")
        return

    # Resumen final
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

    # Mensaje claro en consola
    if fallidas == 0:
        print(
            f"\n✅ CAPTURA CORRECTA: {exitosas}/{total} estaciones → {output_path.name}")
    else:
        print(
            f"\n⚠️  CAPTURA PARCIAL: {exitosas}/{total} estaciones → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
