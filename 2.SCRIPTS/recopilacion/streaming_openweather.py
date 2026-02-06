# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.2: Captura de Datos en Tiempo Real - Meteorología (OpenWeatherMap)
==============================================================================

Descripción:
    Este script captura datos meteorológicos en TIEMPO REAL desde la API
    de OpenWeatherMap para la ciudad de Valencia (España).
    
    Realiza DOS peticiones por ejecución:
    1) /weather  → Condiciones meteorológicas actuales
    2) /forecast → Pronóstico cada 3 horas (próximos 5 días)
    
    Los datos se guardan en JSON SIN TRANSFORMAR (raw), exactamente como
    los devuelve la API, añadiendo únicamente metadatos de captura.

Fuente de datos:
    URL base: https://api.openweathermap.org/data/2.5/
    Método:   GET
    Formato:  JSON
    Auth:     API Key (plan gratuito: 1000 llamadas/día)
    Registro: https://openweathermap.org/api

Endpoints utilizados:
    - /weather?lat=39.4699&lon=-0.3763  → Meteorología actual
    - /forecast?lat=39.4699&lon=-0.3763 → Pronóstico 5 días / 3h

    NO se usa One Call API (requiere suscripción de pago).

Parámetros comunes:
    - units=metric  → Temperaturas en °C, viento en m/s
    - lang=es       → Descripciones en español

Uso:
    1. Añade OPENWEATHER_API_KEY en .env
    2. python streaming_openweather.py
    
Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/meteorologia/openweather_YYYYMMDD_HHMMSS.json
    - Datos RAW sin transformar + metadatos de captura

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_openweather.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import os
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from dotenv import load_dotenv
import sys

# Cargar variables de entorno
load_dotenv()

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "meteorologia"
LOG_DIR = PROJECT_ROOT / "logs"

# API OpenWeatherMap
OWM_BASE_URL = "https://api.openweathermap.org/data/2.5"
OWM_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Coordenadas de Valencia (España)
VALENCIA_LAT = 39.4699
VALENCIA_LON = -0.3763

# Parámetros comunes para todas las peticiones
OWM_COMMON_PARAMS = {
    "lat": VALENCIA_LAT,
    "lon": VALENCIA_LON,
    "units": "metric",   # °C, m/s, mm
    "lang": "es",        # Descripciones en español
}

# Endpoints a consultar (nombre interno → ruta de la API)
ENDPOINTS = {
    "actual": "/weather",
    "pronostico": "/forecast",
}

# Configuración de peticiones HTTP
REQUEST_TIMEOUT = 30
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

    log_file = LOG_DIR / "streaming_openweather.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_OpenWeather")
    logger.setLevel(logging.DEBUG)
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

def fetch_endpoint(
    endpoint_name: str,
    endpoint_path: str,
    logger: logging.Logger
) -> Optional[Any]:
    """
    Realiza una petición GET a un endpoint de OpenWeatherMap.
    
    Construye la URL completa con los parámetros comunes (coordenadas,
    unidades, idioma) y la API Key desde .env.
    
    Args:
        endpoint_name: Nombre descriptivo del endpoint ("actual", "pronostico")
        endpoint_path: Ruta del endpoint ("/weather", "/forecast")
        logger: Logger para registrar eventos
    
    Returns:
        Datos JSON de la respuesta (dict) o None si hay error
    """
    url = f"{OWM_BASE_URL}{endpoint_path}"
    
    # Construir parámetros: comunes + API key
    params = {**OWM_COMMON_PARAMS, "appid": OWM_API_KEY}
    
    logger.debug(f"Solicitando {endpoint_name}: {url}")

    try:
        response = requests.get(
            url,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT
        )

        # --- Manejo de códigos HTTP específicos ---
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Endpoint '{endpoint_name}': respuesta OK")
            return data

        elif response.status_code == 401:
            logger.error(
                f"Endpoint '{endpoint_name}': API Key inválida o expirada "
                f"(HTTP 401). Verifica OPENWEATHER_API_KEY en tu .env"
            )
            return None

        elif response.status_code == 429:
            logger.error(
                f"Endpoint '{endpoint_name}': rate limit alcanzado (HTTP 429). "
                f"Plan gratuito permite 1000 llamadas/día. "
                f"Espera o reduce la frecuencia de ejecución."
            )
            return None

        elif response.status_code == 404:
            logger.warning(
                f"Endpoint '{endpoint_name}': recurso no encontrado (HTTP 404). "
                f"¿Endpoint correcto? → {endpoint_path}"
            )
            return None

        elif response.status_code >= 500:
            logger.error(
                f"Endpoint '{endpoint_name}': error del servidor OpenWeatherMap "
                f"(HTTP {response.status_code}). Reintentar más tarde."
            )
            return None

        else:
            logger.warning(
                f"Endpoint '{endpoint_name}': respuesta inesperada "
                f"(HTTP {response.status_code})"
            )
            return None

    except requests.exceptions.Timeout:
        logger.error(
            f"Endpoint '{endpoint_name}': timeout después de "
            f"{REQUEST_TIMEOUT}s. ¿Problemas de red?"
        )
        return None

    except requests.exceptions.ConnectionError:
        logger.error(
            f"Endpoint '{endpoint_name}': error de conexión. "
            f"Verifica tu conexión a internet."
        )
        return None

    except requests.exceptions.JSONDecodeError:
        logger.error(
            f"Endpoint '{endpoint_name}': la respuesta no es JSON válido. "
            f"Posible mantenimiento del servidor."
        )
        return None

    except requests.exceptions.RequestException as e:
        logger.error(
            f"Endpoint '{endpoint_name}': error inesperado: {e}"
        )
        return None


def capture_all_endpoints(
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Captura datos de TODOS los endpoints configurados.
    
    Consulta /weather (actual) y /forecast (pronóstico 5 días),
    guardando ambas respuestas RAW en un único JSON con metadatos.
    
    Args:
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con estructura:
        {
            "_metadata": { timestamp, fuente, endpoints, ... },
            "weather": { ... datos raw de /weather ... },
            "forecast": { ... datos raw de /forecast ... }
        }
    """
    capture_timestamp = datetime.now()

    logger.info(f"Iniciando captura de {len(ENDPOINTS)} endpoints...")
    logger.info(f"  Coordenadas: lat={VALENCIA_LAT}, lon={VALENCIA_LON}")

    # Estructura del archivo de captura
    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.2 - Streaming OpenWeatherMap",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "OpenWeatherMap API (plan gratuito)",
            "url_base": OWM_BASE_URL,
            "coordenadas": {
                "lat": VALENCIA_LAT,
                "lon": VALENCIA_LON,
                "ciudad": "Valencia, España"
            },
            "parametros": {
                "units": "metric",
                "lang": "es"
            },
            "endpoints_solicitados": len(ENDPOINTS),
            "endpoints_exitosos": 0,
            "endpoints_fallidos": 0,
        },
        "weather": None,
        "forecast": None,
    }

    exitosos = 0
    fallidos = 0

    for name, path in ENDPOINTS.items():
        logger.info(f"  Capturando: {name} ({path})")

        data = fetch_endpoint(name, path, logger)

        if data is not None:
            captured_data[name] = data  # JSON RAW sin modificar
            exitosos += 1
            logger.info(f"  ✔ {name}: captura exitosa")
        else:
            captured_data[name] = None
            fallidos += 1
            logger.warning(f"  ✘ {name}: captura fallida")

    captured_data["_metadata"]["endpoints_exitosos"] = exitosos
    captured_data["_metadata"]["endpoints_fallidos"] = fallidos

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
    
    Formato nombre: openweather_YYYYMMDD_HHMMSS.json
    
    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos
    
    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"openweather_{timestamp_str}.json"
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
    """
    Función principal que orquesta la captura meteorológica.
    
    Flujo:
    1. Verifica que la API Key esté configurada
    2. Captura /weather y /forecast
    3. Guarda el resultado en JSON
    4. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: Meteorología (OpenWeatherMap)")
    logger.info("=" * 70)

    # Verificar API Key
    if not OWM_API_KEY:
        logger.error("OPENWEATHER_API_KEY no encontrada en .env")
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║  CONFIGURACIÓN REQUERIDA                                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  1. Regístrate en: https://openweathermap.org/api                            ║
║  2. Copia tu API Key (plan gratuito: 1000 llamadas/día)                      ║
║  3. Añade a tu archivo .env:                                                 ║
║     OPENWEATHER_API_KEY=tu_clave_aqui                                        ║
║  4. Vuelve a ejecutar este script                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
        return

    logger.info("API Key configurada correctamente")

    # Capturar datos
    captured_data = capture_all_endpoints(logger)

    meta = captured_data["_metadata"]
    exitosos = meta["endpoints_exitosos"]
    fallidos = meta["endpoints_fallidos"]
    total = meta["endpoints_solicitados"]

    # Verificar si se obtuvo al menos algún dato
    if exitosos == 0:
        logger.error(
            "No se pudieron capturar datos de ningún endpoint. "
            "Verifica tu API Key, conexión a internet o el estado del servidor."
        )
        print("\n❌ CAPTURA FALLIDA: sin datos de ningún endpoint.")
        return

    # Guardar datos en JSON
    output_path = save_capture(captured_data, logger)

    if output_path is None:
        print("\n❌ ERROR: no se pudo guardar el archivo.")
        return

    # Resumen final
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Endpoints exitosos: {exitosos}/{total}")
    if fallidos > 0:
        logger.info(f"  Endpoints fallidos: {fallidos}/{total}")
    logger.info(f"  Archivo: {output_path.name}")
    logger.info(f"  Ubicación: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {meta['timestamp_captura']}")

    # Mostrar resumen rápido de datos capturados
    if captured_data.get("weather"):
        weather = captured_data["weather"]
        temp = weather.get("main", {}).get("temp", "N/A")
        humidity = weather.get("main", {}).get("humidity", "N/A")
        desc = weather.get("weather", [{}])[0].get("description", "N/A")
        logger.info(f"  --- Condiciones actuales ---")
        logger.info(f"  Temperatura: {temp}°C | Humedad: {humidity}% | {desc}")

    if captured_data.get("forecast"):
        forecast = captured_data["forecast"]
        entries = forecast.get("cnt", 0)
        logger.info(f"  --- Pronóstico ---")
        logger.info(f"  Entradas de pronóstico: {entries} (cada 3h, ~5 días)")

    logger.info("")

    # Mensaje claro en consola
    if fallidos == 0:
        print(f"\n✅ CAPTURA CORRECTA: {exitosos}/{total} endpoints → {output_path.name}")
    else:
        print(f"\n⚠️  CAPTURA PARCIAL: {exitosos}/{total} endpoints → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
