# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.6: Script Maestro de Streaming (Orquestador)
==============================================================================

Descripción:
    Script maestro que orquesta la ejecución secuencial de los 4 scripts
    de captura de datos en tiempo real del proyecto Data Detective.
    
    Orden de ejecución:
    ───────────────────
    1. streaming_aqicn.py       → Calidad del aire (AQICN/WAQI)
    2. streaming_openweather.py → Meteorología (OpenWeatherMap)
    3. scraping_avamet.py       → Precipitaciones (AVAMET)
    4. streaming_dgt.py         → Tráfico (DGT DATEX II v3.6)
    
    Cada script se ejecuta de forma independiente. Si uno falla,
    los siguientes se ejecutan igualmente. Se aplican reintentos
    automáticos con espera progresiva ante errores de red.

Diseño:
    - Ejecución SECUENCIAL (sin threading ni multiprocessing)
    - Cada módulo aislado en su propio try/except
    - Reintentos: máx 3 intentos con backoff progresivo (5s, 10s, 20s)
    - Logging centralizado en logs/streaming.log
    - Compatible con Windows Task Scheduler

Uso manual:
    python 2.SCRIPTS/recopilacion/streaming_master.py

Uso con Task Scheduler:
    Programa: python
    Argumentos: 2.SCRIPTS\recopilacion\streaming_master.py
    Iniciar en: <raíz del proyecto>

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_master.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import logging
import time
import importlib
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Raíz del proyecto (2 niveles arriba desde 2.SCRIPTS/recopilacion/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "2.SCRIPTS" / "recopilacion"
LOG_DIR = PROJECT_ROOT / "logs"

# Asegurar que el directorio de scripts está en sys.path para imports
# (necesario para que importlib encuentre los módulos hermanos)
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Definición de los módulos a ejecutar (en orden)
# Cada entrada: (nombre_modulo, descripcion, nombre_funcion_main)
STREAMING_MODULES = [
    {
        "module": "streaming_aqicn",
        "name": "Calidad del Aire (AQICN)",
        "fase": "3.1",
    },
    {
        "module": "streaming_openweather",
        "name": "Meteorología (OpenWeatherMap)",
        "fase": "3.2",
    },
    {
        "module": "scraping_avamet",
        "name": "Precipitaciones (AVAMET)",
        "fase": "3.3",
    },
    {
        "module": "streaming_dgt",
        "name": "Tráfico (DGT DATEX II)",
        "fase": "3.4",
    },
]

# Configuración de reintentos
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 20]  # Segundos de espera progresiva entre reintentos

# Errores que justifican un reintento (problemas de red transitorios)
NETWORK_ERROR_KEYWORDS = [
    "connectionerror",
    "timeout",
    "connection",
    "timed out",
    "temporary failure",
    "name resolution",
    "unreachable",
    "reset by peer",
    "broken pipe",
    "429",        # Rate limit
    "503",        # Service unavailable
    "502",        # Bad gateway
    "504",        # Gateway timeout
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING CENTRALIZADO
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging centralizado del orquestador.
    
    Log único para todas las ejecuciones del master en:
    logs/streaming.log
    
    También muestra mensajes INFO en consola para seguimiento
    manual o desde Task Scheduler.
    
    Returns:
        logging.Logger: Logger centralizado del orquestador
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "streaming.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("StreamingMaster")
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
# FUNCIONES DE EJECUCIÓN
# ==============================================================================

def is_network_error(error: Exception) -> bool:
    """
    Determina si un error es de red y justifica un reintento.
    
    Analiza el tipo de excepción y el mensaje de error para
    identificar problemas transitorios de conectividad.
    
    Args:
        error: La excepción capturada
    
    Returns:
        True si es un error de red que podría resolverse reintentando
    """
    error_str = str(error).lower()
    error_type = type(error).__name__.lower()

    # Comprobar tipo de excepción
    if error_type in ("connectionerror", "timeout", "readtimeout", "connecttimeout"):
        return True

    # Comprobar mensaje de error
    for keyword in NETWORK_ERROR_KEYWORDS:
        if keyword in error_str or keyword in error_type:
            return True

    return False


def run_module(
    module_info: Dict[str, str],
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Ejecuta un módulo de streaming con reintentos ante errores de red.
    
    Flujo:
    1. Importa el módulo dinámicamente con importlib
    2. Ejecuta su función main()
    3. Si falla por red → reintenta hasta MAX_RETRIES con backoff
    4. Si falla por otro motivo → registra error y continúa
    
    Args:
        module_info: Diccionario con module, name, fase
        logger: Logger centralizado
    
    Returns:
        Diccionario con resultado de la ejecución:
        - modulo, nombre, fase
        - estado: "exitoso", "fallido", "error_import"
        - intentos: número de intentos realizados
        - error: mensaje de error (si falló)
        - duracion_segundos: tiempo de ejecución
    """
    mod_name = module_info["module"]
    mod_display = module_info["name"]
    mod_fase = module_info["fase"]

    result = {
        "modulo": mod_name,
        "nombre": mod_display,
        "fase": mod_fase,
        "estado": "pendiente",
        "intentos": 0,
        "error": None,
        "duracion_segundos": 0,
    }

    # Importar módulo
    logger.info(f"[{mod_fase}] Cargando módulo: {mod_name}")

    try:
        # Forzar reimportación limpia en cada ejecución del master
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        module = importlib.import_module(mod_name)
    except ImportError as e:
        logger.error(
            f"[{mod_fase}] ERROR IMPORT: no se pudo cargar '{mod_name}': {e}"
        )
        result["estado"] = "error_import"
        result["error"] = f"ImportError: {e}"
        return result
    except Exception as e:
        logger.error(
            f"[{mod_fase}] ERROR cargando '{mod_name}': "
            f"{type(e).__name__}: {e}"
        )
        result["estado"] = "error_import"
        result["error"] = f"{type(e).__name__}: {e}"
        return result

    # Verificar que el módulo tiene función main()
    if not hasattr(module, "main"):
        logger.error(
            f"[{mod_fase}] '{mod_name}' no tiene función main(). "
            f"No se puede ejecutar."
        )
        result["estado"] = "error_import"
        result["error"] = "Módulo sin función main()"
        return result

    # Ejecutar con reintentos
    for attempt in range(1, MAX_RETRIES + 1):
        result["intentos"] = attempt
        start_time = time.time()

        try:
            logger.info(
                f"[{mod_fase}] Ejecutando {mod_display}"
                + (f" (intento {attempt}/{MAX_RETRIES})" if attempt > 1 else "")
            )

            module.main()

            elapsed = time.time() - start_time
            result["duracion_segundos"] = round(elapsed, 2)
            result["estado"] = "exitoso"

            logger.info(
                f"[{mod_fase}] ✔ {mod_display} completado en {elapsed:.1f}s"
            )
            return result

        except Exception as e:
            elapsed = time.time() - start_time
            result["duracion_segundos"] = round(elapsed, 2)
            error_msg = f"{type(e).__name__}: {e}"

            if is_network_error(e) and attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt - 1]
                logger.warning(
                    f"[{mod_fase}] Error de red en {mod_display}: {error_msg}. "
                    f"Reintentando en {delay}s "
                    f"(intento {attempt}/{MAX_RETRIES})..."
                )
                time.sleep(delay)
                continue
            else:
                if is_network_error(e):
                    logger.error(
                        f"[{mod_fase}] ✖ {mod_display} FALLIDO tras "
                        f"{MAX_RETRIES} intentos. Último error: {error_msg}"
                    )
                else:
                    logger.error(
                        f"[{mod_fase}] ✖ {mod_display} FALLIDO "
                        f"(error no recuperable): {error_msg}"
                    )

                result["estado"] = "fallido"
                result["error"] = error_msg
                return result

    return result


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal del orquestador de streaming.
    
    Ejecuta secuencialmente los 4 módulos de captura, registra
    resultados y genera un resumen final con el estado de cada uno.
    """
    logger = setup_logging()

    start_total = time.time()
    timestamp_inicio = datetime.now()

    logger.info("=" * 70)
    logger.info("STREAMING MASTER - Captura de Datos en Tiempo Real")
    logger.info(f"Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Módulos a ejecutar: {len(STREAMING_MODULES)}")
    logger.info("=" * 70)

    # Ejecutar cada módulo secuencialmente
    results: List[Dict[str, Any]] = []

    for i, module_info in enumerate(STREAMING_MODULES, 1):
        logger.info("")
        logger.info(f"── Módulo {i}/{len(STREAMING_MODULES)} ──")

        result = run_module(module_info, logger)
        results.append(result)

        logger.info("")

    # Calcular estadísticas
    elapsed_total = time.time() - start_total
    exitosos = sum(1 for r in results if r["estado"] == "exitoso")
    fallidos = sum(1 for r in results if r["estado"] == "fallido")
    errores_import = sum(1 for r in results if r["estado"] == "error_import")

    # Resumen final
    logger.info("=" * 70)
    logger.info("RESUMEN DE EJECUCIÓN")
    logger.info("=" * 70)
    logger.info(f"  Tiempo total: {elapsed_total:.1f}s")
    logger.info(
        f"  Resultados: {exitosos} exitosos, "
        f"{fallidos} fallidos, {errores_import} errores de import"
    )
    logger.info("")

    for r in results:
        if r["estado"] == "exitoso":
            icon = "✅"
        elif r["estado"] == "fallido":
            icon = "❌"
        else:
            icon = "⛔"

        line = (
            f"  {icon} [{r['fase']}] {r['nombre']}: "
            f"{r['estado']} ({r['duracion_segundos']}s"
            + (f", {r['intentos']} intentos" if r["intentos"] > 1 else "")
            + ")"
        )
        logger.info(line)

        if r["error"]:
            logger.info(f"       Error: {r['error']}")

    logger.info("")
    logger.info(
        f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
        f"({elapsed_total:.1f}s total)"
    )
    logger.info("=" * 70)


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
