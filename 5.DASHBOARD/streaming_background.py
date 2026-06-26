# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Módulo: Hilo de streaming en background para el dashboard
==============================================================================

Lanza un threading.Thread(daemon=True) que ejecuta el ciclo de captura
de datos en tiempo real (AQICN, OpenWeather, AVAMET, DGT, VLCi, eventos)
cada INTERVALO_SEGUNDOS.  El thread muere automáticamente cuando el
proceso Streamlit termina.

Reutiliza run_module() y STREAMING_MODULES de streaming_master.py
para no duplicar lógica de reintentos ni importación dinámica.

Uso (desde app.py):
    from streaming_background import iniciar_streaming_background
    iniciar_streaming_background()        # idempotente

Ruta: 5.DASHBOARD/streaming_background.py
Autor: Joan | Fecha: 2026
"""

import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("StreamingBG")

# Intervalo entre ciclos completos de captura (segundos)
INTERVALO_SEGUNDOS = 600  # 10 minutos

# Ruta al directorio de scripts de streaming
_SCRIPTS_DIR = (
    Path(__file__).resolve().parent.parent / "2.SCRIPTS" / "recopilacion"
)

# Estado compartido entre el thread y el dashboard (thread-safe via GIL)
estado_streaming = {
    "ultima_ejecucion": None,       # datetime de la última ejecución completa
    "resultado_ultimo_ciclo": None, # lista de dicts con resultado por módulo
    "en_ejecucion": False,          # True mientras un ciclo está corriendo
    "ciclos_completados": 0,
}

# Lock para evitar ciclos concurrentes (por si Streamlit recarga rápido)
_lock = threading.Lock()

# Lock para arrancar el thread de forma atomica. Sin el, dos sesiones de
# Streamlit que llamen a iniciar_streaming_background() a la vez podrian
# pasar ambas la comprobacion de threading.enumerate() y lanzar dos hilos.
_lock_arranque = threading.Lock()


def _ejecutar_ciclo() -> None:
    """Ejecuta un ciclo completo de streaming (los 4 módulos secuenciales)."""
    # Asegurar que streaming_master es importable
    if str(_SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(_SCRIPTS_DIR))

    try:
        # Importamos aquí para no fallar en el import global si falta algo
        import importlib
        if "streaming_master" in sys.modules:
            del sys.modules["streaming_master"]

        # Necesitamos importar desde la ruta del proyecto, no del dashboard
        master_path = _SCRIPTS_DIR / "streaming_master.py"
        spec = importlib.util.spec_from_file_location(
            "streaming_master_bg", str(master_path)
        )
        master = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(master)
    except Exception as e:
        logger.error("[StreamingBG] No se pudo importar streaming_master: %s", e)
        return

    resultados = []
    for i, module_info in enumerate(master.STREAMING_MODULES, 1):
        logger.info(
            "[StreamingBG] Módulo %d/%d: %s",
            i, len(master.STREAMING_MODULES), module_info["name"],
        )
        try:
            result = master.run_module(module_info, logger)
            resultados.append(result)
        except Exception as e:
            logger.error(
                "[StreamingBG] Error ejecutando %s: %s",
                module_info["name"], e,
            )
            resultados.append({
                "modulo": module_info["module"],
                "nombre": module_info["name"],
                "fase": module_info["fase"],
                "estado": "fallido",
                "error": str(e),
            })

    estado_streaming["resultado_ultimo_ciclo"] = resultados
    estado_streaming["ultima_ejecucion"] = datetime.now()
    estado_streaming["ciclos_completados"] += 1

    exitosos = sum(1 for r in resultados if r.get("estado") == "exitoso")
    total = len(resultados)
    logger.info(
        "[StreamingBG] Ciclo #%d completado: %d/%d exitosos.",
        estado_streaming["ciclos_completados"], exitosos, total,
    )


def _loop_streaming() -> None:
    """Bucle infinito que ejecuta ciclos de streaming con pausa entre ellos."""
    logger.info(
        "[StreamingBG] Thread iniciado (intervalo: %ds).", INTERVALO_SEGUNDOS,
    )
    # Primera ejecución inmediata al arrancar
    while True:
        if _lock.acquire(blocking=False):
            try:
                estado_streaming["en_ejecucion"] = True
                _ejecutar_ciclo()
            except Exception as e:
                logger.error("[StreamingBG] Error inesperado en ciclo: %s", e)
            finally:
                estado_streaming["en_ejecucion"] = False
                _lock.release()
        else:
            logger.debug("[StreamingBG] Ciclo anterior aún en curso, saltando.")

        time.sleep(INTERVALO_SEGUNDOS)


def iniciar_streaming_background() -> None:
    """
    Lanza el thread de streaming si no está ya corriendo.

    Idempotente: llamar varias veces es seguro.  El thread es daemon,
    así que muere automáticamente cuando Streamlit cierra el proceso.
    """
    # La comprobacion + arranque debe ser atomica para evitar lanzar dos
    # hilos si dos sesiones entran a la vez.
    with _lock_arranque:
        for t in threading.enumerate():
            if t.name == "DataDetective_StreamingBG" and t.is_alive():
                return  # Ya corriendo

        thread = threading.Thread(
            target=_loop_streaming,
            name="DataDetective_StreamingBG",
            daemon=True,
        )
        thread.start()
        logger.info("[StreamingBG] Thread daemon lanzado: %s", thread.name)
