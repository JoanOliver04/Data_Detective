# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.6: Pipeline Maestro de Procesamiento ETL
==============================================================================

Descripción:
    Script maestro que orquesta la ejecución secuencial de los 5 scripts
    de procesamiento ETL del proyecto Data Detective (Fase 5).

    Orden de ejecución:
    ───────────────────
    1. normalizar_contaminacion.py  → Fase 5.1: Normalización contaminación
    2. limpiar_meteorologia.py      → Fase 5.2: Limpieza meteorología
    3. limpiar_trafico.py           → Fase 5.3: Limpieza tráfico
    4. calcular_estadisticas.py     → Fase 5.4: Estadísticas agregadas
    5. correlacion_eventos.py       → Fase 5.5: Correlación eventos

    Cada script se ejecuta de forma independiente. Si uno falla, los
    siguientes se ejecutan igualmente. Se captura el estado y duración
    de cada uno.

    Tras la ejecución, se valida la integridad de los archivos generados:
    existencia, tamaño no vacío y columnas clave esperadas.

Diseño:
    - Ejecución SECUENCIAL (sin threading ni multiprocessing)
    - Cada módulo aislado en su propio try/except
    - Sin reintentos (son procesos locales, no de red)
    - Logging centralizado en logs/pipeline_etl.log
    - Validación de integridad post-ejecución
    - Compatible con Windows Task Scheduler

Uso manual:
    python 2.SCRIPTS/procesamiento/pipeline_etl.py

Uso con Task Scheduler:
    Programa: python
    Argumentos: 2.SCRIPTS\\procesamiento\\pipeline_etl.py
    Iniciar en: <raíz del proyecto>

Ruta esperada del script:
    2.SCRIPTS/procesamiento/pipeline_etl.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import importlib
import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Raíz del proyecto (2 niveles arriba desde 2.SCRIPTS/procesamiento/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "2.SCRIPTS" / "procesamiento"
DATOS_LIMPIOS_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
STATS_DIR = DATOS_LIMPIOS_DIR / "estadisticas"
LOG_DIR = PROJECT_ROOT / "logs"

# Asegurar que el directorio de scripts está en sys.path para imports
# (necesario para que importlib encuentre los módulos hermanos)
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Definición de los módulos a ejecutar (en orden estricto)
# Fases 5.1-5.3 generan datos limpios que 5.4 y 5.5 consumen,
# por eso el orden es crítico.
ETL_MODULES = [
    {
        "module": "normalizar_contaminacion",
        "name": "Normalización Contaminación",
        "fase": "5.1",
    },
    {
        "module": "limpiar_meteorologia",
        "name": "Limpieza Meteorología",
        "fase": "5.2",
    },
    {
        "module": "limpiar_trafico",
        "name": "Limpieza Tráfico",
        "fase": "5.3",
    },
    {
        "module": "calcular_estadisticas",
        "name": "Estadísticas Agregadas",
        "fase": "5.4",
    },
    {
        "module": "correlacion_eventos",
        "name": "Correlación Eventos",
        "fase": "5.5",
    },
]

# Archivos de salida esperados con sus validaciones
# Cada entrada: (ruta_relativa_desde_project_root, columna_clave_esperada)
# columna_clave = None si no se valida estructura interna
EXPECTED_OUTPUTS = [
    {
        "path": DATOS_LIMPIOS_DIR / "contaminacion_normalizada.parquet",
        "description": "Contaminación normalizada (Parquet)",
        "key_column": "fecha_utc",
        "format": "parquet",
    },
    {
        "path": DATOS_LIMPIOS_DIR / "meteorologia_limpio.csv",
        "description": "Meteorología limpia (CSV)",
        "key_column": "precipitacion_mm",
        "format": "csv",
    },
    {
        "path": DATOS_LIMPIOS_DIR / "trafico_limpio.csv",
        "description": "Tráfico limpio (CSV)",
        "key_column": "fecha",
        "format": "csv",
    },
    {
        "path": STATS_DIR / "contaminacion_media_anual_barrio.csv",
        "description": "Estadísticas: contaminación anual por barrio",
        "key_column": None,
        "format": "csv",
    },
    {
        "path": STATS_DIR / "precipitacion_media_mensual.csv",
        "description": "Estadísticas: precipitación mensual",
        "key_column": None,
        "format": "csv",
    },
    {
        "path": STATS_DIR / "tendencias_historicas.csv",
        "description": "Estadísticas: tendencias históricas",
        "key_column": None,
        "format": "csv",
    },
    {
        "path": DATOS_LIMPIOS_DIR / "impacto_eventos.csv",
        "description": "Impacto de eventos (CSV)",
        "key_column": "evento_id",
        "format": "csv",
        "min_rows": 1,
    },
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING CENTRALIZADO
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging centralizado del pipeline ETL.

    Log único para todas las ejecuciones en:
    logs/pipeline_etl.log

    También muestra mensajes INFO en consola para seguimiento
    manual o desde Task Scheduler.

    Returns:
        logging.Logger: Logger centralizado del pipeline
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "pipeline_etl.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("PipelineETL")
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
# EJECUCIÓN DE MÓDULOS
# ==============================================================================

def run_module(
    module_info: Dict[str, str],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Ejecuta un módulo ETL de forma aislada.

    Flujo:
    1. Importa el módulo dinámicamente con importlib
    2. Verifica que tiene función main()
    3. Ejecuta main() dentro de try/except
    4. Registra estado, duración y errores

    A diferencia de streaming_master.py, aquí NO hay reintentos:
    los scripts ETL son procesos locales sin errores de red transitorios.

    Args:
        module_info: Diccionario con module, name, fase
        logger: Logger centralizado

    Returns:
        Diccionario con resultado de la ejecución:
        - modulo, nombre, fase
        - estado: "success", "failed", "import_error"
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
        "estado": "pending",
        "error": None,
        "duracion_segundos": 0.0,
    }

    # ─── Paso 1: Importar módulo ───
    logger.info(f"[{mod_fase}] Cargando módulo: {mod_name}")

    try:
        # Forzar reimportación limpia en cada ejecución del pipeline
        # para evitar estado residual de ejecuciones anteriores
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        module = importlib.import_module(mod_name)
    except ImportError as e:
        logger.error(
            f"[{mod_fase}] ERROR IMPORT: no se pudo cargar '{mod_name}': {e}"
        )
        result["estado"] = "import_error"
        result["error"] = f"ImportError: {e}"
        return result
    except Exception as e:
        logger.error(
            f"[{mod_fase}] ERROR cargando '{mod_name}': "
            f"{type(e).__name__}: {e}"
        )
        result["estado"] = "import_error"
        result["error"] = f"{type(e).__name__}: {e}"
        return result

    # ─── Paso 2: Verificar función main() ───
    if not hasattr(module, "main"):
        logger.error(
            f"[{mod_fase}] '{mod_name}' no tiene función main(). "
            f"No se puede ejecutar."
        )
        result["estado"] = "import_error"
        result["error"] = "Módulo sin función main()"
        return result

    # ─── Paso 3: Ejecutar main() ───
    start_time = time.time()

    try:
        logger.info(f"[{mod_fase}] Ejecutando {mod_display}...")
        module.main()

        elapsed = time.time() - start_time
        result["duracion_segundos"] = round(elapsed, 2)
        result["estado"] = "success"

        logger.info(
            f"[{mod_fase}] ✓ {mod_display} completado en {elapsed:.1f}s"
        )

    except Exception as e:
        elapsed = time.time() - start_time
        result["duracion_segundos"] = round(elapsed, 2)
        result["estado"] = "failed"
        result["error"] = f"{type(e).__name__}: {e}"

        logger.error(
            f"[{mod_fase}] ✘ {mod_display} FALLIDO "
            f"({elapsed:.1f}s): {result['error']}"
        )

    return result


# ==============================================================================
# VALIDACIÓN DE INTEGRIDAD
# ==============================================================================

def validate_outputs(logger: logging.Logger) -> Dict[str, Any]:
    """
    Valida la integridad de los archivos generados por el pipeline.

    Para cada archivo esperado comprueba:
    A) Existencia en disco
    B) Tamaño > 0 bytes
    C) Columnas clave esperadas (si se especificó key_column)
    D) Mínimo de filas (si se especificó min_rows)

    Si algo falla → log WARNING (NUNCA crash).
    Usa pandas solo para las comprobaciones de estructura.

    Args:
        logger: Logger centralizado

    Returns:
        Diccionario resumen:
        - total_checks: número de archivos validados
        - passed: número que pasaron todas las comprobaciones
        - warnings: número con alguna alerta
        - details: lista de resultados por archivo
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("VALIDACIÓN DE INTEGRIDAD")
    logger.info("─" * 40)

    # Importar pandas aquí (lazy) para no forzar su carga si el usuario
    # solo quiere ejecutar los módulos sin validación
    try:
        import pandas as pd
        pandas_available = True
    except ImportError:
        logger.warning(
            "  pandas no disponible → validación limitada (solo existencia)")
        pandas_available = False

    total_checks = len(EXPECTED_OUTPUTS)
    passed = 0
    warnings = 0
    details = []

    for output_spec in EXPECTED_OUTPUTS:
        file_path = output_spec["path"]
        description = output_spec["description"]
        key_column = output_spec.get("key_column")
        fmt = output_spec.get("format", "csv")
        min_rows = output_spec.get("min_rows", 0)

        check_result = {
            "file": file_path.name,
            "description": description,
            "exists": False,
            "non_empty": False,
            "column_ok": None,
            "rows_ok": None,
            "status": "FAIL",
        }

        # ─── A) Existencia ───
        if not file_path.exists():
            logger.warning(f"  ⚠ NO ENCONTRADO: {file_path.name}")
            warnings += 1
            details.append(check_result)
            continue

        check_result["exists"] = True

        # ─── B) Tamaño > 0 ───
        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.warning(f"  ⚠ VACÍO (0 bytes): {file_path.name}")
            warnings += 1
            details.append(check_result)
            continue

        check_result["non_empty"] = True
        size_kb = file_size / 1024

        # ─── C) Columna clave (si pandas disponible y se especificó) ───
        if pandas_available and key_column is not None:
            try:
                if fmt == "parquet":
                    df_check = pd.read_parquet(file_path, columns=[key_column])
                else:
                    # Leer solo cabecera + 5 filas para rapidez
                    df_check = pd.read_csv(file_path, nrows=5)

                if key_column in df_check.columns:
                    check_result["column_ok"] = True
                    logger.debug(
                        f"  ✓ {file_path.name}: columna '{key_column}' presente"
                    )
                else:
                    check_result["column_ok"] = False
                    logger.warning(
                        f"  ⚠ {file_path.name}: falta columna '{key_column}' "
                        f"(columnas encontradas: {list(df_check.columns)[:5]}...)"
                    )
                    warnings += 1
                    details.append(check_result)
                    continue

            except Exception as e:
                logger.warning(
                    f"  ⚠ {file_path.name}: error al leer estructura: {e}"
                )
                check_result["column_ok"] = False
                warnings += 1
                details.append(check_result)
                continue

        # ─── D) Mínimo de filas (si se especificó) ───
        if pandas_available and min_rows > 0:
            try:
                if fmt == "parquet":
                    n_rows = len(pd.read_parquet(file_path))
                else:
                    n_rows = sum(1 for _ in open(
                        file_path, encoding="utf-8")) - 1

                if n_rows >= min_rows:
                    check_result["rows_ok"] = True
                else:
                    check_result["rows_ok"] = False
                    logger.warning(
                        f"  ⚠ {file_path.name}: solo {n_rows} filas "
                        f"(mínimo esperado: {min_rows})"
                    )
                    warnings += 1
                    details.append(check_result)
                    continue

            except Exception as e:
                logger.warning(
                    f"  ⚠ {file_path.name}: error al contar filas: {e}"
                )

        # ─── Todo OK ───
        check_result["status"] = "OK"
        passed += 1
        details.append(check_result)

        logger.info(
            f"  ✓ {file_path.name} ({size_kb:.1f} KB)"
        )

    # Resumen de validación
    logger.info("")
    logger.info(
        f"  Validación: {passed}/{total_checks} archivos OK, "
        f"{warnings} con alertas"
    )

    return {
        "total_checks": total_checks,
        "passed": passed,
        "warnings": warnings,
        "details": details,
    }


# ==============================================================================
# RESUMEN FINAL
# ==============================================================================

def print_summary(
    results: List[Dict[str, Any]],
    validation: Dict[str, Any],
    elapsed_total: float,
    logger: logging.Logger,
) -> None:
    """
    Imprime un resumen tabular del pipeline en consola y log.

    Formato:
    ==================================================
    ETL PIPELINE SUMMARY
    ==================================================
    Module                          Status     Time (s)
    --------------------------------------------------
    normalizar_contaminacion        OK           3.21
    ...
    --------------------------------------------------
    Total time: XX.XX seconds
    Integrity check: PASSED / WARNINGS
    ==================================================

    Args:
        results: Lista de resultados de run_module()
        validation: Resultado de validate_outputs()
        elapsed_total: Tiempo total del pipeline
        logger: Logger centralizado
    """
    # Calcular estadísticas
    n_success = sum(1 for r in results if r["estado"] == "success")
    n_failed = sum(1 for r in results if r["estado"] == "failed")
    n_import_err = sum(1 for r in results if r["estado"] == "import_error")

    # Determinar estado de integridad
    if validation["warnings"] == 0:
        integrity_status = "PASSED"
    else:
        integrity_status = f"WARNINGS ({validation['warnings']})"

    # ─── Construir tabla ───
    # Calcular ancho dinámico para la columna de módulo
    max_name_len = max(len(r["modulo"]) for r in results)
    col_width = max(max_name_len, len("Module")) + 2

    separator = "=" * 58
    line_sep = "-" * 58

    lines = [
        "",
        separator,
        "ETL PIPELINE SUMMARY",
        separator,
        f"{'Module':<{col_width}} {'Status':<14} {'Time (s)':>10}",
        line_sep,
    ]

    for r in results:
        # Mapear estado a etiqueta para consola
        if r["estado"] == "success":
            status_label = "OK"
        elif r["estado"] == "failed":
            status_label = "FAILED"
        else:
            status_label = "IMPORT_ERROR"

        time_str = f"{r['duracion_segundos']:.2f}"
        lines.append(
            f"{r['modulo']:<{col_width}} {status_label:<14} {time_str:>10}"
        )

    lines.append(line_sep)
    lines.append(
        f"Modules: {n_success} OK, {n_failed} failed, "
        f"{n_import_err} import errors"
    )
    lines.append(f"Total time: {elapsed_total:.2f} seconds")
    lines.append(f"Integrity check: {integrity_status}")
    lines.append(separator)

    # Imprimir todo
    for line in lines:
        logger.info(line)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal del pipeline maestro ETL.

    Orquesta secuencialmente los 5 módulos de procesamiento (Fases 5.1-5.5),
    registra resultados, valida la integridad de los archivos generados
    y genera un resumen final con el estado de cada módulo.

    Flujo:
        1. Configurar logging centralizado
        2. Ejecutar cada módulo ETL en orden
        3. Validar archivos de salida
        4. Imprimir resumen
    """
    logger = setup_logging()

    start_total = time.time()
    timestamp_inicio = datetime.now(timezone.utc)

    logger.info("=" * 70)
    logger.info("PIPELINE ETL MAESTRO - Procesamiento de Datos (Fase 5)")
    logger.info(
        f"Inicio (UTC): {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Módulos a ejecutar: {len(ETL_MODULES)}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")
    logger.info("=" * 70)

    # ------------------------------------------------------------------
    # PASO 1: Ejecutar cada módulo secuencialmente
    # ------------------------------------------------------------------
    results: List[Dict[str, Any]] = []

    for i, module_info in enumerate(ETL_MODULES, 1):
        logger.info("")
        logger.info(f"── Módulo {i}/{len(ETL_MODULES)} ──")

        result = run_module(module_info, logger)
        results.append(result)

        logger.info("")

    # ------------------------------------------------------------------
    # PASO 2: Validar integridad de archivos generados
    # ------------------------------------------------------------------
    validation = validate_outputs(logger)

    # ------------------------------------------------------------------
    # PASO 3: Resumen final
    # ------------------------------------------------------------------
    elapsed_total = time.time() - start_total

    print_summary(results, validation, elapsed_total, logger)

    # Mensaje de cierre para el log
    logger.info("")
    logger.info(
        f"Fin (UTC): "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} "
        f"({elapsed_total:.1f}s total)"
    )
    logger.info("=" * 70)

    # ─── Exit code para Task Scheduler ───
    # Si todos los módulos fueron exitosos → exit 0
    # Si alguno falló → exit 1 (pero el pipeline NO se interrumpió)
    n_failed = sum(
        1 for r in results
        if r["estado"] in ("failed", "import_error")
    )
    if n_failed > 0:
        sys.exit(1)


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
