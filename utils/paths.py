# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utils: Sistema de Rutas Portable
==============================================================================
Ruta: utils/paths.py
Autor: Joan | Fecha: 2026

Uso:
    from utils.paths import PROJECT_ROOT, DATOS_LIMPIOS_DIR

get_project_root() busca el archivo .env subiendo por los directorios padres
desde este mismo archivo. El .env actua como marcador de la raiz del proyecto.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Localiza la raiz del proyecto buscando el archivo .env hacia arriba.

    Parte desde la ubicacion de este archivo (utils/paths.py) y sube por
    los directorios padres hasta encontrar un .env. Si no lo encuentra,
    usa Path.cwd() como fallback con aviso de advertencia.

    Returns:
        Path: Ruta absoluta a la raiz del proyecto.
    """
    # Empezar desde el directorio de este archivo (utils/)
    # y subir un nivel para llegar a la raiz del proyecto
    current = Path(__file__).resolve().parent

    # Subir por los directorios padres buscando .env
    for directory in [current, *current.parents]:
        if (directory / ".env").exists():
            logger.debug(f"[paths] PROJECT_ROOT encontrado: {directory}")
            return directory

    # Fallback: directorio de trabajo actual
    fallback = Path.cwd()
    logger.warning(
        f"[paths] No se encontro .env en ningun directorio padre. "
        f"Usando cwd() como raiz: {fallback}"
    )
    return fallback


# ==============================================================================
# CONSTANTES DE RUTAS
# ==============================================================================

PROJECT_ROOT: Path = get_project_root()

DATOS_CRUDOS_DIR: Path = PROJECT_ROOT / "1.DATOS_EN_CRUDO"
DATOS_LIMPIOS_DIR: Path = PROJECT_ROOT / "3.DATOS_LIMPIOS"
VISUALIZACIONES_DIR: Path = PROJECT_ROOT / "4.VISUALIZACIONES"
DASHBOARD_DIR: Path = PROJECT_ROOT / "5.DASHBOARD"
