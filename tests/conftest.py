# -*- coding: utf-8 -*-
"""
Configuracion de pytest para el proyecto Data Detective.

Este archivo se ejecuta ANTES de cualquier modulo de tests, lo que permite:
  1. Anadir 5.DASHBOARD/ al sys.path para importar config, data_loader, etc.
  2. Sustituir st.cache_data/st.cache_resource por decoradores pass-through,
     de modo que las funciones decoradas funcionen sin un servidor Streamlit.
"""

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "5.DASHBOARD"
PROCESAMIENTO_DIR = REPO_ROOT / "2.SCRIPTS" / "procesamiento"

for _p in (DASHBOARD_DIR, PROCESAMIENTO_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

# ---------------------------------------------------------------------------
# Parche de Streamlit: cache_data y cache_resource como no-ops
# Los decoradores @st.cache_data(ttl=...) se evaluan en el momento en que
# Python importa data_loader.py.  Si este parche se aplica antes de esa
# importacion (conftest.py siempre carga primero), los decoradores no hacen
# nada y las funciones se ejecutan directamente, sin necesitar un servidor.
# ---------------------------------------------------------------------------
import streamlit as st  # noqa: E402  (importacion post-path)


def _noop_cache(**kwargs):
    """Sustituye st.cache_data / st.cache_resource por un decorator nulo."""

    def decorator(func):
        return func

    return decorator


st.cache_data = _noop_cache
st.cache_resource = _noop_cache
