# -*- coding: utf-8 -*-
"""
Script de ejecucion rapida de los tests del dashboard.

Uso:
    python tests/run_tests.py            # todos los tests
    python tests/run_tests.py -v         # verbose
    python tests/run_tests.py -k calidad # solo tests que contengan 'calidad'
"""

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    tests_dir = Path(__file__).resolve().parent
    # Argumentos base: directorio de tests + failfast + color
    args = [str(tests_dir), "--tb=short", "--color=yes"]
    # Reenviar cualquier argumento extra pasado al script
    args += sys.argv[1:]
    sys.exit(pytest.main(args))
