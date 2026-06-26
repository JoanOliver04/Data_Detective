# -*- coding: utf-8 -*-
"""
Tests de la utilidad de exportacion (5.DASHBOARD/utils/exportador.py).

Cubren:
  - Sanitizacion frente a inyeccion de formulas en CSV/Excel.
  - Que las columnas numericas no se degradan.
  - Generacion de bytes XLSX validos (firma ZIP).
  - BOM UTF-8 en CSV.
"""

import io

import pandas as pd
import pytest

from utils.exportador import (
    _sanitizar_celda_inyeccion,
    dataframe_a_csv,
    dataframe_a_excel,
    dataframe_a_xml,
)

# ---------------------------------------------------------------------------
# Sanitizacion de celdas
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "entrada,esperado",
    [
        ("=SUM(A1:A2)", "'=SUM(A1:A2)"),
        ("+1+1", "'+1+1"),
        ("-cmd|' /C calc'!A0", "'-cmd|' /C calc'!A0"),
        ("@SUM(1)", "'@SUM(1)"),
        ("\tTAB", "'\tTAB"),
        ("texto normal", "texto normal"),
        ("", ""),
        ("NO2", "NO2"),
    ],
)
def test_sanitizar_celda_inyeccion(entrada, esperado):
    assert _sanitizar_celda_inyeccion(entrada) == esperado


def test_sanitizar_no_toca_numeros():
    # Un numero negativo real no es str -> no debe modificarse.
    assert _sanitizar_celda_inyeccion(-5) == -5
    assert _sanitizar_celda_inyeccion(3.14) == 3.14
    assert _sanitizar_celda_inyeccion(None) is None


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def test_csv_protege_inyeccion_por_defecto():
    df = pd.DataFrame({"texto": ["=HYPERLINK('http://x')"], "valor": [10]})
    csv = dataframe_a_csv(df).decode("utf-8-sig")
    # La celda de formula debe quedar precedida por apostrofo.
    assert "'=HYPERLINK" in csv
    # La columna numerica se mantiene intacta.
    assert "10" in csv


def test_csv_bom_utf8():
    df = pd.DataFrame({"ciudad": ["València"]})
    raw = dataframe_a_csv(df)
    assert raw.startswith(b"\xef\xbb\xbf")
    assert "València" in raw.decode("utf-8-sig")


def test_csv_inyeccion_desactivable():
    df = pd.DataFrame({"texto": ["=1+1"]})
    csv = dataframe_a_csv(df, proteger_inyeccion=False).decode("utf-8-sig")
    assert "'=1+1" not in csv
    assert "=1+1" in csv


def test_csv_numeros_no_se_degradan():
    df = pd.DataFrame({"valor": [-5, -10, 3]})
    csv = dataframe_a_csv(df).decode("utf-8-sig")
    # Columna numerica: los negativos NO deben llevar apostrofo.
    assert "'-5" not in csv
    assert "-5" in csv


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------


def test_excel_genera_zip_valido():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    raw = dataframe_a_excel(df)
    # Un .xlsx es un contenedor ZIP -> firma 'PK'.
    assert raw[:2] == b"PK"
    # Reabrible por pandas.
    df2 = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    assert list(df2.columns) == ["a", "b"]
    assert len(df2) == 2


def test_excel_sanea_formula():
    df = pd.DataFrame({"texto": ["=1+1"]})
    raw = dataframe_a_excel(df)
    df2 = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    assert df2["texto"].iloc[0] == "'=1+1"


def test_excel_datetime_tz_naive():
    df = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2026-01-01 10:00"], utc=True),
            "v": [1],
        }
    )
    # No debe lanzar por timezone-aware datetimes.
    raw = dataframe_a_excel(df)
    assert raw[:2] == b"PK"


# ---------------------------------------------------------------------------
# XML (regresion: sigue funcionando)
# ---------------------------------------------------------------------------


def test_xml_basico():
    df = pd.DataFrame({"PM2.5": [12.0], "fecha utc": ["2026-01-01"]})
    raw = dataframe_a_xml(df)
    txt = raw.decode("utf-8")
    assert "<dataset>" in txt
    # Nombres de columna saneados a tags XML validos.
    assert "<PM2_5>" in txt
    assert "<fecha_utc>" in txt
