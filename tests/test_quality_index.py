# -*- coding: utf-8 -*-
"""Tests del indice de calidad del aire (5.DASHBOARD/utils/quality_index.py)."""

import pandas as pd

from utils.quality_index import calcular_indice_calidad, nivel_desde_score

UMBRALES = {"NO2": 10.0, "PM2.5": 5.0, "PM10": 15.0, "O3": 60.0, "SO2": 40.0, "CO": 4000.0}


def test_nivel_desde_score_fronteras():
    assert nivel_desde_score(9.5)[0] == "Excelente"
    assert nivel_desde_score(7.0)[0] == "Bueno"
    assert nivel_desde_score(5.0)[0] == "Aceptable"
    assert nivel_desde_score(3.0)[0] == "Deficiente"
    assert nivel_desde_score(1.0)[0] == "Malo"
    assert nivel_desde_score(0.0)[0] == "Peligroso"


def test_indice_none_y_vacio():
    assert calcular_indice_calidad(None, UMBRALES) is None
    assert calcular_indice_calidad(pd.DataFrame(), UMBRALES) is None


def test_indice_ratio_formula():
    # media == umbral -> ratio 1 -> score 5 para esa variable.
    df = pd.DataFrame({
        "variable": ["NO2", "NO2"],
        "valor": [10.0, 10.0],
        "calidad_dato": ["ok", "ok"],
    })
    res = calcular_indice_calidad(df, UMBRALES)
    assert res is not None
    assert res["score"] == 5.0
    assert res["detalle_variables"]["NO2"]["ratio"] == 1.0


def test_indice_filtra_calidad_y_redistribuye():
    # Solo NO2 valido (PM2.5 marcado sospechoso) -> usa solo NO2.
    df = pd.DataFrame({
        "variable": ["NO2", "PM2.5"],
        "valor": [0.0, 999.0],
        "calidad_dato": ["ok", "sospechoso"],
    })
    res = calcular_indice_calidad(df, UMBRALES)
    assert res is not None
    # NO2 media 0 -> score 10.
    assert res["score"] == 10.0
    assert "PM2.5" not in res["detalle_variables"]


def test_indice_score_acotado():
    # media muy alta -> ratio>2 -> score var 0, global 0.
    df = pd.DataFrame({
        "variable": ["NO2"],
        "valor": [1000.0],
        "calidad_dato": ["ok"],
    })
    res = calcular_indice_calidad(df, UMBRALES)
    assert res["score"] == 0.0
