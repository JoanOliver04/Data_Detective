# -*- coding: utf-8 -*-
"""
Tests del pronostico estadistico (5.DASHBOARD/utils/pronostico_estadistico.py).

Portados desde el bloque __main__ y ampliados. Se fija la semilla de numpy
para que los datos sinteticos sean deterministas.
"""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from utils.pronostico_estadistico import (
    _calcular_tendencia,
    _extraer_medias_rt,
    _pronosticar_variable,
    obtener_distribucion_mensual,
    pronosticar_contaminacion_manana,
)


@pytest.fixture(autouse=True)
def _seed():
    np.random.seed(42)


def _df_sintetico(valor_base=25.0, ruido=5.0, anios=range(2020, 2026), mes=4):
    registros = []
    for anio in anios:
        for dia in range(1, 29):
            dt = datetime(anio, mes, dia)
            registros.append({
                "valor": valor_base + np.random.normal(0, ruido),
                "anio": anio,
                "mes": mes,
                "dia_semana": dt.weekday(),
                "fecha_utc": pd.Timestamp(dt, tz="UTC"),
            })
    df = pd.DataFrame(registros)
    df["variable"] = "NO2"
    return df


def test_calcular_tendencia_a_la_baja():
    registros = []
    for anio in range(2018, 2026):
        for _ in range(20):
            registros.append({
                "valor": 30.0 - (anio - 2018) * 1.5 + np.random.normal(0, 2),
                "anio": anio, "mes": 4, "dia_semana": 2,
            })
    df = pd.DataFrame(registros)
    tendencia, factor = _calcular_tendencia(df, "NO2", 4)
    assert factor < 1.0
    assert "baja" in tendencia.lower()


def test_calcular_tendencia_insuficiente():
    df = pd.DataFrame({"valor": [10.0, 11.0], "anio": [2024, 2025], "mes": [4, 4]})
    tendencia, factor = _calcular_tendencia(df, "NO2", 4)
    assert factor == 1.0


def test_extraer_medias_rt():
    rt = {"estaciones": [
        {"no2": 20.0, "o3": 40.0, "pm10": None, "pm25": 10.0},
        {"no2": 30.0, "o3": 50.0, "pm10": 25.0, "pm25": 15.0},
    ]}
    medias = _extraer_medias_rt(rt)
    assert medias["NO2"] == 25.0
    assert medias["O3"] == 45.0
    assert medias["PM10"] == 25.0
    assert medias["PM2.5"] == 12.5
    assert _extraer_medias_rt(None) == {}


def test_pronosticar_variable_sintetico():
    df = _df_sintetico()
    r = _pronosticar_variable(df, "NO2", 4, 2, None)
    assert r is not None
    assert 10 < r["prediccion"] < 50
    assert r["confianza"] in ("Alta", "Media", "Baja")
    assert r["rango_min"] <= r["prediccion"] <= r["rango_max"] + 5


def test_pronosticar_con_rt_sube_prediccion():
    df = _df_sintetico(valor_base=20.0, ruido=0.0)
    r1 = _pronosticar_variable(df, "NO2", 4, 2, None)
    r2 = _pronosticar_variable(df, "NO2", 4, 2, 40.0)
    assert r2["prediccion"] > r1["prediccion"]


def test_pronosticar_contaminacion_manana_end_to_end():
    df = _df_sintetico()
    objetivo = datetime(2026, 4, 15)
    res = pronosticar_contaminacion_manana(df, contam_rt=None, fecha_objetivo=objetivo)
    assert res is not None
    assert "NO2" in res
    assert res["NO2"]["prediccion"] > 0


def test_pronosticar_sin_datos():
    assert pronosticar_contaminacion_manana(None) is None
    assert pronosticar_contaminacion_manana(pd.DataFrame()) is None


def test_distribucion_mensual():
    df = _df_sintetico()
    serie = obtener_distribucion_mensual(df, "NO2", 4)
    assert serie is not None and len(serie) > 0
    assert obtener_distribucion_mensual(df, "NO2", 12) is None


def test_pronosticar_variable_fallback_mensual():
    # Muchos registros en el mes pero pocos en el weekday objetivo ->
    # confianza Baja con metodo "media mensual".
    registros = []
    for dia in range(1, 25):  # abril 2025, varios dias
        dt = datetime(2025, 4, dia)
        registros.append({
            "valor": 20.0, "anio": 2025, "mes": 4,
            "dia_semana": dt.weekday(),
            "fecha_utc": pd.Timestamp(dt, tz="UTC"),
        })
    df = pd.DataFrame(registros)
    df["variable"] = "NO2"
    # weekday objetivo 6 (domingo): pocos en el mes -> usa fallback mensual.
    r = _pronosticar_variable(df, "NO2", 4, 6, None)
    assert r is not None
    assert r["confianza"] == "Baja"
    assert "mensual" in r["metodo"]


def test_pronosticar_variable_sin_datos_mes():
    df = pd.DataFrame({
        "valor": [10.0], "anio": [2025], "mes": [1], "dia_semana": [2],
        "fecha_utc": pd.to_datetime(["2025-01-08"]).tz_localize("UTC"),
        "variable": ["NO2"],
    })
    # Mes objetivo 7 sin datos -> None.
    assert _pronosticar_variable(df, "NO2", 7, 2, None) is None
