# -*- coding: utf-8 -*-
"""
Tests del Indice de Calidad Urbana (5.DASHBOARD/utils/urban_quality_index.py).

Portados desde el bloque __main__ del modulo a pytest para que entren en la
suite y en la medicion de cobertura.
"""

import pytest

from utils.urban_quality_index import (
    _nivel_desde_score_urbano,
    _score_humedad,
    _score_lluvia,
    _score_meteorologia,
    _score_temperatura,
    _score_trafico,
    calcular_indice_urbano,
)

# ---------------------------------------------------------------------------
# Sub-scores meteorologicos
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "temp,esperado",
    [
        (20.0, 10.0),
        (18.0, 10.0),
        (25.0, 10.0),
        (0.0, 0.0),
        (45.0, 0.0),
    ],
)
def test_score_temperatura_extremos(temp, esperado):
    assert _score_temperatura(temp) == esperado


def test_score_temperatura_intermedios():
    assert 4.0 < _score_temperatura(9.0) < 6.0
    assert 4.0 < _score_temperatura(35.0) < 6.0


@pytest.mark.parametrize(
    "h,esperado",
    [
        (55.0, 10.0),
        (40.0, 10.0),
        (70.0, 10.0),
        (0.0, 0.0),
        (100.0, 0.0),
    ],
)
def test_score_humedad(h, esperado):
    assert _score_humedad(h) == esperado


@pytest.mark.parametrize(
    "mm,esperado",
    [
        (0.0, 10.0),
        (0.5, 10.0),
        (5.0, 5.0),
        (15.0, 2.0),
    ],
)
def test_score_lluvia(mm, esperado):
    assert _score_lluvia(mm) == esperado


def test_score_meteorologia_ideal_y_none():
    r = _score_meteorologia({"temp": 22.0, "humedad": 55.0})
    assert r is not None and r["score"] == 10.0
    assert _score_meteorologia(None) is None


# ---------------------------------------------------------------------------
# Trafico
# ---------------------------------------------------------------------------


def test_score_trafico_niveles():
    assert _score_trafico(None) is None
    assert _score_trafico({"incidencias": []})["score"] == 10.0
    assert _score_trafico({"incidencias": [{"severidad": "low"}] * 2})["score"] == 7.0
    assert (
        _score_trafico({"incidencias": [{"severidad": "high"}, {"severidad": "low"}]})[
            "score"
        ]
        == 4.0
    )
    assert _score_trafico({"incidencias": [{"severidad": "low"}] * 12})["score"] == 1.0
    assert (
        _score_trafico(
            {"incidencias": [{"severidad": "highest"}, {"severidad": "high"}]}
        )["score"]
        == 1.0
    )


# ---------------------------------------------------------------------------
# Indice global + niveles
# ---------------------------------------------------------------------------


def test_indice_urbano_parcial_y_vacio():
    r = calcular_indice_urbano(trafico_rt={"incidencias": []})
    assert r is not None and r["score"] == 10.0 and r["ejes_disponibles"] == 1

    r2 = calcular_indice_urbano(
        meteo_rt={"temp": 22.0, "humedad": 55.0},
        trafico_rt={"incidencias": []},
    )
    assert r2["ejes_disponibles"] == 2

    assert calcular_indice_urbano() is None


def test_indice_urbano_redistribuye_pesos():
    # Solo meteo (peso base 0.25) debe dar el score del eje, no escalarlo.
    r = calcular_indice_urbano(meteo_rt={"temp": 22.0, "humedad": 55.0})
    assert r["score"] == 10.0
    assert r["ejes_totales"] == 3


@pytest.mark.parametrize(
    "score,nivel",
    [
        (9.0, "Excelente"),
        (7.5, "Bueno"),
        (6.0, "Aceptable"),
        (3.5, "Deficiente"),
        (2.0, "Malo"),
        (0.5, "Crítico"),
    ],
)
def test_niveles_urbano(score, nivel):
    assert _nivel_desde_score_urbano(score)[0] == nivel


def test_indice_urbano_con_contaminacion_rt():
    # Cubre el eje de contaminacion (_score_contaminacion via AQICN RT).
    contam = {
        "estaciones": [
            {"no2": 5.0, "o3": 30.0, "pm10": 7.0, "pm25": 2.0, "so2": 5.0, "co": 100.0},
            {"no2": 6.0, "o3": 28.0, "pm10": 8.0, "pm25": 3.0},
        ]
    }
    r = calcular_indice_urbano(
        contam_rt=contam,
        meteo_rt={"temp": 22.0, "humedad": 55.0},
        trafico_rt={"incidencias": []},
    )
    assert r is not None
    assert "contaminacion" in r["ejes"]
    assert r["ejes_disponibles"] == 3
    assert 0.0 <= r["score"] <= 10.0


def test_indice_urbano_contam_rt_vacio():
    # estaciones vacias -> eje contaminacion ausente.
    r = calcular_indice_urbano(
        contam_rt={"estaciones": []}, trafico_rt={"incidencias": []}
    )
    assert r is not None
    assert "contaminacion" not in r["ejes"]
