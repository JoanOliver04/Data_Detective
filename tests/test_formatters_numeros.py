# -*- coding: utf-8 -*-
"""Tests de los formateadores numericos de utils/formatters.py."""

from utils.formatters import (
    color_impacto,
    formato_concentracion,
    formato_numero,
    formato_porcentaje,
)


def test_formato_numero():
    assert formato_numero(None) == "-"
    assert formato_numero(950) == "950.0"
    assert formato_numero(1500) == "1.5K"
    assert formato_numero(2_500_000) == "2.5M"
    assert formato_numero(1234.5, decimales=0) == "1K"


def test_formato_porcentaje():
    assert formato_porcentaje(None) == "-"
    assert formato_porcentaje(12.34) == "+12.3%"
    assert formato_porcentaje(-5.0) == "-5.0%"
    assert formato_porcentaje(7.0, con_signo=False) == "7.0%"


def test_formato_concentracion():
    assert formato_concentracion(None) == "-"
    assert formato_concentracion(12.0) == "12.0 µg/m³"
    assert formato_concentracion(3.5, unidad="mg/m³") == "3.5 mg/m³"


def test_color_impacto():
    assert color_impacto(None) == "#7f7f7f"
    assert color_impacto(30) == "#d62728"     # > 20
    assert color_impacto(10) == "#ff7f0e"     # > 5
    assert color_impacto(0) == "#ffbb33"      # > -5
    assert color_impacto(-20) == "#2ca02c"    # <= -5
