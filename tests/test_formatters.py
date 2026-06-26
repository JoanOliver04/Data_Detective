# -*- coding: utf-8 -*-
"""
Tests de utils/formatters.py, en particular el escape de HTML usado para
sanear cadenas de fuentes externas (AQICN, OpenWeather, DGT) antes de
inyectarlas en HTML inline o popups de Folium.
"""

import pytest

from utils.formatters import escape_html


@pytest.mark.parametrize(
    "entrada,esperado",
    [
        ("<script>alert(1)</script>", "&lt;script&gt;alert(1)&lt;/script&gt;"),
        ('a"b', "a&quot;b"),
        ("a'b", "a&#x27;b"),
        ("A & B", "A &amp; B"),
        ("V-30", "V-30"),  # carretera normal, sin cambios
        ("Despejado", "Despejado"),  # descripcion normal
    ],
)
def test_escape_html(entrada, esperado):
    assert escape_html(entrada) == esperado


def test_escape_html_none_y_numeros():
    assert escape_html(None) == ""
    assert escape_html(42) == "42"
    assert escape_html(3.5) == "3.5"


def test_escape_html_neutraliza_onerror():
    # Vector tipico de XSS en atributo de imagen.
    payload = '<img src=x onerror="alert(document.cookie)">'
    out = escape_html(payload)
    assert "<img" not in out
    assert "onerror" in out  # el texto permanece, pero escapado
    assert "&lt;img" in out
