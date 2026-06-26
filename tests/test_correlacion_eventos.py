# -*- coding: utf-8 -*-
"""
Tests de las funciones puras de la Fase 5.5 (correlacion eventos <-> datos).

Modulo: 2.SCRIPTS/procesamiento/correlacion_eventos.py
(en sys.path via conftest.py).
"""

import logging

import pandas as pd

from correlacion_eventos import (
    _build_baseline_mask,
    _generate_event_id,
    _get_all_event_dates,
    _parse_event_date,
)

_LOG = logging.getLogger("test")


# ---------------------------------------------------------------------------
# Parsing de fechas
# ---------------------------------------------------------------------------


def test_parse_event_date_formatos():
    assert _parse_event_date("2026-03-15") == pd.Timestamp("2026-03-15")
    # dayfirst: 03/04/2026 -> 4 de marzo
    assert _parse_event_date("03/04/2026") == pd.Timestamp("2026-04-03")
    assert _parse_event_date("2026-03-15T20:00:00").year == 2026


def test_parse_event_date_invalida():
    assert _parse_event_date("") is None
    assert _parse_event_date(None) is None
    assert _parse_event_date("no-es-fecha") is None


def test_parse_event_date_quita_tz():
    ts = _parse_event_date("2026-03-15T20:00:00+02:00")
    assert ts is not None
    assert ts.tzinfo is None


# ---------------------------------------------------------------------------
# IDs de evento (deduplicacion)
# ---------------------------------------------------------------------------


def test_generate_event_id_determinista():
    ev = {"nombre": "Fallas", "fecha_inicio": "2026-03-01", "fuente": "visit"}
    id1 = _generate_event_id(ev)
    id2 = _generate_event_id(dict(ev))
    assert id1 == id2
    assert len(id1) == 12


def test_generate_event_id_distinto_por_fecha():
    a = _generate_event_id({"nombre": "X", "fecha_inicio": "2026-03-01"})
    b = _generate_event_id({"nombre": "X", "fecha_inicio": "2026-03-02"})
    assert a != b


# ---------------------------------------------------------------------------
# Conjunto de fechas cubiertas por eventos
# ---------------------------------------------------------------------------


def test_get_all_event_dates():
    eventos = [
        {
            "fecha_inicio": pd.Timestamp("2026-03-01"),
            "fecha_fin": pd.Timestamp("2026-03-03"),
        }
    ]
    fechas = _get_all_event_dates(eventos)
    assert pd.Timestamp("2026-03-01").date() in fechas
    assert pd.Timestamp("2026-03-03").date() in fechas
    assert len(fechas) == 3


# ---------------------------------------------------------------------------
# Mascara de baseline (control estacional + semanal + evento + lluvia)
# ---------------------------------------------------------------------------


def test_build_baseline_mask_criterios():
    # Evento: miercoles 2026-03-04 (marzo, weekday=2).
    evento = {
        "fecha_inicio": pd.Timestamp("2026-03-04"),
        "fecha_fin": pd.Timestamp("2026-03-04"),
    }
    all_event_dates = {pd.Timestamp("2026-03-04").date()}

    # Serie de fechas candidatas: 4 miercoles de marzo + 1 de abril + 1 lunes.
    fechas = pd.Series(
        pd.to_datetime(
            [
                "2026-03-04",  # el propio evento -> excluido
                "2026-03-11",  # miercoles marzo -> valido
                "2026-03-18",  # miercoles marzo -> valido (pero lluvia abajo)
                "2026-04-01",  # miercoles pero ABRIL -> excluido (mes)
                "2026-03-09",  # LUNES marzo -> excluido (weekday)
            ]
        )
    )

    # Meteo: 2026-03-18 con lluvia > 5mm -> excluido.
    meteo = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2026-03-11", "2026-03-18"]),
            "precip_media": [0.0, 12.0],
        }
    )

    mask = _build_baseline_mask(fechas, evento, all_event_dates, meteo, _LOG)
    validas = set(fechas[mask].dt.date)

    assert pd.Timestamp("2026-03-11").date() in validas
    assert pd.Timestamp("2026-03-04").date() not in validas  # evento
    assert pd.Timestamp("2026-04-01").date() not in validas  # otro mes
    assert pd.Timestamp("2026-03-09").date() not in validas  # otro weekday
    assert pd.Timestamp("2026-03-18").date() not in validas  # lluvia >5mm
