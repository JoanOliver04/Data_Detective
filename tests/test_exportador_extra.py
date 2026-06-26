# -*- coding: utf-8 -*-
"""Tests adicionales del exportador: PDF, JSON con metadata y nombres."""

import io
import json

import pandas as pd

from utils.exportador import (
    dataframe_a_excel,
    dataframe_a_json,
    dataframe_a_pdf,
    generar_nombre_archivo,
)


def test_pdf_firma_y_unicode():
    df = pd.DataFrame({
        "variable": ["PM2.5", "NO₂"],
        "valor µg/m³": [12.0, 30.5],
        "fecha": pd.to_datetime(["2026-01-01", "2026-01-02"]),
    })
    raw = dataframe_a_pdf(df, subtitulo="Contaminación")
    assert raw[:4] == b"%PDF"
    assert len(raw) > 500


def test_pdf_trunca_filas():
    df = pd.DataFrame({"a": range(20)})
    raw = dataframe_a_pdf(df, max_filas=5)
    assert raw[:4] == b"%PDF"


def test_json_metadata_y_estructura():
    df = pd.DataFrame({
        "x": [1, 2],
        "fecha": pd.to_datetime(["2026-01-01", "2026-01-02"]),
    })
    raw = dataframe_a_json(df, metadata={"dataset": "demo"})
    obj = json.loads(raw.decode("utf-8"))
    assert obj["_metadata"]["dataset"] == "demo"
    assert obj["_metadata"]["proyecto"] == "Data Detective Valencia"
    assert obj["total_registros"] == 2
    assert obj["columnas"] == ["x", "fecha"]
    assert len(obj["datos"]) == 2


def test_excel_sanea_nombre_hoja():
    df = pd.DataFrame({"a": [1]})
    # Nombre con caracteres prohibidos y > 31 chars.
    raw = dataframe_a_excel(df, nombre_hoja="Datos/raros:[2026]*?" + "x" * 40)
    df2 = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    assert list(df2.columns) == ["a"]


def test_generar_nombre_archivo():
    sin_ts = generar_nombre_archivo("contaminacion", "csv", con_timestamp=False)
    assert sin_ts == "data_detective_contaminacion.csv"
    con_ts = generar_nombre_archivo("trafico", "xlsx")
    assert con_ts.startswith("data_detective_trafico_")
    assert con_ts.endswith(".xlsx")
