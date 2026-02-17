# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utilidad: Exportacion de datos en multiples formatos (CSV, JSON, XML)
==============================================================================

Modulo reutilizable que convierte DataFrames de pandas a bytes listos
para descarga via st.download_button(). Soporta CSV, JSON y XML.

Caracteristicas:
  - CSV:  Separador ';' (estandar europeo), encoding UTF-8 con BOM
          para que Excel lo abra correctamente con tildes.
  - JSON: Formato 'records' (lista de objetos), indentado, UTF-8.
  - XML:  Generacion manual con xml.etree (sin dependencias extra).
          Cada fila es un <registro> dentro de <dataset>.

Todas las funciones retornan bytes, listos para st.download_button(data=...).

Ruta: 5.DASHBOARD/utils/exportador.py
Autor: Joan | Fecha: 2026
"""

import io
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger("Exportador")

# BOM UTF-8 para que Excel reconozca tildes/acentos automaticamente
_UTF8_BOM = b"\xef\xbb\xbf"


# ==============================================================================
# CSV
# ==============================================================================

def dataframe_a_csv(
    df: pd.DataFrame,
    separador: str = ";",
    incluir_indice: bool = False,
) -> bytes:
    """
    Convierte un DataFrame a bytes CSV con BOM UTF-8.

    El separador por defecto es ';' porque en Espana/Europa la coma
    es el separador decimal, y Excel interpreta ';' correctamente.

    Args:
        df: DataFrame a exportar.
        separador: Caracter separador de columnas (default ';').
        incluir_indice: Si True, incluye el indice del DataFrame.

    Returns:
        bytes listos para st.download_button(data=...).
    """
    buffer = io.StringIO()
    df.to_csv(
        buffer,
        sep=separador,
        index=incluir_indice,
        encoding="utf-8",
    )
    csv_str = buffer.getvalue()
    logger.debug(f"CSV generado: {len(df)} filas, {len(df.columns)} columnas")
    return _UTF8_BOM + csv_str.encode("utf-8")


# ==============================================================================
# JSON
# ==============================================================================

def dataframe_a_json(
    df: pd.DataFrame,
    metadata: Optional[dict] = None,
) -> bytes:
    """
    Convierte un DataFrame a bytes JSON (formato 'records').

    Estructura del JSON resultante:
    {
        "_metadata": { ... },
        "total_registros": N,
        "columnas": ["col1", "col2", ...],
        "datos": [ {col1: val1, ...}, ... ]
    }

    Args:
        df: DataFrame a exportar.
        metadata: Diccionario opcional con metadatos adicionales.

    Returns:
        bytes listos para st.download_button(data=...).
    """
    # Preparar DataFrame para serializacion JSON
    # (convertir Timestamps a ISO strings, NaN a None)
    df_export = df.copy()
    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = df_export[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    registros = json.loads(
        df_export.to_json(orient="records", date_format="iso", default_handler=str)
    )

    output = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "exportado_en": datetime.now().isoformat(),
            "formato": "JSON (records)",
            **(metadata or {}),
        },
        "total_registros": len(registros),
        "columnas": list(df.columns),
        "datos": registros,
    }

    json_str = json.dumps(output, ensure_ascii=False, indent=2, default=str)
    logger.debug(f"JSON generado: {len(registros)} registros")
    return json_str.encode("utf-8")


# ==============================================================================
# XML
# ==============================================================================

def dataframe_a_xml(
    df: pd.DataFrame,
    nombre_raiz: str = "dataset",
    nombre_fila: str = "registro",
) -> bytes:
    """
    Convierte un DataFrame a bytes XML.

    Estructura:
    <dataset>
        <metadata>
            <proyecto>Data Detective Valencia</proyecto>
            <exportado_en>2026-...</exportado_en>
            <total_registros>N</total_registros>
        </metadata>
        <registro>
            <columna1>valor1</columna1>
            <columna2>valor2</columna2>
        </registro>
        ...
    </dataset>

    Los nombres de columna se sanitizan para ser nombres XML validos:
    - Espacios -> guiones bajos
    - Caracteres especiales (., /) -> guiones bajos
    - Si empieza por numero, se antepone '_'

    Args:
        df: DataFrame a exportar.
        nombre_raiz: Nombre del elemento raiz XML.
        nombre_fila: Nombre de cada elemento fila.

    Returns:
        bytes listos para st.download_button(data=...).
    """
    root = ET.Element(nombre_raiz)

    # Metadata
    meta_elem = ET.SubElement(root, "metadata")
    ET.SubElement(meta_elem, "proyecto").text = "Data Detective Valencia"
    ET.SubElement(meta_elem, "exportado_en").text = datetime.now().isoformat()
    ET.SubElement(meta_elem, "total_registros").text = str(len(df))

    # Datos
    for _, row in df.iterrows():
        fila_elem = ET.SubElement(root, nombre_fila)
        for col in df.columns:
            tag_name = _sanitizar_nombre_xml(col)
            valor = row[col]
            elem = ET.SubElement(fila_elem, tag_name)
            # Convertir valor a string legible
            if pd.isna(valor):
                elem.text = ""
            elif hasattr(valor, "isoformat"):
                elem.text = valor.isoformat()
            else:
                elem.text = str(valor)

    # Serializar a bytes con declaracion XML
    tree = ET.ElementTree(root)
    buffer = io.BytesIO()
    tree.write(buffer, encoding="utf-8", xml_declaration=True)
    xml_bytes = buffer.getvalue()

    logger.debug(f"XML generado: {len(df)} registros")
    return xml_bytes


def _sanitizar_nombre_xml(nombre: str) -> str:
    """
    Convierte un nombre de columna a un nombre XML valido.

    XML no permite espacios, puntos ni barras en nombres de elementos.
    Tampoco permite que empiecen por un numero.

    Ejemplos:
        'PM2.5'       -> 'PM2_5'
        'fecha utc'   -> 'fecha_utc'
        '1er_dato'    -> '_1er_dato'

    Args:
        nombre: Nombre original de la columna.

    Returns:
        Nombre sanitizado valido para XML.
    """
    # Reemplazar caracteres no permitidos por guion bajo
    resultado = nombre.replace(" ", "_").replace(".", "_").replace("/", "_")
    resultado = resultado.replace("-", "_").replace("(", "").replace(")", "")
    # Si empieza por numero, anteponer '_'
    if resultado and resultado[0].isdigit():
        resultado = f"_{resultado}"
    # Eliminar caracteres raros restantes (solo alfanumericos y _)
    resultado = "".join(c if c.isalnum() or c == "_" else "_" for c in resultado)
    return resultado or "campo"


# ==============================================================================
# FUNCION AUXILIAR: GENERAR NOMBRE DE ARCHIVO
# ==============================================================================

def generar_nombre_archivo(
    dataset: str,
    extension: str,
    con_timestamp: bool = True,
) -> str:
    """
    Genera un nombre de archivo descriptivo para la exportacion.

    Formato: data_detective_{dataset}_{YYYYMMDD_HHMM}.{ext}

    Ejemplos:
        generar_nombre_archivo("contaminacion", "csv")
        -> "data_detective_contaminacion_20260217_1430.csv"

    Args:
        dataset: Nombre del dataset (contaminacion, meteorologia, etc.).
        extension: Extension del archivo (csv, json, xml).
        con_timestamp: Si True, anade timestamp al nombre.

    Returns:
        Nombre de archivo como string.
    """
    base = f"data_detective_{dataset}"
    if con_timestamp:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        base = f"{base}_{ts}"
    return f"{base}.{extension}"
