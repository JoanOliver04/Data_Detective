# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utilidad: Exportacion de datos en multiples formatos (CSV, JSON, XML)
==============================================================================

Modulo reutilizable que convierte DataFrames de pandas a bytes listos
para descarga via st.download_button(). Soporta CSV, JSON, XML y PDF.

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
# PDF
# ==============================================================================

def dataframe_a_pdf(
    df: pd.DataFrame,
    titulo: str = "Data Detective Valencia",
    subtitulo: str = "",
    max_filas: int = 5000,
) -> bytes:
    """
    Convierte un DataFrame a bytes PDF con tabla formateada.

    Genera un documento en formato apaisado con:
      - Cabecera con titulo del proyecto y timestamp
      - Tabla con columnas auto-dimensionadas
      - Pie de pagina con numero de registros

    Si el DataFrame tiene mas de max_filas, se trunca para evitar
    PDFs excesivamente largos, y se indica en el pie de pagina.

    Args:
        df: DataFrame a exportar.
        titulo: Titulo principal del PDF.
        subtitulo: Subtitulo opcional (p.ej. nombre del dataset).
        max_filas: Maximo de filas a incluir (default 5000).

    Returns:
        bytes listos para st.download_button(data=...).
    """
    from fpdf import FPDF

    truncado = len(df) > max_filas
    df_export = df.head(max_filas).copy()

    # Convertir todo a string para renderizar
    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = df_export[col].dt.strftime("%Y-%m-%d %H:%M")
        df_export[col] = df_export[col].astype(str).replace("nan", "")

    # Sanitizar texto: las fuentes estandar de fpdf2 (Helvetica, etc.)
    # no soportan caracteres Unicode extendidos. Reemplazamos los mas
    # comunes del castellano y simbolos tipograficos por equivalentes ASCII.
    def _sanitizar_texto_pdf(texto: str) -> str:
        reemplazos = {
            "\u2014": "-",   # em dash
            "\u2013": "-",   # en dash
            "\u2018": "'",   # left single quote
            "\u2019": "'",   # right single quote
            "\u201c": '"',   # left double quote
            "\u201d": '"',   # right double quote
            "\u2026": "...", # ellipsis
            "\u00b5": "u",   # µ (micro sign)
            "\u00b3": "3",   # ³ (superscript 3)
            "\u2192": "->",  # right arrow
            "\u2264": "<=",  # less-than or equal
            "\u2265": ">=",  # greater-than or equal
        }
        for orig, repl in reemplazos.items():
            texto = texto.replace(orig, repl)
        # Eliminar cualquier caracter restante fuera de latin-1
        return texto.encode("latin-1", errors="replace").decode("latin-1")

    # Sanitizar columnas y datos
    columnas = [_sanitizar_texto_pdf(str(c)) for c in df_export.columns]
    for col in df_export.columns:
        df_export[col] = df_export[col].apply(
            lambda v: _sanitizar_texto_pdf(str(v))
        )

    n_cols = len(columnas)

    # --- Configurar PDF en apaisado ---
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Cabecera
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, _sanitizar_texto_pdf(titulo), new_x="LMARGIN", new_y="NEXT", align="C")

    if subtitulo:
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 6, _sanitizar_texto_pdf(subtitulo), new_x="LMARGIN", new_y="NEXT", align="C")

    pdf.set_font("Helvetica", "", 8)
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf.cell(
        0, 5,
        f"Exportado: {ts}  |  Registros: {len(df):,}",
        new_x="LMARGIN", new_y="NEXT", align="C",
    )
    pdf.ln(4)

    # --- Calcular anchos de columna proporcionales ---
    ancho_pagina = pdf.w - pdf.l_margin - pdf.r_margin

    # Estimar ancho por contenido (header + primeras filas)
    anchos = []
    for i, col in enumerate(columnas):
        max_len = len(col)
        for val in df_export.iloc[:50, i]:
            max_len = max(max_len, len(str(val)))
        anchos.append(min(max_len, 40))

    total_chars = sum(anchos) or 1
    col_widths = [(a / total_chars) * ancho_pagina for a in anchos]

    # --- Cabecera de tabla ---
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(30, 30, 46)
    pdf.set_text_color(220, 220, 220)

    for i, col in enumerate(columnas):
        pdf.cell(col_widths[i], 6, col[:30], border=1, fill=True, align="C")
    pdf.ln()

    # --- Filas de datos ---
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(50, 50, 50)

    for row_idx in range(len(df_export)):
        if row_idx % 2 == 0:
            pdf.set_fill_color(245, 245, 250)
        else:
            pdf.set_fill_color(255, 255, 255)

        for i in range(n_cols):
            valor = str(df_export.iat[row_idx, i])[:40]
            pdf.cell(col_widths[i], 5, valor, border=1, fill=True)
        pdf.ln()

    # --- Pie de pagina ---
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(130, 130, 130)

    pie = f"Data Detective Valencia - {len(df_export):,} registros exportados"
    if truncado:
        pie += f" (de {len(df):,} totales, truncado a {max_filas:,})"
    pdf.cell(0, 5, pie, new_x="LMARGIN", new_y="NEXT", align="C")

    pdf_bytes = pdf.output()
    logger.debug(f"PDF generado: {len(df_export)} filas, {n_cols} columnas")
    return bytes(pdf_bytes)


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
