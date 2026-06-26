# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Panel de Exportacion de Datos
==============================================================================

Componente reutilizable que renderiza un panel expandible de exportacion
dentro de cualquier tab del dashboard. Permite al usuario descargar los
datos filtrados en CSV, JSON, XML o PDF.

Uso en cualquier tab:
    from components.exportar import render_panel_exportacion
    render_panel_exportacion(df, nombre_dataset="contaminacion")

Caracteristicas:
  - Panel dentro de st.expander (no ocupa espacio visual hasta que se abre)
  - Muestra preview de los primeros registros antes de descargar
  - Indica el numero de filas/columnas que se van a exportar
  - Diferencia visualmente datos ESTATICOS vs DINAMICOS con un badge
  - Cuatro botones de descarga: CSV (principal), JSON, XML, PDF

Ruta: 5.DASHBOARD/components/exportar.py
Autor: Joan | Fecha: 2026
"""

import logging
from typing import Optional, Dict

import pandas as pd
import streamlit as st

from utils.exportador import (
    dataframe_a_csv,
    dataframe_a_excel,
    dataframe_a_json,
    dataframe_a_xml,
    dataframe_a_pdf,
    generar_nombre_archivo,
)

logger = logging.getLogger("PanelExportacion")


# ==============================================================================
# CONFIGURACION DE DATASETS: metadatos de cada dataset
# ==============================================================================

# Origen de cada dataset: 'estatico', 'dinamico' o 'mixto'
# Esto permite mostrar un badge informativo al usuario
DATASET_INFO: Dict[str, Dict[str, str]] = {
    "contaminacion": {
        "label": "Contaminación atmosférica",
        "origen": "estatico",
        "fuentes": "GVA (Generalitat Valenciana), EEA",
        "descripcion": (
            "Series historicas de calidad del aire (NO2, O3, PM10, PM2.5) "
            "desde estaciones de medicion en Valencia."
        ),
    },
    "meteorologia": {
        "label": "Meteorología / Precipitaciones",
        "origen": "estatico",
        "fuentes": "AEMET OpenData",
        "descripcion": (
            "Registros historicos de precipitacion, temperatura y humedad "
            "desde estaciones meteorologicas."
        ),
    },
    "trafico": {
        "label": "Tráfico",
        "origen": "estatico",
        "fuentes": "DGT (Direccion General de Trafico)",
        "descripcion": (
            "Incidencias de trafico en la red viaria de la " "Comunidad Valenciana."
        ),
    },
    "eventos": {
        "label": "Impacto de eventos masivos",
        "origen": "mixto",
        "fuentes": "Visit Valencia, Ayuntamiento, Valencia CF",
        "descripcion": (
            "Analisis de correlacion entre eventos masivos (Fallas, "
            "partidos, conciertos) y variaciones en contaminacion/trafico."
        ),
    },
    "contam_anual": {
        "label": "Estadísticas anuales de contaminación",
        "origen": "estatico",
        "fuentes": "GVA (procesado ETL)",
        "descripcion": "Medias anuales por barrio y contaminante.",
    },
    "precip_mensual": {
        "label": "Precipitación media mensual",
        "origen": "estatico",
        "fuentes": "AEMET (procesado ETL)",
        "descripcion": "Precipitacion media por mes y anio.",
    },
    "tendencias": {
        "label": "Tendencias históricas",
        "origen": "estatico",
        "fuentes": "Multiples (procesado ETL)",
        "descripcion": "Series de tendencia anual de todas las variables.",
    },
    "pronostico": {
        "label": "Pronóstico meteorológico 72h",
        "origen": "dinamico",
        "fuentes": "OpenWeatherMap API (streaming)",
        "descripcion": (
            "Pronostico de temperatura, humedad y precipitacion "
            "para las proximas 72 horas. Se actualiza periodicamente."
        ),
    },
}


def _badge_origen(origen: str) -> str:
    """
    Retorna HTML de un badge coloreado segun el tipo de dato.

    Colores:
      - estatico  -> azul (dato historico, no cambia)
      - dinamico  -> verde (dato en tiempo real, se actualiza)
      - mixto     -> naranja (combina ambos)

    Args:
        origen: 'estatico', 'dinamico' o 'mixto'.

    Returns:
        String HTML con el badge.
    """
    estilos = {
        "estatico": ("background:#1f4e79", "ESTATICO"),
        "dinamico": ("background:#1a7431", "DINAMICO"),
        "mixto": ("background:#7d5a00", "MIXTO"),
    }
    style, texto = estilos.get(origen, ("background:#555", origen.upper()))
    return (
        f'<span style="{style};color:#fff;padding:2px 8px;'
        f"border-radius:4px;font-size:0.7rem;font-weight:600;"
        f'letter-spacing:0.5px;">{texto}</span>'
    )


def _badge_formato(formato: str) -> str:
    """
    Badge visual para el formato de exportacion.

    Args:
        formato: 'CSV', 'JSON', 'XML' o 'PDF'.

    Returns:
        String HTML con el badge del formato.
    """
    colores = {
        "CSV": "#2e7d32",
        "JSON": "#1565c0",
        "XML": "#6a1b9a",
        "PDF": "#c62828",
    }
    color = colores.get(formato.upper(), "#555")
    return (
        f'<span style="background:{color};color:#fff;padding:1px 6px;'
        f'border-radius:3px;font-size:0.65rem;font-weight:600;">'
        f"{formato.upper()}</span>"
    )


# ==============================================================================
# COMPONENTE PRINCIPAL
# ==============================================================================


def render_panel_exportacion(
    df: Optional[pd.DataFrame],
    nombre_dataset: str,
    metadata_extra: Optional[dict] = None,
) -> None:
    """
    Renderiza un panel expandible de exportacion de datos.

    El panel incluye:
      1. Informacion del dataset (origen, fuentes, badge estatico/dinamico)
      2. Preview de los primeros registros
      3. Cuatro botones de descarga: CSV, JSON, XML, PDF

    Se integra al final de cualquier tab del dashboard:

        render_panel_exportacion(df, "contaminacion")

    Args:
        df: DataFrame a exportar (ya filtrado). Si es None, muestra aviso.
        nombre_dataset: Clave del dataset (debe existir en DATASET_INFO).
        metadata_extra: Metadatos adicionales para incluir en JSON/XML.
    """
    # Obtener info del dataset o usar defaults
    info = DATASET_INFO.get(
        nombre_dataset,
        {
            "label": nombre_dataset.replace("_", " ").title(),
            "origen": "estatico",
            "fuentes": "No especificada",
            "descripcion": "",
        },
    )

    origen = info["origen"]
    badge = _badge_origen(origen)

    with st.expander("Exportar datos", icon=":material/download:"):

        # --- Header con badge de origen ---
        st.markdown(
            f"**{info['label']}** {badge}",
            unsafe_allow_html=True,
        )

        if info.get("descripcion"):
            st.caption(info["descripcion"])

        st.markdown(
            f"<small><b>Fuentes:</b> {info['fuentes']}</small>",
            unsafe_allow_html=True,
        )

        # --- Guard clause: sin datos ---
        if df is None or df.empty:
            st.warning(
                "No hay datos disponibles para exportar con los filtros actuales."
            )
            return

        # --- Info del dataset ---
        n_filas, n_cols = df.shape
        st.markdown(
            f"<small>Registros a exportar: <b>{n_filas:,}</b> filas x "
            f"<b>{n_cols}</b> columnas</small>",
            unsafe_allow_html=True,
        )

        # --- Preview ---
        with st.popover("Vista previa de los datos"):
            st.dataframe(
                df.head(50),
                use_container_width=True,
                height=300,
            )
            if n_filas > 50:
                st.caption(f"Mostrando 50 de {n_filas:,} registros.")

        st.markdown("")  # Espaciado

        # --- Botones de descarga ---
        # Preparar metadata comun para JSON
        metadata_json = {
            "dataset": nombre_dataset,
            "origen_datos": origen,
            "fuentes": info["fuentes"],
            "filas_exportadas": n_filas,
            "columnas": list(df.columns),
        }
        if metadata_extra:
            metadata_json.update(metadata_extra)

        col_csv, col_xlsx, col_json, col_xml, col_pdf = st.columns(5)

        with col_csv:
            csv_bytes = dataframe_a_csv(df)
            st.download_button(
                label="CSV",
                data=csv_bytes,
                file_name=generar_nombre_archivo(nombre_dataset, "csv"),
                mime="text/csv",
                icon=":material/table:",
                use_container_width=True,
                help=(
                    "Descarga en CSV con separador ';' (compatible con "
                    "Excel europeo). Codificación UTF-8 con BOM. "
                    "Protegido frente a inyección de fórmulas."
                ),
            )

        with col_xlsx:
            xlsx_bytes = dataframe_a_excel(
                df, nombre_hoja=info.get("label", nombre_dataset)
            )
            st.download_button(
                label="Excel",
                data=xlsx_bytes,
                file_name=generar_nombre_archivo(nombre_dataset, "xlsx"),
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
                icon=":material/grid_on:",
                use_container_width=True,
                help=(
                    "Descarga en Excel nativo (.xlsx). Preserva tipos y no "
                    "depende del separador regional."
                ),
            )

        with col_json:
            json_bytes = dataframe_a_json(df, metadata=metadata_json)
            st.download_button(
                label="JSON",
                data=json_bytes,
                file_name=generar_nombre_archivo(nombre_dataset, "json"),
                mime="application/json",
                icon=":material/code:",
                use_container_width=True,
                help=(
                    "Descarga en JSON con metadatos del proyecto. "
                    "Formato 'records' (lista de objetos)."
                ),
            )

        with col_xml:
            xml_bytes = dataframe_a_xml(df)
            st.download_button(
                label="XML",
                data=xml_bytes,
                file_name=generar_nombre_archivo(nombre_dataset, "xml"),
                mime="application/xml",
                icon=":material/code_blocks:",
                use_container_width=True,
                help=(
                    "Descarga en XML estructurado. Cada fila es un "
                    "elemento <registro> dentro de <dataset>."
                ),
            )

        with col_pdf:
            label_dataset = info.get("label", nombre_dataset)
            pdf_bytes = dataframe_a_pdf(df, subtitulo=label_dataset)
            st.download_button(
                label="PDF",
                data=pdf_bytes,
                file_name=generar_nombre_archivo(nombre_dataset, "pdf"),
                mime="application/pdf",
                icon=":material/picture_as_pdf:",
                use_container_width=True,
                help=(
                    "Descarga en PDF con tabla formateada. "
                    "Máximo 5.000 filas por documento."
                ),
            )

        # --- Nota sobre tamanio ---
        if n_filas > 100_000:
            st.info(
                f"El dataset tiene {n_filas:,} registros. "
                f"La generacion del archivo puede tardar unos segundos.",
                icon=":material/info:",
            )

        logger.info(
            f"[Exportar] Panel renderizado para '{nombre_dataset}' "
            f"({n_filas:,} filas, origen={origen})"
        )


# ==============================================================================
# COMPONENTE MULTI-DATASET (para exportar varios datasets a la vez)
# ==============================================================================


def render_exportacion_multiple(
    datasets: Dict[str, Optional[pd.DataFrame]],
) -> None:
    """
    Renderiza un panel de exportacion con selector de dataset.

    Util para una seccion global de exportacion donde el usuario
    elige que dataset descargar.

    Args:
        datasets: Diccionario {nombre_dataset: DataFrame}.
                  Las claves deben coincidir con DATASET_INFO.
    """
    # Filtrar datasets disponibles (no None, no vacios)
    disponibles = {k: v for k, v in datasets.items() if v is not None and not v.empty}

    if not disponibles:
        st.warning("No hay datasets disponibles para exportar.")
        return

    # Selector
    opciones = list(disponibles.keys())
    [
        DATASET_INFO.get(k, {}).get("label", k.replace("_", " ").title())
        for k in opciones
    ]

    seleccion = st.selectbox(
        "Dataset a exportar",
        options=opciones,
        format_func=lambda x: DATASET_INFO.get(x, {}).get(
            "label", x.replace("_", " ").title()
        ),
        help="Selecciona el conjunto de datos que quieres descargar.",
    )

    if seleccion:
        render_panel_exportacion(
            df=disponibles[seleccion],
            nombre_dataset=seleccion,
        )
