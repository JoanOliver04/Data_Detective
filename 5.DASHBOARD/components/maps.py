# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.2: Mapa de Contaminacion con Folium
==============================================================================
Componente modular que renderiza un mapa de contaminacion embebido:
  - Prioridad 1: Carga mapa HTML pre-generado (Fase 6.1)
  - Prioridad 2: Genera mapa dinamico desde datos agregados

Ruta: 5.DASHBOARD/components/maps.py
Autor: Joan | Fecha: 2026
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import (
    MAPAS_DIR, MAPA_NO2_HTML, MAPA_PM25_HTML,
    VALENCIA_CENTER_LAT, VALENCIA_CENTER_LON,
    VARIABLE_COLORS, ESTACION_BARRIO_MAP,
    UMBRALES_OMS,
)
from data_loader import leer_html_visualizacion

logger = logging.getLogger("Maps")

# Altura del mapa embebido en el dashboard (px)
MAP_HEIGHT = 500

# Mapeo de variables a sus mapas pre-generados
MAPAS_PREGENERADOS = {
    "NO2": MAPA_NO2_HTML,
    "PM2.5": MAPA_PM25_HTML,
}

# Coordenadas aproximadas de estaciones (para mapa dinamico)
# Fuente: Posiciones reales de las estaciones de la red GVA en Valencia
ESTACION_COORDS = {
    "46250001": {"lat": 39.4561, "lon": -0.3522, "nombre": "Valencia - Pista de Silla"},
    "46250004": {"lat": 39.4600, "lon": -0.3850, "nombre": "Valencia - Viveros"},
    "46250030": {"lat": 39.4580, "lon": -0.3900, "nombre": "Valencia - Moli del Sol"},
    "46250047": {"lat": 39.4830, "lon": -0.3590, "nombre": "Valencia - Benimaclet"},
    "46250050": {"lat": 39.4620, "lon": -0.3920, "nombre": "Valencia - Patraix"},
    "46250054": {"lat": 39.4750, "lon": -0.3760, "nombre": "Valencia - Ciutat Vella"},
}


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def render_mapa_contaminacion(
    df: pd.DataFrame,
    variable: str,
    df_contam_anual: Optional[pd.DataFrame] = None,
) -> None:
    """
    Renderiza el mapa de contaminacion embebido en Streamlit.

    Estrategia:
      1. Intenta cargar el mapa HTML pre-generado (Fase 6.1)
      2. Si no existe, genera un mapa dinamico con Folium desde los datos

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable seleccionada (NO2, O3, PM10, PM2.5...).
        df_contam_anual: (Opcional) DataFrame con estadisticas anuales por barrio.
    """
    st.markdown(
        '<div class="section-header">'
        '<h4>Mapa de calidad del aire</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Distribucion espacial por estaciones de medicion'
        '</p></div>',
        unsafe_allow_html=True,
    )

    # --- Estrategia 1: Mapa pre-generado ---
    ruta_mapa = MAPAS_PREGENERADOS.get(variable)
    if ruta_mapa and ruta_mapa.exists():
        html_content = leer_html_visualizacion(str(ruta_mapa))
        if html_content:
            st.caption(
                f"Mapa pre-generado: `{ruta_mapa.name}` "
                f"(regenerar con generar_mapas.py)"
            )
            components.html(html_content, height=MAP_HEIGHT, scrolling=False)
            return

    # --- Estrategia 2: Mapa dinamico ---
    logger.info(
        f"[Mapa] No hay mapa pre-generado para {variable}, "
        f"generando mapa dinamico..."
    )
    _generar_mapa_dinamico(df, variable)


# ==============================================================================
# MAPA DINAMICO (FALLBACK)
# ==============================================================================

def _generar_mapa_dinamico(df: pd.DataFrame, variable: str) -> None:
    """
    Genera un mapa Folium dinamico con CircleMarkers por estacion.

    Cada marcador muestra:
      - Nombre de la estacion y barrio
      - Media de la variable
      - Numero de registros
      - Color proporcional al nivel de contaminacion

    Args:
        df: DataFrame filtrado.
        variable: Variable seleccionada.
    """
    try:
        import folium
    except ImportError:
        st.error(
            "Folium no esta instalado. "
            "Ejecuta: pip install folium"
        )
        return

    if df is None or df.empty:
        st.info("Sin datos para generar el mapa dinamico.")
        return

    # Filtrar por variable y calidad
    mask = (df["variable"] == variable)
    if "calidad_dato" in df.columns:
        mask = mask & (df["calidad_dato"] == "ok")
    df_var = df[mask].copy()

    if df_var.empty:
        st.info(f"Sin registros validos de {variable} para el mapa.")
        return

    # Agregar por estacion
    estacion_stats = (
        df_var
        .groupby("estacion_id", as_index=False)
        .agg(
            media=("valor", "mean"),
            registros=("valor", "count"),
            mediana=("valor", "median"),
        )
    )

    if estacion_stats.empty:
        st.info("Sin estaciones con datos para mapear.")
        return

    # Asignar barrio
    estacion_stats["barrio"] = (
        estacion_stats["estacion_id"].map(ESTACION_BARRIO_MAP)
    )

    # --- Crear mapa base ---
    m = folium.Map(
        location=[VALENCIA_CENTER_LAT, VALENCIA_CENTER_LON],
        zoom_start=13,
        tiles="CartoDB dark_matter",
    )

    # --- Color scale basada en umbral OMS ---
    umbral_oms = UMBRALES_OMS.get(variable, 40)
    color_base = VARIABLE_COLORS.get(variable, "#1f77b4")

    for _, row in estacion_stats.iterrows():
        est_id = str(row["estacion_id"])
        coords = ESTACION_COORDS.get(est_id)
        if coords is None:
            logger.debug(f"Sin coordenadas para estacion {est_id}")
            continue

        media = row["media"]
        registros = int(row["registros"])
        barrio = row["barrio"] or "Desconocido"

        # Color segun ratio con umbral OMS
        ratio = media / umbral_oms if umbral_oms else 0.5
        if ratio > 1.5:
            color = "#d62728"   # Rojo: muy por encima
            nivel = "CRITICO"
        elif ratio > 1.0:
            color = "#ff7f0e"   # Naranja: por encima
            nivel = "ALTO"
        elif ratio > 0.75:
            color = "#ffbb33"   # Amarillo: moderado
            nivel = "MODERADO"
        else:
            color = "#2ca02c"   # Verde: bueno
            nivel = "BUENO"

        # Radio proporcional al valor (min 8, max 25)
        radio = max(8, min(25, int(media / umbral_oms * 15)))

        # Popup con informacion detallada
        nombre_est = coords.get("nombre", est_id)
        popup_html = (
            f"<div style='font-family:Arial,sans-serif;min-width:180px;'>"
            f"<b>{nombre_est}</b><br>"
            f"<small>Barrio: {barrio}</small><hr style='margin:4px 0;'>"
            f"<b>{variable}</b>: {media:.1f} ug/m3<br>"
            f"Mediana: {row['mediana']:.1f} ug/m3<br>"
            f"Registros: {registros:,}<br>"
            f"Nivel: <span style='color:{color};font-weight:bold;'>"
            f"{nivel}</span><br>"
            f"<small>Umbral OMS: {umbral_oms} ug/m3</small>"
            f"</div>"
        )

        tooltip_text = f"{barrio} | {variable}: {media:.1f} ug/m3"

        folium.CircleMarker(
            location=[coords["lat"], coords["lon"]],
            radius=radio,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            weight=2,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=tooltip_text,
        ).add_to(m)

    # --- Leyenda manual ---
    leyenda_html = _crear_leyenda_html(variable, umbral_oms)
    m.get_root().html.add_child(folium.Element(leyenda_html))

    # --- Renderizar en Streamlit ---
    html_str = m._repr_html_()
    st.caption(
        f"Mapa generado dinamicamente | {len(estacion_stats)} estaciones | "
        f"Variable: {variable}"
    )
    components.html(html_str, height=MAP_HEIGHT, scrolling=False)

    logger.info(
        f"[Mapa dinamico] {variable}: "
        f"{len(estacion_stats)} estaciones renderizadas"
    )


# ==============================================================================
# LEYENDA HTML PARA FOLIUM
# ==============================================================================

def _crear_leyenda_html(variable: str, umbral_oms: float) -> str:
    """
    Genera HTML para una leyenda flotante en el mapa.

    Args:
        variable: Nombre de la variable.
        umbral_oms: Valor del umbral OMS.

    Returns:
        String HTML con la leyenda.
    """
    return f"""
    <div style="
        position: fixed;
        bottom: 30px; left: 30px;
        z-index: 1000;
        background: rgba(30,30,46,0.92);
        padding: 10px 14px;
        border-radius: 8px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        color: #ccc;
        border: 1px solid #555;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.4);
    ">
        <b>{variable} - Nivel</b><br>
        <span style="color:#2ca02c;">&#9679;</span> Bueno (&lt;75% OMS)<br>
        <span style="color:#ffbb33;">&#9679;</span> Moderado (75-100% OMS)<br>
        <span style="color:#ff7f0e;">&#9679;</span> Alto (&gt;100% OMS)<br>
        <span style="color:#d62728;">&#9679;</span> Critico (&gt;150% OMS)<br>
        <small>Umbral OMS: {umbral_oms} ug/m3</small>
    </div>
    """
