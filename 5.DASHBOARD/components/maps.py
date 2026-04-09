# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.2: Mapa de Contaminacion con Folium
==============================================================================
Componente modular que renderiza un mapa de contaminacion embebido:
  - Prioridad 1: Carga mapa HTML pre-generado (Fase 6.1)  [SIN CAMBIOS]
  - Prioridad 2: Genera mapa dinamico desde datos agregados [MEJORADO]

Mejoras en el mapa dinamico:
  - Popup multi-variable: tabla con TODAS las variables de cada estacion
  - Score de calidad 0-10 por estacion (via utils.quality_index)
  - Tooltip enriquecido: barrio + score + variable seleccionada
  - PolyLine del Jardin del Turia (pulmón verde de Valencia)
  - Marcador especial Avinguda del Turia

Ruta: 5.DASHBOARD/components/maps.py
Autor: Joan | Fecha: 2026
"""

import logging
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import (
    MAPAS_DIR, MAPA_NO2_HTML, MAPA_PM25_HTML,
    VALENCIA_CENTER_LAT, VALENCIA_CENTER_LON,
    VARIABLE_COLORS, ESTACION_BARRIO_MAP,
    UMBRALES_OMS, UMBRALES_UE,
)
from data_loader import leer_html_visualizacion

logger = logging.getLogger("Maps")

# Importacion con degradacion graceful si el modulo no esta disponible
try:
    from utils.quality_index import calcular_indice_calidad, nivel_desde_score
    _QUALITY_INDEX_DISPONIBLE = True
except ImportError:
    _QUALITY_INDEX_DISPONIBLE = False
    logger.warning("[Maps] utils.quality_index no disponible; score omitido en popups.")

# Altura del mapa embebido en el dashboard (px)
MAP_HEIGHT = 520

# Mapeo de variables a sus mapas pre-generados
MAPAS_PREGENERADOS = {
    "NO2":  MAPA_NO2_HTML,
    "PM2.5": MAPA_PM25_HTML,
}

# Coordenadas aproximadas de estaciones (red GVA en Valencia)
ESTACION_COORDS: Dict[str, Dict] = {
    "46250001": {"lat": 39.4561, "lon": -0.3522, "nombre": "Valencia - Pista de Silla"},
    "46250004": {"lat": 39.4600, "lon": -0.3850, "nombre": "Valencia - Viveros"},
    "46250030": {"lat": 39.4580, "lon": -0.3900, "nombre": "Valencia - Moli del Sol"},
    "46250047": {"lat": 39.4830, "lon": -0.3590, "nombre": "Valencia - Benimaclet"},
    "46250050": {"lat": 39.4620, "lon": -0.3920, "nombre": "Valencia - Patraix"},
    "46250054": {"lat": 39.4750, "lon": -0.3760, "nombre": "Valencia - Ciutat Vella"},
}

# Recorrido aproximado del Jardin del Turia (~9 km, de Cabecera a Ciudad de las Artes)
TURIA_COORDS: List[List[float]] = [
    [39.4805, -0.3419],   # Cabecera (inicio parque)
    [39.4788, -0.3755],   # Puente de Serranos
    [39.4751, -0.3720],   # Puente del Real
    [39.4724, -0.3637],   # Palau de la Musica
    [39.4540, -0.3509],   # Ciudad de las Artes
    [39.4529, -0.3484],   # Final (IVAM)
]

# Coordenadas del marcador Avinguda del Turia
TURIA_MARKER_LAT = 39.4620
TURIA_MARKER_LON = -0.3620


# ==============================================================================
# FUNCION PRINCIPAL (sin cambios en la estrategia de seleccion)
# ==============================================================================

def render_mapa_contaminacion(
    df: pd.DataFrame,
    variable: str,
    df_contam_anual: Optional[pd.DataFrame] = None,
    trafico_rt: Optional[dict] = None,
) -> None:
    """
    Renderiza el mapa de contaminacion embebido en Streamlit.

    Estrategia 1 (prioritaria): HTML pre-generado (Fase 6.1).
    Estrategia 2 (fallback):    Mapa dinamico Folium con popups multi-variable.

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable seleccionada en el sidebar (determina color de marcadores).
        df_contam_anual: No utilizado en el mapa dinamico (backward compat).
        trafico_rt: Diccionario de cargar_trafico_realtime() con incidencias en
                    tiempo real. Si presente, se anaden marcadores de trafico al mapa.
    """
    st.markdown(
        '<div class="section-header">'
        '<h4>Mapa de calidad del aire</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Distribucion espacial por estaciones · Popup: todas las variables · '
        'Linea verde: Jardin del Turia'
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

    # --- Estrategia 2: Mapa dinamico mejorado ---
    logger.info(
        "[Mapa] Sin pre-generado para %s; generando mapa dinamico.", variable
    )
    _generar_mapa_dinamico(df, variable, trafico_rt=trafico_rt)


# ==============================================================================
# MAPA DINAMICO MEJORADO
# ==============================================================================

def _generar_mapa_dinamico(
    df: pd.DataFrame,
    variable: str,
    trafico_rt: Optional[dict] = None,
) -> None:
    """
    Genera un mapa Folium dinamico con:
      - CircleMarkers coloreados segun la variable seleccionada
      - Popups con tabla multi-variable (todas las variables de la estacion)
      - Score de calidad 0-10 por estacion
      - Tooltip: barrio + score + variable seleccionada
      - PolyLine del Jardin del Turia
      - Marcador especial Avinguda del Turia
      - (Opcional) Capa de incidencias de trafico en tiempo real

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable que determina el color de los CircleMarkers.
        trafico_rt: Diccionario de trafico RT con 'incidencias_valencia'.
    """
    try:
        import folium
    except ImportError:
        st.error("Folium no esta instalado. Ejecuta: pip install folium")
        return

    if df is None or df.empty:
        st.info("Sin datos para generar el mapa dinamico.")
        return

    df_valido = df.copy()
    if "calidad_dato" in df_valido.columns:
        df_valido = df_valido[df_valido["calidad_dato"] == "ok"]

    if df_valido.empty:
        st.info("Sin registros válidos para el mapa.")
        return

    # --- Estadisticas por estacion x variable (TODAS las variables) ---
    stats_multivariable = (
        df_valido
        .groupby(["estacion_id", "variable"], as_index=False)
        .agg(media=("valor", "mean"), registros=("valor", "count"))
    )

    # Stats de la variable seleccionada (para color y tooltip)
    stats_var_sel = (
        stats_multivariable[stats_multivariable["variable"] == variable]
        .set_index("estacion_id")
    )

    # Total de registros por estacion (todas las variables)
    total_registros = (
        df_valido.groupby("estacion_id")["valor"].count().rename("total_registros")
    )

    estaciones_con_datos = stats_multivariable["estacion_id"].unique()

    # --- Mapa base ---
    m = folium.Map(
        location=[VALENCIA_CENTER_LAT, VALENCIA_CENTER_LON],
        zoom_start=13,
        tiles="CartoDB dark_matter",
    )

    # --- Umbral y color para la variable seleccionada ---
    umbral_sel = UMBRALES_OMS.get(variable, 40.0)

    for est_id_raw in estaciones_con_datos:
        est_id = str(est_id_raw)
        coords = ESTACION_COORDS.get(est_id)
        if coords is None:
            logger.debug("[Mapa] Sin coordenadas para estacion %s", est_id)
            continue

        # Sub-df de esta estacion (multivariable)
        df_est = df_valido[df_valido["estacion_id"].astype(str) == est_id]
        barrio = ESTACION_BARRIO_MAP.get(est_id, "Desconocido")
        nombre_est = coords.get("nombre", est_id)
        n_total = int(total_registros.get(est_id_raw, 0))

        # Score de calidad de esta estacion
        score_est, nivel_est, color_nivel = _score_estacion(df_est)

        # Color y radio segun la variable seleccionada
        if est_id_raw in stats_var_sel.index:
            media_sel = float(stats_var_sel.loc[est_id_raw, "media"])
        else:
            media_sel = None

        color_marker, nivel_marker, radio = _color_y_radio(media_sel, umbral_sel)

        # --- Popup multi-variable ---
        popup_html = _construir_popup(
            nombre_est=nombre_est,
            barrio=barrio,
            est_id=est_id,
            df_est=df_est,
            score=score_est,
            nivel_score=nivel_est,
            color_nivel=color_nivel,
            n_total=n_total,
            variable_seleccionada=variable,
        )

        # --- Tooltip ---
        if media_sel is not None:
            tooltip_txt = (
                f"{barrio} | Score: {score_est}/10 | "
                f"{variable}: {media_sel:.1f} µg/m³"
            )
        else:
            tooltip_txt = f"{barrio} | Score: {score_est}/10 | {variable}: sin datos"

        folium.CircleMarker(
            location=[coords["lat"], coords["lon"]],
            radius=radio,
            color=color_marker,
            fill=True,
            fill_color=color_marker,
            fill_opacity=0.75,
            weight=2,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=tooltip_txt,
        ).add_to(m)

    # --- Jardin del Turia (PolyLine) ---
    _anadir_turia(m)

    # --- Capa de trafico en tiempo real ---
    n_trafico = _anadir_capa_trafico(m, trafico_rt)

    # --- LayerControl para toggle de capas ---
    if n_trafico > 0:
        folium.LayerControl(collapsed=False).add_to(m)

    # --- Leyenda ---
    leyenda_html = _crear_leyenda_html(variable, umbral_sel, n_trafico > 0)
    m.get_root().html.add_child(folium.Element(leyenda_html))

    # --- Renderizar ---
    n_estaciones = len([
        e for e in estaciones_con_datos if str(e) in ESTACION_COORDS
    ])
    caption_parts = [
        f"Mapa dinámico | {n_estaciones} estaciones",
        f"Variable activa: {variable}",
        f"Score por estación: {'activo' if _QUALITY_INDEX_DISPONIBLE else 'no disponible'}",
    ]
    if n_trafico > 0:
        caption_parts.append(f"Tráfico RT: {n_trafico} incidencias")
    st.caption(" | ".join(caption_parts))
    components.html(m._repr_html_(), height=MAP_HEIGHT, scrolling=False)
    logger.info(
        "[Mapa dinamico] %s: %d estaciones, %d inc. trafico.",
        variable, n_estaciones, n_trafico,
    )


# ==============================================================================
# HELPERS DE CALCULO
# ==============================================================================

def _score_estacion(df_est: pd.DataFrame):
    """
    Calcula el score de calidad (0-10) para una estacion concreta.

    Args:
        df_est: DataFrame filtrado para una unica estacion.

    Returns:
        Tupla (score_str, nivel, color). Si quality_index no disponible, '-'.
    """
    if not _QUALITY_INDEX_DISPONIBLE:
        return "-", "N/D", "#888"

    indice = calcular_indice_calidad(df_est, UMBRALES_OMS)
    if indice is None:
        return "-", "N/D", "#888"

    score = indice["score"]
    nivel, color, _ = nivel_desde_score(score)
    return f"{score:.1f}", nivel, color


def _color_y_radio(media: Optional[float], umbral: float):
    """
    Calcula color y radio del CircleMarker segun el ratio media/umbral OMS.

    Args:
        media: Media de la variable seleccionada (None si sin datos).
        umbral: Umbral OMS de la variable.

    Returns:
        Tupla (color_hex, nivel_str, radio_px).
    """
    if media is None or umbral == 0:
        return "#7f7f7f", "Sin datos", 8

    ratio = media / umbral
    if ratio > 1.5:
        return "#d62728", "CRITICO",  min(25, max(12, int(ratio * 8)))
    elif ratio > 1.0:
        return "#ff7f0e", "ALTO",     min(22, max(10, int(ratio * 7)))
    elif ratio > 0.75:
        return "#ffbb33", "MODERADO", min(18, max(9,  int(ratio * 6)))
    else:
        return "#2ca02c", "BUENO",    min(14, max(8,  int(ratio * 5) + 6))


# ==============================================================================
# CONSTRUCCION DEL POPUP MULTI-VARIABLE
# ==============================================================================

def _construir_popup(
    nombre_est: str,
    barrio: str,
    est_id: str,
    df_est: pd.DataFrame,
    score: str,
    nivel_score: str,
    color_nivel: str,
    n_total: int,
    variable_seleccionada: str,
) -> str:
    """
    Genera el HTML completo del popup para una estacion.

    Incluye cabecera, score de calidad, tabla multi-variable y pie de pagina.

    Args:
        nombre_est: Nombre legible de la estacion.
        barrio: Nombre del barrio.
        est_id: ID de la estacion (p. ej. '46250047').
        df_est: DataFrame filtrado a esta estacion (todas las variables).
        score: Score de calidad como string ('7.2' o '-').
        nivel_score: Nivel textual ('Bueno', 'N/D', ...).
        color_nivel: Color hex asociado al nivel.
        n_total: Total de registros de todas las variables.
        variable_seleccionada: Variable activa en el sidebar (se resalta).

    Returns:
        String HTML listo para folium.Popup.
    """
    # Calcular stats por variable para la tabla
    stats = (
        df_est
        .groupby("variable")["valor"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "media", "count": "n"})
    )

    # Filas de la tabla
    filas_tabla = _filas_tabla_variables(stats, variable_seleccionada)

    tabla_html = (
        '<table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:6px;">'
        '<thead>'
        '<tr style="background:#2a2a3a;color:#ccc;">'
        '<th style="padding:3px 5px;text-align:left;">Variable</th>'
        '<th style="padding:3px 5px;text-align:right;">Media</th>'
        '<th style="padding:3px 5px;text-align:right;">OMS</th>'
        '<th style="padding:3px 5px;text-align:center;">Estado</th>'
        '</tr>'
        '</thead>'
        '<tbody>'
        + filas_tabla +
        '</tbody>'
        '</table>'
    )

    score_html = (
        f'<div style="margin:6px 0;padding:4px 8px;background:#1e1e2e;'
        f'border-radius:4px;border-left:3px solid {color_nivel};">'
        f'<span style="color:#aaa;font-size:10px;">Score calidad: </span>'
        f'<span style="color:{color_nivel};font-weight:700;font-size:13px;">'
        f'{score}/10</span>'
        f'<span style="color:#aaa;font-size:10px;"> {nivel_score}</span>'
        f'</div>'
    )

    return (
        f'<div style="font-family:Arial,sans-serif;min-width:260px;max-width:310px;'
        f'background:#141424;color:#ddd;padding:2px;">'
        f'<b style="font-size:13px;">{nombre_est}</b><br>'
        f'<span style="color:#aaa;font-size:11px;">Barrio: {barrio} | '
        f'ID: {est_id}</span>'
        f'<hr style="margin:5px 0;border-color:#333;">'
        + score_html
        + tabla_html +
        f'<div style="margin-top:6px;color:#666;font-size:10px;">'
        f'Total registros (todas las variables): {n_total:,}'
        f'</div>'
        f'</div>'
    )


def _filas_tabla_variables(
    stats: pd.DataFrame,
    variable_seleccionada: str,
) -> str:
    """
    Genera las filas <tr> de la tabla de variables del popup.

    Args:
        stats: DataFrame con index=variable, columnas [media, n].
        variable_seleccionada: Variable que se resalta con fondo diferente.

    Returns:
        String HTML con las filas <tr>.
    """
    # Orden preferido de visualizacion
    orden = ["NO2", "PM2.5", "PM10", "O3", "SO2", "CO"]
    variables_ordenadas = [v for v in orden if v in stats.index]
    variables_ordenadas += [v for v in stats.index if v not in orden]

    filas = []
    for var in variables_ordenadas:
        media  = float(stats.loc[var, "media"])
        umbral = UMBRALES_OMS.get(var)
        color_var = VARIABLE_COLORS.get(var, "#888")

        # Estado segun umbral OMS
        if umbral:
            ratio = media / umbral
            if ratio > 1.5:
                estado = "🔴"
                color_estado = "#d62728"
            elif ratio > 1.0:
                estado = "🟠"
                color_estado = "#ff7f0e"
            elif ratio > 0.75:
                estado = "🟡"
                color_estado = "#ffbb33"
            else:
                estado = "🟢"
                color_estado = "#2ca02c"
            umbral_str = f"{umbral:.0f}"
        else:
            ratio = None
            estado = "⬜"
            color_estado = "#888"
            umbral_str = "-"

        # Fondo resaltado si es la variable seleccionada
        bg = "background:#1e2a1e;" if var == variable_seleccionada else ""
        peso = "font-weight:700;" if var == variable_seleccionada else ""

        filas.append(
            f'<tr style="{bg}border-bottom:1px solid #2a2a3a;">'
            f'<td style="padding:3px 5px;{peso}color:{color_var};">{var}</td>'
            f'<td style="padding:3px 5px;text-align:right;{peso}">{media:.1f} µg/m³</td>'
            f'<td style="padding:3px 5px;text-align:right;color:#888;">{umbral_str}</td>'
            f'<td style="padding:3px 5px;text-align:center;">{estado}</td>'
            f'</tr>'
        )

    return "".join(filas)


# ==============================================================================
# CAPA DE TRAFICO EN TIEMPO REAL
# ==============================================================================

# Mapeo de causa DGT a icono y color de Folium
_TRAFICO_ICONO_MAP = {
    "roadMaintenance":       ("wrench",              "orange"),
    "roadworks":             ("wrench",              "orange"),
    "accident":              ("exclamation-triangle", "red"),
    "abnormalTraffic":       ("car",                 "darkred"),
    "congestion":            ("car",                 "darkred"),
    "infrastructureDamage":  ("exclamation-triangle", "orange"),
    "obstruction":           ("ban",                 "red"),
    "poorEnvironment":       ("cloud",               "blue"),
    "weatherRelated":        ("cloud",               "blue"),
}

_SEVERIDAD_ES = {
    "low": "Baja", "medium": "Media",
    "high": "Alta", "highest": "Muy alta",
}


def _anadir_capa_trafico(m, trafico_rt: Optional[dict]) -> int:
    """
    Anade una FeatureGroup con marcadores de incidencias de trafico al mapa.

    Cada incidencia se representa con un folium.Marker cuyo icono depende
    del tipo de causa (obras, accidente, congestion, etc.).

    Args:
        m: Objeto folium.Map.
        trafico_rt: Diccionario de cargar_trafico_realtime(). Puede ser None.

    Returns:
        Numero de marcadores anadidos.
    """
    if trafico_rt is None:
        return 0

    incidencias = trafico_rt.get("incidencias_valencia", [])
    if not incidencias:
        return 0

    try:
        import folium
    except ImportError:
        return 0

    fg = folium.FeatureGroup(name="🚗 Tráfico RT")
    n_added = 0

    timestamp = trafico_rt.get("timestamp", "")

    for inc in incidencias:
        lat = inc.get("lat")
        lon = inc.get("lon")
        if lat is None or lon is None:
            continue

        causa = inc.get("causa", "")
        tipo_raw = inc.get("tipo", "")
        severidad = inc.get("severidad", "unknown")
        carretera = inc.get("carretera", "")
        municipio = inc.get("municipio", "")

        # Seleccionar icono segun causa
        icon_name, icon_color = _TRAFICO_ICONO_MAP.get(
            causa, ("info-sign", "blue")
        )

        # Tipo legible
        tipo_legible = (
            tipo_raw
            .replace("RoadOrCarriagewayOrLaneManagement", "Gestión de carril")
            .replace("AbnormalTraffic", "Tráfico anómalo")
            .replace("Accident", "Accidente")
            .replace("Conditions", "Condiciones")
        )

        causa_legible = (
            causa
            .replace("roadMaintenance", "Obras/mantenimiento")
            .replace("roadworks", "Obras")
            .replace("accident", "Accidente")
            .replace("abnormalTraffic", "Tráfico anómalo")
            .replace("congestion", "Congestión")
            .replace("infrastructureDamage", "Daño infraestructura")
            .replace("obstruction", "Obstrucción")
            .replace("poorEnvironment", "Condiciones ambientales")
            .replace("weatherRelated", "Meteorología adversa")
        )

        sev_es = _SEVERIDAD_ES.get(severidad, severidad)

        # Popup HTML
        popup_html = (
            f'<div style="font-family:Arial,sans-serif;min-width:200px;'
            f'max-width:260px;font-size:12px;">'
            f'<b style="color:#ff7f0e;">🚗 Incidencia de tráfico</b><br>'
            f'<hr style="margin:4px 0;border-color:#ddd;">'
            f'<b>Tipo:</b> {tipo_legible}<br>'
            f'<b>Causa:</b> {causa_legible}<br>'
            f'<b>Severidad:</b> {sev_es}<br>'
            f'<b>Carretera:</b> {carretera}<br>'
        )
        if municipio:
            popup_html += f'<b>Municipio:</b> {municipio}<br>'
        if timestamp:
            popup_html += (
                f'<hr style="margin:4px 0;border-color:#ddd;">'
                f'<small style="color:#888;">Captura: {timestamp[:16]}</small>'
            )
        popup_html += '</div>'

        # Tooltip
        tooltip_txt = f"{causa_legible} — {carretera}" if carretera else causa_legible

        folium.Marker(
            location=[lat, lon],
            icon=folium.Icon(color=icon_color, icon=icon_name, prefix="fa"),
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=tooltip_txt,
        ).add_to(fg)
        n_added += 1

    if n_added > 0:
        fg.add_to(m)
        logger.info("[Mapa] Capa trafico RT: %d marcadores.", n_added)

    return n_added


# ==============================================================================
# JARDIN DEL TURIA
# ==============================================================================

def _anadir_turia(m) -> None:
    """
    Anade la PolyLine del Jardin del Turia y su marcador a un mapa Folium.

    Args:
        m: Objeto folium.Map al que se anade la capa.
    """
    try:
        import folium
    except ImportError:
        return

    # PolyLine del recorrido
    folium.PolyLine(
        locations=TURIA_COORDS,
        color="#2ca02c",
        weight=3,
        opacity=0.7,
        dash_array="8 4",
        tooltip="Jardín del Túria - Pulmón verde de Valencia",
        popup=folium.Popup(
            '<div style="font-family:Arial;font-size:12px;color:#222;">'
            '<b style="color:#2ca02c;">🌿 Jardí del Túria</b><br>'
            'Parque lineal de ~9 km.<br>'
            'Zona de referencia de calidad ambiental en Valencia.'
            '</div>',
            max_width=220,
        ),
    ).add_to(m)

    # Marcador especial en Avinguda del Turia
    folium.Marker(
        location=[TURIA_MARKER_LAT, TURIA_MARKER_LON],
        icon=folium.Icon(color="green", icon="leaf", prefix="fa"),
        tooltip="Avinguda/Jardí del Túria - Zona de referencia",
        popup=folium.Popup(
            '<div style="font-family:Arial;font-size:12px;color:#222;">'
            '<b style="color:#2ca02c;">🌿 Avinguda del Túria</b><br>'
            'Zona de referencia de calidad del aire en Valencia.<br>'
            '<small>Coordenadas: 39.4620°N, 0.3620°W</small>'
            '</div>',
            max_width=220,
        ),
    ).add_to(m)

    logger.debug("[Mapa] Jardin del Turia anadido al mapa.")


# ==============================================================================
# LEYENDA HTML PARA FOLIUM
# ==============================================================================

def _crear_leyenda_html(
    variable: str,
    umbral_oms: float,
    con_trafico: bool = False,
) -> str:
    """
    Genera HTML para una leyenda flotante en el mapa.

    Args:
        variable: Nombre de la variable activa.
        umbral_oms: Valor del umbral OMS.
        con_trafico: Si True, incluye seccion de trafico en la leyenda.

    Returns:
        String HTML con la leyenda.
    """
    trafico_html = ""
    if con_trafico:
        trafico_html = (
            '<hr style="border-color:#333;margin:5px 0;">'
            '<b style="font-size:11px;">🚗 Tráfico RT</b><br>'
            '<span style="color:#ff7f0e;">&#9873;</span>'
            ' <small>Obras/mantenimiento</small><br>'
            '<span style="color:#d62728;">&#9873;</span>'
            ' <small>Accidente/obstrucción</small><br>'
            '<span style="color:#85144b;">&#9873;</span>'
            ' <small>Congestión</small><br>'
            '<span style="color:#1f77b4;">&#9873;</span>'
            ' <small>Otros</small>'
        )

    return f"""
    <div style="
        position: fixed;
        bottom: 30px; left: 30px;
        z-index: 1000;
        background: rgba(20,20,36,0.93);
        padding: 10px 14px;
        border-radius: 8px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        color: #ccc;
        border: 1px solid #444;
        box-shadow: 2px 2px 8px rgba(0,0,0,0.5);
    ">
        <b style="font-size:13px;">{variable}</b>
        <span style="color:#888;font-size:10px;"> vs umbral OMS</span><br>
        <span style="color:#2ca02c;">&#9679;</span> Bueno (&lt;75% OMS)<br>
        <span style="color:#ffbb33;">&#9679;</span> Moderado (75-100% OMS)<br>
        <span style="color:#ff7f0e;">&#9679;</span> Alto (&gt;100% OMS)<br>
        <span style="color:#d62728;">&#9679;</span> Crítico (&gt;150% OMS)<br>
        <span style="color:#7f7f7f;">&#9679;</span> Sin datos<br>
        <hr style="border-color:#333;margin:5px 0;">
        <small>OMS: {umbral_oms} µg/m³</small><br>
        <span style="color:#2ca02c;">&#9135;&#9135;</span>
        <small style="color:#2ca02c;"> Jardí del Túria</small>
        {trafico_html}
    </div>
    """
