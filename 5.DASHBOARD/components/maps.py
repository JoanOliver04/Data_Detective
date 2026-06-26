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
from datetime import datetime
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import (
    MAPA_NO2_HTML,
    MAPA_PM25_HTML,
    VALENCIA_CENTER_LAT,
    VALENCIA_CENTER_LON,
    VARIABLE_COLORS,
    ESTACION_BARRIO_MAP,
    UMBRALES_OMS,
    DISTRITOS_VALENCIA,
    ESTACION_DISTRITO_MAP,
)
from data_loader import leer_html_visualizacion
from theme import get_theme
from utils.formatters import escape_html

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
    "NO2": MAPA_NO2_HTML,
    "PM2.5": MAPA_PM25_HTML,
}

# Coordenadas verificadas de estaciones activas (red GVA + AQICN en Valencia)
# Claves: código GVA (46250xxx) para datos históricos, o identificador propio.
# Coords verificadas contra la API AQICN (2026-04-09).
ESTACION_COORDS: Dict[str, Dict] = {
    "46250001": {"lat": 39.4575, "lon": -0.3428, "nombre": "Avd. Francia, València"},
    "46250004": {"lat": 39.4600, "lon": -0.3850, "nombre": "Valencia - Viveros"},
    "46250030": {"lat": 39.4561, "lon": -0.3758, "nombre": "Pista de Silla, València"},
    "46250047": {"lat": 39.4803, "lon": -0.3364, "nombre": "Politècnic, València"},
    "46250050": {"lat": 39.4811, "lon": -0.4083, "nombre": "Molí del Sol, València"},
    "46250060": {
        "lat": 39.4811,
        "lon": -0.4472,
        "nombre": "Quart de Poblet (metropolitana)",
    },
    "torre_navis": {
        "lat": 39.4731,
        "lon": -0.4081,
        "nombre": "Torre Navis (sensor ciudadano)",
    },
}

# Puente AQICN UID (clave en ESTACION_DISTRITO_MAP) -> clave en ESTACION_COORDS.
# Necesario porque ESTACION_DISTRITO_MAP usa UIDs AQICN y ESTACION_COORDS usa
# códigos GVA (46250xxx) para mantener compatibilidad con datos históricos.
_UID_A_COORDS_KEY: Dict[str, str] = {
    "6639": "46250001",
    "6637": "46250030",
    "6640": "46250047",
    "6638": "46250050",
    "6644": "46250060",
    "torre_navis": "torre_navis",
}

# Recorrido aproximado del Jardin del Turia (~9 km, de Cabecera a Ciudad de las Artes)
TURIA_COORDS: List[List[float]] = [
    [39.4805, -0.3419],  # Cabecera (inicio parque)
    [39.4788, -0.3755],  # Puente de Serranos
    [39.4751, -0.3720],  # Puente del Real
    [39.4724, -0.3637],  # Palau de la Musica
    [39.4540, -0.3509],  # Ciudad de las Artes
    [39.4529, -0.3484],  # Final (IVAM)
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
    contam_rt: Optional[dict] = None,
    meteo_rt: Optional[dict] = None,
) -> None:
    """
    Renderiza el mapa de contaminacion embebido en Streamlit.

    Estrategia 1 (prioritaria): HTML pre-generado (Fase 6.1).
    Estrategia 2 (fallback):    Mapa dinamico Folium con popups multi-variable.

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable seleccionada en el sidebar (determina color de marcadores).
        df_contam_anual: No utilizado en el mapa dinamico (backward compat).
        trafico_rt: Diccionario de cargar_trafico_realtime() con incidencias RT.
        contam_rt: Diccionario de cargar_contaminacion_realtime() con AQI y
                   contaminantes actuales por estacion. Enriquece los popups.
        meteo_rt: Diccionario de cargar_meteo_realtime() con temperatura y
                  humedad actuales (ciudad-wide). Aparece en todos los popups.
    """
    st.markdown(
        '<div class="section-header">'
        "<h4>Mapa de calidad del aire</h4>"
        '<p style="color:#888;font-size:0.85rem;">'
        "Distribucion espacial por estaciones · Popup: todas las variables · "
        "Linea verde: Jardin del Turia"
        "</p></div>",
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
    logger.info("[Mapa] Sin pre-generado para %s; generando mapa dinamico.", variable)
    _generar_mapa_dinamico(
        df,
        variable,
        trafico_rt=trafico_rt,
        contam_rt=contam_rt,
        meteo_rt=meteo_rt,
    )


# ==============================================================================
# MAPA DINAMICO MEJORADO
# ==============================================================================


def _generar_mapa_dinamico(
    df: pd.DataFrame,
    variable: str,
    trafico_rt: Optional[dict] = None,
    contam_rt: Optional[dict] = None,
    meteo_rt: Optional[dict] = None,
) -> None:
    """
    Genera un mapa Folium dinamico con:
      - CircleMarkers coloreados segun la variable seleccionada
      - Popups con tabla multi-variable (todas las variables de la estacion)
      - Score de calidad 0-10 por estacion
      - Tooltip: barrio + score + variable seleccionada
      - Seccion RT en cada popup: AQI actual, contaminantes, temp/humedad
      - PolyLine del Jardin del Turia
      - Marcador especial Avinguda del Turia
      - (Opcional) Capa de incidencias de trafico en tiempo real

    Args:
        df: DataFrame filtrado de contaminacion.
        variable: Variable que determina el color de los CircleMarkers.
        trafico_rt: Diccionario de trafico RT con 'incidencias' (lista).
        contam_rt: Resultado de cargar_contaminacion_realtime(). Enriquece popups.
        meteo_rt: Resultado de cargar_meteo_realtime(). Aparece en todos los popups.
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

    # --- Toggle modo mapa (marcadores vs mapa de calor) ---
    hay_datos_rt = contam_rt is not None and len(contam_rt.get("estaciones", [])) > 0
    modo_mapa = "Marcadores"
    if hay_datos_rt:
        modo_mapa = st.radio(
            "Modo de mapa",
            ["Marcadores", "Mapa de calor"],
            horizontal=True,
            help="Marcadores: sensores individuales. Mapa de calor: gradiente AQI.",
            key="mapa_modo_radio",
        )

    # --- Estadisticas por estacion x variable (TODAS las variables) ---
    stats_multivariable = df_valido.groupby(
        ["estacion_id", "variable"], as_index=False
    ).agg(media=("valor", "mean"), registros=("valor", "count"))

    # Stats de la variable seleccionada (para color y tooltip)
    stats_var_sel = stats_multivariable[
        stats_multivariable["variable"] == variable
    ].set_index("estacion_id")

    # Total de registros por estacion (todas las variables)
    total_registros = (
        df_valido.groupby("estacion_id")["valor"].count().rename("total_registros")
    )

    estaciones_con_datos = stats_multivariable["estacion_id"].unique()

    # --- Lookup RT por estacion_id (incluye timestamp compartido) ---
    rt_por_estacion: Dict[str, dict] = {}
    if contam_rt:
        rt_ts = contam_rt.get("timestamp", "")
        for e in contam_rt.get("estaciones", []):
            entry = dict(e)
            entry["_timestamp"] = rt_ts
            rt_por_estacion[str(e["estacion_id"])] = entry

    # --- Tema activo (para tiles, popups, leyenda) ---
    t = get_theme()

    # --- Mapa base ---
    m = folium.Map(
        location=[VALENCIA_CENTER_LAT, VALENCIA_CENTER_LON],
        zoom_start=13,
        tiles=t["folium_tiles"],
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
            rt_data=rt_por_estacion.get(est_id),
            meteo_rt=meteo_rt,
            theme=t,
        )

        # --- Tooltip (con badge LIVE si tiene datos RT) ---
        live_badge = ""
        if est_id in rt_por_estacion:
            live_badge = " 🔴 LIVE"
        if media_sel is not None:
            tooltip_txt = (
                f"{barrio} | Score: {score_est}/10 | "
                f"{variable}: {media_sel:.1f} µg/m³{live_badge}"
            )
        else:
            tooltip_txt = (
                f"{barrio} | Score: {score_est}/10 | "
                f"{variable}: sin datos{live_badge}"
            )

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

    # --- Capa de distritos (19 distritos de Valencia) ---
    # ESTACION_DISTRITO_MAP usa UIDs AQICN; _UID_A_COORDS_KEY los traduce a claves GVA.
    distritos_sensores = {
        distrito: ESTACION_COORDS[coords_key]["nombre"]
        for uid, distrito in ESTACION_DISTRITO_MAP.items()
        if (coords_key := _UID_A_COORDS_KEY.get(uid)) and coords_key in ESTACION_COORDS
    }
    # Datos RT para distritos que tienen sensor asignado
    distritos_rt: Dict[str, dict] = {
        distrito: rt_por_estacion[coords_key]
        for uid, distrito in ESTACION_DISTRITO_MAP.items()
        if (coords_key := _UID_A_COORDS_KEY.get(uid)) and coords_key in rt_por_estacion
    }
    _anadir_marcadores_distritos(
        m,
        DISTRITOS_VALENCIA,
        distritos_sensores,
        distritos_rt=distritos_rt,
        meteo_rt=meteo_rt,
        theme=t,
    )

    # --- Capa de sensores RT ---
    n_sensores_rt = _anadir_capa_sensores_rt(m, contam_rt, meteo_rt, theme=t)

    # --- Mapa de calor RT (solo en modo calor) ---
    tiene_heatmap = False
    if modo_mapa == "Mapa de calor":
        tiene_heatmap = _anadir_heatmap_rt(m, contam_rt)

    # --- Capa de trafico en tiempo real ---
    n_trafico = _anadir_capa_trafico(m, trafico_rt, theme=t)

    # --- Jardin del Turia (PolyLine) ---
    _anadir_turia(m)

    # --- LayerControl (siempre, porque la capa de distritos esta siempre presente) ---
    folium.LayerControl(collapsed=False).add_to(m)

    # --- Leyenda ---
    rt_timestamp = ""
    if contam_rt:
        rt_timestamp = contam_rt.get("timestamp", "")
    leyenda_html = _crear_leyenda_html(
        variable,
        umbral_sel,
        con_trafico=n_trafico > 0,
        con_rt=n_sensores_rt > 0,
        rt_timestamp=rt_timestamp,
        theme=t,
    )
    m.get_root().html.add_child(folium.Element(leyenda_html))

    # --- Renderizar ---
    n_estaciones = len([e for e in estaciones_con_datos if str(e) in ESTACION_COORDS])
    caption_parts = [
        f"Mapa dinámico | {n_estaciones} estaciones",
        f"Variable activa: {variable}",
        f"Score por estación: {'activo' if _QUALITY_INDEX_DISPONIBLE else 'no disponible'}",
    ]
    if n_sensores_rt > 0:
        caption_parts.append(f"📡 RT: {n_sensores_rt} sensores")
    if tiene_heatmap:
        caption_parts.append("🔥 Mapa de calor activo")
    if n_trafico > 0:
        caption_parts.append(f"Tráfico RT: {n_trafico} incidencias")
    st.caption(" | ".join(caption_parts))
    components.html(m._repr_html_(), height=MAP_HEIGHT, scrolling=False)
    logger.info(
        "[Mapa dinamico] %s: %d estaciones, %d sensores RT, %d inc. trafico.",
        variable,
        n_estaciones,
        n_sensores_rt,
        n_trafico,
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
        return "-", "N/D", get_theme()["text_muted"]

    indice = calcular_indice_calidad(df_est, UMBRALES_OMS)
    if indice is None:
        return "-", "N/D", get_theme()["text_muted"]

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
        return "#d62728", "CRITICO", min(25, max(12, int(ratio * 8)))
    elif ratio > 1.0:
        return "#ff7f0e", "ALTO", min(22, max(10, int(ratio * 7)))
    elif ratio > 0.75:
        return "#ffbb33", "MODERADO", min(18, max(9, int(ratio * 6)))
    else:
        return "#2ca02c", "BUENO", min(14, max(8, int(ratio * 5) + 6))


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
    rt_data: Optional[dict] = None,
    meteo_rt: Optional[dict] = None,
    theme: Optional[dict] = None,
) -> str:
    """
    Genera el HTML completo del popup para una estacion.

    Incluye dos secciones claramente separadas:
      - DATOS HISTORICOS: score de calidad + tabla multi-variable con medias.
      - DATOS EN TIEMPO REAL: AQI actual, contaminantes RT, temp/humedad y
        timestamp de captura. Solo aparece si rt_data o meteo_rt estan disponibles.

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
        rt_data: Dict con AQI y contaminantes actuales de esta estacion.
                 Incluye clave '_timestamp'. None si no hay datos RT.
        meteo_rt: Dict con temperatura y humedad actuales (ciudad-wide).
                  None si no hay datos de meteorologia RT.
        theme: Diccionario de tokens de tema. Si None, usa get_theme().

    Returns:
        String HTML listo para folium.Popup.
    """
    t = theme or get_theme()
    # Calcular stats por variable para la tabla
    stats = (
        df_est.groupby("variable")["valor"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "media", "count": "n"})
    )

    # Filas de la tabla
    filas_tabla = _filas_tabla_variables(stats, variable_seleccionada, theme=t)

    tabla_html = (
        '<table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:4px;">'
        "<thead>"
        f'<tr style="background:{t["bg_table_header"]};color:{t["text_secondary"]};">'
        '<th style="padding:3px 5px;text-align:left;">Variable</th>'
        '<th style="padding:3px 5px;text-align:right;">Media</th>'
        '<th style="padding:3px 5px;text-align:right;">OMS</th>'
        '<th style="padding:3px 5px;text-align:center;">Estado</th>'
        "</tr>"
        "</thead>"
        "<tbody>" + filas_tabla + "</tbody>"
        "</table>"
    )

    score_html = (
        f'<div style="margin:6px 0;padding:4px 8px;background:{t["bg_score"]};'
        f'border-radius:4px;border-left:3px solid {color_nivel};">'
        f'<span style="color:{t["text_label"]};font-size:10px;">Score calidad: </span>'
        f'<span style="color:{color_nivel};font-weight:700;font-size:13px;">'
        f"{score}/10</span>"
        f'<span style="color:{t["text_label"]};font-size:10px;"> {nivel_score}</span>'
        f"</div>"
    )

    historico_header = (
        f'<div style="font-size:10px;color:{t["text_dim"]};font-weight:600;'
        f'letter-spacing:0.5px;margin-top:4px;">── DATOS HISTÓRICOS ──</div>'
    )

    footer_html = (
        f'<div style="margin-top:6px;color:{t["text_faint"]};font-size:10px;">'
        f"Total registros (todas las variables): {n_total:,}"
        f"</div>"
    )

    rt_html = _seccion_rt_popup(rt_data, meteo_rt, theme=t)

    return (
        f'<div style="font-family:Arial,sans-serif;min-width:260px;max-width:320px;'
        f'background:{t["bg_popup"]};color:{t["text"]};padding:4px;">'
        f'<b style="font-size:13px;">{nombre_est}</b><br>'
        f'<span style="color:{t["text_label"]};font-size:11px;">Barrio: {barrio} | '
        f"ID: {est_id}</span>"
        f'<hr style="margin:5px 0;border-color:{t["popup_hr"]};">'
        + score_html
        + historico_header
        + tabla_html
        + footer_html
        + rt_html
        + "</div>"
    )


def _filas_tabla_variables(
    stats: pd.DataFrame,
    variable_seleccionada: str,
    theme: Optional[dict] = None,
) -> str:
    """
    Genera las filas <tr> de la tabla de variables del popup.

    Args:
        stats: DataFrame con index=variable, columnas [media, n].
        variable_seleccionada: Variable que se resalta con fondo diferente.
        theme: Diccionario de tokens de tema.

    Returns:
        String HTML con las filas <tr>.
    """
    t = theme or get_theme()
    # Orden preferido de visualizacion
    orden = ["NO2", "PM2.5", "PM10", "O3", "SO2", "CO"]
    variables_ordenadas = [v for v in orden if v in stats.index]
    variables_ordenadas += [v for v in stats.index if v not in orden]

    filas = []
    for var in variables_ordenadas:
        media = float(stats.loc[var, "media"])
        umbral = UMBRALES_OMS.get(var)
        color_var = VARIABLE_COLORS.get(var, "#888")

        # Estado segun umbral OMS
        if umbral:
            ratio = media / umbral
            if ratio > 1.5:
                estado = "🔴"
            elif ratio > 1.0:
                estado = "🟠"
            elif ratio > 0.75:
                estado = "🟡"
            else:
                estado = "🟢"
            umbral_str = f"{umbral:.0f}"
        else:
            ratio = None
            estado = "⬜"
            umbral_str = "-"

        # Fondo resaltado si es la variable seleccionada
        bg = f"background:{t['bg_highlight']};" if var == variable_seleccionada else ""
        peso = "font-weight:700;" if var == variable_seleccionada else ""

        filas.append(
            f'<tr style="{bg}border-bottom:1px solid {t["border_subtle"]};">'
            f'<td style="padding:3px 5px;{peso}color:{color_var};">{var}</td>'
            f'<td style="padding:3px 5px;text-align:right;{peso}">{media:.1f} µg/m³</td>'
            f'<td style="padding:3px 5px;text-align:right;color:{t["text_muted"]};">'
            f"{umbral_str}</td>"
            f'<td style="padding:3px 5px;text-align:center;">{estado}</td>'
            f"</tr>"
        )

    return "".join(filas)


# ==============================================================================
# HELPERS PARA LA SECCION RT DE LOS POPUPS
# ==============================================================================

# Mapeo (label display, clave en rt_data, clave en VARIABLE_COLORS)
_CONTAM_RT_CAMPOS = [
    ("NO₂", "no2", "NO2"),
    ("O₃", "o3", "O3"),
    ("PM10", "pm10", "PM10"),
    ("PM2.5", "pm25", "PM2.5"),
    ("SO₂", "so2", "SO2"),
    ("CO", "co", "CO"),
]


def _frescura_rt(timestamp_str: str) -> str:
    """
    Convierte un timestamp ISO a string legible para popups HTML.

    Args:
        timestamp_str: Timestamp ISO (puede tener timezone).

    Returns:
        String como 'hace 5 min', 'hace 2h', o '' si no parseable.
    """
    if not timestamp_str:
        return ""
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        delta = datetime.now() - ts.replace(tzinfo=None)
        minutos = int(delta.total_seconds() / 60)
        if minutos < 1:
            return "hace menos de 1 min"
        if minutos < 60:
            return f"hace {minutos} min"
        horas = minutos // 60
        return f"hace {horas}h"
    except (ValueError, TypeError):
        return ""


def _seccion_rt_popup(
    rt_data: Optional[dict],
    meteo_rt: Optional[dict],
    theme: Optional[dict] = None,
) -> str:
    """
    Genera el bloque HTML de la seccion 'DATOS EN TIEMPO REAL' del popup.

    Solo incluye campos que tienen valor (None se omite silenciosamente).
    Devuelve cadena vacia si no hay ningun dato RT disponible.

    Args:
        rt_data: Dict con AQI, contaminantes y '_timestamp' de esta estacion.
        meteo_rt: Dict con temp y humedad actuales (ciudad-wide).
        theme: Diccionario de tokens de tema.

    Returns:
        String HTML con la seccion RT, o '' si rt_data y meteo_rt son None.
    """
    t = theme or get_theme()
    tiene_contam = rt_data is not None
    tiene_meteo = meteo_rt is not None and (
        meteo_rt.get("temp") is not None or meteo_rt.get("humedad") is not None
    )
    if not tiene_contam and not tiene_meteo:
        return ""

    partes = [
        f'<hr style="margin:6px 0;border-color:{t["popup_hr_rt"]};">',
        '<div style="font-size:10px;color:#17becf;font-weight:600;'
        'letter-spacing:0.5px;margin-bottom:4px;">── DATOS EN TIEMPO REAL ──</div>',
    ]

    # AQI
    if tiene_contam:
        aqi = rt_data.get("aqi")
        if aqi is not None:
            if aqi <= 50:
                color_aqi, icono_aqi, nivel_aqi = "#2ca02c", "🟢", "Buena"
            elif aqi <= 100:
                color_aqi, icono_aqi, nivel_aqi = "#ffbb33", "🟡", "Moderada"
            elif aqi <= 150:
                color_aqi, icono_aqi, nivel_aqi = "#ff7f0e", "🟠", "Dañina (sensibles)"
            else:
                color_aqi, icono_aqi, nivel_aqi = "#d62728", "🔴", "Dañina"
            partes.append(
                f'<div style="margin-bottom:3px;">'
                f'<b style="color:{color_aqi};">AQI: {aqi}</b> {icono_aqi} '
                f'<span style="color:{t["text_label"]};font-size:10px;">{nivel_aqi}</span>'
                f"</div>"
            )

        # Contaminantes RT
        filas_rt = []
        for label, key, color_key in _CONTAM_RT_CAMPOS:
            val = rt_data.get(key)
            if val is None:
                continue
            color_var = VARIABLE_COLORS.get(color_key, "#888")
            filas_rt.append(
                f'<div style="margin:1px 0;font-size:11px;">'
                f'<span style="color:{color_var};">{label}:</span> '
                f"<b>{val:.1f} µg/m³</b>"
                f"</div>"
            )
        partes.extend(filas_rt)

    # Temperatura y humedad (meteo_rt es ciudad-wide, misma para todas las estaciones)
    if tiene_meteo:
        meteo_partes = []
        temp = meteo_rt.get("temp")
        humedad = meteo_rt.get("humedad")
        if temp is not None:
            meteo_partes.append(f"🌡️ {temp:.1f}°C")
        if humedad is not None:
            meteo_partes.append(f"💧 {int(humedad)}%")
        if meteo_partes:
            partes.append(
                f'<div style="margin-top:3px;font-size:11px;color:{t["text_secondary"]};">'
                + " &nbsp; ".join(meteo_partes)
                + "</div>"
            )

    # Timestamp de la captura
    ts_str = rt_data.get("_timestamp", "") if tiene_contam else ""
    if not ts_str and tiene_meteo:
        ts_str = meteo_rt.get("timestamp", "")  # type: ignore[union-attr]
    frescura = _frescura_rt(ts_str)
    if frescura:
        partes.append(
            f'<div style="margin-top:4px;color:{t["text_faint"]};font-size:10px;">'
            f"Última captura: {frescura}"
            f"</div>"
        )

    return "".join(partes)


# ==============================================================================
# CAPA DE DISTRITOS DE VALENCIA
# ==============================================================================


def _anadir_marcadores_distritos(
    m,
    distritos: Dict[str, Dict],
    distritos_con_sensor: Dict[str, str],
    distritos_rt: Optional[Dict[str, dict]] = None,
    meteo_rt: Optional[dict] = None,
    theme: Optional[dict] = None,
) -> None:
    """
    Añade una FeatureGroup toggleable con los 19 distritos de Valencia al mapa.

    Para cada distrito muestra un marcador con icono 'building':
      - Azul si el distrito tiene una estación de contaminación activa.
      - Gris si el distrito no tiene sensor (datos interpolados de estaciones cercanas).
    Los distritos con sensor incluyen la seccion RT si hay datos disponibles.

    Args:
        m: Objeto folium.Map al que se añade la capa.
        distritos: Diccionario DISTRITOS_VALENCIA con datos de cada distrito
            (lat, lon, habitantes, descripcion).
        distritos_con_sensor: Dict {nombre_distrito: nombre_estacion} con los
            distritos que disponen de sensor activo.
        distritos_rt: Dict {nombre_distrito: rt_data} con datos RT por distrito.
        meteo_rt: Dict con temperatura y humedad actuales (ciudad-wide).
    """
    try:
        import folium
    except ImportError:
        return

    t = theme or get_theme()
    fg = folium.FeatureGroup(name="🏘️ Distritos")
    _dr = distritos_rt or {}

    for nombre_distrito, datos in distritos.items():
        lat = datos["lat"]
        lon = datos["lon"]
        habitantes = datos.get("habitantes", 0)
        descripcion = datos.get("descripcion", "")
        nombre_estacion = distritos_con_sensor.get(nombre_distrito)
        tiene_sensor = nombre_estacion is not None

        if tiene_sensor:
            rt_distrito = _dr.get(nombre_distrito)
            aqi_distrito = rt_distrito.get("aqi") if rt_distrito else None
            color_icono = _color_aqi_folium(aqi_distrito) if aqi_distrito else "blue"
            rt_bloque = _seccion_rt_popup(rt_distrito, meteo_rt, theme=t)
            popup_content = (
                f'<div style="font-family:Arial,sans-serif;font-size:12px;'
                f'min-width:200px;max-width:260px;background:{t["bg_popup"]};'
                f'color:{t["text"]};padding:4px;">'
                f"<b>{nombre_distrito}</b><br>"
                f'<span style="color:{t["text_muted"]};font-size:11px;">'
                f"{descripcion}</span>"
                f'<hr style="margin:4px 0;border-color:{t["popup_hr"]};">'
                f'<span style="color:{t["text_secondary"]};">'
                f"<b>Habitantes:</b> {habitantes:,}</span><br>"
                f'<b style="color:#3498db;">Sensor:</b> {nombre_estacion}'
                + rt_bloque
                + "</div>"
            )
        else:
            color_icono = "gray"
            popup_content = (
                f'<div style="font-family:Arial,sans-serif;font-size:12px;'
                f'min-width:200px;max-width:260px;background:{t["bg_popup"]};'
                f'color:{t["text"]};padding:4px;">'
                f"<b>{nombre_distrito}</b><br>"
                f'<span style="color:{t["text_muted"]};font-size:11px;">'
                f"{descripcion}</span>"
                f'<hr style="margin:4px 0;border-color:{t["popup_hr"]};">'
                f'<span style="color:{t["text_secondary"]};">'
                f"<b>Habitantes:</b> {habitantes:,}</span><br>"
                f'<span style="color:#e67e22;">⚠️ Sin sensor de contaminación '
                f"en este distrito. Se usan datos interpolados de estaciones "
                f"cercanas.</span>"
                f"</div>"
            )

        folium.Marker(
            location=[lat, lon],
            icon=folium.Icon(color=color_icono, icon="building", prefix="fa"),
            popup=folium.Popup(popup_content, max_width=250),
            tooltip=f"{nombre_distrito} | {habitantes:,} hab.",
        ).add_to(fg)

    fg.add_to(m)
    logger.debug("[Mapa] Capa distritos: %d marcadores añadidos.", len(distritos))


# ==============================================================================
# CAPA DE TRAFICO EN TIEMPO REAL
# ==============================================================================

# Mapeo de causa DGT a icono y color de Folium
_TRAFICO_ICONO_MAP = {
    "roadMaintenance": ("wrench", "orange"),
    "roadworks": ("wrench", "orange"),
    "accident": ("exclamation-triangle", "red"),
    "abnormalTraffic": ("car", "darkred"),
    "congestion": ("car", "darkred"),
    "infrastructureDamage": ("exclamation-triangle", "orange"),
    "obstruction": ("ban", "red"),
    "poorEnvironment": ("cloud", "blue"),
    "weatherRelated": ("cloud", "blue"),
}

_SEVERIDAD_ES = {
    "low": "Baja",
    "medium": "Media",
    "high": "Alta",
    "highest": "Muy alta",
}


def _anadir_capa_trafico(
    m, trafico_rt: Optional[dict], theme: Optional[dict] = None
) -> int:
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

    incidencias = trafico_rt.get("incidencias", [])
    if not incidencias:
        return 0

    try:
        import folium
    except ImportError:
        return 0

    t = theme or get_theme()
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
        icon_name, icon_color = _TRAFICO_ICONO_MAP.get(causa, ("info-sign", "blue"))

        # Tipo legible
        tipo_legible = (
            tipo_raw.replace("RoadOrCarriagewayOrLaneManagement", "Gestión de carril")
            .replace("AbnormalTraffic", "Tráfico anómalo")
            .replace("Accident", "Accidente")
            .replace("Conditions", "Condiciones")
        )

        causa_legible = (
            causa.replace("roadMaintenance", "Obras/mantenimiento")
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

        # Escapar valores de fuente externa (DGT) antes de inyectarlos en HTML
        tipo_legible_e = escape_html(tipo_legible)
        causa_legible_e = escape_html(causa_legible)
        sev_es_e = escape_html(sev_es)
        carretera_e = escape_html(carretera)
        municipio_e = escape_html(municipio)

        # Popup HTML
        popup_html = (
            f'<div style="font-family:Arial,sans-serif;min-width:200px;'
            f'max-width:260px;font-size:12px;background:{t["bg_popup"]};'
            f'color:{t["text"]};padding:4px;">'
            f'<b style="color:#ff7f0e;">🚗 Incidencia de tráfico</b><br>'
            f'<hr style="margin:4px 0;border-color:{t["popup_hr"]};">'
            f"<b>Tipo:</b> {tipo_legible_e}<br>"
            f"<b>Causa:</b> {causa_legible_e}<br>"
            f"<b>Severidad:</b> {sev_es_e}<br>"
            f"<b>Carretera:</b> {carretera_e}<br>"
        )
        if municipio:
            popup_html += f"<b>Municipio:</b> {municipio_e}<br>"
        if timestamp:
            popup_html += (
                f'<hr style="margin:4px 0;border-color:{t["popup_hr"]};">'
                f'<small style="color:{t["text_muted"]};">'
                f"Captura: {escape_html(timestamp[:16])}</small>"
            )
        popup_html += "</div>"

        # Tooltip
        tooltip_txt = (
            f"{causa_legible_e} — {carretera_e}" if carretera else causa_legible_e
        )

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
            "Parque lineal de ~9 km.<br>"
            "Zona de referencia de calidad ambiental en Valencia."
            "</div>",
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
            "Zona de referencia de calidad del aire en Valencia.<br>"
            "<small>Coordenadas: 39.4620°N, 0.3620°W</small>"
            "</div>",
            max_width=220,
        ),
    ).add_to(m)

    logger.debug("[Mapa] Jardin del Turia anadido al mapa.")


# ==============================================================================
# HELPERS AQI -> COLOR
# ==============================================================================


def _color_aqi(aqi: Optional[int]) -> str:
    """
    Devuelve un color hex segun el valor AQI.

    Args:
        aqi: Valor AQI entero (0-500+). None devuelve gris.

    Returns:
        Color hex.
    """
    if aqi is None:
        return "#7f7f7f"
    if aqi <= 50:
        return "#2ca02c"
    if aqi <= 100:
        return "#ffbb33"
    if aqi <= 150:
        return "#ff7f0e"
    return "#d62728"


def _color_aqi_folium(aqi: Optional[int]) -> str:
    """
    Devuelve nombre de color valido para folium.Icon segun AQI.

    Args:
        aqi: Valor AQI.

    Returns:
        Nombre de color Folium ('green', 'orange', 'red', 'gray').
    """
    if aqi is None:
        return "gray"
    if aqi <= 50:
        return "green"
    if aqi <= 100:
        return "orange"
    if aqi <= 150:
        return "orange"
    return "red"


# ==============================================================================
# CAPA DE SENSORES EN TIEMPO REAL
# ==============================================================================


def _anadir_capa_sensores_rt(
    m,
    contam_rt: Optional[dict],
    meteo_rt: Optional[dict],
    theme: Optional[dict] = None,
) -> int:
    """
    Anade una FeatureGroup '📡 Sensores RT' con marcadores por estacion.

    Cada estacion se muestra como un Marker con icono fa-broadcast-tower
    coloreado segun su AQI actual. El popup muestra TODOS los datos RT
    disponibles: AQI, contaminantes, temp, humedad, timestamp.

    Args:
        m: Objeto folium.Map.
        contam_rt: Resultado de cargar_contaminacion_realtime().
        meteo_rt: Resultado de cargar_meteo_realtime().

    Returns:
        Numero de marcadores anadidos.
    """
    if contam_rt is None:
        return 0

    estaciones = contam_rt.get("estaciones", [])
    if not estaciones:
        return 0

    try:
        import folium
    except ImportError:
        return 0

    t = theme or get_theme()
    fg = folium.FeatureGroup(name="📡 Sensores RT")
    n_added = 0
    rt_timestamp = contam_rt.get("timestamp", "")

    for est in estaciones:
        est_id = str(est.get("estacion_id", ""))
        coords = ESTACION_COORDS.get(est_id)
        if coords is None:
            # Intentar buscar via _UID_A_COORDS_KEY
            mapped = _UID_A_COORDS_KEY.get(est_id)
            if mapped:
                coords = ESTACION_COORDS.get(mapped)
        if coords is None:
            continue

        aqi = est.get("aqi")
        barrio = ESTACION_BARRIO_MAP.get(est_id, est.get("barrio", "Desconocido"))
        nombre = est.get("nombre", coords.get("nombre", est_id))
        nombre_e = escape_html(nombre)
        barrio_e = escape_html(barrio)
        icon_color = _color_aqi_folium(aqi)
        color_hex = _color_aqi(aqi)

        # Popup completo con todos los datos RT
        popup_parts = [
            f'<div style="font-family:Arial,sans-serif;min-width:240px;'
            f'max-width:300px;background:{t["bg_popup"]};color:{t["text"]};'
            f'padding:6px;">',
            f'<b style="font-size:13px;">📡 {nombre_e}</b><br>',
            f'<span style="color:{t["text_label"]};font-size:11px;">'
            f"Barrio: {barrio_e}</span>",
            f'<hr style="margin:5px 0;border-color:{t["popup_hr"]};">',
        ]

        # AQI
        if aqi is not None:
            nivel_txt = (
                "Buena"
                if aqi <= 50
                else (
                    "Moderada"
                    if aqi <= 100
                    else "Dañina (sensibles)" if aqi <= 150 else "Dañina"
                )
            )
            popup_parts.append(
                f'<div style="margin-bottom:4px;">'
                f'<b style="color:{color_hex};font-size:14px;">AQI: {aqi}</b> '
                f'<span style="color:{t["text_label"]};font-size:10px;">'
                f"{nivel_txt}</span>"
                f"</div>"
            )
            dominante = est.get("dominante", "")
            if dominante:
                popup_parts.append(
                    f'<div style="font-size:10px;color:{t["text_muted"]};">'
                    f"Contaminante dominante: {escape_html(dominante)}</div>"
                )

        # Contaminantes
        contam_filas = []
        for label, key, color_key in _CONTAM_RT_CAMPOS:
            val = est.get(key)
            if val is None:
                continue
            color_var = VARIABLE_COLORS.get(color_key, "#888")
            contam_filas.append(
                f'<div style="margin:1px 0;font-size:11px;">'
                f'<span style="color:{color_var};">{label}:</span> '
                f"<b>{val:.1f} µg/m³</b></div>"
            )
        if contam_filas:
            popup_parts.append(
                '<div style="margin-top:4px;">' + "".join(contam_filas) + "</div>"
            )

        # Temp y humedad del sensor (si disponible en iaqi) o meteo_rt
        meteo_partes = []
        temp_sensor = est.get("temp")
        hum_sensor = est.get("humedad")
        if temp_sensor is not None:
            meteo_partes.append(f"🌡️ {temp_sensor:.1f}°C")
        elif meteo_rt and meteo_rt.get("temp") is not None:
            meteo_partes.append(f"🌡️ {meteo_rt['temp']:.1f}°C")
        if hum_sensor is not None:
            meteo_partes.append(f"💧 {int(hum_sensor)}%")
        elif meteo_rt and meteo_rt.get("humedad") is not None:
            meteo_partes.append(f"💧 {int(meteo_rt['humedad'])}%")
        if meteo_partes:
            popup_parts.append(
                f'<div style="margin-top:3px;font-size:11px;'
                f'color:{t["text_secondary"]};">'
                + " &nbsp; ".join(meteo_partes)
                + "</div>"
            )

        # Timestamp
        frescura = _frescura_rt(rt_timestamp)
        if frescura:
            popup_parts.append(
                f'<div style="margin-top:4px;color:{t["text_faint"]};'
                f'font-size:10px;">Última captura: {frescura}</div>'
            )

        popup_parts.append("</div>")
        popup_html = "".join(popup_parts)

        # Tooltip
        aqi_txt = f"AQI: {aqi}" if aqi is not None else "AQI: —"
        tooltip_txt = f"📡 {barrio_e} | {aqi_txt} | Click para detalles"

        folium.Marker(
            location=[coords["lat"], coords["lon"]],
            icon=folium.Icon(
                color=icon_color,
                icon="broadcast-tower",
                prefix="fa",
            ),
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=tooltip_txt,
        ).add_to(fg)
        n_added += 1

    if n_added > 0:
        fg.add_to(m)
        logger.info("[Mapa] Capa sensores RT: %d marcadores.", n_added)

    return n_added


# ==============================================================================
# MAPA DE CALOR RT
# ==============================================================================


def _anadir_heatmap_rt(m, contam_rt: Optional[dict]) -> bool:
    """
    Anade una capa HeatMap con valores AQI de las estaciones RT.

    Args:
        m: Objeto folium.Map.
        contam_rt: Resultado de cargar_contaminacion_realtime().

    Returns:
        True si se anadio la capa, False si no habia datos.
    """
    if contam_rt is None:
        return False

    estaciones = contam_rt.get("estaciones", [])
    if not estaciones:
        return False

    try:
        from folium.plugins import HeatMap
    except ImportError:
        logger.warning("[Mapa] folium.plugins.HeatMap no disponible.")
        return False

    heat_data = []
    for est in estaciones:
        est_id = str(est.get("estacion_id", ""))
        coords = ESTACION_COORDS.get(est_id)
        if coords is None:
            mapped = _UID_A_COORDS_KEY.get(est_id)
            if mapped:
                coords = ESTACION_COORDS.get(mapped)
        if coords is None:
            continue

        aqi = est.get("aqi")
        if aqi is not None and aqi > 0:
            # Peso normalizado (AQI 0-300 → intensidad 0.1-1.0)
            peso = min(1.0, max(0.1, aqi / 200.0))
            heat_data.append([coords["lat"], coords["lon"], peso])

    if not heat_data:
        return False

    HeatMap(
        heat_data,
        name="🔥 Mapa de calor AQI",
        radius=40,
        blur=25,
        max_zoom=15,
        gradient={0.2: "#2ca02c", 0.5: "#ffbb33", 0.75: "#ff7f0e", 1.0: "#d62728"},
    ).add_to(m)
    logger.info("[Mapa] HeatMap RT añadido con %d puntos.", len(heat_data))
    return True


# ==============================================================================
# LEYENDA HTML PARA FOLIUM
# ==============================================================================


def _crear_leyenda_html(
    variable: str,
    umbral_oms: float,
    con_trafico: bool = False,
    con_rt: bool = False,
    rt_timestamp: str = "",
    theme: Optional[dict] = None,
) -> str:
    """
    Genera HTML para una leyenda flotante en el mapa.

    Args:
        variable: Nombre de la variable activa.
        umbral_oms: Valor del umbral OMS.
        con_trafico: Si True, incluye seccion de trafico en la leyenda.
        con_rt: Si True, incluye seccion de datos en tiempo real.
        rt_timestamp: Timestamp ISO de ultima captura RT.
        theme: Diccionario de tokens de tema.

    Returns:
        String HTML con la leyenda.
    """
    t = theme or get_theme()
    trafico_html = ""
    if con_trafico:
        trafico_html = (
            f'<hr style="border-color:{t["popup_hr"]};margin:5px 0;">'
            '<b style="font-size:11px;">🚗 Tráfico RT</b><br>'
            '<span style="color:#ff7f0e;">&#9873;</span>'
            " <small>Obras/mantenimiento</small><br>"
            '<span style="color:#d62728;">&#9873;</span>'
            " <small>Accidente/obstrucción</small><br>"
            '<span style="color:#85144b;">&#9873;</span>'
            " <small>Congestión</small><br>"
            '<span style="color:#1f77b4;">&#9873;</span>'
            " <small>Otros</small>"
        )

    rt_html = ""
    if con_rt:
        frescura = _frescura_rt(rt_timestamp) if rt_timestamp else ""
        ts_display = f" ({frescura})" if frescura else ""
        rt_html = (
            f'<hr style="border-color:{t["popup_hr"]};margin:5px 0;">'
            '<b style="font-size:11px;color:#17becf;">📡 Datos en tiempo real</b><br>'
            '<span style="color:#2ca02c;">&#9873;</span>'
            " <small>AQI ≤ 50 (Buena)</small><br>"
            '<span style="color:#ffbb33;">&#9873;</span>'
            " <small>AQI 51-100 (Moderada)</small><br>"
            '<span style="color:#ff7f0e;">&#9873;</span>'
            " <small>AQI 101-150 (Sensibles)</small><br>"
            '<span style="color:#d62728;">&#9873;</span>'
            " <small>AQI &gt; 150 (Dañina)</small><br>"
            f'<small style="color:{t["text_faint"]};">Última captura{ts_display}</small>'
        )

    return f"""
    <div style="
        position: fixed;
        bottom: 30px; left: 30px;
        z-index: 1000;
        background: {t["bg_legend"]};
        padding: 10px 14px;
        border-radius: 8px;
        font-family: Arial, sans-serif;
        font-size: 12px;
        color: {t["text_secondary"]};
        border: 1px solid {t["border"]};
        box-shadow: 2px 2px 8px rgba(0,0,0,0.3);
        max-height: 400px;
        overflow-y: auto;
    ">
        <b style="font-size:13px;">{variable}</b>
        <span style="color:{t["text_muted"]};font-size:10px;"> vs umbral OMS</span><br>
        <span style="color:#2ca02c;">&#9679;</span> Bueno (&lt;75% OMS)<br>
        <span style="color:#ffbb33;">&#9679;</span> Moderado (75-100% OMS)<br>
        <span style="color:#ff7f0e;">&#9679;</span> Alto (&gt;100% OMS)<br>
        <span style="color:#d62728;">&#9679;</span> Crítico (&gt;150% OMS)<br>
        <span style="color:#7f7f7f;">&#9679;</span> Sin datos<br>
        <hr style="border-color:{t["popup_hr"]};margin:5px 0;">
        <small>OMS: {umbral_oms} µg/m³</small><br>
        <span style="color:#2ca02c;">&#9135;&#9135;</span>
        <small style="color:#2ca02c;"> Jardí del Túria</small>
        {rt_html}
        {trafico_html}
    </div>
    """
