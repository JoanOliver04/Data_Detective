# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Rutas de Pulmon Limpio
==============================================================================
Ruta: 5.DASHBOARD/components/rutas_limpias.py
Autor: Joan | Fecha: 2026

Muestra rutas predefinidas por las zonas verdes y de menor contaminacion
de Valencia sobre un mapa Folium. Si hay datos RT de AQICN, colorea los
tramos segun AQI estimado de la zona.

Rutas disponibles:
  1. Jardi del Turia completo (~9 km)
  2. Jardins del Real / Vivers (~3.5 km)
  3. Albufera Norte — Pinedo / El Saler (~6 km)
  4. Cabanyal — Malvarrosa paseo maritimo (~4 km)
  5. Benimaclet — Huerta Nord (~5 km)
"""

import logging
import math
from typing import Dict, Optional

import folium
import streamlit as st
from streamlit_folium import st_folium

from theme import get_theme

logger = logging.getLogger(__name__)

# Altura del mapa embebido (px)
_MAP_HEIGHT = 520

# Colores AQI para tramos
_AQI_BUENO = "#2ca02c"  # AQI < 50
_AQI_MODERADO = "#f0c428"  # AQI 50-100
_AQI_MALO = "#e74c3c"  # AQI > 100
_COLOR_RUTA_DEFAULT = "#2ca02c"


# ==============================================================================
# DEFINICION DE RUTAS
# ==============================================================================

RUTAS: Dict[str, dict] = {
    "Ruta Jardí del Túria completo": {
        "nombre": "Ruta Jardí del Túria completo",
        "descripcion": (
            "Recorrido completo del antiguo cauce del río Turia, "
            "de Bioparc a la Ciudad de las Artes y las Ciencias. "
            "El mayor parque urbano lineal de España."
        ),
        "distancia_km": 9.2,
        "tiempo_estimado_min": 120,
        "coordenadas": [
            [39.4785, -0.4119],  # Bioparc / Cabecera oeste
            [39.4793, -0.4040],  # Parc de Capçalera
            [39.4800, -0.3967],  # Pont Nou d'Octubre
            [39.4805, -0.3903],  # Pont de Campanar
            [39.4808, -0.3838],  # Jardins de Monforte (altura)
            [39.4797, -0.3782],  # Pont de les Arts / IVAM
            [39.4788, -0.3756],  # Torres de Serranos (norte)
            [39.4770, -0.3718],  # Pont de Fusta
            [39.4752, -0.3690],  # Pont del Real
            [39.4729, -0.3648],  # Palau de la Música
            [39.4700, -0.3608],  # Pont de l'Àngel Custodi
            [39.4670, -0.3572],  # Pont de l'Exposició / Pont de la Mar
            [39.4639, -0.3540],  # Gulliver / Parc infantil
            [39.4600, -0.3510],  # Pont de Monteolivete
            [39.4570, -0.3485],  # L'Umbracle / Museu de les Ciències
            [39.4545, -0.3470],  # L'Hemisfèric
            [39.4529, -0.3456],  # Palau de les Arts Reina Sofía
            [39.4520, -0.3430],  # L'Àgora / final CAC
        ],
        "puntos_interes": [
            {
                "nombre": "Bioparc",
                "lat": 39.4785,
                "lon": -0.4119,
                "descripcion": "Zoo de inmersión — inicio de la ruta",
            },
            {
                "nombre": "Torres de Serranos",
                "lat": 39.4788,
                "lon": -0.3756,
                "descripcion": "Portal gótico del siglo XIV",
            },
            {
                "nombre": "Palau de la Música",
                "lat": 39.4729,
                "lon": -0.3648,
                "descripcion": "Auditorio al aire libre junto al río",
            },
            {
                "nombre": "Gulliver",
                "lat": 39.4639,
                "lon": -0.3540,
                "descripcion": "Parque infantil gigante — zona de descanso",
            },
            {
                "nombre": "Ciudad de las Artes y las Ciencias",
                "lat": 39.4545,
                "lon": -0.3470,
                "descripcion": "Complejo cultural de Calatrava — final de la ruta",
            },
        ],
    },
    "Ruta Jardins del Real / Vivers": {
        "nombre": "Ruta Jardins del Real / Vivers",
        "descripcion": (
            "Paseo circular por los jardines históricos de Viveros, "
            "el Jardín Botánico y el entorno del Pla del Real. "
            "Zona arbolada con baja exposición al tráfico."
        ),
        "distancia_km": 3.5,
        "tiempo_estimado_min": 50,
        "coordenadas": [
            [39.4785, -0.3703],  # Entrada Viveros desde Pont de Fusta
            [39.4793, -0.3685],  # Jardins del Real — zona norte
            [39.4800, -0.3665],  # Estanque de Viveros
            [39.4798, -0.3640],  # Rosaleda
            [39.4788, -0.3628],  # Museo de Ciencias Naturales
            [39.4775, -0.3640],  # Passeig de la Petxina (sur viveros)
            [39.4760, -0.3668],  # Jardí Botànic — entrada este
            [39.4750, -0.3698],  # Jardí Botànic — invernaderos
            [39.4748, -0.3725],  # Gran Via Fernando el Católico
            [39.4758, -0.3738],  # Pont del Real — lado sur
            [39.4770, -0.3718],  # Pont del Real — lado norte
            [39.4785, -0.3703],  # Cierre bucle en Viveros
        ],
        "puntos_interes": [
            {
                "nombre": "Jardins del Real (Viveros)",
                "lat": 39.4793,
                "lon": -0.3685,
                "descripcion": "Parque histórico con árboles centenarios",
            },
            {
                "nombre": "Jardí Botànic",
                "lat": 39.4750,
                "lon": -0.3698,
                "descripcion": "Jardín botánico universitario (fundado 1567)",
            },
            {
                "nombre": "Museu de Ciències Naturals",
                "lat": 39.4788,
                "lon": -0.3628,
                "descripcion": "Museo de historia natural dentro de Viveros",
            },
        ],
    },
    "Ruta Albufera Norte — Pinedo / El Saler": {
        "nombre": "Ruta Albufera Norte — Pinedo / El Saler",
        "descripcion": (
            "Recorrido costero desde Pinedo hasta la entrada del "
            "Parque Natural de l'Albufera. Playas vírgenes, "
            "dunas y pinares con el aire más limpio de la zona."
        ),
        "distancia_km": 6.0,
        "tiempo_estimado_min": 80,
        "coordenadas": [
            [39.4320, -0.3325],  # Playa de Pinedo — norte
            [39.4280, -0.3310],  # Pinedo — zona central
            [39.4230, -0.3290],  # Senda entre dunas
            [39.4180, -0.3275],  # Platja del Saler — norte
            [39.4130, -0.3268],  # Pasarela dunar
            [39.4080, -0.3262],  # Playa del Saler — centro
            [39.4030, -0.3260],  # Pinar del Saler
            [39.3985, -0.3265],  # Devesa del Saler — entrada
            [39.3940, -0.3275],  # Sendero pinada
            [39.3895, -0.3290],  # Mirador Albufera — norte
            [39.3850, -0.3310],  # Entrada Parc Natural
        ],
        "puntos_interes": [
            {
                "nombre": "Playa de Pinedo",
                "lat": 39.4320,
                "lon": -0.3325,
                "descripcion": "Playa familiar — inicio de ruta",
            },
            {
                "nombre": "Devesa del Saler",
                "lat": 39.3985,
                "lon": -0.3265,
                "descripcion": "Bosque dunar protegido de pinos y enebros",
            },
            {
                "nombre": "Mirador Albufera Norte",
                "lat": 39.3895,
                "lon": -0.3290,
                "descripcion": "Vistas al lago de l'Albufera",
            },
        ],
    },
    "Ruta Cabanyal — Malvarrosa": {
        "nombre": "Ruta Cabanyal — Malvarrosa",
        "descripcion": (
            "Paseo marítimo de Las Arenas a la Patacona. Brisa marina "
            "constante que dispersa contaminantes. Ideal al amanecer."
        ),
        "distancia_km": 4.0,
        "tiempo_estimado_min": 55,
        "coordenadas": [
            [39.4545, -0.3250],  # Marina Real Juan Carlos I
            [39.4558, -0.3238],  # Platja de les Arenes — sur
            [39.4580, -0.3230],  # Platja de les Arenes — centro
            [39.4605, -0.3218],  # Passeig Marítim — Cabanyal
            [39.4630, -0.3205],  # Platja del Cabanyal
            [39.4660, -0.3192],  # Platja de la Malvarrosa — sur
            [39.4690, -0.3180],  # Malvarrosa — centro
            [39.4720, -0.3168],  # Malvarrosa — norte
            [39.4750, -0.3155],  # Límite Alboraya
            [39.4780, -0.3140],  # Platja de la Patacona — sur
            [39.4810, -0.3128],  # Patacona — centro
            [39.4835, -0.3118],  # Patacona — norte (final)
        ],
        "puntos_interes": [
            {
                "nombre": "Marina Real Juan Carlos I",
                "lat": 39.4545,
                "lon": -0.3250,
                "descripcion": "Puerto deportivo — inicio de la ruta",
            },
            {
                "nombre": "Platja de la Malvarrosa",
                "lat": 39.4690,
                "lon": -0.3180,
                "descripcion": "Playa principal del barrio marinero",
            },
            {
                "nombre": "La Patacona",
                "lat": 39.4810,
                "lon": -0.3128,
                "descripcion": "Playa de Alboraya — chiringuitos y vistas",
            },
        ],
    },
    "Ruta Benimaclet — Huerta Nord": {
        "nombre": "Ruta Benimaclet — Huerta Nord",
        "descripcion": (
            "Salida desde el barrio de Benimaclet hacia la huerta norte "
            "de Valencia. Caminos agrícolas entre campos de naranjos "
            "y alquerías históricas. Aire limpio rural."
        ),
        "distancia_km": 5.0,
        "tiempo_estimado_min": 70,
        "coordenadas": [
            [39.4870, -0.3570],  # Plaça de Benimaclet
            [39.4890, -0.3555],  # Carrer de Dolores Marqués
            [39.4915, -0.3540],  # Entrada huerta — camino agrícola
            [39.4945, -0.3528],  # Alquería del Moro
            [39.4975, -0.3515],  # Cruce Camí de Vera
            [39.5005, -0.3505],  # Campos de naranjos
            [39.5035, -0.3490],  # Alquería de Barrinto
            [39.5060, -0.3478],  # Acequia de Mestalla
            [39.5085, -0.3468],  # Camí del Farinós
            [39.5110, -0.3460],  # Ermita de Vera
            [39.5135, -0.3470],  # Retorno — borde huerta
            [39.5100, -0.3500],  # Camí de les Fonts
            [39.5050, -0.3525],  # Vuelta por camino paralelo
            [39.4980, -0.3545],  # Huertos urbanos Benimaclet
            [39.4870, -0.3570],  # Cierre en Plaça de Benimaclet
        ],
        "puntos_interes": [
            {
                "nombre": "Plaça de Benimaclet",
                "lat": 39.4870,
                "lon": -0.3570,
                "descripcion": "Centro del barrio — terrazas y vida local",
            },
            {
                "nombre": "Alquería del Moro",
                "lat": 39.4945,
                "lon": -0.3528,
                "descripcion": "Alquería histórica del siglo XV",
            },
            {
                "nombre": "Acequia de Mestalla",
                "lat": 39.5060,
                "lon": -0.3478,
                "descripcion": "Acequia medieval del sistema de riego de l'Horta",
            },
            {
                "nombre": "Ermita de Vera",
                "lat": 39.5110,
                "lon": -0.3460,
                "descripcion": "Ermita rural con vistas a la huerta",
            },
        ],
    },
}


# ==============================================================================
# CALCULO DE AQI POR TRAMO
# ==============================================================================


def _distancia_aprox(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia aproximada en km entre dos puntos (formula Haversine simplificada)."""
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _aqi_color(aqi: float) -> str:
    """Retorna el color hex segun el nivel AQI."""
    if aqi < 50:
        return _AQI_BUENO
    elif aqi <= 100:
        return _AQI_MODERADO
    return _AQI_MALO


def _aqi_estacion_mas_cercana(
    lat: float,
    lon: float,
    estaciones: list,
) -> Optional[int]:
    """
    Retorna el AQI de la estacion RT mas cercana al punto dado.

    Args:
        lat: Latitud del punto del tramo.
        lon: Longitud del punto del tramo.
        estaciones: Lista de dicts de estaciones RT con 'aqi', 'lat'/'lon'
                    (o extraidos de coordenadas conocidas).

    Returns:
        AQI entero de la estacion mas cercana, o None si sin datos.
    """
    mejor_dist = float("inf")
    mejor_aqi = None

    for est in estaciones:
        aqi = est.get("aqi")
        est_lat = est.get("lat")
        est_lon = est.get("lon")
        if aqi is None or est_lat is None or est_lon is None:
            continue

        dist = _distancia_aprox(lat, lon, est_lat, est_lon)
        if dist < mejor_dist:
            mejor_dist = dist
            mejor_aqi = aqi

    return mejor_aqi


def _preparar_estaciones_con_coords(contam_rt: Optional[dict]) -> list:
    """
    Enriquece las estaciones RT con coordenadas conocidas de ESTACION_COORDS.

    Las estaciones de contam_rt no siempre tienen lat/lon, pero maps.py
    define ESTACION_COORDS con las coordenadas verificadas.

    Args:
        contam_rt: Diccionario RT de AQICN.

    Returns:
        Lista de estaciones con aqi, lat, lon.
    """
    if contam_rt is None:
        return []

    # Importar coords conocidas
    try:
        from components.maps import ESTACION_COORDS
    except ImportError:
        ESTACION_COORDS = {}

    estaciones_out = []
    for est in contam_rt.get("estaciones", []):
        aqi = est.get("aqi")
        if aqi is None:
            continue

        lat = est.get("lat")
        lon = est.get("lon")

        # Si no tiene coords, buscar en ESTACION_COORDS por id
        if lat is None or lon is None:
            est_id = est.get("estacion_id", "")
            coords = ESTACION_COORDS.get(est_id, {})
            lat = coords.get("lat")
            lon = coords.get("lon")

        if lat is not None and lon is not None:
            estaciones_out.append({"aqi": int(aqi), "lat": lat, "lon": lon})

    return estaciones_out


# ==============================================================================
# MAPA FOLIUM
# ==============================================================================


def _crear_mapa_ruta(
    ruta: dict,
    estaciones_rt: list,
) -> folium.Map:
    """
    Crea el mapa Folium con la ruta dibujada y puntos de interes.

    Si hay estaciones RT, colorea los tramos segun AQI de la estacion
    mas cercana. Si no, toda la ruta se dibuja en verde.

    Args:
        ruta: Diccionario de ruta con 'coordenadas' y 'puntos_interes'.
        estaciones_rt: Lista de estaciones con aqi, lat, lon (puede ser vacia).

    Returns:
        Objeto folium.Map listo para renderizar.
    """
    coords = ruta["coordenadas"]

    # Centro del mapa: punto medio de la ruta
    lat_media = sum(c[0] for c in coords) / len(coords)
    lon_media = sum(c[1] for c in coords) / len(coords)

    m = folium.Map(
        location=[lat_media, lon_media],
        zoom_start=14,
        tiles=get_theme()["folium_tiles"],
        control_scale=True,
    )

    # Dibujar tramos
    if estaciones_rt:
        # Colorear tramos segun AQI de estacion mas cercana
        for i in range(len(coords) - 1):
            p1 = coords[i]
            p2 = coords[i + 1]
            mid_lat = (p1[0] + p2[0]) / 2
            mid_lon = (p1[1] + p2[1]) / 2

            aqi = _aqi_estacion_mas_cercana(mid_lat, mid_lon, estaciones_rt)
            color = _aqi_color(aqi) if aqi is not None else _COLOR_RUTA_DEFAULT

            folium.PolyLine(
                locations=[p1, p2],
                color=color,
                weight=5,
                opacity=0.85,
                tooltip=f"AQI ≈ {aqi}" if aqi is not None else "Sin datos AQI",
            ).add_to(m)
    else:
        # Ruta completa en verde
        folium.PolyLine(
            locations=coords,
            color=_COLOR_RUTA_DEFAULT,
            weight=4,
            opacity=0.8,
            tooltip=ruta["nombre"],
        ).add_to(m)

    # Marcadores de inicio y fin
    folium.Marker(
        location=coords[0],
        icon=folium.Icon(color="green", icon="play", prefix="fa"),
        tooltip="Inicio",
        popup=f"<b>Inicio</b><br>{ruta['nombre']}",
    ).add_to(m)

    folium.Marker(
        location=coords[-1],
        icon=folium.Icon(color="red", icon="flag-checkered", prefix="fa"),
        tooltip="Final",
        popup=f"<b>Final</b><br>{ruta['nombre']}",
    ).add_to(m)

    # Puntos de interes
    for poi in ruta.get("puntos_interes", []):
        folium.Marker(
            location=[poi["lat"], poi["lon"]],
            icon=folium.Icon(color="blue", icon="info-sign"),
            tooltip=poi["nombre"],
            popup=folium.Popup(
                f"<b>{poi['nombre']}</b><br>"
                f"<span style='font-size:0.9em;'>{poi['descripcion']}</span>",
                max_width=250,
            ),
        ).add_to(m)

    # Leyenda si hay datos AQI
    if estaciones_rt:
        legend_html = (
            '<div style="position:fixed;bottom:30px;left:30px;z-index:1000;'
            "background:rgba(0,0,0,0.75);padding:10px 14px;border-radius:8px;"
            'font-size:12px;color:#fff;line-height:1.6;">'
            "<b>Calidad del aire (AQI)</b><br>"
            f'<span style="color:{_AQI_BUENO};">━━</span> Buena (&lt;50)<br>'
            f'<span style="color:{_AQI_MODERADO};">━━</span> Moderada (50-100)<br>'
            f'<span style="color:{_AQI_MALO};">━━</span> Mala (&gt;100)'
            "</div>"
        )
        m.get_root().html.add_child(folium.Element(legend_html))

    return m


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================


def render_mapa_rutas_limpias(contam_rt: Optional[dict] = None) -> None:
    """
    Renderiza la tab completa de Rutas Limpias.

    Muestra un selector de ruta, KPIs (distancia, tiempo, AQI estimado),
    y el mapa Folium con la ruta seleccionada.

    Args:
        contam_rt: Datos RT de AQICN (de cargar_contaminacion_realtime).
                   Si None, la ruta se muestra sin coloreo AQI.
    """
    st.markdown(
        '<div class="section-header">'
        "<h3>🌿 Rutas Pulmón Limpio</h3>"
        "<p>Recorridos por las zonas verdes y de menor contaminación de Valencia. "
        "Ideales para deporte al aire libre y paseo.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Selector de ruta
    nombres = list(RUTAS.keys())
    seleccion = st.selectbox(
        "Elige una ruta",
        options=nombres,
        help="Cada ruta tiene coordenadas reales y puntos de interés verificados.",
    )

    ruta = RUTAS[seleccion]

    # KPIs de la ruta
    estaciones_rt = _preparar_estaciones_con_coords(contam_rt)

    # Calcular AQI medio estimado para la zona de la ruta
    aqi_medio = None
    if estaciones_rt:
        coords = ruta["coordenadas"]
        aqis_tramo = []
        for coord in coords:
            aqi = _aqi_estacion_mas_cercana(coord[0], coord[1], estaciones_rt)
            if aqi is not None:
                aqis_tramo.append(aqi)
        if aqis_tramo:
            aqi_medio = round(sum(aqis_tramo) / len(aqis_tramo))

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Distancia",
        value=f"{ruta['distancia_km']:.1f} km",
        help="Distancia total aproximada de la ruta.",
    )
    c2.metric(
        label="Tiempo estimado",
        value=f"{ruta['tiempo_estimado_min']} min",
        help="Tiempo estimado caminando a paso tranquilo.",
    )

    if aqi_medio is not None:
        if aqi_medio < 50:
            aqi_txt = "🟢 Buena"
        elif aqi_medio <= 100:
            aqi_txt = "🟡 Moderada"
        else:
            aqi_txt = "🔴 Mala"
        c3.metric(
            label="AQI medio zona",
            value=str(aqi_medio),
            delta=aqi_txt,
            delta_color="off",
            help="Índice de calidad del aire medio estimado en la zona de la ruta (datos RT).",
        )
    else:
        c3.metric(
            label="AQI medio zona",
            value="—",
            help="Sin datos RT disponibles. Ejecuta streaming_master.py.",
        )

    c4.metric(
        label="Puntos de interés",
        value=str(len(ruta.get("puntos_interes", []))),
        help="Lugares destacados a lo largo de la ruta.",
    )

    # Descripcion de la ruta
    st.markdown(
        f'<div style="background:rgba(44,160,44,0.06);border-left:4px solid #2ca02c;'
        f"padding:10px 14px;border-radius:6px;margin:8px 0 12px 0;"
        f'font-size:0.92rem;">'
        f'{ruta["descripcion"]}'
        f"</div>",
        unsafe_allow_html=True,
    )

    # Mapa
    mapa = _crear_mapa_ruta(ruta, estaciones_rt)
    st_folium(mapa, height=_MAP_HEIGHT, use_container_width=True)

    # Leyenda de puntos de interes
    pois = ruta.get("puntos_interes", [])
    if pois:
        with st.expander(f"📍 Puntos de interés ({len(pois)})"):
            for poi in pois:
                st.markdown(f"**{poi['nombre']}** — {poi['descripcion']}")

    logger.info("[RutasLimpias] Ruta '%s' renderizada.", seleccion)
