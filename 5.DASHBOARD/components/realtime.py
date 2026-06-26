# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Panel de Datos en Tiempo Real
==============================================================================

Banner colapsable que muestra los datos mas recientes capturados por
los scripts de streaming (AQICN, OpenWeatherMap, DGT).

Se renderiza antes de las tabs del dashboard, proporcionando un vistazo
rapido del estado actual de la ciudad.

Uso:
    from components.realtime import render_datos_realtime
    render_datos_realtime(datos)

Ruta: 5.DASHBOARD/components/realtime.py
Autor: Joan | Fecha: 2026
"""

import logging
from datetime import datetime
from typing import Optional

import streamlit as st

from config import UMBRALES_OMS
from streaming_background import estado_streaming
from utils.urban_quality_index import calcular_indice_urbano
from utils.formatters import escape_html

logger = logging.getLogger("Realtime")


# ==============================================================================
# UTILIDADES
# ==============================================================================

def _calcular_frescura(timestamp_str: str) -> str:
    """
    Calcula cuanto tiempo hace desde el timestamp dado.

    Args:
        timestamp_str: Timestamp ISO del momento de captura.

    Returns:
        String legible como 'Hace 5 minutos', 'Hace 2 horas', etc.
    """
    if not timestamp_str:
        return "Fecha desconocida"
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        # Quitar timezone para comparar con now()
        ts_naive = ts.replace(tzinfo=None)
        delta = datetime.now() - ts_naive
        minutos = int(delta.total_seconds() / 60)

        if minutos < 1:
            return "Hace menos de 1 minuto"
        elif minutos < 60:
            return f"Hace {minutos} minuto{'s' if minutos != 1 else ''}"
        elif minutos < 1440:
            horas = minutos // 60
            return f"Hace {horas} hora{'s' if horas != 1 else ''}"
        else:
            dias = minutos // 1440
            return f"Hace {dias} día{'s' if dias != 1 else ''}"
    except (ValueError, TypeError):
        return "Fecha desconocida"


def _icono_aqi(aqi: Optional[int]) -> str:
    """Retorna emoji segun nivel AQI."""
    if aqi is None:
        return ""
    if aqi <= 50:
        return "🟢"
    elif aqi <= 100:
        return "🟡"
    elif aqi <= 150:
        return "🟠"
    else:
        return "🔴"


def _nivel_aqi(aqi: Optional[int]) -> str:
    """Retorna texto descriptivo del nivel AQI."""
    if aqi is None:
        return "Sin datos"
    if aqi <= 50:
        return "Buena"
    elif aqi <= 100:
        return "Moderada"
    elif aqi <= 150:
        return "Dañina (sensibles)"
    elif aqi <= 200:
        return "Dañina"
    else:
        return "Muy dañina"


def _es_dato_antiguo(timestamp_str: str, umbral_minutos: int = 60) -> bool:
    """Comprueba si el dato tiene mas de umbral_minutos de antigüedad."""
    if not timestamp_str:
        return True
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        ts_naive = ts.replace(tzinfo=None)
        delta = datetime.now() - ts_naive
        return delta.total_seconds() > umbral_minutos * 60
    except (ValueError, TypeError):
        return True


def _severidad_color(severidad: str) -> str:
    """Color CSS segun severidad DGT."""
    colores = {
        "low": "#2ca02c",
        "medium": "#ffbb33",
        "high": "#ff7f0e",
        "highest": "#d62728",
    }
    return colores.get(severidad, "#7f7f7f")


# ==============================================================================
# COLUMNA: CALIDAD DEL AIRE
# ==============================================================================

def _render_calidad_aire(contam_rt: Optional[dict]) -> None:
    """Renderiza la columna de calidad del aire en tiempo real."""
    st.markdown(
        '<h5 style="margin:0 0 8px 0;">🌫️ Calidad del aire</h5>',
        unsafe_allow_html=True,
    )

    if contam_rt is None:
        st.caption("Sin datos de calidad del aire en tiempo real.")
        return

    frescura = _calcular_frescura(contam_rt["timestamp"])
    antiguo = _es_dato_antiguo(contam_rt["timestamp"])

    if antiguo:
        st.warning(f"Datos no recientes ({frescura}). Ejecuta streaming_master.py.")

    estaciones = contam_rt.get("estaciones", [])
    if not estaciones:
        st.caption("Sin estaciones disponibles.")
        return

    # AQI medio de todas las estaciones
    aqis = [e["aqi"] for e in estaciones if e.get("aqi") is not None]
    if aqis:
        aqi_medio = round(sum(aqis) / len(aqis))
        icono = _icono_aqi(aqi_medio)
        nivel = _nivel_aqi(aqi_medio)
        st.metric(
            label="Índice AQI medio",
            value=f"{icono} {aqi_medio}",
            help=f"Calidad: {nivel}. Media de {len(aqis)} estaciones.",
        )

    # Contaminantes principales (media de estaciones)
    for var, key in [("NO₂", "no2"), ("O₃", "o3"), ("PM10", "pm10"), ("PM2.5", "pm25")]:
        vals = [e[key] for e in estaciones if e.get(key) is not None]
        if vals:
            media = round(sum(vals) / len(vals), 1)
            var_clean = var.replace("₂", "2").replace("₃", "3")
            umbral = UMBRALES_OMS.get(var_clean)
            if umbral and media > umbral:
                st.markdown(
                    f"<small>{var}: <b style='color:#e74c3c;'>"
                    f"{media} µg/m³</b> (supera OMS: {umbral})</small>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<small>{var}: <b>{media} µg/m³</b></small>",
                    unsafe_allow_html=True,
                )

    st.caption(frescura)


# ==============================================================================
# COLUMNA: METEOROLOGIA
# ==============================================================================

def _render_meteorologia(meteo_rt: Optional[dict]) -> None:
    """Renderiza la columna de meteorologia en tiempo real."""
    st.markdown(
        '<h5 style="margin:0 0 8px 0;">🌤️ Meteorología</h5>',
        unsafe_allow_html=True,
    )

    if meteo_rt is None:
        st.caption("Sin datos meteorológicos en tiempo real.")
        return

    frescura = _calcular_frescura(meteo_rt["timestamp"])
    antiguo = _es_dato_antiguo(meteo_rt["timestamp"])

    if antiguo:
        st.warning(f"Datos no recientes ({frescura}). Ejecuta streaming_master.py.")

    temp = meteo_rt.get("temp")
    if temp is not None:
        sensacion = meteo_rt.get("sensacion")
        delta_str = None
        if sensacion is not None:
            diff = round(sensacion - temp, 1)
            if abs(diff) >= 0.5:
                delta_str = f"{diff:+.1f}°C sensación"
        st.metric(
            label="Temperatura",
            value=f"{temp:.1f}°C",
            delta=delta_str,
            delta_color="off",
        )

    humedad = meteo_rt.get("humedad")
    if humedad is not None:
        st.metric(label="Humedad", value=f"{humedad}%")

    descripcion = meteo_rt.get("descripcion", "")
    if descripcion:
        st.markdown(
            f"<small>Condición: <b>{escape_html(descripcion.capitalize())}</b></small>",
            unsafe_allow_html=True,
        )

    viento = meteo_rt.get("viento_ms")
    if viento is not None:
        kmh = round(viento * 3.6, 1)
        st.markdown(
            f"<small>Viento: <b>{kmh} km/h</b></small>",
            unsafe_allow_html=True,
        )

    presion = meteo_rt.get("presion")
    if presion is not None:
        st.markdown(
            f"<small>Presión: <b>{presion} hPa</b></small>",
            unsafe_allow_html=True,
        )

    st.caption(frescura)


# ==============================================================================
# COLUMNA: TRAFICO
# ==============================================================================

def _render_trafico(trafico_rt: Optional[dict]) -> None:
    """Renderiza la columna de trafico en tiempo real."""
    st.markdown(
        '<h5 style="margin:0 0 8px 0;">🚗 Tráfico</h5>',
        unsafe_allow_html=True,
    )

    if trafico_rt is None:
        st.caption("Sin datos de tráfico en tiempo real.")
        return

    frescura = _calcular_frescura(trafico_rt["timestamp"])
    antiguo = _es_dato_antiguo(trafico_rt["timestamp"])

    if antiguo:
        st.warning(f"Datos no recientes ({frescura}). Ejecuta streaming_master.py.")

    incs_valencia = trafico_rt.get("incidencias", [])
    total_espana = trafico_rt.get("total_incidencias", 0)

    st.metric(
        label="Incidencias activas (Valencia)",
        value=str(len(incs_valencia)),
        help=f"De {total_espana:,} incidencias totales en España.",
    )

    if incs_valencia:
        # Desglose por severidad
        sev_counts = {}
        for inc in incs_valencia:
            sev = inc.get("severidad", "unknown")
            sev_counts[sev] = sev_counts.get(sev, 0) + 1

        sev_labels = {
            "low": "Baja", "medium": "Media",
            "high": "Alta", "highest": "Muy alta",
        }
        partes = []
        for sev in ["highest", "high", "medium", "low"]:
            if sev in sev_counts:
                color = _severidad_color(sev)
                label = sev_labels.get(sev, sev)
                partes.append(
                    f"<span style='color:{color};font-weight:600;'>"
                    f"{sev_counts[sev]} {label}</span>"
                )
        if partes:
            st.markdown(
                f"<small>Severidad: {' · '.join(partes)}</small>",
                unsafe_allow_html=True,
            )

        # Carreteras afectadas (top 5)
        carreteras = {}
        for inc in incs_valencia:
            carr = inc.get("carretera", "")
            if carr:
                carreteras[carr] = carreteras.get(carr, 0) + 1
        if carreteras:
            top = sorted(carreteras.items(), key=lambda x: -x[1])[:5]
            lineas = [f"{escape_html(c)}: {n}" for c, n in top]
            st.markdown(
                "<small><b>Carreteras afectadas:</b><br>"
                + "<br>".join(lineas) + "</small>",
                unsafe_allow_html=True,
            )
    else:
        st.success("Sin incidencias activas en Valencia.")

    st.caption(frescura)


# ==============================================================================
# INDICADOR DE AUTO-ACTUALIZACION
# ==============================================================================

def _render_estado_streaming() -> None:
    """Muestra una línea con el estado del thread de streaming en background."""
    ultima = estado_streaming.get("ultima_ejecucion")
    en_curso = estado_streaming.get("en_ejecucion", False)
    ciclos = estado_streaming.get("ciclos_completados", 0)

    if ultima is None and not en_curso:
        st.caption("🔄 Auto-actualización en background: pendiente de primer ciclo...")
        return

    partes = []
    if en_curso:
        partes.append("⏳ Capturando datos ahora...")
    elif ultima is not None:
        frescura = _calcular_frescura(ultima.isoformat())
        partes.append(f"🔄 Última captura automática: {frescura}")

    if ciclos > 0:
        partes.append(f"({ciclos} ciclo{'s' if ciclos != 1 else ''} completado{'s' if ciclos != 1 else ''})")

    # Mostrar resultados del último ciclo
    resultados = estado_streaming.get("resultado_ultimo_ciclo")
    if resultados:
        exitosos = sum(1 for r in resultados if r.get("estado") == "exitoso")
        total = len(resultados)
        if exitosos == total:
            partes.append(f"— {exitosos}/{total} fuentes OK")
        else:
            partes.append(f"— {exitosos}/{total} fuentes OK")

    st.caption(" ".join(partes))


# ==============================================================================
# COMPONENTE PRINCIPAL
# ==============================================================================

def _render_indice_urbano(
    contam_rt: Optional[dict],
    meteo_rt: Optional[dict],
    trafico_rt: Optional[dict],
) -> None:
    """
    Renderiza el Indice de Calidad Urbana como KPI destacado.

    Muestra score global con gauge visual (barra de progreso HTML),
    nivel descriptivo y desglose por eje.

    Args:
        contam_rt: Datos RT de contaminacion.
        meteo_rt: Datos RT de meteorologia.
        trafico_rt: Datos RT de trafico.
    """
    indice = calcular_indice_urbano(contam_rt, meteo_rt, trafico_rt)
    if indice is None:
        return

    score = indice["score"]
    nivel = indice["nivel"]
    color = indice["color"]
    emoji = indice["emoji"]
    ejes = indice["ejes"]
    n_ejes = indice["ejes_disponibles"]

    # Etiquetas y emojis por eje
    eje_info = {
        "contaminacion": ("Aire", "🌫️"),
        "meteorologia": ("Meteo", "🌤️"),
        "trafico": ("Tráfico", "🚗"),
    }

    # Desglose compacto de ejes
    partes_ejes = []
    for eje_key, (label, ico) in eje_info.items():
        if eje_key in ejes:
            s = ejes[eje_key]["score"]
            partes_ejes.append(f"{ico} {label}: <b>{s:.1f}</b>")
        else:
            partes_ejes.append(
                f"{ico} {label}: <span style='color:#666;'>—</span>"
            )
    desglose_html = " &nbsp;·&nbsp; ".join(partes_ejes)

    # Porcentaje para la barra (0-100)
    pct = min(100, max(0, int(score * 10)))

    # Color de fondo de la barra con gradiente
    st.markdown(
        f"""
        <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
                    border-radius:12px;padding:16px 20px;margin-bottom:12px;">
            <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
                <div style="flex-shrink:0;">
                    <span style="font-size:1.6rem;">{emoji}</span>
                    <span style="font-size:1.8rem;font-weight:800;color:{color};
                                 margin-left:4px;">{score:.1f}</span>
                    <span style="color:#888;font-size:0.9rem;">/10</span>
                </div>
                <div style="flex-grow:1;min-width:200px;">
                    <div style="display:flex;justify-content:space-between;
                                margin-bottom:4px;">
                        <span style="font-weight:600;font-size:0.95rem;">
                            Índice de Calidad Urbana</span>
                        <span style="color:{color};font-weight:600;
                                     font-size:0.9rem;">{nivel}</span>
                    </div>
                    <div style="background:rgba(255,255,255,0.08);border-radius:6px;
                                height:10px;overflow:hidden;">
                        <div style="width:{pct}%;height:100%;background:{color};
                                    border-radius:6px;transition:width 0.5s;">
                        </div>
                    </div>
                    <div style="margin-top:6px;font-size:0.78rem;color:#999;">
                        {desglose_html}
                        &nbsp;·&nbsp;
                        <span style="color:#666;">{n_ejes}/3 ejes</span>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_datos_realtime(datos: dict) -> None:
    """
    Renderiza el banner de datos en tiempo real.

    Muestra el Indice de Calidad Urbana como KPI destacado encima de
    3 columnas: calidad del aire, meteorologia y trafico, con los datos
    mas recientes capturados por streaming.

    Args:
        datos: Diccionario con claves 'contam_rt', 'meteo_rt', 'trafico_rt'.
    """
    contam_rt = datos.get("contam_rt")
    meteo_rt = datos.get("meteo_rt")
    trafico_rt = datos.get("trafico_rt")

    # Si no hay ningun dato realtime, no mostrar nada
    if contam_rt is None and meteo_rt is None and trafico_rt is None:
        return

    with st.expander("📡 Datos en tiempo real — Valencia", expanded=True):
        # KPI destacado: Indice de Calidad Urbana
        _render_indice_urbano(contam_rt, meteo_rt, trafico_rt)

        col_aire, col_meteo, col_trafico = st.columns(3)

        with col_aire:
            _render_calidad_aire(contam_rt)

        with col_meteo:
            _render_meteorologia(meteo_rt)

        with col_trafico:
            _render_trafico(trafico_rt)

        # Indicador de auto-actualización en background
        _render_estado_streaming()

    logger.info("[Realtime] Panel renderizado")
