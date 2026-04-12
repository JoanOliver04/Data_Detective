# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Sidebar - Filtros Globales del Dashboard
==============================================================================
Ruta: 5.DASHBOARD/components/sidebar.py
Autor: Joan | Fecha: 2026
"""

from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import streamlit as st
from config import VARIABLES_PRINCIPALES, ESTACION_BARRIO_MAP
from components.realtime import _calcular_frescura
from streaming_background import _ejecutar_ciclo, _lock, estado_streaming
from theme import THEME_TOGGLE_KEY

FiltrosGlobales = Dict[str, Any]

# Umbrales de frescura para semáforo RT (en minutos)
_RT_VERDE_MINUTOS = 30
_RT_AMARILLO_MINUTOS = 120


def render_sidebar(
    df_contam: Optional[pd.DataFrame],
    df_eventos: Optional[pd.DataFrame],
    df_meteo: Optional[pd.DataFrame] = None,
    df_trafico: Optional[pd.DataFrame] = None,
    datos_rt: Optional[dict] = None,
) -> FiltrosGlobales:
    """
    Renderiza el sidebar y retorna filtros seleccionados.

    Args:
        df_contam: DataFrame de contaminacion historica.
        df_eventos: DataFrame de impacto de eventos.
        df_meteo: DataFrame de meteorologia historica.
        df_trafico: DataFrame de trafico historico.
        datos_rt: Dict con 'contam_rt', 'meteo_rt', 'trafico_rt'.

    Returns:
        Dict con: anio_min, anio_max, variable, barrios, tipos_evento.
    """
    with st.sidebar:
        st.markdown(
            '<div style="text-align:center;padding:0.5rem 0 1rem 0;">'
            '<h1 style="font-size:1.6rem;margin-bottom:0.2rem;">Data Detective</h1>'
            '<p style="font-size:0.85rem;color:var(--dd-text-label);margin:0;">'
            'Valencia - Análisis Urbano</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.toggle("🌙 Tema oscuro", value=True, key=THEME_TOGGLE_KEY)
        st.divider()
        st.subheader("Filtros Globales")

        filtros = {}
        filtros["anio_min"], filtros["anio_max"] = _filtro_rango_anios(df_contam)
        filtros["variable"] = _filtro_variable(df_contam)
        filtros["barrios"] = _filtro_barrios(df_contam)
        filtros["tipos_evento"] = _filtro_tipos_evento(df_eventos)

        st.divider()
        _render_boton_actualizar()

        st.divider()
        _render_info_datos(df_contam, df_meteo, df_trafico, df_eventos, datos_rt)

        hora_actual = datetime.now().strftime("%H:%M:%S")
        st.caption(f"🔄 Última actualización: {hora_actual}")

        return filtros


# ==============================================================================
# FILTROS
# ==============================================================================

def _filtro_rango_anios(df_contam):
    if df_contam is not None and "anio" in df_contam.columns:
        anio_min = int(df_contam["anio"].min())
        anio_max = int(df_contam["anio"].max())
    else:
        anio_min, anio_max = 2020, 2026
    rango = st.slider(
        "Rango de años",
        min_value=anio_min, max_value=anio_max,
        value=(anio_min, anio_max),
        help="Filtra datos por periodo temporal.",
    )
    return rango[0], rango[1]


def _filtro_variable(df_contam):
    if df_contam is not None and "variable" in df_contam.columns:
        disponibles = sorted(df_contam["variable"].unique().tolist())
        opciones = [v for v in VARIABLES_PRINCIPALES if v in disponibles]
        if not opciones:
            opciones = disponibles
    else:
        opciones = VARIABLES_PRINCIPALES
    default_idx = opciones.index("O3") if "O3" in opciones else 0
    return st.selectbox(
        "Variable contaminante", options=opciones, index=default_idx,
        help="O3 y NO2 son los más relevantes para calidad del aire urbana.",
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _opciones_barrios(df_contam: Optional[pd.DataFrame]) -> list:
    """Calcula la lista de barrios disponibles. Cacheada 1 h."""
    if df_contam is not None and "barrio" in df_contam.columns:
        return sorted(df_contam["barrio"].dropna().unique().tolist())
    return sorted(set(ESTACION_BARRIO_MAP.values()))


@st.cache_data(ttl=3600, show_spinner=False)
def _opciones_tipos_evento(df_eventos: Optional[pd.DataFrame]) -> list:
    """Calcula la lista de tipos de evento disponibles. Cacheada 1 h."""
    if df_eventos is not None and "tipo_evento" in df_eventos.columns:
        return sorted(df_eventos["tipo_evento"].dropna().unique().tolist())
    return []


def _filtro_barrios(df_contam):
    disponibles = _opciones_barrios(df_contam)
    return st.multiselect(
        "Distritos", options=disponibles, default=[],
        help="Si no seleccionas ninguno, se muestran todos.",
    )


def _filtro_tipos_evento(df_eventos):
    disponibles = _opciones_tipos_evento(df_eventos)
    if not disponibles:
        st.caption("Tipos de evento: sin datos disponibles")
        return []
    return st.multiselect(
        "Tipo de evento", options=disponibles, default=[],
        help="Si no seleccionas ninguno, se muestran todos.",
    )


# ==============================================================================
# ESTADO DE DATOS
# ==============================================================================

def _semaforo_rt(timestamp_str: Optional[str]):
    """
    Calcula el semaforo de frescura para una fuente RT.

    Args:
        timestamp_str: Timestamp ISO de ultima captura. None si sin datos.

    Returns:
        Tupla (emoji, color_hex, texto_frescura).
        emoji: '🟢', '🟡' o '🔴'.
    """
    if not timestamp_str:
        return "🔴", "#d62728", "Sin datos"
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        delta_min = int((datetime.now() - ts.replace(tzinfo=None)).total_seconds() / 60)
    except (ValueError, TypeError):
        return "🔴", "#d62728", "Sin datos"

    frescura = _calcular_frescura(timestamp_str)
    if delta_min < _RT_VERDE_MINUTOS:
        return "🟢", "#2ca02c", frescura
    if delta_min < _RT_AMARILLO_MINUTOS:
        return "🟡", "#ffbb33", frescura
    return "🔴", "#d62728", frescura


def _linea_historico(nombre: str, df: Optional[pd.DataFrame], extra: str = "") -> None:
    """
    Renderiza una fila de estado para un dataset historico.

    Args:
        nombre: Nombre del dataset.
        df: DataFrame del dataset (None si no disponible).
        extra: Texto adicional (ej. rango de años).
    """
    if df is not None and not df.empty:
        n = len(df)
        texto = f"{nombre}: Cargado ({n:,} registros{extra})"
        st.markdown(
            f'<small><span style="color:#2ca02c;">●</span> {texto}</small>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<small><span style="color:#d62728;">○</span> '
            f'{nombre}: No disponible</small>',
            unsafe_allow_html=True,
        )


def _linea_rt(nombre: str, timestamp_str: Optional[str]) -> None:
    """
    Renderiza una fila de estado para una fuente RT.

    Args:
        nombre: Nombre de la fuente.
        timestamp_str: Timestamp ISO de ultima captura.
    """
    emoji, color, frescura = _semaforo_rt(timestamp_str)
    nivel = "Activo" if color == "#2ca02c" else (
        "Antiguo" if color == "#ffbb33" else "Sin datos"
    )
    st.markdown(
        f'<small>📡 {nombre}: '
        f'<span style="color:{color};">{emoji} {nivel}</span> '
        f'<span style="color:#555;">({frescura})</span></small>',
        unsafe_allow_html=True,
    )


def _render_boton_actualizar() -> None:
    """
    Renderiza el botón de actualización manual de datos.

    Ejecuta el ciclo completo de streaming de forma síncrona, mostrando
    un spinner. Limpia la caché de Streamlit al terminar para forzar
    una recarga de los datos frescos.
    """
    en_curso = bool(estado_streaming.get("en_ejecucion"))

    if st.button(
        "🔄 Actualizar datos",
        use_container_width=True,
        disabled=en_curso,
        help="Ejecuta los módulos de streaming para capturar datos frescos.",
    ):
        if not _lock.acquire(blocking=False):
            st.warning("Ya hay una actualización en curso. Espera unos segundos.")
            return

        try:
            estado_streaming["en_ejecucion"] = True
            with st.spinner("Actualizando datos... (puede tardar ~1 min)"):
                _ejecutar_ciclo()

            st.cache_data.clear()

            resultados = estado_streaming.get("resultado_ultimo_ciclo") or []
            exitosos = sum(1 for r in resultados if r.get("estado") == "exitoso")
            total = len(resultados)
            hora = datetime.now().strftime("%H:%M:%S")
            st.session_state["_actualizacion_msg"] = (
                f"✅ {exitosos}/{total} módulos actualizados ({hora})"
            )
        finally:
            estado_streaming["en_ejecucion"] = False
            try:
                _lock.release()
            except RuntimeError:
                pass
        st.rerun()

    msg = st.session_state.pop("_actualizacion_msg", None)
    if msg:
        st.success(msg)


def _render_info_datos(
    df_contam: Optional[pd.DataFrame],
    df_meteo: Optional[pd.DataFrame],
    df_trafico: Optional[pd.DataFrame],
    df_eventos: Optional[pd.DataFrame],
    datos_rt: Optional[dict],
) -> None:
    """
    Muestra el estado de todos los datasets (historicos y RT).

    Args:
        df_contam: DataFrame de contaminacion historica.
        df_meteo: DataFrame de meteorologia historica.
        df_trafico: DataFrame de trafico historico.
        df_eventos: DataFrame de eventos.
        datos_rt: Dict con 'contam_rt', 'meteo_rt', 'trafico_rt'.
    """
    st.markdown(
        '<small style="color:#aaa;font-weight:600;">Estado de los datos</small>',
        unsafe_allow_html=True,
    )

    # --- Datos históricos ---
    st.markdown(
        '<small style="color:#666;">— Históricos —</small>',
        unsafe_allow_html=True,
    )

    extra_contam = ""
    if df_contam is not None and "anio" in df_contam.columns:
        extra_contam = f" · {int(df_contam['anio'].min())}-{int(df_contam['anio'].max())}"
    _linea_historico("Contaminación", df_contam, extra_contam)
    _linea_historico("Meteorología", df_meteo)
    _linea_historico("Tráfico", df_trafico)
    _linea_historico("Eventos", df_eventos)

    # --- Datos en tiempo real ---
    rt = datos_rt or {}
    contam_rt = rt.get("contam_rt")
    meteo_rt = rt.get("meteo_rt")
    trafico_rt = rt.get("trafico_rt")

    hay_alguno_rt = any(x is not None for x in (contam_rt, meteo_rt, trafico_rt))

    st.markdown(
        '<small style="color:#666;">— Tiempo real —</small>',
        unsafe_allow_html=True,
    )
    if not hay_alguno_rt:
        st.markdown(
            '<small><span style="color:#d62728;">🔴</span> '
            'Sin datos RT. Ejecuta streaming_master.py.</small>',
            unsafe_allow_html=True,
        )
    else:
        ts_contam = contam_rt.get("timestamp") if contam_rt else None
        ts_meteo = meteo_rt.get("timestamp") if meteo_rt else None
        ts_trafico = trafico_rt.get("timestamp") if trafico_rt else None
        _linea_rt("Contaminación RT", ts_contam)
        _linea_rt("Meteorología RT", ts_meteo)
        _linea_rt("Tráfico RT", ts_trafico)

    st.divider()
    st.markdown(
        '<div style="text-align:center;padding:0.3rem 0;">'
        '<small style="color:#555;">'
        '🔍 Data Detective Valencia<br>'
        'Proyecto Big Data · Universitat de València<br>'
        '2026 · Joan Oliver'
        '</small></div>',
        unsafe_allow_html=True,
    )
