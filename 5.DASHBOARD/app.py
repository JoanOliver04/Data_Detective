# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.6: Dashboard Streamlit - Orquestador Principal
==============================================================================

Changelog:
  - v2: Integra panel de exportacion (CSV/JSON/XML) en cada tab.
        Cada tab incluye render_panel_exportacion() al final con los
        datos filtrados, respetando los filtros globales del sidebar.

Ejecucion: streamlit run 5.DASHBOARD/app.py
Ruta: 5.DASHBOARD/app.py | Autor: Joan | Fecha: 2026
"""

from components.maps import render_mapa_contaminacion
from components.trends import render_grafico_contaminacion, render_tendencia_anual
from components.kpis import render_kpis_contaminacion, render_indice_calidad
from components.ranking_barrios import render_ranking_barrios
from components.alertas import render_alertas, render_alertas_realtime
from components.comparador import render_comparador
from components.meteorologia import render_tab_meteorologia
from components.trafico import render_tab_trafico
from components.eventos import render_tab_eventos
from components.pronostico import render_tab_pronostico
from components.rutas_limpias import render_mapa_rutas_limpias
from components.sidebar import render_sidebar
from components.exportar import render_panel_exportacion
from components.realtime import render_datos_realtime
from data_loader import (
    cargar_contaminacion, cargar_meteorologia, cargar_trafico,
    cargar_impacto_eventos, cargar_contam_anual_barrio,
    cargar_precip_mensual, cargar_tendencias, cargar_pronostico_72h,
    cargar_contaminacion_realtime, cargar_meteo_realtime,
    cargar_trafico_realtime, cargar_espiras_realtime,
    diagnostico_datos,
)
from config import PAGE_CONFIG, TAB_NAMES, DESCRIPCION_TABS
from streaming_background import iniciar_streaming_background, estado_streaming
from theme import get_theme
import streamlit as st
import pandas as pd

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


st.set_page_config(**PAGE_CONFIG)

# --- Lanzar thread de streaming en background (una sola vez por proceso) ---
iniciar_streaming_background()


# ==============================================================================
# CSS
# ==============================================================================

def _aplicar_estilos():
    t = get_theme()
    st.markdown(f"""<style>
    /* --- Override global de Streamlit para respetar nuestro tema --- */
    .stApp {{
        background-color: {t["app_bg"]};
        color: {t["app_text"]};
    }}
    [data-testid="stSidebar"] {{
        background-color: {t["sidebar_bg"]};
    }}
    [data-testid="stSidebar"] [data-testid="stMarkdown"] {{
        color: {t["app_text"]};
    }}

    /* --- CSS custom properties para HTML inline --- */
    :root {{
        --dd-text: {t["text"]};
        --dd-text-secondary: {t["text_secondary"]};
        --dd-text-muted: {t["text_muted"]};
        --dd-text-faint: {t["text_faint"]};
        --dd-text-label: {t["text_label"]};
        --dd-text-dim: {t["text_dim"]};
        --dd-border: {t["border"]};
        --dd-border-subtle: {t["border_subtle"]};
        --dd-bg-card: {t["bg_card"]};
        --dd-bg-tabs: {t["bg_tabs"]};
        --dd-bg-tab-selected: {t["bg_tab_selected"]};
    }}

    /* --- Metricas con hover sutil --- */
    [data-testid="stMetric"]{{
        background: {t["bg_card"]};
        border: 1px solid {t["border"]};
        border-radius: 10px;
        padding: 14px 18px;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }}
    [data-testid="stMetric"]:hover{{
        transform: translateY(-2px);
        border-color: {t["border_hover"]};
    }}
    [data-testid="stMetricLabel"]{{font-size:.85rem;letter-spacing:0.3px}}
    [data-testid="stMetricValue"]{{font-size:1.6rem;font-weight:700}}

    /* --- Tabs mejorados --- */
    .stTabs [data-baseweb="tab-list"]{{
        gap: 4px;
        background-color: {t["bg_tabs"]};
        border-radius: 10px;
        padding: 4px;
    }}
    .stTabs [data-baseweb="tab"]{{
        padding: 10px 22px;
        font-size: .92rem;
        border-radius: 8px;
        font-weight: 500;
    }}
    .stTabs [aria-selected="true"]{{
        background-color: {t["bg_tab_selected"]} !important;
    }}

    /* --- Cabeceras de seccion --- */
    .section-header{{
        border-left: 4px solid #1f77b4;
        padding-left: 12px;
        margin: 1.5rem 0 1rem 0;
    }}
    .section-header h3, .section-header h4{{
        margin-bottom: 2px;
    }}
    .section-header p{{
        color: {t["text_muted"]};
        font-size: 0.85rem;
        margin-top: 0;
    }}

    /* --- Expanders --- */
    .streamlit-expanderHeader{{
        font-weight: 600;
        font-size: 0.95rem;
    }}

    /* --- Dividers mas sutiles --- */
    hr{{
        border-color: {t["border_subtle"]} !important;
        opacity: 0.5;
    }}

    /* --- Footer --- */
    .footer-text{{
        text-align: center;
        color: {t["text_faint"]};
        font-size: .75rem;
        padding: 2rem 0 1rem 0;
        border-top: 1px solid {t["border_subtle"]};
        margin-top: 2rem;
    }}

    /* --- Sidebar branding --- */
    [data-testid="stSidebar"] [data-testid="stMarkdown"] h1{{
        background: linear-gradient(135deg, #1f77b4, #17becf);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }}

    /* --- Download buttons uniformes --- */
    .stDownloadButton > button{{
        border-radius: 6px;
        font-weight: 600;
        transition: transform 0.15s ease;
    }}
    .stDownloadButton > button:hover{{
        transform: scale(1.03);
    }}
    </style>""", unsafe_allow_html=True)


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def _cargar_datos_estaticos():
    """
    Carga los datasets historicos con cache de 1 hora.
    Separados de los datos RT para que un refresco de 5 min no invalide
    la cache de datos pesados que cambian muy poco.
    """
    return {
        "contaminacion": cargar_contaminacion(),
        "meteorologia": cargar_meteorologia(),
        "trafico": cargar_trafico(),
        "eventos": cargar_impacto_eventos(),
        "contam_anual": cargar_contam_anual_barrio(),
        "precip_mensual": cargar_precip_mensual(),
        "tendencias": cargar_tendencias(),
        "pronostico": cargar_pronostico_72h(),
    }


def _cargar_todos_los_datos():
    """
    Combina datasets estaticos (cache 1h) con datos en tiempo real (cache 5min).
    Los loaders RT tienen su propio @st.cache_data(ttl=300) en data_loader.py,
    por lo que no necesitan cache aqui.
    """
    datos = _cargar_datos_estaticos()
    datos["contam_rt"] = cargar_contaminacion_realtime()
    datos["meteo_rt"] = cargar_meteo_realtime()
    datos["trafico_rt"] = cargar_trafico_realtime()
    datos["espiras_rt"] = cargar_espiras_realtime()
    return datos


# ==============================================================================
# FILTRADO GLOBAL
# ==============================================================================

def _aplicar_filtros_globales(datos, filtros):
    cache_key = (
        filtros["anio_min"], filtros["anio_max"],
        filtros["variable"],
        tuple(sorted(filtros.get("barrios", []))),
        tuple(sorted(filtros.get("tipos_evento", []))),
    )
    if st.session_state.get("_filtros_cache_key") == cache_key:
        return st.session_state["_datos_filtrados"]

    datos_f = dict(datos)
    a_min, a_max = filtros["anio_min"], filtros["anio_max"]
    barrios = filtros["barrios"]
    tipos_ev = filtros["tipos_evento"]

    for key in ["contaminacion", "meteorologia", "trafico"]:
        df = datos.get(key)
        if df is not None and "anio" in df.columns:
            mask = (df["anio"] >= a_min) & (df["anio"] <= a_max)
            if key == "contaminacion" and barrios:
                mask = mask & (df["barrio"].isin(barrios))
            datos_f[key] = df[mask]

    df = datos.get("contam_anual")
    if df is not None and "anio" in df.columns:
        mask = (df["anio"] >= a_min) & (df["anio"] <= a_max)
        if barrios:
            mask = mask & (df["barrio"].isin(barrios))
        datos_f["contam_anual"] = df[mask]

    df = datos.get("eventos")
    if df is not None and tipos_ev:
        datos_f["eventos"] = df[df["tipo_evento"].isin(tipos_ev)]

    datos_f["espiras_rt"] = datos.get("espiras_rt")
    datos_f["_variable"] = filtros["variable"]
    datos_f["_filtros"] = filtros

    st.session_state["_filtros_cache_key"] = cache_key
    st.session_state["_datos_filtrados"] = datos_f
    return datos_f


# ==============================================================================
# DIAGNOSTICO (si faltan datos)
# ==============================================================================

def _render_diagnostico():
    st.warning("Algunos datasets no están disponibles.")
    informe = diagnostico_datos()
    st.markdown("### Estado de los datasets")
    for nombre, info in informe.items():
        estado = "OK" if info["existe"] else "No encontrado"
        tam = f" ({info['tamanio_kb']} KB)" if info["existe"] else ""
        st.markdown(f"**{nombre}** - {estado}{tam} `{info['ruta']}`")
    st.info(
        "Ejecuta desde la raiz del proyecto:\n\n```\n"
        "python 2.SCRIPTS/procesamiento/pipeline_etl.py\n"
        "python 2.SCRIPTS/procesamiento/generar_mapas.py\n"
        "python 2.SCRIPTS/procesamiento/generar_graficos.py\n"
        "python 2.SCRIPTS/procesamiento/generar_visualizaciones_eventos.py\n"
        "python 2.SCRIPTS/procesamiento/generar_pronostico.py\n```"
    )


# ==============================================================================
# TAB CONTAMINACION (Fase 7.2 - COMPLETA)
# ==============================================================================

@st.fragment
def _tab_contaminacion(datos):
    """
    Tab de contaminacion completa con componentes modulares:
      1. Header con descripcion
      2. KPIs dinamicos (kpis.py)
      3. Grafico temporal interactivo (trends.py)
      4. Mapa Folium embebido (maps.py)
      5. Tendencia anual con cambio porcentual (trends.py)
      6. Panel de exportacion (CSV/JSON/XML)
    """
    var = datos.get("_variable", "NO2")
    df = datos.get("contaminacion")
    df_contam_anual = datos.get("contam_anual")

    # Header de seccion
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["contaminacion"]}</h3>'
        f'<p>{DESCRIPCION_TABS["contaminacion"]}</p></div>',
        unsafe_allow_html=True,
    )

    # Guard clause: sin datos
    if df is None:
        st.error(
            "Sin datos de contaminación. "
            "Ejecuta pipeline_etl.py para generar los datos limpios."
        )
        return

    # 1. Indice sintetico de calidad del aire (KPI principal)
    render_indice_calidad(df)

    # 2. Alertas de contaminacion: primero RT, luego historicas
    render_alertas_realtime(datos.get("contam_rt"), df)
    render_alertas(df)

    st.divider()

    # 3. KPIs por variable seleccionada
    render_kpis_contaminacion(df, var)

    st.divider()

    # 2. Grafico temporal interactivo
    render_grafico_contaminacion(df, var)

    st.divider()

    # 3. Mapa + Tendencia en dos columnas
    col_mapa, col_tendencia = st.columns([3, 2])

    with col_mapa:
        render_mapa_contaminacion(
            df, var, df_contam_anual,
            trafico_rt=datos.get("trafico_rt"),
            contam_rt=datos.get("contam_rt"),
            meteo_rt=datos.get("meteo_rt"),
        )

    with col_tendencia:
        render_tendencia_anual(df, var)

        # Seccion extra: resumen de filtros activos
        filtros = datos.get("_filtros", {})
        barrios = filtros.get("barrios", [])
        anio_min = filtros.get("anio_min", "?")
        anio_max = filtros.get("anio_max", "?")

        st.markdown("---")
        st.caption("Filtros activos")
        st.markdown(
            f"**Variable:** {var}  \n"
            f"**Periodo:** {anio_min} - {anio_max}  \n"
            f"**Distritos:** {', '.join(barrios) if barrios else 'Todos'}",
        )

    # 4. Ranking de barrios
    st.divider()
    with st.expander("🏆 Ranking de barrios por calidad del aire / calidad urbana"):
        render_ranking_barrios(
            df,
            contam_rt=datos.get("contam_rt"),
            meteo_rt=datos.get("meteo_rt"),
            trafico_rt=datos.get("trafico_rt"),
        )

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=df,
        nombre_dataset="contaminacion",
        metadata_extra={
            "variable_filtrada": var,
            "periodo": f"{filtros.get('anio_min', '?')}-{filtros.get('anio_max', '?')}",
            "barrios_filtrados": barrios if barrios else "Todos",
        },
    )


# ==============================================================================
# TABS COMPLETADAS (Fases 7.2-7.6) + EXPORTACION
# ==============================================================================

@st.fragment
def _tab_precipitaciones(datos):
    """Tab de precipitaciones/meteorologia (Fase 7.3 - COMPLETA)."""
    render_tab_meteorologia(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("meteorologia"),
        nombre_dataset="meteorologia",
    )


@st.fragment
def _tab_trafico(datos):
    """Tab de trafico (Fase 7.4 - COMPLETA)."""
    render_tab_trafico(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("trafico"),
        nombre_dataset="trafico",
    )


@st.fragment
def _tab_eventos(datos):
    """Tab de eventos masivos (Fase 7.5 - COMPLETA)."""
    render_tab_eventos(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("eventos"),
        nombre_dataset="eventos",
    )


@st.fragment
def _tab_comparador(datos):
    """Tab del comparador historico (Evento vs Baseline / Periodo vs Periodo)."""
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["comparador"]}</h3>'
        f'<p>{DESCRIPCION_TABS["comparador"]}</p></div>',
        unsafe_allow_html=True,
    )
    render_comparador(
        df_contaminacion=datos.get("contaminacion"),
        df_eventos=datos.get("eventos"),
    )


@st.fragment
def _tab_pronostico(datos):
    """Tab de pronostico 72h (Fase 7.6 - COMPLETA)."""
    render_tab_pronostico(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("pronostico"),
        nombre_dataset="pronostico",
    )


@st.fragment
def _tab_rutas_limpias(datos):
    """Tab de rutas pulmon limpio (zonas verdes, baja contaminacion)."""
    render_mapa_rutas_limpias(contam_rt=datos.get("contam_rt"))


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    _aplicar_estilos()

    if st_autorefresh:
        st_autorefresh(interval=1800000, limit=None, key="data_refresh")
    else:
        st.info("Auto-refresh no disponible. Instala streamlit-autorefresh==1.0.1")

    datos = _cargar_todos_los_datos()  # cache estatico evita spinner en recargas

    tiene_datos = any(v is not None for k, v in datos.items()
                      if not k.startswith("_"))
    if not tiene_datos:
        _render_diagnostico()
        return

    filtros = render_sidebar(
        df_contam=datos.get("contaminacion"),
        df_eventos=datos.get("eventos"),
        df_meteo=datos.get("meteorologia"),
        df_trafico=datos.get("trafico"),
        datos_rt={
            "contam_rt": datos.get("contam_rt"),
            "meteo_rt": datos.get("meteo_rt"),
            "trafico_rt": datos.get("trafico_rt"),
        },
    )
    datos_f = _aplicar_filtros_globales(datos, filtros)

    # Banner de datos en tiempo real (antes de las tabs)
    render_datos_realtime(datos)

    tabs = st.tabs([
        TAB_NAMES["contaminacion"], TAB_NAMES["precipitaciones"],
        TAB_NAMES["trafico"], TAB_NAMES["eventos"],
        TAB_NAMES["comparador"], TAB_NAMES["pronostico"],
        TAB_NAMES["rutas_limpias"],
    ])

    with tabs[0]:
        _tab_contaminacion(datos_f)
    with tabs[1]:
        _tab_precipitaciones(datos_f)
    with tabs[2]:
        _tab_trafico(datos_f)
    with tabs[3]:
        _tab_eventos(datos_f)
    with tabs[4]:
        _tab_comparador(datos_f)
    with tabs[5]:
        _tab_pronostico(datos_f)
    with tabs[6]:
        _tab_rutas_limpias(datos)

    st.markdown(
        '<div class="footer-text">'
        '🔍 <b>Data Detective Valencia</b><br>'
        '<span style="color:#666;">'
        'Proyecto Big Data · Universitat de València · 2026 · Joan Oliver'
        '</span><br>'
        '<span style="color:#444;font-size:0.65rem;">'
        'Streamlit · Plotly · Folium · Pandas · OpenWeatherMap · AQICN · DGT'
        '</span>'
        '</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
