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
from components.alertas import render_alertas
from components.meteorologia import render_tab_meteorologia
from components.trafico import render_tab_trafico
from components.eventos import render_tab_eventos
from components.pronostico import render_tab_pronostico
from components.sidebar import render_sidebar
from components.exportar import render_panel_exportacion
from data_loader import (
    cargar_contaminacion, cargar_meteorologia, cargar_trafico,
    cargar_impacto_eventos, cargar_contam_anual_barrio,
    cargar_precip_mensual, cargar_tendencias, cargar_pronostico_72h,
    diagnostico_datos,
)
from config import PAGE_CONFIG, TAB_NAMES, DESCRIPCION_TABS
import streamlit as st
import pandas as pd

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


st.set_page_config(**PAGE_CONFIG)


# ==============================================================================
# CSS
# ==============================================================================

def _aplicar_estilos():
    st.markdown("""<style>
    [data-testid="stMetric"]{background-color:#1e1e2e;border:1px solid #333;border-radius:8px;padding:12px 16px}
    [data-testid="stMetricLabel"]{font-size:.85rem}
    [data-testid="stMetricValue"]{font-size:1.6rem}
    .stTabs [data-baseweb="tab-list"]{gap:8px}
    .stTabs [data-baseweb="tab"]{padding:8px 20px;font-size:.95rem}
    .section-header{border-left:4px solid #1f77b4;padding-left:12px;margin:1.5rem 0 1rem 0}
    .footer-text{text-align:center;color:#555;font-size:.75rem;padding:2rem 0 1rem 0}
    </style>""", unsafe_allow_html=True)


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def _cargar_todos_los_datos():
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


# ==============================================================================
# FILTRADO GLOBAL
# ==============================================================================

def _aplicar_filtros_globales(datos, filtros):
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

    datos_f["_variable"] = filtros["variable"]
    datos_f["_filtros"] = filtros
    return datos_f


# ==============================================================================
# DIAGNOSTICO (si faltan datos)
# ==============================================================================

def _render_diagnostico():
    st.warning("Algunos datasets no estan disponibles.")
    informe = diagnostico_datos()
    st.markdown("### Estado de los Datasets")
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
            "Sin datos de contaminacion. "
            "Ejecuta pipeline_etl.py para generar los datos limpios."
        )
        return

    # 1. Indice sintetico de calidad del aire (KPI principal)
    render_indice_calidad(df)

    # 2. Alertas de contaminacion (periodo reciente vs umbrales OMS)
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
        render_mapa_contaminacion(df, var, df_contam_anual)

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
    with st.expander("🏆 Ranking de barrios por calidad del aire"):
        render_ranking_barrios(df)

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

def _tab_precipitaciones(datos):
    """Tab de precipitaciones/meteorologia (Fase 7.3 - COMPLETA)."""
    render_tab_meteorologia(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("meteorologia"),
        nombre_dataset="meteorologia",
    )


def _tab_trafico(datos):
    """Tab de trafico (Fase 7.4 - COMPLETA)."""
    render_tab_trafico(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("trafico"),
        nombre_dataset="trafico",
    )


def _tab_eventos(datos):
    """Tab de eventos masivos (Fase 7.5 - COMPLETA)."""
    render_tab_eventos(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("eventos"),
        nombre_dataset="eventos",
    )


def _tab_pronostico(datos):
    """Tab de pronostico 72h (Fase 7.6 - COMPLETA)."""
    render_tab_pronostico(datos)

    # --- EXPORTACION ---
    st.divider()
    render_panel_exportacion(
        df=datos.get("pronostico"),
        nombre_dataset="pronostico",
    )


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    _aplicar_estilos()

    if st_autorefresh:
        st_autorefresh(interval=300000, limit=None, key="data_refresh")
    else:
        st.info("Auto-refresh no disponible. Instala streamlit-autorefresh==1.0.1")

    with st.spinner("Cargando datos del proyecto..."):
        datos = _cargar_todos_los_datos()

    tiene_datos = any(v is not None for k, v in datos.items()
                      if not k.startswith("_"))
    if not tiene_datos:
        _render_diagnostico()
        return

    filtros = render_sidebar(
        df_contam=datos.get("contaminacion"),
        df_eventos=datos.get("eventos"),
    )
    datos_f = _aplicar_filtros_globales(datos, filtros)

    tabs = st.tabs([
        TAB_NAMES["contaminacion"], TAB_NAMES["precipitaciones"],
        TAB_NAMES["trafico"], TAB_NAMES["eventos"], TAB_NAMES["pronostico"],
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
        _tab_pronostico(datos_f)

    st.markdown('<p class="footer-text">Data Detective Valencia - Big Data 2026 - Joan</p>',
                unsafe_allow_html=True)


if __name__ == "__main__":
    main()
