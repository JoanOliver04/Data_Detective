# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.1: Dashboard Streamlit - Orquestador Principal
==============================================================================
Ejecucion: streamlit run 5.DASHBOARD/app.py
Ruta: 5.DASHBOARD/app.py | Autor: Joan | Fecha: 2026
"""

import sys
from pathlib import Path

# Asegurar que el directorio del dashboard esta en sys.path
# (necesario cuando se ejecuta desde la raiz: streamlit run 5.DASHBOARD/app.py)
_DASHBOARD_DIR = str(Path(__file__).resolve().parent)
if _DASHBOARD_DIR not in sys.path:
    sys.path.insert(0, _DASHBOARD_DIR)

import pandas as pd
import streamlit as st

from config import PAGE_CONFIG, TAB_NAMES, DESCRIPCION_TABS
st.set_page_config(**PAGE_CONFIG)

from data_loader import (
    cargar_contaminacion, cargar_meteorologia, cargar_trafico,
    cargar_impacto_eventos, cargar_contam_anual_barrio,
    cargar_precip_mensual, cargar_tendencias, cargar_pronostico_72h,
    diagnostico_datos,
)
from components.sidebar import render_sidebar


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
# TABS PLACEHOLDER (Fase 7.2-7.5 los completaran)
# ==============================================================================

def _tab_contaminacion(datos):
    var = datos.get("_variable", "NO2")
    df = datos.get("contaminacion")
    st.markdown(f'<div class="section-header"><h3>{TAB_NAMES["contaminacion"]}</h3>'
                f'<p>{DESCRIPCION_TABS["contaminacion"]}</p></div>', unsafe_allow_html=True)
    if df is None:
        st.error("Sin datos de contaminacion. Ejecuta pipeline_etl.py.")
        return
    df_ok = df[(df["calidad_dato"] == "ok") & (df["variable"] == var)]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"Registros {var}", f"{len(df_ok):,}")
    c2.metric(f"Media {var}", f"{df_ok['valor'].mean():.1f} ug/m3" if not df_ok.empty else "-")
    c3.metric("Periodo", f"{df_ok['anio'].min()}-{df_ok['anio'].max()}" if not df_ok.empty else "-")
    c4.metric("Estaciones", str(df_ok["estacion_id"].nunique()) if not df_ok.empty else "0")
    st.info("Componente completo en Fase 7.2: mapas Folium, graficos Plotly, tendencias.")


def _tab_precipitaciones(datos):
    st.markdown(f'<div class="section-header"><h3>{TAB_NAMES["precipitaciones"]}</h3>'
                f'<p>{DESCRIPCION_TABS["precipitaciones"]}</p></div>', unsafe_allow_html=True)
    df = datos.get("meteorologia")
    if df is None:
        st.error("Sin datos meteorologicos. Ejecuta pipeline_etl.py.")
        return
    df_ok = df[df["calidad_dato"] == "ok"]
    c1, c2, c3 = st.columns(3)
    c1.metric("Registros", f"{len(df_ok):,}")
    c2.metric("Precip. media", f"{df_ok['precipitacion_mm'].mean():.2f} mm" if not df_ok.empty else "-")
    c3.metric("Temp. media", f"{df_ok['temp_c'].mean():.1f} C" if not df_ok.empty else "-")
    st.info("Componente completo en Fase 7.3: pronostico 72h y tendencias.")


def _tab_trafico(datos):
    st.markdown(f'<div class="section-header"><h3>{TAB_NAMES["trafico"]}</h3>'
                f'<p>{DESCRIPCION_TABS["trafico"]}</p></div>', unsafe_allow_html=True)
    df = datos.get("trafico")
    if df is None:
        st.error("Sin datos de trafico. Ejecuta pipeline_etl.py.")
        return
    c1, c2 = st.columns(2)
    c1.metric("Incidencias totales", f"{len(df):,}")
    c2.metric("Ubicaciones", str(df["ubicacion"].nunique()))
    st.info("Componente completo en Fase 7.4: mapa trafico y distribuciones.")


def _tab_eventos(datos):
    st.markdown(f'<div class="section-header"><h3>{TAB_NAMES["eventos"]}</h3>'
                f'<p>{DESCRIPCION_TABS["eventos"]}</p></div>', unsafe_allow_html=True)
    df = datos.get("eventos")
    if df is None:
        st.error("Sin datos de impacto de eventos. Ejecuta correlacion_eventos.py.")
        return
    c1, c2, c3 = st.columns(3)
    c1.metric("Eventos analizados", str(df["evento_id"].nunique()))
    c2.metric("Tipos de evento", str(df["tipo_evento"].nunique()))
    imp = df["impacto_pct"].dropna().mean()
    c3.metric("Impacto medio", f"{imp:+.1f}%" if not pd.isna(imp) else "-")
    st.info("Componente completo en Fase 7.5: graficos de impacto y timeline.")


def _tab_pronostico(datos):
    st.markdown(f'<div class="section-header"><h3>{TAB_NAMES["pronostico"]}</h3>'
                f'<p>{DESCRIPCION_TABS["pronostico"]}</p></div>', unsafe_allow_html=True)
    df = datos.get("pronostico")
    if df is None:
        st.error("Sin pronostico. Ejecuta streaming_openweather.py y generar_pronostico.py.")
        return
    st.caption(f"Fuente: `{df.attrs.get('archivo_fuente', '?')}` | "
               f"Captura: {df.attrs.get('timestamp_captura', '?')}")
    c1, c2, c3 = st.columns(3)
    tmax = df["temp_c"].max()
    c1.metric("Temp. maxima", f"{tmax:.1f} C" if pd.notna(tmax) else "-")
    c2.metric("Lluvia total", f"{df['rain_mm'].sum():.1f} mm")
    c3.metric("Prob. max. lluvia", f"{df['precip_probability_pct'].max():.0f}%")
    st.info("Componente completo en Fase 7.5: grafico interactivo pronostico 72h.")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    _aplicar_estilos()

    with st.spinner("Cargando datos del proyecto..."):
        datos = _cargar_todos_los_datos()

    tiene_datos = any(v is not None for k, v in datos.items() if not k.startswith("_"))
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
