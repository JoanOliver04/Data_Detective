# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 7.4: Tab Trafico
==============================================================================
Componente modular que renderiza la seccion completa de trafico:
  1. KPIs: total incidencias, media diaria, dia pico, flujo espiras RT
  2. Grafico temporal adaptativo (barras anual / linea mensual)
  3. Distribucion semanal (barras horizontales lun-dom)
  4. Mapa combinado: espiras VLCi (flujo) + incidencias DGT
  5. Funcion orquestadora render_tab_trafico()

Columnas esperadas del DataFrame (data_loader.cargar_trafico):
  fecha (datetime), anio, mes, dia_semana, ubicacion, calidad_dato

Ruta: 5.DASHBOARD/components/trafico.py
Autor: Joan | Fecha: 2026
"""

import logging
from collections import Counter
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from theme import get_theme
from config import (
    TAB_NAMES, DESCRIPCION_TABS, MAPA_TRAFICO_HTML,
)
from data_loader import leer_html_visualizacion

logger = logging.getLogger("Trafico")

# Colores de la seccion
COLOR_TRAFICO = "#ff7f0e"           # Naranja principal
COLOR_TRAFICO_LIGHT = "#ffbb78"     # Naranja claro (barras)

# Orden correcto de dias (lunes a domingo) con nombres en ingles
# tal como genera pandas con dt.day_name()
DIAS_ORDEN = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]
DIAS_NOMBRE_ES = {
    "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
    "Thursday": "Jueves", "Friday": "Viernes",
    "Saturday": "Sábado", "Sunday": "Domingo",
}

# Colores, iconos y etiquetas para tipos de incidencia DGT
_COLOR_TIPO = {
    "roadMaintenance": "#1f77b4",
    "vehicleObstruction": "#d62728",
    "environmentalObstruction": "#2ca02c",
    "infrastructureDamageObstruction": "#9467bd",
    "obstruction": "#ff7f0e",
}
_COLOR_SEVERIDAD = {
    "highest": "#d62728", "high": "#ff4444",
    "medium": "#ff7f0e", "low": "#ffdd57", "desconocida": "#aec7e8",
}
_ICONO_TIPO = {
    "roadMaintenance": "\U0001f6a7", "vehicleObstruction": "\U0001f697",
    "environmentalObstruction": "\U0001f33f",
    "infrastructureDamageObstruction": "\u26a0\ufe0f", "obstruction": "\U0001f6ab",
}
_LABEL_TIPO = {
    "roadMaintenance": "Obras/mantenimiento",
    "vehicleObstruction": "Veh\u00edculo/obst\u00e1culo",
    "environmentalObstruction": "Obst\u00e1culo ambiental",
    "infrastructureDamageObstruction": "Da\u00f1o en infraestructura",
    "obstruction": "Obst\u00e1culo gen\u00e9rico",
}

# Altura del mapa embebido (px)
MAP_HEIGHT = 580

# --------------------------------------------------------------------------
# Espiras: umbrales de congestion (vehiculos/hora por punto sensor)
# --------------------------------------------------------------------------
_IH_LIBRE = 200       # < 200 veh/h → libre (verde)
_IH_FLUIDO = 500      # 200-500     → fluido (lima)
_IH_DENSO = 900       # 500-900     → denso (naranja)
_IH_SATURADO = 1400   # 900-1400    → saturado (rojo)
                       # > 1400      → congestionado (rojo oscuro)


def _color_espira(ih: float) -> str:
    """Returns hex color based on vehicles/hour."""
    if ih < _IH_LIBRE:
        return "#2ca02c"    # green
    if ih < _IH_FLUIDO:
        return "#98df8a"    # lime
    if ih < _IH_DENSO:
        return "#ff7f0e"    # orange
    if ih < _IH_SATURADO:
        return "#d62728"    # red
    return "#8B0000"        # dark red


def _label_congestion(ih: float) -> str:
    """Returns Spanish label for congestion level."""
    if ih < _IH_LIBRE:
        return "Libre"
    if ih < _IH_FLUIDO:
        return "Fluido"
    if ih < _IH_DENSO:
        return "Denso"
    if ih < _IH_SATURADO:
        return "Saturado"
    return "Congestionado"


# ==============================================================================
# 1. KPIs
# ==============================================================================

def render_kpis_trafico(
    df: pd.DataFrame,
    espiras_rt: Optional[dict] = None,
) -> None:
    """
    Renderiza 4 KPIs de trafico:
      - Total incidencias
      - Media diaria de incidencias
      - Dia de la semana con mas incidencias
      - Flujo ahora (espiras) OR Anio mas congestionado

    Args:
        df: DataFrame de trafico filtrado.
        espiras_rt: Datos RT de espiras VLCi (puede ser None).
    """
    if df is None or df.empty:
        st.warning("No hay datos de tráfico para los filtros seleccionados.")
        return

    # --- Calculos ---
    total = len(df)

    # Media diaria: dias unicos con datos
    if "fecha" in df.columns:
        try:
            dias_unicos = df["fecha"].dt.date.nunique()
        except AttributeError:
            dias_unicos = df["fecha"].nunique()
    else:
        dias_unicos = 1
    media_diaria = total / max(dias_unicos, 1)

    # Dia de la semana con mas incidencias
    if "dia_semana" in df.columns and not df["dia_semana"].dropna().empty:
        conteo_dia = df["dia_semana"].value_counts()
        dia_pico_en = conteo_dia.idxmax()
        dia_pico = DIAS_NOMBRE_ES.get(dia_pico_en, dia_pico_en)
        val_dia_pico = int(conteo_dia.max())
    else:
        dia_pico = "-"
        val_dia_pico = None

    # --- Renderizado ---
    st.markdown(
        f'<div style="border-left:4px solid {COLOR_TRAFICO};padding-left:12px;'
        f'margin-bottom:1rem;">'
        f'<h4 style="margin:0;">Indicadores de tráfico</h4>'
        f'<small style="color:#888;">Incidencias en la red viaria</small>'
        f'</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Total incidencias",
        value=f"{total:,}",
        help="Número total de incidencias de tráfico registradas.",
    )

    c2.metric(
        label="Media diaria",
        value=f"{media_diaria:.1f}",
        help=f"Incidencias por día ({dias_unicos:,} días con datos).",
    )

    c3.metric(
        label="Día con más tráfico",
        value=dia_pico,
        delta=f"{val_dia_pico:,} incidencias" if val_dia_pico else None,
        delta_color="off",
        help="Día de la semana con mayor número de incidencias.",
    )

    # 4th KPI: espiras RT if available, otherwise historical peak year
    sensores = (espiras_rt or {}).get("sensores_activos", [])
    if sensores:
        ihs = [s["ih"] for s in sensores]
        avg_ih = sum(ihs) / len(ihs)
        max_ih = max(ihs)
        c4.metric(
            label="Flujo ahora",
            value=f"{avg_ih:.0f} veh/h",
            delta=f"máx {max_ih:.0f} veh/h",
            delta_color="off",
            help=f"Media de {len(sensores)} espiras activas en Valencia.",
        )
    else:
        if "anio" in df.columns and not df["anio"].dropna().empty:
            conteo_anio = df.groupby("anio").size()
            anio_pico = int(conteo_anio.idxmax())
            val_anio_pico = int(conteo_anio.max())
        else:
            anio_pico = "-"
            val_anio_pico = None

        c4.metric(
            label="Año más congestionado",
            value=str(anio_pico),
            delta=f"{val_anio_pico:,} incidencias" if val_anio_pico else None,
            delta_color="off",
            help="Año con mayor volumen de incidencias registradas.",
        )

    logger.info(
        f"[KPIs Trafico] Total={total:,}, Media diaria={media_diaria:.1f}, "
        f"Dia pico={dia_pico}"
    )


# ==============================================================================
# 2. GRAFICO TEMPORAL ADAPTATIVO
# ==============================================================================

def render_grafico_trafico(df: pd.DataFrame) -> None:
    """
    Grafico de incidencias mensuales del año más reciente con datos.
    Se muestra siempre en vista mensual porque los datos DGT históricos
    son escasos — solo el año en curso tiene volumen significativo.
    """
    if df is None or df.empty:
        st.info("Sin datos para generar el gráfico de tráfico.")
        return

    if "anio" not in df.columns or "mes" not in df.columns:
        st.info("Columnas de año/mes no disponibles para graficar.")
        return

    # Usar el año con más registros (normalmente el año en curso)
    anio_principal = int(df.groupby("anio").size().idxmax())
    df_anio = df[df["anio"] == anio_principal].copy()

    _grafico_trafico_mensual(df_anio, anio_principal)


_MESES_ES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}


def _grafico_trafico_mensual(df: pd.DataFrame, anio: int) -> None:
    """Barras de incidencias por mes para un año concreto."""
    conteo = (
        df.groupby("mes", as_index=False)
        .size()
        .rename(columns={"size": "incidencias"})
    )

    # Rango completo 1-12 para que los meses sin datos aparezcan como 0
    todos_meses = pd.DataFrame({"mes": range(1, 13)})
    serie = todos_meses.merge(conteo, on="mes", how="left")
    serie["incidencias"] = serie["incidencias"].fillna(0).astype(int)
    serie["mes_nombre"] = serie["mes"].map(_MESES_ES)

    pico_idx = serie["incidencias"].idxmax()
    colores = [
        COLOR_TRAFICO if i == pico_idx else COLOR_TRAFICO_LIGHT
        for i in serie.index
    ]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=serie["mes_nombre"],
        y=serie["incidencias"],
        marker_color=colores,
        hovertemplate=(
            f"<b>%{{x}} {anio}</b><br>"
            "Incidencias: %{y:,}<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text=f"Incidencias de tráfico por mes \u2014 {anio}",
            font=dict(size=16),
        ),
        xaxis_title="Mes",
        yaxis_title="Incidencias",
        template=get_theme()["plotly_template"],
        height=420,
        margin=dict(l=60, r=30, t=60, b=50),
        hovermode="x unified",
        bargap=0.18,
    )

    st.plotly_chart(fig, use_container_width=True)

    total = int(serie["incidencias"].sum())
    mes_pico = _MESES_ES[int(serie.loc[pico_idx, "mes"])]
    pico = int(serie.loc[pico_idx, "incidencias"])
    st.caption(
        f"Total {anio}: {total:,} incidencias · "
        f"Mes pico: {mes_pico} ({pico:,})"
    )

    meses_con_datos = int((serie["incidencias"] > 0).sum())
    logger.info(f"[Grafico] Trafico mensual {anio}: {meses_con_datos} meses con datos")


# ==============================================================================
# 3. DISTRIBUCION SEMANAL
# ==============================================================================

def render_distribucion_semana(df: pd.DataFrame) -> None:
    """
    Media de incidencias por dia de la semana (lunes a domingo).
    Barras horizontales para mejor lectura.

    Args:
        df: DataFrame de trafico filtrado.
    """
    if df is None or df.empty:
        st.info("Sin datos para la distribución semanal.")
        return

    if "dia_semana" not in df.columns:
        st.info("Columna 'dia_semana' no disponible.")
        return

    # Contar incidencias por dia_semana
    conteo = df["dia_semana"].value_counts()

    # Calcular semanas totales en el dataset para obtener media
    if "fecha" in df.columns:
        try:
            n_semanas = max(
                (df["fecha"].max() - df["fecha"].min()).days / 7, 1
            )
        except Exception:
            n_semanas = 1
    else:
        n_semanas = 1

    # Construir DataFrame ordenado lun-dom
    datos_semana = []
    for dia_en in DIAS_ORDEN:
        total_dia = conteo.get(dia_en, 0)
        datos_semana.append({
            "dia_en": dia_en,
            "dia": DIAS_NOMBRE_ES.get(dia_en, dia_en),
            "total": int(total_dia),
            "media_semanal": round(total_dia / n_semanas, 1),
        })

    df_semana = pd.DataFrame(datos_semana)

    # Invertir orden para que lunes quede arriba en barras horizontales
    df_semana = df_semana.iloc[::-1]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df_semana["dia"],
        x=df_semana["media_semanal"],
        orientation="h",
        marker_color=[
            COLOR_TRAFICO if dia in ("Viernes", "Sábado", "Domingo")
            else COLOR_TRAFICO_LIGHT
            for dia in df_semana["dia"]
        ],
        opacity=0.85,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Media semanal: %{x:.1f} incidencias<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(
            text="Distribución de incidencias por día de la semana",
            font=dict(size=15),
        ),
        xaxis_title="Media de incidencias por semana",
        yaxis_title="",
        template=get_theme()["plotly_template"],
        height=350,
        margin=dict(l=100, r=30, t=50, b=40),
        showlegend=False,
    )

    st.plotly_chart(fig, width="stretch")

    # Insight automatico
    if not df_semana.empty:
        idx_max = df_semana["media_semanal"].idxmax()
        dia_max = df_semana.loc[idx_max, "dia"]
        val_max = df_semana.loc[idx_max, "media_semanal"]
        st.caption(
            f"Día con más tráfico: **{dia_max}** "
            f"({val_max:.1f} incidencias/semana de media)."
        )

    logger.info(f"[Distribucion] {len(df_semana)} dias procesados")


# ==============================================================================
# 4. MAPA COMBINADO: Espiras VLCi + Incidencias DGT
# ==============================================================================

def _render_mapa_combinado(
    trafico_rt: Optional[dict],
    espiras_rt: Optional[dict],
) -> None:
    """
    Builds a Folium map with two layers:
      1. VLCi espiras: CircleMarkers colored by vehicles/hour
      2. DGT incidents: CircleMarkers with MarkerCluster

    Args:
        trafico_rt: from cargar_trafico_realtime() (may be None).
        espiras_rt: from cargar_espiras_realtime() (may be None).
    """
    try:
        import folium
        from folium.plugins import MarkerCluster
    except ImportError:
        st.warning(
            "Folium no está instalado. Ejecuta: `pip install folium`"
        )
        return

    mapa = folium.Map(
        location=[39.4699, -0.3763],
        zoom_start=12,
        tiles=None,
        prefer_canvas=True,
    )

    folium.TileLayer(
        "CartoDB dark_matter", name="\U0001f311 Oscuro",
    ).add_to(mapa)
    folium.TileLayer(
        "CartoDB positron", name="\u2600\ufe0f Claro",
    ).add_to(mapa)

    # ------------------------------------------------------------------
    # LAYER 1 — Espiras VLCi (flujo vehicular)
    # ------------------------------------------------------------------
    sensores = []
    if espiras_rt is not None:
        sensores = espiras_rt.get("sensores_activos", [])

    if sensores:
        fg_espiras = folium.FeatureGroup(
            name="\U0001f6a6 Flujo vehicular (espiras)", show=True,
        )

        for s in sensores:
            ih = s.get("ih", 0)
            color = _color_espira(ih)
            label = _label_congestion(ih)
            radius = max(4, min(10, 4 + int(ih / 200)))

            popup_html = (
                '<table style="font-size:12px;min-width:180px;">'
                f'<tr><td><b>ID Sensor</b></td><td>{s.get("idpm", "—")}</td></tr>'
                f'<tr><td><b>Intensidad</b></td><td>{ih:.0f} veh/h</td></tr>'
                f'<tr><td><b>Estado</b></td>'
                f'<td style="color:{color};font-weight:bold;">{label}</td></tr>'
                f'<tr><td><b>Dirección</b></td><td>{s.get("angulo", "—")}°</td></tr>'
                f'<tr><td><b>Actualizado</b></td>'
                f'<td>{s.get("fecha_actualizacion", "—")} '
                f'{s.get("hora_actualizacion", "")}</td></tr>'
                '</table>'
            )

            folium.CircleMarker(
                location=[s["lat"], s["lon"]],
                radius=radius,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.75,
                weight=1,
                popup=folium.Popup(popup_html, max_width=260),
                tooltip=f"\U0001f6a6 {ih:.0f} veh/h \u2014 {label}",
            ).add_to(fg_espiras)

        fg_espiras.add_to(mapa)

    # ------------------------------------------------------------------
    # LAYER 2 — Incidencias DGT
    # ------------------------------------------------------------------
    incs_geo = []
    if trafico_rt is not None:
        incs_raw = trafico_rt.get("incidencias", [])
        incs_geo = [
            inc for inc in incs_raw
            if inc.get("lat") is not None and inc.get("lon") is not None
        ]

    if incs_geo:
        fg_dgt = folium.FeatureGroup(
            name="\u26a0\ufe0f Incidencias DGT", show=True,
        )

        cluster = MarkerCluster(
            options={
                "maxClusterRadius": 45,
                "disableClusteringAtZoom": 13,
                "showCoverageOnHover": False,
            },
        ).add_to(fg_dgt)

        for inc in incs_geo:
            causa = inc.get("causa", "obstruction")
            severidad = inc.get("severidad", "desconocida")
            carretera = inc.get("carretera", "\u2014")
            municipio = inc.get("municipio", "\u2014")

            color = _COLOR_TIPO.get(causa, COLOR_TRAFICO)
            radius = 8 if severidad in ("highest", "high") else 5
            emoji = _ICONO_TIPO.get(causa, "\U0001f6ab")
            label = _LABEL_TIPO.get(causa, causa)
            sev_color = _COLOR_SEVERIDAD.get(severidad, "#aec7e8")

            popup_html = (
                '<table style="font-size:12px;min-width:180px;">'
                f'<tr><td><b>Carretera</b></td><td>{carretera}</td></tr>'
                f'<tr><td><b>Municipio</b></td><td>{municipio}</td></tr>'
                f'<tr><td><b>Severidad</b></td>'
                f'<td style="color:{sev_color};font-weight:bold;">'
                f'{severidad}</td></tr>'
                f'<tr><td><b>Tipo</b></td><td>{label}</td></tr>'
                '</table>'
            )

            folium.CircleMarker(
                location=[inc["lat"], inc["lon"]],
                radius=radius,
                color=color,
                fill=True,
                fill_opacity=0.80,
                weight=1.5,
                popup=folium.Popup(popup_html, max_width=260),
                tooltip=f"{emoji} {carretera} \u2014 {municipio} ({severidad})",
            ).add_to(cluster)

        fg_dgt.add_to(mapa)

    # ------------------------------------------------------------------
    # FLOATING LEGEND (two-column)
    # ------------------------------------------------------------------
    # Left column: espiras congestion levels
    espira_items = "".join(
        f'<div style="margin:2px 0;">'
        f'<span style="display:inline-block;width:10px;height:10px;'
        f'background:{c};border-radius:50%;margin-right:5px;'
        f'vertical-align:middle;"></span>'
        f'<span style="vertical-align:middle;">{lbl}</span></div>'
        for lbl, c in [
            (f"Libre (<{_IH_LIBRE})", "#2ca02c"),
            (f"Fluido ({_IH_LIBRE}-{_IH_FLUIDO})", "#98df8a"),
            (f"Denso ({_IH_FLUIDO}-{_IH_DENSO})", "#ff7f0e"),
            (f"Saturado ({_IH_DENSO}-{_IH_SATURADO})", "#d62728"),
            (f"Congestionado (>{_IH_SATURADO})", "#8B0000"),
        ]
    )

    # Right column: DGT incident types
    dgt_items = "".join(
        f'<div style="margin:2px 0;">'
        f'<span style="display:inline-block;width:10px;height:10px;'
        f'background:{c};border-radius:50%;margin-right:5px;'
        f'vertical-align:middle;"></span>'
        f'<span style="vertical-align:middle;">'
        f'{_ICONO_TIPO.get(t, "")} {_LABEL_TIPO.get(t, t)}</span></div>'
        for t, c in _COLOR_TIPO.items()
    )

    legend_html = (
        '<div style="position:fixed;bottom:30px;left:30px;z-index:9999;'
        'background:rgba(30,30,30,0.88);color:#eee;padding:10px 14px;'
        'border-radius:8px;font-size:11px;max-width:480px;'
        'box-shadow:0 2px 8px rgba(0,0,0,0.4);">'
        '<div style="display:flex;gap:20px;">'
        '<div style="min-width:160px;">'
        '<b style="font-size:12px;">Flujo vehicular</b>'
        f'{espira_items}</div>'
        '<div style="min-width:170px;">'
        '<b style="font-size:12px;">Incidencias DGT</b>'
        f'{dgt_items}</div>'
        '</div></div>'
    )
    mapa.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(mapa)

    # Render — try three approaches in order of reliability
    map_html = mapa._repr_html_()
    rendered = False

    # Approach 1: st.components.v1.html (most reliable for Folium)
    try:
        import streamlit.components.v1 as components
        components.html(map_html, height=600, scrolling=False)
        rendered = True
    except Exception:
        pass

    # Approach 2: st.html (Streamlit 1.32+)
    if not rendered:
        try:
            st.html(map_html)
            rendered = True
        except Exception:
            pass

    # Approach 3: base64 iframe fallback
    if not rendered:
        import base64
        b64 = base64.b64encode(map_html.encode()).decode()
        st.markdown(
            f'<iframe src="data:text/html;base64,{b64}" '
            f'width="100%" height="600" frameborder="0"></iframe>',
            unsafe_allow_html=True,
        )
        rendered = True

    if not rendered:
        incs_con_coords = [
            inc for inc in (trafico_rt or {}).get("incidencias", [])
            if inc.get("lat") is not None and inc.get("lon") is not None
        ]
        st.info(
            f"Mapa generado ({len(map_html):,} bytes) pero no se puede "
            f"renderizar en este entorno. Espiras: "
            f"{len(sensores)} sensores. "
            f"DGT: {len(incs_con_coords)} incidencias."
        )

    # ------------------------------------------------------------------
    # STATS BELOW MAP (two columns)
    # ------------------------------------------------------------------
    col_esp, col_dgt = st.columns(2)

    with col_esp:
        if sensores:
            ihs = [s["ih"] for s in sensores]
            avg_ih = sum(ihs) / len(ihs)
            max_ih = max(ihs)
            busiest = next(s for s in sensores if s["ih"] == max_ih)

            # Congestion distribution
            n_libre = sum(1 for v in ihs if v < _IH_LIBRE)
            n_fluido = sum(1 for v in ihs if _IH_LIBRE <= v < _IH_FLUIDO)
            n_denso = sum(1 for v in ihs if _IH_FLUIDO <= v < _IH_DENSO)
            n_saturado = sum(1 for v in ihs if _IH_DENSO <= v < _IH_SATURADO)
            n_congest = sum(1 for v in ihs if v >= _IH_SATURADO)
            n = len(ihs)

            st.caption(
                f"\U0001f6a6 **Espiras VLCi** — {len(sensores)} sensores activos\n\n"
                f"Media: **{avg_ih:.0f}** veh/h · "
                f"Máx: **{max_ih:.0f}** veh/h (sensor {busiest.get('idpm', '?')})\n\n"
                f"\U0001f7e2 Libre: {n_libre} ({100*n_libre//n}%) · "
                f"\U0001f7e1 Fluido: {n_fluido} ({100*n_fluido//n}%) · "
                f"\U0001f7e0 Denso: {n_denso} ({100*n_denso//n}%)\n\n"
                f"\U0001f534 Saturado: {n_saturado} ({100*n_saturado//n}%) · "
                f"\u26ab Congest.: {n_congest} ({100*n_congest//n}%)"
            )
        else:
            st.caption(
                "\U0001f6a6 **Espiras VLCi** — Sin datos. "
                "Ejecuta streaming_vlci_trafico.py"
            )

    with col_dgt:
        if incs_geo:
            total_espana = (trafico_rt or {}).get(
                "total_incidencias", len(incs_geo)
            )
            timestamp = (trafico_rt or {}).get("timestamp", "\u2014")
            conteo_tipo = Counter(
                inc.get("causa", "obstruction") for inc in incs_geo
            )
            top3 = conteo_tipo.most_common(3)
            top3_text = " · ".join(
                f"{_ICONO_TIPO.get(t, '')} {_LABEL_TIPO.get(t, t)}: **{n}**"
                for t, n in top3
            )
            st.caption(
                f"\u26a0\ufe0f **Incidencias DGT** — "
                f"**{len(incs_geo)}** en Valencia "
                f"(de {total_espana} en España)\n\n"
                f"{top3_text}\n\n"
                f"\U0001f552 {timestamp}"
            )
        else:
            st.caption(
                "\u26a0\ufe0f **Incidencias DGT** — Sin datos geolocalizados. "
                "Ejecuta streaming_master.py"
            )

    logger.info(
        f"[Mapa combinado] {len(sensores)} espiras + "
        f"{len(incs_geo)} incidencias DGT renderizadas"
    )


def render_mapa_trafico(
    trafico_rt: Optional[dict] = None,
    espiras_rt: Optional[dict] = None,
) -> None:
    """
    Renderiza el mapa de trafico combinado con hasta 3 niveles de prioridad:
      1. Mapa Folium combinado (espiras + DGT)
      2. HTML pre-generado (Fase 6.1)
      3. Mensaje informativo

    Args:
        trafico_rt: Diccionario de datos RT de DGT (puede ser None).
        espiras_rt: Diccionario de datos RT de espiras VLCi (puede ser None).
    """
    st.markdown(
        '<div class="section-header">'
        '<h4>Mapa de tr\u00e1fico en tiempo real</h4>'
        '<p style="color:#888;font-size:0.85rem;">'
        'Flujo vehicular (espiras municipales) + incidencias DGT'
        '</p></div>',
        unsafe_allow_html=True,
    )

    # Prioridad 1: mapa combinado RT si hay algun dato
    has_espiras = (
        espiras_rt is not None
        and espiras_rt.get("sensores_activos")
    )
    has_dgt = False
    if trafico_rt is not None:
        incs = trafico_rt.get("incidencias", [])
        has_dgt = any(
            inc.get("lat") is not None and inc.get("lon") is not None
            for inc in incs
        )

    if has_espiras or has_dgt:
        _render_mapa_combinado(trafico_rt, espiras_rt)
        return

    # Prioridad 2: HTML pre-generado
    if MAPA_TRAFICO_HTML.exists():
        html_content = leer_html_visualizacion(str(MAPA_TRAFICO_HTML))
        if html_content:
            st.caption(
                f"Mapa pre-generado: `{MAPA_TRAFICO_HTML.name}` "
                "(regenerar con generar_mapas.py)"
            )
            try:
                import streamlit.components.v1 as components
                components.html(html_content, height=600, scrolling=False)
            except Exception:
                import base64
                b64 = base64.b64encode(html_content.encode()).decode()
                st.markdown(
                    f'<iframe src="data:text/html;base64,{b64}" '
                    f'width="100%" height="600" frameborder="0"></iframe>',
                    unsafe_allow_html=True,
                )
            return

    # Prioridad 3: sin mapa
    st.info(
        "Mapa de tr\u00e1fico no disponible. "
        "Genera datos ejecutando:\n\n"
        "`python 2.SCRIPTS/recopilacion/streaming_vlci_trafico.py`\n\n"
        "`python 2.SCRIPTS/recopilacion/streaming_master.py`"
    )
    logger.warning("[Mapa] Sin datos RT ni mapa_trafico.html")


# ==============================================================================
# 5. FUNCION ORQUESTADORA
# ==============================================================================

def render_tab_trafico(datos: dict) -> None:
    """
    Orquesta la tab completa de Trafico.

    Estructura:
      1. Header con descripcion
      2. KPIs (4 metricas)
      3. Grafico temporal adaptativo
      4. Distribucion semanal + Mapa en dos columnas
      5. Resumen de filtros activos

    Args:
        datos: Diccionario con datos filtrados.
              Claves usadas: 'trafico', 'trafico_rt', 'espiras_rt', '_filtros'.
    """
    # Header
    st.markdown(
        f'<div class="section-header"><h3>{TAB_NAMES["trafico"]}</h3>'
        f'<p>{DESCRIPCION_TABS["trafico"]}</p></div>',
        unsafe_allow_html=True,
    )

    df = datos.get("trafico")

    # Guard clause
    if df is None:
        st.error(
            "Sin datos de tráfico. "
            "Ejecuta pipeline_etl.py para generar los datos limpios."
        )
        return

    # 1. KPIs
    render_kpis_trafico(df, espiras_rt=datos.get("espiras_rt"))

    st.divider()

    # 2. Grafico temporal
    render_grafico_trafico(df)

    st.divider()

    # 3. Distribucion semanal + Mapa en columnas
    col_semana, col_mapa = st.columns([2, 3])

    with col_semana:
        render_distribucion_semana(df)

    with col_mapa:
        render_mapa_trafico(
            trafico_rt=datos.get("trafico_rt"),
            espiras_rt=datos.get("espiras_rt"),
        )

    # 4. Resumen de filtros
    filtros = datos.get("_filtros", {})
    anio_min = filtros.get("anio_min", "?")
    anio_max = filtros.get("anio_max", "?")
    n_registros = len(df) if df is not None else 0

    st.markdown("---")
    st.caption(
        f"Filtros activos — Periodo: {anio_min}-{anio_max} · "
        f"{n_registros:,} registros"
    )
