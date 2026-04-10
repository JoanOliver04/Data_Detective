# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Ranking de Barrios por Calidad del Aire / Calidad Urbana
==============================================================================
Ruta: 5.DASHBOARD/components/ranking_barrios.py
Autor: Joan | Fecha: 2026

Renderiza el ranking de barrios de Valencia en dos modos:
  - "Solo contaminacion": indice 0-10 basado en datos historicos de calidad
    del aire, identificando el contaminante mas problematico por barrio.
  - "Fusion Urbana": score combinado 0-10 que pondera contaminacion (hist.),
    meteorologia RT y trafico RT. Incluye delta RT vs historico si disponible.

Un toggle permite cambiar entre modos en tiempo real.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, VARIABLE_COLORS
from utils.quality_index import calcular_indice_calidad, nivel_desde_score

logger = logging.getLogger(__name__)

# Registros minimos para considerar un barrio con datos fiables
MIN_REGISTROS = 100

# Medallas para el podio
_MEDALLAS = {1: "🥇", 2: "🥈", 3: "🥉"}

# Pesos del indice urbano por eje (deben coincidir con urban_quality_index.py)
_PESO_CONTAM = 0.50
_PESO_METEO = 0.25
_PESO_TRAFICO = 0.25


# ==============================================================================
# FUNCION AUXILIAR: PEOR VARIABLE
# ==============================================================================

def _peor_variable(df_barrio: pd.DataFrame) -> Optional[str]:
    """
    Identifica el contaminante con el ratio mas alto respecto a su umbral OMS.

    Args:
        df_barrio: DataFrame filtrado para un barrio concreto.

    Returns:
        Nombre de la variable mas problematica, o None si sin datos.
    """
    df_valido = (
        df_barrio[df_barrio["calidad_dato"] == "ok"]
        if "calidad_dato" in df_barrio.columns
        else df_barrio
    )

    peor_var: Optional[str] = None
    peor_ratio: float = -1.0

    for variable, umbral in UMBRALES_OMS.items():
        if not umbral:
            continue
        valores = df_valido[df_valido["variable"] == variable]["valor"].dropna()
        if valores.empty:
            continue
        ratio = float(valores.mean()) / umbral
        if ratio > peor_ratio:
            peor_ratio = ratio
            peor_var = variable

    return peor_var


# ==============================================================================
# HELPERS: SCORES PARA FUSION URBANA
# ==============================================================================

def _score_contam_rt_barrio(
    contam_rt: Optional[dict],
    barrio: str,
) -> Optional[float]:
    """
    Calcula el score de contaminacion RT (0-10) para un barrio concreto.

    Filtra las estaciones de contam_rt cuyo campo 'barrio' coincide con el
    barrio indicado y ejecuta calcular_indice_calidad sobre esos datos.

    Args:
        contam_rt: Diccionario RT de AQICN con clave 'estaciones'.
        barrio: Nombre del barrio.

    Returns:
        Score float 0-10, o None si no hay estaciones para ese barrio.
    """
    if contam_rt is None:
        return None

    estaciones_barrio = [
        e for e in contam_rt.get("estaciones", [])
        if e.get("barrio") == barrio
    ]
    if not estaciones_barrio:
        return None

    key_map = {
        "no2": "NO2", "o3": "O3", "pm10": "PM10",
        "pm25": "PM2.5", "so2": "SO2", "co": "CO",
    }
    registros = []
    for est in estaciones_barrio:
        for key_rt, variable in key_map.items():
            valor = est.get(key_rt)
            if valor is not None:
                registros.append({
                    "variable": variable,
                    "valor": float(valor),
                    "calidad_dato": "ok",
                })

    if not registros:
        return None

    indice = calcular_indice_calidad(pd.DataFrame(registros), UMBRALES_OMS)
    return indice["score"] if indice else None


def _score_meteo_citywide(meteo_rt: Optional[dict]) -> Optional[float]:
    """
    Calcula el score meteorologico (0-10) a partir de los datos RT de ciudad.

    Replica la logica de urban_quality_index._score_meteorologia sin importar
    funciones privadas: score medio de temperatura, humedad y lluvia.

    Args:
        meteo_rt: Diccionario RT de OpenWeather con claves temp, humedad, etc.

    Returns:
        Score float 0-10, o None si no hay datos.
    """
    if meteo_rt is None:
        return None

    sub_scores = []

    temp = meteo_rt.get("temp")
    if temp is not None:
        t = float(temp)
        if 18.0 <= t <= 25.0:
            sub_scores.append(10.0)
        elif t < 18.0:
            sub_scores.append(max(0.0, 10.0 * t / 18.0))
        else:
            sub_scores.append(max(0.0, 10.0 * (1.0 - (t - 25.0) / 20.0)))

    humedad = meteo_rt.get("humedad")
    if humedad is not None:
        h = float(humedad)
        if 40.0 <= h <= 70.0:
            sub_scores.append(10.0)
        elif h < 40.0:
            sub_scores.append(max(0.0, 10.0 * h / 40.0))
        else:
            sub_scores.append(max(0.0, 10.0 * (1.0 - (h - 70.0) / 30.0)))

    rain = float(meteo_rt.get("rain_mm") or meteo_rt.get("lluvia_mm") or 0.0)
    if rain < 1.0:
        sub_scores.append(10.0)
    elif rain <= 10.0:
        sub_scores.append(5.0)
    else:
        sub_scores.append(2.0)

    return round(sum(sub_scores) / len(sub_scores), 2) if sub_scores else None


def _score_trafico_citywide(trafico_rt: Optional[dict]) -> Optional[float]:
    """
    Calcula el score de trafico (0-10) a partir de incidencias en Valencia.

    Args:
        trafico_rt: Diccionario RT de DGT con clave 'incidencias_valencia'.

    Returns:
        Score float 0-10, o None si no hay datos.
    """
    if trafico_rt is None:
        return None

    incidencias = trafico_rt.get("incidencias_valencia", [])
    total = len(incidencias)
    graves = sum(
        1 for inc in incidencias
        if inc.get("severidad") in ("high", "highest")
    )

    if total == 0:
        return 10.0
    elif total <= 3 and graves == 0:
        return 7.0
    elif total > 10 or graves >= 2:
        return 1.0
    else:
        return 4.0


def _calcular_score_urbano_barrio(
    score_hist: float,
    score_meteo: Optional[float],
    score_trafico: Optional[float],
) -> float:
    """
    Calcula el score urbano fusionado para un barrio.

    Pondera contaminacion historica + meteo RT + trafico RT.
    Si faltan ejes RT, redistribuye el peso al eje de contaminacion.

    Args:
        score_hist: Score historico de contaminacion del barrio (0-10).
        score_meteo: Score meteorologico ciudad (0-10) o None.
        score_trafico: Score trafico ciudad (0-10) o None.

    Returns:
        Score urbano fusionado entre 0 y 10.
    """
    ejes: List[tuple] = [(_PESO_CONTAM, score_hist)]
    if score_meteo is not None:
        ejes.append((_PESO_METEO, score_meteo))
    if score_trafico is not None:
        ejes.append((_PESO_TRAFICO, score_trafico))

    peso_total = sum(p for p, _ in ejes)
    score = sum(p * s for p, s in ejes) / peso_total
    return round(min(10.0, max(0.0, score)), 2)


# ==============================================================================
# CALCULO DEL RANKING
# ==============================================================================

def _calcular_filas_contaminacion(
    df_contaminacion: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Calcula la lista de filas del ranking en modo 'Solo contaminacion'.

    Args:
        df_contaminacion: DataFrame de contaminacion con columna 'barrio'.

    Returns:
        Lista de dicts ordenada de mayor a menor score.
    """
    barrios = sorted(df_contaminacion["barrio"].dropna().unique().tolist())
    filas = []

    for barrio in barrios:
        df_b = df_contaminacion[df_contaminacion["barrio"] == barrio]
        n_ok = int((df_b["calidad_dato"] == "ok").sum()) if "calidad_dato" in df_b.columns else len(df_b)

        indice = calcular_indice_calidad(df_b, UMBRALES_OMS)
        if indice is None:
            logger.warning("[Ranking] Sin indice para barrio '%s'", barrio)
            continue

        filas.append({
            "barrio":        barrio,
            "score":         indice["score"],
            "nivel":         indice["nivel"],
            "color":         indice["color"],
            "emoji":         indice["emoji"],
            "peor_variable": _peor_variable(df_b) or "-",
            "n_registros":   n_ok,
            "pocos_datos":   n_ok < MIN_REGISTROS,
        })

    filas.sort(key=lambda x: x["score"], reverse=True)
    return filas


def _calcular_filas_fusion(
    df_contaminacion: pd.DataFrame,
    contam_rt: Optional[dict],
    meteo_rt: Optional[dict],
    trafico_rt: Optional[dict],
) -> List[Dict[str, Any]]:
    """
    Calcula la lista de filas del ranking en modo 'Fusion Urbana'.

    Para cada barrio combina el score historico de contaminacion con los
    scores RT de meteorologia y trafico (ambos a nivel ciudad).
    El delta muestra el cambio respecto a la base historica cuando hay RT.

    Args:
        df_contaminacion: DataFrame de contaminacion con columna 'barrio'.
        contam_rt: Datos RT de AQICN (o None).
        meteo_rt: Datos RT de OpenWeather (o None).
        trafico_rt: Datos RT de DGT (o None).

    Returns:
        Lista de dicts ordenada de mayor a menor score urbano.
    """
    barrios = sorted(df_contaminacion["barrio"].dropna().unique().tolist())

    # Scores ciudad (iguales para todos los barrios)
    score_meteo = _score_meteo_citywide(meteo_rt)
    score_trafico = _score_trafico_citywide(trafico_rt)
    n_incidencias = len(trafico_rt.get("incidencias_valencia", [])) if trafico_rt else None

    filas = []
    for barrio in barrios:
        df_b = df_contaminacion[df_contaminacion["barrio"] == barrio]
        n_ok = int((df_b["calidad_dato"] == "ok").sum()) if "calidad_dato" in df_b.columns else len(df_b)

        indice_hist = calcular_indice_calidad(df_b, UMBRALES_OMS)
        if indice_hist is None:
            continue

        score_hist = indice_hist["score"]
        score_urbano = _calcular_score_urbano_barrio(score_hist, score_meteo, score_trafico)
        nivel, color, emoji = nivel_desde_score(score_urbano)

        # Delta RT: score RT del barrio vs score historico
        score_rt_barrio = _score_contam_rt_barrio(contam_rt, barrio)
        if score_rt_barrio is not None:
            delta_rt = round(score_rt_barrio - score_hist, 1)
        else:
            delta_rt = None

        # Ejes activos para tooltip
        ejes_str = "Contam."
        if score_meteo is not None:
            ejes_str += " + Meteo"
        if score_trafico is not None:
            ejes_str += " + Tráfico"

        filas.append({
            "barrio":          barrio,
            "score":           score_hist,          # score historico contam.
            "score_urbano":    score_urbano,         # score fusionado (clave de orden)
            "nivel":           nivel,
            "color":           color,
            "emoji":           emoji,
            "score_meteo":     score_meteo,
            "score_trafico":   score_trafico,
            "delta_rt":        delta_rt,
            "score_rt_barrio": score_rt_barrio,
            "peor_variable":   _peor_variable(df_b) or "-",
            "n_registros":     n_ok,
            "pocos_datos":     n_ok < MIN_REGISTROS,
            "n_incidencias":   n_incidencias,
            "ejes_str":        ejes_str,
        })

    filas.sort(key=lambda x: x["score_urbano"], reverse=True)
    return filas


# ==============================================================================
# RENDERIZADO: MODO CONTAMINACION
# ==============================================================================

def _render_resumen_rapido(filas: List[Dict[str, Any]]) -> None:
    """
    Lineas compactas de verificacion rapida: posicion, barrio, score, peor var.

    Args:
        filas: Lista de dicts, ordenada de mejor a peor.
    """
    lineas_html = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, f"{i}.")
        color = fila["color"]
        aviso = " ⚠️" if fila["pocos_datos"] else ""
        peor_color = VARIABLE_COLORS.get(fila["peor_variable"], "#888")

        lineas_html.append(
            f'<div style="padding:6px 0;border-bottom:1px solid #2a2a3a;">'
            f'<span style="font-size:1.05rem;font-weight:600;">'
            f'{pos_str} {fila["barrio"]}{aviso}</span>'
            f'<span style="color:#888;"> — </span>'
            f'<span style="color:{color};font-weight:700;">'
            f'{fila["score"]:.1f}/10 {fila["emoji"]} {fila["nivel"]}</span>'
            f'<span style="color:#888;"> | Peor: </span>'
            f'<span style="color:{peor_color};font-weight:600;">'
            f'{fila["peor_variable"]}</span>'
            f'</div>'
        )

    st.markdown(
        '<div style="font-family:monospace;padding:4px 0;">'
        + "".join(lineas_html)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_tabla(filas: List[Dict[str, Any]]) -> None:
    """
    Tabla completa en modo contaminacion con ProgressColumn para el score.

    Args:
        filas: Lista de dicts, ordenada de mejor a peor.
    """
    registros_tabla = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, str(i))
        aviso = " ⚠️" if fila["pocos_datos"] else ""
        registros_tabla.append({
            "Pos":               pos_str,
            "Barrio":            fila["barrio"] + aviso,
            "Score":             fila["score"],
            "Nivel":             f'{fila["emoji"]} {fila["nivel"]}',
            "Peor variable":     fila["peor_variable"],
            "Registros validos": fila["n_registros"],
        })

    df_tabla = pd.DataFrame(registros_tabla)
    st.dataframe(
        df_tabla,
        column_config={
            "Pos": st.column_config.TextColumn("Pos", width="small"),
            "Barrio": st.column_config.TextColumn("Barrio", width="medium"),
            "Score": st.column_config.ProgressColumn(
                "Score calidad aire (0-10)",
                min_value=0, max_value=10, format="%.2f", width="large",
            ),
            "Nivel": st.column_config.TextColumn("Nivel", width="medium"),
            "Peor variable": st.column_config.TextColumn("Peor contaminante", width="small"),
            "Registros validos": st.column_config.NumberColumn("Registros", format="%d", width="small"),
        },
        hide_index=True,
        use_container_width=True,
        height=min(36 + len(filas) * 35 + 36, 400),
    )


# ==============================================================================
# RENDERIZADO: MODO FUSION URBANA
# ==============================================================================

def _render_resumen_rapido_fusion(filas: List[Dict[str, Any]]) -> None:
    """
    Lineas compactas en modo fusion: posicion, barrio, score urbano, delta RT.

    Args:
        filas: Lista de dicts fusion, ordenada de mayor a menor score_urbano.
    """
    lineas_html = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, f"{i}.")
        color = fila["color"]
        aviso = " ⚠️" if fila["pocos_datos"] else ""
        peor_color = VARIABLE_COLORS.get(fila["peor_variable"], "#888")
        score_urbano = fila["score_urbano"]
        score_hist = fila["score"]

        # Delta RT badge
        delta_rt = fila.get("delta_rt")
        if delta_rt is not None:
            delta_color = "#2ca02c" if delta_rt >= 0 else "#e74c3c"
            flecha = "↑" if delta_rt >= 0 else "↓"
            delta_badge = (
                f'<span style="color:{delta_color};font-size:0.85rem;margin-left:6px;">'
                f'RT {flecha}{abs(delta_rt):.1f}</span>'
            )
        else:
            delta_badge = ""

        lineas_html.append(
            f'<div style="padding:6px 0;border-bottom:1px solid #2a2a3a;">'
            f'<span style="font-size:1.05rem;font-weight:600;">'
            f'{pos_str} {fila["barrio"]}{aviso}</span>'
            f'<span style="color:#888;"> — </span>'
            f'<span style="color:{color};font-weight:700;">'
            f'{score_urbano:.1f}/10 {fila["emoji"]} {fila["nivel"]}</span>'
            f'<span style="color:#555;font-size:0.8rem;margin-left:6px;">'
            f'(Contam.: {score_hist:.1f})</span>'
            f'{delta_badge}'
            f'<span style="color:#888;"> | Peor: </span>'
            f'<span style="color:{peor_color};font-weight:600;">'
            f'{fila["peor_variable"]}</span>'
            f'</div>'
        )

    st.markdown(
        '<div style="font-family:monospace;padding:4px 0;">'
        + "".join(lineas_html)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_tabla_fusion(filas: List[Dict[str, Any]]) -> None:
    """
    Tabla completa en modo fusion con columnas adicionales de score urbano,
    delta RT e incidencias de trafico.

    Args:
        filas: Lista de dicts fusion, ordenada de mayor a menor score_urbano.
    """
    registros_tabla = []
    for i, fila in enumerate(filas, start=1):
        pos_str = _MEDALLAS.get(i, str(i))
        aviso = " ⚠️" if fila["pocos_datos"] else ""

        # Delta RT
        delta_rt = fila.get("delta_rt")
        if delta_rt is not None:
            flecha = "↑" if delta_rt >= 0 else "↓"
            delta_str = f"{flecha}{abs(delta_rt):.1f}"
        else:
            delta_str = "—"

        # Incidencias trafico
        n_inc = fila.get("n_incidencias")
        inc_str = str(n_inc) if n_inc is not None else "—"

        registros_tabla.append({
            "Pos":             pos_str,
            "Barrio":          fila["barrio"] + aviso,
            "Score Urbano":    fila["score_urbano"],
            "Score Contam.":   fila["score"],
            "Nivel":           f'{fila["emoji"]} {fila["nivel"]}',
            "Delta RT":        delta_str,
            "Peor contam.":    fila["peor_variable"],
            "Incid. tráfico":  inc_str,
            "Registros":       fila["n_registros"],
        })

    df_tabla = pd.DataFrame(registros_tabla)
    st.dataframe(
        df_tabla,
        column_config={
            "Pos": st.column_config.TextColumn("Pos", width="small"),
            "Barrio": st.column_config.TextColumn("Barrio", width="medium"),
            "Score Urbano": st.column_config.ProgressColumn(
                "Score Urbano (0-10)",
                min_value=0, max_value=10, format="%.2f", width="large",
            ),
            "Score Contam.": st.column_config.ProgressColumn(
                "Contam. hist. (0-10)",
                min_value=0, max_value=10, format="%.2f", width="medium",
            ),
            "Nivel": st.column_config.TextColumn("Nivel", width="medium"),
            "Delta RT": st.column_config.TextColumn(
                "Δ RT vs hist.",
                width="small",
                help="Cambio del score de contaminación RT vs histórico. "
                     "↑ = mejora en RT, ↓ = empeora.",
            ),
            "Peor contam.": st.column_config.TextColumn("Peor contam.", width="small"),
            "Incid. tráfico": st.column_config.TextColumn(
                "Incid. tráfico",
                width="small",
                help="Incidencias activas en Valencia (dato ciudad, no por barrio).",
            ),
            "Registros": st.column_config.NumberColumn("Registros", format="%d", width="small"),
        },
        hide_index=True,
        use_container_width=True,
        height=min(36 + len(filas) * 35 + 36, 420),
    )


def _render_leyenda_fusion(
    score_meteo: Optional[float],
    score_trafico: Optional[float],
    n_incidencias: Optional[int],
) -> None:
    """
    Muestra una leyenda compacta con los scores ciudad usados en la fusion.

    Args:
        score_meteo: Score meteorologico ciudad (o None).
        score_trafico: Score trafico ciudad (o None).
        n_incidencias: Numero de incidencias activas en Valencia (o None).
    """
    partes = []
    if score_meteo is not None:
        nivel_m, color_m, emoji_m = nivel_desde_score(score_meteo)
        partes.append(
            f'<span style="margin-right:16px;">'
            f'🌤️ Meteo ciudad: '
            f'<b style="color:{color_m};">{score_meteo:.1f}/10 {emoji_m}</b>'
            f'</span>'
        )
    if score_trafico is not None:
        nivel_t, color_t, emoji_t = nivel_desde_score(score_trafico)
        inc_txt = f" ({n_incidencias} incid.)" if n_incidencias is not None else ""
        partes.append(
            f'<span style="margin-right:16px;">'
            f'🚗 Tráfico ciudad: '
            f'<b style="color:{color_t};">{score_trafico:.1f}/10 {emoji_t}</b>'
            f'{inc_txt}'
            f'</span>'
        )
    if not partes:
        return

    st.markdown(
        f'<div style="background:rgba(255,255,255,0.03);border-radius:8px;'
        f'padding:8px 14px;margin-bottom:10px;font-size:0.85rem;">'
        f'<b>Contexto ciudad (mismo para todos los barrios):</b> '
        + "".join(partes)
        + f'<span style="color:#666;font-size:0.75rem;">'
        f' · Pesos: contam. {int(_PESO_CONTAM*100)}%'
        + (f', meteo {int(_PESO_METEO*100)}%' if score_meteo is not None else '')
        + (f', tráfico {int(_PESO_TRAFICO*100)}%' if score_trafico is not None else '')
        + '</span></div>',
        unsafe_allow_html=True,
    )


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def render_ranking_barrios(
    df_contaminacion: pd.DataFrame,
    contam_rt: Optional[dict] = None,
    meteo_rt: Optional[dict] = None,
    trafico_rt: Optional[dict] = None,
) -> None:
    """
    Renderiza el ranking de barrios con toggle entre dos modos.

    Modo "Solo contaminacion":
      - Score 0-10 historico por barrio via calcular_indice_calidad()
      - Contaminante mas problematico
      - Tabla con ProgressColumn

    Modo "Fusion Urbana" (requiere urban_quality_index disponible):
      - Score 0-10 fusionado: contam. hist. (50%) + meteo RT (25%) + trafico RT (25%)
      - Delta RT vs historico si hay datos de estacion RT en el barrio
      - Incidencias de trafico activas (dato ciudad)
      - Ordenado por score urbano fusionado

    Si no hay datos RT disponibles, la fusion usa solo contaminacion historica
    y degrada al mismo resultado que el modo solo contaminacion.

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion (filtros globales
            ya aplicados), debe contener columna 'barrio'.
        contam_rt: Datos RT de AQICN (de cargar_contaminacion_realtime()).
        meteo_rt: Datos RT de OpenWeather (de cargar_meteo_realtime()).
        trafico_rt: Datos RT de DGT (de cargar_trafico_realtime()).
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.warning("Sin datos de contaminación para generar el ranking.")
        return

    if "barrio" not in df_contaminacion.columns:
        st.warning("El DataFrame no contiene la columna 'barrio'.")
        return

    barrios_disponibles = df_contaminacion["barrio"].dropna().unique().tolist()
    if not barrios_disponibles:
        st.info("No hay barrios identificados en los datos filtrados.")
        return

    # Determinar si los datos RT aportan algo a la fusion
    hay_rt = contam_rt is not None or meteo_rt is not None or trafico_rt is not None

    # Toggle entre modos
    col_toggle, col_info = st.columns([2, 3])
    with col_toggle:
        modo_fusion = st.toggle(
            "🏙️ Modo Fusión Urbana",
            value=False,
            key="ranking_modo_fusion",
            disabled=not hay_rt,
            help=(
                "Combina contaminación histórica con datos RT de meteorología "
                "y tráfico. Requiere datos en tiempo real."
                if hay_rt
                else "Activa el streaming para habilitar el Modo Fusión Urbana."
            ),
        )
    with col_info:
        if modo_fusion:
            ejes_activos = []
            if meteo_rt is not None:
                ejes_activos.append("🌤️ Meteo RT")
            if trafico_rt is not None:
                ejes_activos.append("🚗 Tráfico RT")
            if contam_rt is not None:
                ejes_activos.append("🌫️ Contam. RT")
            st.caption(
                f"Fusión: Contam. hist. + {' + '.join(ejes_activos)}"
                if ejes_activos
                else "Fusión: solo contaminación histórica (sin datos RT)"
            )

    st.markdown("")

    if not modo_fusion:
        # --- MODO SOLO CONTAMINACION ---
        filas = _calcular_filas_contaminacion(df_contaminacion)
        if not filas:
            st.warning("No se pudo calcular el índice para ningún barrio.")
            return

        logger.info(
            "[Ranking] Solo contam. %d barrios. Mejor: %s (%.1f), Peor: %s (%.1f)",
            len(filas), filas[0]["barrio"], filas[0]["score"],
            filas[-1]["barrio"], filas[-1]["score"],
        )

        _render_resumen_rapido(filas)
        st.markdown("")
        _render_tabla(filas)

    else:
        # --- MODO FUSION URBANA ---
        filas = _calcular_filas_fusion(df_contaminacion, contam_rt, meteo_rt, trafico_rt)
        if not filas:
            st.warning("No se pudo calcular el índice urbano para ningún barrio.")
            return

        # Leyenda con scores ciudad
        score_meteo = _score_meteo_citywide(meteo_rt)
        score_trafico = _score_trafico_citywide(trafico_rt)
        n_inc = len(trafico_rt.get("incidencias_valencia", [])) if trafico_rt else None
        _render_leyenda_fusion(score_meteo, score_trafico, n_inc)

        logger.info(
            "[Ranking] Fusion urbana %d barrios. Mejor: %s (%.1f), Peor: %s (%.1f)",
            len(filas), filas[0]["barrio"], filas[0]["score_urbano"],
            filas[-1]["barrio"], filas[-1]["score_urbano"],
        )

        _render_resumen_rapido_fusion(filas)
        st.markdown("")
        _render_tabla_fusion(filas)

    # Aviso barrios con pocos datos
    barrios_aviso = [f["barrio"] for f in filas if f["pocos_datos"]]
    if barrios_aviso:
        st.caption(
            f"⚠️ Datos limitados (<{MIN_REGISTROS} registros válidos): "
            + ", ".join(barrios_aviso)
            + ". Los scores pueden no ser representativos."
        )
