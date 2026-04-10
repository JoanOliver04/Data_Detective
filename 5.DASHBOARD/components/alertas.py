# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Componente: Sistema de Alertas de Contaminacion
==============================================================================
Ruta: 5.DASHBOARD/components/alertas.py
Autor: Joan | Fecha: 2026

Evalua si algun barrio supera los umbrales OMS en el periodo mas reciente
disponible (ultimos 30 dias) y muestra alertas ordenadas por gravedad.

Niveles:
  - Critica  (ratio > 1.5x OMS): st.error  con mensaje 🚨
  - Aviso    (ratio > 1.0x OMS): st.warning con mensaje ⚠️
  - OK       (todo dentro):       st.success con mensaje ✅
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set

import pandas as pd
import streamlit as st

from config import UMBRALES_OMS, VARIABLE_COLORS

logger = logging.getLogger(__name__)

# Maximo de alertas visibles directamente (el resto van al expander)
MAX_ALERTAS_VISIBLES = 5

# Ventana temporal preferida en dias
VENTANA_DIAS = 30

# Ratio a partir del cual la alerta es critica
RATIO_CRITICO = 1.5


# ==============================================================================
# LOGICA DE CALCULO
# ==============================================================================

def _filtrar_periodo_reciente(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Filtra el DataFrame a los ultimos VENTANA_DIAS dias de datos disponibles.

    Parte de la fecha maxima presente en el df (no de hoy), para que el
    dashboard funcione tanto con datos en tiempo real como con historicos.

    Args:
        df: DataFrame de contaminacion con columna 'fecha_utc'.

    Returns:
        Tupla (df_filtrado, descripcion_periodo).
    """
    if "fecha_utc" not in df.columns:
        return df, "periodo completo (sin columna fecha_utc)"

    # Asegurar que fecha_utc es datetime
    col = df["fecha_utc"]
    if not pd.api.types.is_datetime64_any_dtype(col):
        try:
            col = pd.to_datetime(col, utc=True, errors="coerce")
        except Exception:
            return df, "periodo completo (error al parsear fechas)"

    fecha_max = col.max()
    if pd.isna(fecha_max):
        return df, "periodo completo (sin fechas validas)"

    fecha_min_ventana = fecha_max - timedelta(days=VENTANA_DIAS)
    df_filtrado = df[col >= fecha_min_ventana]

    if df_filtrado.empty:
        # Fallback: usar todos los datos disponibles
        logger.warning("[Alertas] Sin datos en los ultimos %d dias; usando todos.", VENTANA_DIAS)
        return df, f"todos los datos disponibles (sin datos recientes)"

    fecha_min_real = col[col >= fecha_min_ventana].min()
    descripcion = (
        f"{fecha_min_real.strftime('%d/%m/%Y')} → "
        f"{fecha_max.strftime('%d/%m/%Y')}"
    )
    return df_filtrado, descripcion


def _calcular_alertas(df_reciente: pd.DataFrame) -> List[Dict]:
    """
    Genera la lista de alertas comparando medias por barrio/variable vs OMS.

    Args:
        df_reciente: DataFrame ya filtrado al periodo de evaluacion.

    Returns:
        Lista de dicts ordenada de mayor a menor ratio (mas grave primero).
        Cada dict contiene: barrio, variable, media, umbral, ratio, critica.
    """
    df_valido = (
        df_reciente[df_reciente["calidad_dato"] == "ok"]
        if "calidad_dato" in df_reciente.columns
        else df_reciente
    )

    if df_valido.empty:
        return []

    # Agrupar por barrio + variable, calcular media
    grupo = (
        df_valido
        .groupby(["barrio", "variable"], as_index=False)["valor"]
        .mean()
        .rename(columns={"valor": "media"})
    )

    alertas = []
    for _, fila in grupo.iterrows():
        barrio   = fila["barrio"]
        variable = fila["variable"]
        media    = float(fila["media"])
        umbral   = UMBRALES_OMS.get(variable)

        if umbral is None or pd.isna(media):
            continue

        ratio = media / umbral
        if ratio <= 1.0:
            continue  # Dentro del umbral, sin alerta

        alertas.append({
            "barrio":   barrio,
            "variable": variable,
            "media":    round(media, 2),
            "umbral":   umbral,
            "ratio":    round(ratio, 3),
            "critica":  ratio >= RATIO_CRITICO,
        })

    # Ordenar: criticas primero, luego por ratio descendente
    alertas.sort(key=lambda a: (not a["critica"], -a["ratio"]))

    logger.info(
        "[Alertas] %d alertas generadas (%d criticas).",
        len(alertas),
        sum(1 for a in alertas if a["critica"]),
    )
    for a in alertas:
        nivel = "CRITICA" if a["critica"] else "AVISO"
        logger.warning(
            "[Alertas] %s | %s | %s: %.1f µg/m³ (OMS: %.1f, ratio: %.2fx)",
            nivel, a["barrio"], a["variable"], a["media"], a["umbral"], a["ratio"],
        )

    return alertas


def _texto_alerta(alerta: Dict) -> str:
    """
    Genera el texto de una alerta individual.

    Args:
        alerta: Dict con barrio, variable, media, umbral, ratio, critica.

    Returns:
        String formateado para mostrar en st.warning / st.error.
    """
    barrio   = alerta["barrio"]
    variable = alerta["variable"]
    media    = alerta["media"]
    umbral   = alerta["umbral"]
    ratio    = alerta["ratio"]

    if alerta["critica"]:
        return (
            f"🚨 ALERTA CRÍTICA: {barrio} — {variable} a {media:.1f} µg/m³ "
            f"(umbral OMS: {umbral:.0f} µg/m³ | {ratio:.1f}x el límite)"
        )
    return (
        f"⚠️ {barrio}: {variable} a {media:.1f} µg/m³ "
        f"(umbral OMS: {umbral:.0f} µg/m³ | {ratio:.2f}x el límite)"
    )


# ==============================================================================
# ALERTAS EN TIEMPO REAL
# ==============================================================================

# Mapeo clave RT -> nombre de variable canonica
_RT_KEY_MAP = {
    "no2":  "NO2",
    "o3":   "O3",
    "pm10": "PM10",
    "pm25": "PM2.5",
    "so2":  "SO2",
    "co":   "CO",
}

# CSS de animacion pulse (se inyecta una sola vez si hay alertas criticas RT)
_CSS_PULSE = """
<style>
@keyframes _dd_pulse {
    0%   { box-shadow: 0 0 0 0   rgba(231, 76, 60, 0.75); }
    70%  { box-shadow: 0 0 0 10px rgba(231, 76, 60, 0.00); }
    100% { box-shadow: 0 0 0 0   rgba(231, 76, 60, 0.00); }
}
.dd-alerta-critica-rt {
    border: 2px solid #e74c3c;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 6px 0;
    background: rgba(231, 76, 60, 0.08);
    animation: _dd_pulse 2s ease-in-out infinite;
    font-size: 0.92rem;
    line-height: 1.5;
}
.dd-alerta-aviso-rt {
    border: 1px solid #f39c12;
    border-radius: 8px;
    padding: 10px 14px;
    margin: 6px 0;
    background: rgba(243, 156, 18, 0.07);
    font-size: 0.92rem;
    line-height: 1.5;
}
</style>
"""


def _calcular_alertas_rt(contam_rt: dict) -> List[Dict]:
    """
    Genera la lista de alertas comparando valores RT por estacion vs OMS.

    Args:
        contam_rt: Diccionario RT de AQICN con claves 'estaciones' y 'timestamp'.

    Returns:
        Lista de dicts ordenada criticas primero, luego por ratio descendente.
        Cada dict: estacion_id, nombre, barrio, variable, valor, umbral,
                   ratio, exceso_pct, critica, timestamp.
    """
    estaciones = contam_rt.get("estaciones", [])
    timestamp = contam_rt.get("timestamp", "")
    alertas: List[Dict] = []

    for est in estaciones:
        barrio = est.get("barrio", "Desconocido")
        nombre = est.get("nombre", est.get("estacion_id", "?"))

        for key_rt, variable in _RT_KEY_MAP.items():
            valor = est.get(key_rt)
            if valor is None:
                continue
            valor = float(valor)
            umbral = UMBRALES_OMS.get(variable)
            if not umbral:
                continue

            ratio = valor / umbral
            if ratio <= 1.0:
                continue

            alertas.append({
                "estacion_id": est.get("estacion_id", ""),
                "nombre":      nombre,
                "barrio":      barrio,
                "variable":    variable,
                "valor":       round(valor, 1),
                "umbral":      umbral,
                "ratio":       round(ratio, 3),
                "exceso_pct":  round((ratio - 1.0) * 100, 1),
                "critica":     ratio >= RATIO_CRITICO,
                "timestamp":   timestamp,
            })

    alertas.sort(key=lambda a: (not a["critica"], -a["ratio"]))

    n_criticas = sum(1 for a in alertas if a["critica"])
    logger.info(
        "[AlertasRT] %d alertas RT (%d criticas), %d estaciones evaluadas.",
        len(alertas), n_criticas, len(estaciones),
    )
    return alertas


def _barrios_con_alerta_historica(
    df_contaminacion: Optional[pd.DataFrame],
) -> Set[str]:
    """
    Devuelve el conjunto de barrios que tienen alertas en el periodo reciente.

    Reutiliza _filtrar_periodo_reciente y _calcular_alertas internamente.

    Args:
        df_contaminacion: DataFrame de contaminacion con columna 'barrio'.

    Returns:
        Set de nombres de barrio con alguna alerta historica activa.
    """
    if df_contaminacion is None or df_contaminacion.empty:
        return set()
    if "barrio" not in df_contaminacion.columns:
        return set()

    df_rec, _ = _filtrar_periodo_reciente(df_contaminacion)
    alertas = _calcular_alertas(df_rec)
    return {a["barrio"] for a in alertas}


def _fmt_timestamp_rt(ts: str) -> str:
    """
    Formatea el timestamp ISO del dato RT como cadena legible.

    Args:
        ts: String ISO (p.ej. '2026-04-10T14:35:00').

    Returns:
        String como '10/04/2026 14:35', o el original si no parsea.
    """
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except (ValueError, TypeError):
        return ts


def _html_alerta_rt(alerta: Dict, persistente: bool) -> str:
    """
    Genera el HTML de una alerta RT individual.

    Args:
        alerta: Dict de alerta RT.
        persistente: True si el barrio tambien tiene alerta historica activa.

    Returns:
        String HTML listo para st.markdown(unsafe_allow_html=True).
    """
    color_var = VARIABLE_COLORS.get(alerta["variable"], "#888")
    css_class = "dd-alerta-critica-rt" if alerta["critica"] else "dd-alerta-aviso-rt"
    nivel_txt = "🚨 ALERTA CRÍTICA" if alerta["critica"] else "⚠️ Aviso"
    persistente_badge = (
        ' &nbsp;<span style="background:#8b0000;color:#fff;border-radius:4px;'
        'padding:1px 6px;font-size:0.78rem;font-weight:700;">⚡ PERSISTENTE</span>'
        if persistente else ""
    )
    ts_fmt = _fmt_timestamp_rt(alerta["timestamp"])

    return (
        f'<div class="{css_class}">'
        f'<span style="font-weight:700;">{nivel_txt}</span>{persistente_badge}'
        f'<br>'
        f'<b>{alerta["barrio"]}</b> — '
        f'<span style="color:{color_var};font-weight:700;">{alerta["variable"]}</span>'
        f': <b>{alerta["valor"]:.1f} µg/m³</b>'
        f' &nbsp;·&nbsp; OMS: {alerta["umbral"]:.0f} µg/m³'
        f' &nbsp;·&nbsp; <b>{alerta["exceso_pct"]:.0f}% por encima</b>'
        f' &nbsp;·&nbsp; {alerta["ratio"]:.2f}× el límite'
        f'<br>'
        f'<span style="color:#888;font-size:0.8rem;">'
        f'Estación: {alerta["nombre"]} &nbsp;·&nbsp; Medición: {ts_fmt}'
        f'</span>'
        f'</div>'
    )


def render_alertas_realtime(
    contam_rt: Optional[dict],
    df_contaminacion: Optional[pd.DataFrame] = None,
) -> None:
    """
    Renderiza el panel de alertas de contaminacion en tiempo real.

    Compara los valores actuales de cada estacion RT con los umbrales OMS.
    Las alertas criticas (ratio >= 1.5x) tienen borde rojo pulsante.
    Si el barrio tambien tiene alerta historica activa, se marca como
    '⚡ PERSISTENTE'.

    No muestra nada si contam_rt es None o no hay excesos.

    Args:
        contam_rt: Diccionario RT de AQICN (de cargar_contaminacion_realtime).
                   Claves esperadas: 'estaciones', 'timestamp'.
        df_contaminacion: DataFrame historico de contaminacion (opcional).
                          Si se provee, permite detectar alertas persistentes.
    """
    if contam_rt is None:
        return

    alertas = _calcular_alertas_rt(contam_rt)

    if not alertas:
        return

    # Barrios con alerta historica (para detectar persistencia)
    barrios_hist = _barrios_con_alerta_historica(df_contaminacion)

    n_criticas = sum(1 for a in alertas if a["critica"])
    n_persistentes = sum(1 for a in alertas if a["barrio"] in barrios_hist)

    # Titulo del expander
    titulo_partes = [f"🚨 Alertas en Tiempo Real ({len(alertas)} activas"]
    if n_criticas:
        titulo_partes.append(f", {n_criticas} críticas")
    if n_persistentes:
        titulo_partes.append(f", {n_persistentes} ⚡ persistentes")
    titulo_partes.append(")")
    titulo = "".join(titulo_partes)

    with st.expander(titulo, expanded=n_criticas > 0):
        ts_global = _fmt_timestamp_rt(contam_rt.get("timestamp", ""))
        n_estaciones = len(contam_rt.get("estaciones", []))
        st.caption(
            f"Evaluación en tiempo real · {n_estaciones} estaciones · "
            f"Última captura: {ts_global}"
        )

        # Inyectar CSS de animacion una sola vez si hay criticas
        if n_criticas > 0:
            st.markdown(_CSS_PULSE, unsafe_allow_html=True)

        # Renderizar cada alerta como HTML
        for alerta in alertas:
            persistente = alerta["barrio"] in barrios_hist
            st.markdown(
                _html_alerta_rt(alerta, persistente),
                unsafe_allow_html=True,
            )

    logger.info(
        "[AlertasRT] Panel renderizado: %d alertas, %d criticas, %d persistentes.",
        len(alertas), n_criticas, n_persistentes,
    )


# ==============================================================================
# RENDERIZADO
# ==============================================================================

def render_alertas(df_contaminacion: Optional[pd.DataFrame]) -> None:
    """
    Renderiza el panel de alertas de contaminacion en el dashboard.

    Muestra hasta MAX_ALERTAS_VISIBLES alertas directamente; el resto
    quedan ocultas en un st.expander. Si todo esta dentro de OMS,
    muestra un mensaje de confirmacion verde.

    Args:
        df_contaminacion: DataFrame filtrado de contaminacion
                          (con filtros globales ya aplicados).
    """
    if df_contaminacion is None or df_contaminacion.empty:
        st.info("Sin datos de contaminación para evaluar alertas.")
        return

    if "barrio" not in df_contaminacion.columns:
        st.info("Sin columna 'barrio' en los datos; no se pueden evaluar alertas.")
        return

    # --- Filtrar al periodo reciente ---
    df_reciente, descripcion_periodo = _filtrar_periodo_reciente(df_contaminacion)

    if df_reciente.empty:
        st.warning("Sin datos recientes para evaluar alertas.")
        return

    # --- Calcular alertas ---
    alertas = _calcular_alertas(df_reciente)

    # --- Cabecera del panel ---
    n_barrios = df_reciente["barrio"].nunique() if "barrio" in df_reciente.columns else "?"
    st.caption(
        f"Evaluación sobre {n_barrios} barrios | Periodo: {descripcion_periodo}"
    )

    # --- Caso: sin alertas ---
    if not alertas:
        st.success(
            "✅ Todos los barrios dentro de los límites OMS "
            f"en el periodo evaluado ({descripcion_periodo})."
        )
        return

    # --- Separar alertas visibles del resto ---
    visibles  = alertas[:MAX_ALERTAS_VISIBLES]
    restantes = alertas[MAX_ALERTAS_VISIBLES:]

    _render_lista_alertas(visibles)

    if restantes:
        with st.expander(
            f"Ver todas las alertas ({len(alertas)} en total — "
            f"{len(restantes)} adicionales)"
        ):
            _render_lista_alertas(restantes)


def _render_lista_alertas(alertas: List[Dict]) -> None:
    """
    Renderiza una lista de alertas usando st.error / st.warning segun gravedad.

    Args:
        alertas: Lista de dicts de alerta, ya ordenada.
    """
    for alerta in alertas:
        texto = _texto_alerta(alerta)
        if alerta["critica"]:
            st.error(texto)
        else:
            st.warning(texto)
