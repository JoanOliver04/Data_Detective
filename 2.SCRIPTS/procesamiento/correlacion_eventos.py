# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.5: Correlación Eventos ↔ Contaminación / Tráfico
==============================================================================

Descripción:
    Cuantifica el impacto de eventos masivos (Fallas, partidos Valencia CF,
    conciertos, etc.) sobre:
      1) Niveles de contaminación (NO₂, O₃, PM10, PM2.5)
      2) Incidencias de tráfico

    Metodología quasi-experimental:
    ─────────────────────────────────
    Para cada evento se construye un BASELINE de referencia usando días
    "comparables" que cumplen simultáneamente:
      • Mismo mes del año (control estacional)
      • Mismo día de la semana (control de patrón semanal)
      • Sin solapamiento con ningún otro evento (evitar contaminación cruzada)
      • Sin lluvia significativa (precipitación ≤ 5mm)
      • Solo registros con calidad_dato == "ok" (contaminación)

    Métrica de impacto:
      impact_pct = ((media_evento - media_baseline) / media_baseline) * 100

    Además se almacenan las condiciones meteorológicas (temp, precip) tanto
    durante el evento como en el baseline, como CONTROL DESCRIPTIVO
    (no regresión, sino transparencia para análisis posterior).

Archivos de entrada:
    3.DATOS_LIMPIOS/contaminacion_normalizada.parquet
    3.DATOS_LIMPIOS/trafico_limpio.csv
    3.DATOS_LIMPIOS/meteorologia_limpio.csv
    1.DATOS_EN_CRUDO/eventos/eventos_clasificados.json

Archivo de salida:
    3.DATOS_LIMPIOS/impacto_eventos.csv

Ruta esperada del script:
    2.SCRIPTS/procesamiento/correlacion_eventos.py

Uso:
    python correlacion_eventos.py

Commit sugerido:
    feat: implement advanced event impact correlation with baseline control

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
import hashlib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- Archivos de entrada ---
CONTAMINACION_PATH = PROJECT_ROOT / "3.DATOS_LIMPIOS" / \
    "contaminacion_normalizada.parquet"
TRAFICO_PATH = PROJECT_ROOT / "3.DATOS_LIMPIOS" / "trafico_limpio.csv"
METEOROLOGIA_PATH = PROJECT_ROOT / "3.DATOS_LIMPIOS" / "meteorologia_limpio.csv"
EVENTOS_PATH = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / \
    "eventos" / "eventos_clasificados.json"

# --- Archivo de salida ---
OUTPUT_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
OUTPUT_FILE = OUTPUT_DIR / "impacto_eventos.csv"

# --- Logs ---
LOG_DIR = PROJECT_ROOT / "logs"

# --- Umbrales ---
PRECIPITACION_UMBRAL_MM = 5.0  # Días con >5mm se excluyen del baseline

# --- Timezone ---
TZ_LOCAL = "Europe/Madrid"


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola) siguiendo el patrón del proyecto.
    Mismo estilo que las fases anteriores (5.1, 5.2, 5.3, 5.4).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "correlacion_eventos.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Correlacion_Eventos")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Archivo (todo)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    # Consola (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

def load_data(logger: logging.Logger) -> Tuple[
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    Optional[List[Dict[str, Any]]]
]:
    """
    Carga los 4 datasets necesarios para el análisis.

    Returns:
        Tupla: (df_contam, df_trafico, df_meteo, eventos_list)
        Cualquiera puede ser None si falla la carga.
    """
    logger.info("─" * 40)
    logger.info("PASO 1: Carga de datos")
    logger.info("─" * 40)

    df_contam = None
    df_trafico = None
    df_meteo = None
    eventos_list = None

    # --- 1A: Contaminación (Parquet) ---
    logger.info(f"  1A: Contaminación → {CONTAMINACION_PATH.name}")
    if CONTAMINACION_PATH.exists():
        try:
            df_contam = pd.read_parquet(CONTAMINACION_PATH)
            logger.info(f"      ✓ {len(df_contam):,} registros cargados")
            logger.info(f"      Columnas: {list(df_contam.columns)}")
            logger.info(
                f"      Rango: {df_contam['fecha_utc'].min()} → "
                f"{df_contam['fecha_utc'].max()}"
            )
        except Exception as e:
            logger.error(f"      ✘ Error leyendo parquet: {e}")
    else:
        logger.warning(f"      ⚠ Archivo no encontrado: {CONTAMINACION_PATH}")

    # --- 1B: Tráfico (CSV) ---
    logger.info(f"  1B: Tráfico → {TRAFICO_PATH.name}")
    if TRAFICO_PATH.exists():
        try:
            df_trafico = pd.read_csv(TRAFICO_PATH, parse_dates=["fecha"])
            logger.info(f"      ✓ {len(df_trafico):,} registros cargados")
            logger.info(f"      Columnas: {list(df_trafico.columns)}")
        except Exception as e:
            logger.error(f"      ✘ Error leyendo CSV tráfico: {e}")
    else:
        logger.warning(f"      ⚠ Archivo no encontrado: {TRAFICO_PATH}")

        # --- 1C: Meteorología (CSV) ---
    logger.info(f"  1C: Meteorología → {METEOROLOGIA_PATH.name}")
    if METEOROLOGIA_PATH.exists():
        try:
            # Cargar sin parsear fechas primero
            df_meteo = pd.read_csv(METEOROLOGIA_PATH)

            # Conversión robusta de fechas con ISO8601 (maneja microsegundos)
            df_meteo["fecha"] = pd.to_datetime(
                df_meteo["fecha"], format='ISO8601', utc=True, errors='coerce')

            fechas_invalidas = df_meteo["fecha"].isna().sum()
            if fechas_invalidas > 0:
                logger.warning(
                    f"      ⚠ {fechas_invalidas:,} fechas inválidas encontradas")
                df_meteo = df_meteo.dropna(subset=["fecha"])

            logger.info(f"      ✓ {len(df_meteo):,} registros cargados")
            logger.info(f"      Columnas: {list(df_meteo.columns)}")
        except Exception as e:
            logger.error(f"      ✘ Error leyendo CSV meteorología: {e}")
    else:
        logger.warning(f"      ⚠ Archivo no encontrado: {METEOROLOGIA_PATH}")

    # --- 1D: Eventos (JSON) ---
    logger.info(f"  1D: Eventos → {EVENTOS_PATH.name}")
    if EVENTOS_PATH.exists():
        try:
            with open(EVENTOS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            eventos_list = data.get("eventos", [])
            logger.info(f"      ✓ {len(eventos_list)} eventos cargados")
        except Exception as e:
            logger.error(f"      ✘ Error leyendo JSON eventos: {e}")
    else:
        logger.warning(f"      ⚠ Archivo no encontrado: {EVENTOS_PATH}")

    return df_contam, df_trafico, df_meteo, eventos_list


# ==============================================================================
# PREPARACIÓN DE DATOS (AGREGACIONES DIARIAS)
# ==============================================================================

def build_daily_aggregations(
    df_contam: Optional[pd.DataFrame],
    df_trafico: Optional[pd.DataFrame],
    df_meteo: Optional[pd.DataFrame],
    logger: logging.Logger,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Agrega los datos al nivel DIARIO para poder cruzarlos con las ventanas
    de eventos.

    Returns:
        Tupla: (contam_diaria, trafico_diario, meteo_diaria)
        Cada uno es un DataFrame con 'fecha' (date) como columna,
        o None si los datos de entrada no estaban disponibles.
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 2: Agregaciones diarias")
    logger.info("─" * 40)

    contam_diaria = None
    trafico_diario = None
    meteo_diaria = None

    # ─── 2A: Contaminación diaria ───
    # Esquema entrada: fecha_utc, variable, valor, calidad_dato
    # Salida: fecha (date) | variable | valor_medio | n_registros
    if df_contam is not None and not df_contam.empty:
        logger.info("  2A: Agregando contaminación a nivel diario...")

        df_ok = df_contam[df_contam["calidad_dato"] == "ok"].copy()
        logger.info(
            f"      Registros con calidad='ok': {len(df_ok):,} de {len(df_contam):,}")

        # Asegurar datetime y extraer fecha (date)
        df_ok["fecha_utc"] = pd.to_datetime(df_ok["fecha_utc"], utc=True)
        df_ok["fecha"] = df_ok["fecha_utc"].dt.date

        # Agrupar: media diaria por variable (promedio de todas las estaciones)
        contam_diaria = (
            df_ok
            .groupby(["fecha", "variable"], as_index=False)
            .agg(
                valor_medio=("valor", "mean"),
                n_registros=("valor", "count"),
            )
        )
        contam_diaria["fecha"] = pd.to_datetime(contam_diaria["fecha"])

        logger.info(
            f"      ✓ {len(contam_diaria):,} filas "
            f"({contam_diaria['variable'].nunique()} variables × "
            f"{contam_diaria['fecha'].nunique()} días)"
        )
    else:
        logger.warning("  2A: Sin datos de contaminación disponibles")

    # ─── 2B: Tráfico diario ───
    # Esquema entrada: fecha, incidencias, calidad_dato
    # Salida: fecha (date) | n_incidencias
    if df_trafico is not None and not df_trafico.empty:
        logger.info("  2B: Agregando tráfico a nivel diario...")

        df_traf = df_trafico.copy()
        df_traf["fecha"] = pd.to_datetime(df_traf["fecha"], utc=True)
        df_traf["fecha_dia"] = df_traf["fecha"].dt.date

        # Contar incidencias por día
        trafico_diario = (
            df_traf
            .groupby("fecha_dia", as_index=False)
            .agg(n_incidencias=("fecha", "count"))
        )
        trafico_diario.rename(columns={"fecha_dia": "fecha"}, inplace=True)
        trafico_diario["fecha"] = pd.to_datetime(trafico_diario["fecha"])

        logger.info(
            f"      ✓ {len(trafico_diario):,} días con datos de tráfico"
        )
    else:
        logger.warning("  2B: Sin datos de tráfico disponibles")

      # ─── 2C: Meteorología diaria ───
    # Esquema entrada: fecha, precipitacion_mm, temp_c, humedad_pct
    # Salida: fecha (date) | precip_media | temp_media
    if df_meteo is not None and not df_meteo.empty:
        logger.info("  2C: Agregando meteorología a nivel diario...")

        df_met = df_meteo.copy()

        # Conversión robusta de fecha con formato ISO8601 (maneja microsegundos)
        try:
            df_met["fecha"] = pd.to_datetime(
                df_met["fecha"], format='ISO8601', utc=True)
        except ValueError:
            # Fallback: intentar con formato mixed si ISO8601 falla
            df_met["fecha"] = pd.to_datetime(
                df_met["fecha"], format='mixed', utc=True, errors='coerce')
            fechas_invalidas = df_met["fecha"].isna().sum()
            if fechas_invalidas > 0:
                logger.warning(
                    f"      ⚠ {fechas_invalidas:,} fechas inválidas convertidas a NaT")
                df_met = df_met.dropna(subset=["fecha"])

        # Extraer fecha del día (date)
        df_met["fecha_dia"] = df_met["fecha"].dt.date

        meteo_diaria = (
            df_met
            .groupby("fecha_dia", as_index=False)
            .agg(
                precip_media=("precipitacion_mm", "mean"),
                temp_media=("temp_c", "mean"),
            )
        )
        meteo_diaria.rename(columns={"fecha_dia": "fecha"}, inplace=True)
        meteo_diaria["fecha"] = pd.to_datetime(meteo_diaria["fecha"])

        logger.info(
            f"      ✓ {len(meteo_diaria):,} días con datos meteorológicos"
        )
    else:
        logger.warning("  2C: Sin datos de meteorología disponibles")

    return contam_diaria, trafico_diario, meteo_diaria


# ==============================================================================
# PARSING Y DEDUPLICACIÓN DE EVENTOS
# ==============================================================================

def _parse_event_date(date_str: str) -> Optional[pd.Timestamp]:
    """
    Intenta parsear una fecha de evento con múltiples formatos.
    Los eventos pueden traer formatos variados:
      - "2026-03-15"
      - "15/03/2026"
      - "2026-03-15T20:00:00"
      - etc.

    Returns:
        pd.Timestamp (tz-naive) o None si no se puede parsear.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()
    if not date_str:
        return None

    try:
        ts = pd.to_datetime(date_str, dayfirst=True)
        # Quitar timezone si la tiene (trabajamos a nivel date)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts
    except Exception:
        return None


def _generate_event_id(evento: Dict[str, Any]) -> str:
    """
    Genera un ID único para un evento basado en nombre + fechas + fuente.
    Esto permite deduplicar eventos que aparecen en múltiples fuentes.
    """
    nombre = evento.get("nombre", evento.get("rival", "desconocido"))
    fecha_ini = evento.get("fecha_inicio", "")
    fuente = evento.get("fuente", "")
    raw = f"{nombre}|{fecha_ini}|{fuente}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def parse_and_deduplicate_events(
    eventos_raw: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Parsea fechas de eventos, genera IDs, y elimina duplicados.

    Para cada evento se extraen:
      - nombre (o 'rival' para Valencia CF)
      - fecha_inicio, fecha_fin (como pd.Timestamp)
      - tipo, impacto_esperado, fuente
      - evento_id (hash)

    Se descartan eventos con fecha_inicio no parseable.
    Se deducplican por evento_id.

    Returns:
        Lista de dicts enriquecidos y deduplicados.
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 3: Parsing y deduplicación de eventos")
    logger.info("─" * 40)

    parsed = []
    skipped_no_date = 0
    skipped_malformed = 0

    for evento in eventos_raw:
        try:
            # Nombre: visitvalencia/ayuntamiento usan "nombre", valenciacf usa "rival"
            nombre = evento.get("nombre", evento.get(
                "rival", "Evento desconocido"))

            # Parsear fechas
            fecha_inicio = _parse_event_date(evento.get("fecha_inicio", ""))
            if fecha_inicio is None:
                skipped_no_date += 1
                logger.debug(f"    Saltado (sin fecha): {nombre}")
                continue

            fecha_fin = _parse_event_date(evento.get("fecha_fin", ""))
            if fecha_fin is None:
                # Evento de un solo día
                fecha_fin = fecha_inicio

            # Validar coherencia temporal
            if fecha_fin < fecha_inicio:
                # Swap si están invertidas
                fecha_inicio, fecha_fin = fecha_fin, fecha_inicio
                logger.debug(f"    Fechas invertidas corregidas: {nombre}")

            # Generar ID
            evento_id = _generate_event_id(evento)

            parsed.append({
                "evento_id": evento_id,
                "nombre": nombre,
                "tipo_evento": evento.get("tipo", "desconocido"),
                "impacto_esperado": evento.get("impacto_esperado", "desconocido"),
                "fuente_evento": evento.get("fuente", "desconocida"),
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
            })

        except Exception as e:
            skipped_malformed += 1
            logger.debug(f"    Saltado (error): {e}")

    logger.info(f"  Eventos parseados correctamente: {len(parsed)}")
    logger.info(f"  Saltados por fecha inválida:     {skipped_no_date}")
    logger.info(f"  Saltados por error/malformado:   {skipped_malformed}")

    # --- Deduplicar por evento_id ---
    seen_ids = set()
    deduped = []
    for ev in parsed:
        if ev["evento_id"] not in seen_ids:
            seen_ids.add(ev["evento_id"])
            deduped.append(ev)

    duplicados = len(parsed) - len(deduped)
    if duplicados > 0:
        logger.info(f"  Duplicados eliminados: {duplicados}")

    logger.info(f"  Eventos finales para análisis: {len(deduped)}")

    return deduped


# ==============================================================================
# CONSTRUCCIÓN DEL BASELINE
# ==============================================================================

def _get_all_event_dates(events: List[Dict[str, Any]]) -> set:
    """
    Construye el conjunto de TODAS las fechas cubiertas por algún evento.
    Se usa para excluir del baseline días que coinciden con otros eventos.

    Returns:
        Set de datetime.date
    """
    all_dates = set()
    for ev in events:
        start = ev["fecha_inicio"]
        end = ev["fecha_fin"]
        rango = pd.date_range(start, end, freq="D")
        for d in rango:
            all_dates.add(d.date())
    return all_dates


def _build_baseline_mask(
    fechas_serie: pd.Series,
    evento: Dict[str, Any],
    all_event_dates: set,
    meteo_diaria: Optional[pd.DataFrame],
    logger: logging.Logger,
) -> pd.Index:
    """
    Construye la máscara booleana que identifica días válidos para el baseline
    de un evento concreto.

    Criterios (todos deben cumplirse):
      1. Mismo mes que algún día del evento
      2. Mismo día de la semana que algún día del evento
      3. No solaparse con NINGÚN otro evento
      4. No ser día de lluvia significativa (>5mm)

    Args:
        fechas_serie: Serie pd.DatetimeIndex con las fechas disponibles
        evento: Dict del evento con fecha_inicio/fecha_fin
        all_event_dates: Conjunto de todas las fechas con eventos
        meteo_diaria: DataFrame con 'fecha' y 'precip_media'
        logger: Logger

    Returns:
        pd.Index con las posiciones de los días baseline
    """
    start = evento["fecha_inicio"]
    end = evento["fecha_fin"]
    event_range = pd.date_range(start, end, freq="D")

    # Meses del evento
    event_months = set(d.month for d in event_range)

    # Días de la semana del evento (0=Mon, 6=Sun)
    event_weekdays = set(d.dayofweek for d in event_range)

    # Fechas del propio evento (para excluir del baseline)
    event_dates_self = set(d.date() for d in event_range)

    # Criterio 1: mismo mes
    mask_month = fechas_serie.dt.month.isin(event_months)

    # Criterio 2: mismo día de la semana
    mask_weekday = fechas_serie.dt.dayofweek.isin(event_weekdays)

    # Criterio 3: no solaparse con ningún evento
    mask_no_event = ~fechas_serie.dt.date.isin(all_event_dates)

    # Criterio 4: no lluvia significativa
    mask_no_rain = pd.Series(True, index=fechas_serie.index)
    if meteo_diaria is not None and not meteo_diaria.empty:
        # Crear lookup: fecha → precip_media
        meteo_lookup = meteo_diaria.set_index(
            meteo_diaria["fecha"].dt.date
        )["precip_media"]

        precip_values = fechas_serie.dt.date.map(meteo_lookup)
        # Días sin dato meteorológico se consideran "no lluvia" (NaN ≤ 5 → True)
        mask_no_rain = (precip_values.isna()) | (
            precip_values <= PRECIPITACION_UMBRAL_MM)

    # Combinar todos los criterios
    mask_final = mask_month & mask_weekday & mask_no_event & mask_no_rain

    return mask_final


# ==============================================================================
# CÁLCULO DE IMPACTO POR EVENTO
# ==============================================================================

def compute_event_impact(
    events: List[Dict[str, Any]],
    contam_diaria: Optional[pd.DataFrame],
    trafico_diario: Optional[pd.DataFrame],
    meteo_diaria: Optional[pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Para cada evento, calcula el impacto en contaminación y tráfico
    comparando con el baseline.

    Returns:
        DataFrame con una fila por (evento × variable_contaminante),
        más el impacto de tráfico incluido en cada fila.
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 4: Cálculo de impacto por evento")
    logger.info("─" * 40)

    if not events:
        logger.warning("  Sin eventos para procesar")
        return pd.DataFrame()

    # Pre-calcular conjunto de todas las fechas con eventos
    all_event_dates = _get_all_event_dates(events)
    logger.info(
        f"  Fechas cubiertas por eventos: {len(all_event_dates)} días únicos")

    # Variables de contaminación disponibles
    variables_contam = []
    if contam_diaria is not None and not contam_diaria.empty:
        variables_contam = sorted(contam_diaria["variable"].unique())
        logger.info(f"  Variables de contaminación: {variables_contam}")

    results = []
    eventos_procesados = 0
    eventos_saltados = 0

    for i, evento in enumerate(events):
        nombre = evento["nombre"][:50]
        start = evento["fecha_inicio"]
        end = evento["fecha_fin"]
        event_range = pd.date_range(start, end, freq="D")
        event_dates = set(d.date() for d in event_range)
        n_dias_evento = len(event_dates)

        logger.debug(
            f"  [{i+1}/{len(events)}] {nombre} "
            f"({start.date()} → {end.date()}, {n_dias_evento}d)"
        )

        # === METEOROLOGÍA del evento y baseline ===
        media_temp_evento = np.nan
        media_precip_evento = np.nan
        media_temp_baseline = np.nan
        media_precip_baseline = np.nan

        if meteo_diaria is not None and not meteo_diaria.empty:
            # Meteo durante el evento
            mask_ev_meteo = meteo_diaria["fecha"].dt.date.isin(event_dates)
            meteo_ev = meteo_diaria[mask_ev_meteo]

            if not meteo_ev.empty:
                media_temp_evento = meteo_ev["temp_media"].mean()
                media_precip_evento = meteo_ev["precip_media"].mean()

            # Meteo baseline
            mask_bl_meteo = _build_baseline_mask(
                meteo_diaria["fecha"],
                evento,
                all_event_dates,
                meteo_diaria,
                logger,
            )
            meteo_bl = meteo_diaria[mask_bl_meteo]

            if not meteo_bl.empty:
                media_temp_baseline = meteo_bl["temp_media"].mean()
                media_precip_baseline = meteo_bl["precip_media"].mean()

        # === TRÁFICO ===
        impacto_trafico_pct = np.nan

        if trafico_diario is not None and not trafico_diario.empty:
            # Tráfico durante el evento
            mask_ev_traf = trafico_diario["fecha"].dt.date.isin(event_dates)
            traf_ev = trafico_diario[mask_ev_traf]
            media_traf_evento = traf_ev["n_incidencias"].mean(
            ) if not traf_ev.empty else np.nan

            # Tráfico baseline
            mask_bl_traf = _build_baseline_mask(
                trafico_diario["fecha"],
                evento,
                all_event_dates,
                meteo_diaria,
                logger,
            )
            traf_bl = trafico_diario[mask_bl_traf]
            media_traf_baseline = traf_bl["n_incidencias"].mean(
            ) if not traf_bl.empty else np.nan

            if (
                not np.isnan(media_traf_evento)
                and not np.isnan(media_traf_baseline)
                and media_traf_baseline > 0
            ):
                impacto_trafico_pct = (
                    (media_traf_evento - media_traf_baseline)
                    / media_traf_baseline * 100
                )

        # === CONTAMINACIÓN (una fila por variable) ===
        if variables_contam:
            tiene_datos = False

            for variable in variables_contam:
                # Filtrar por variable
                df_var = contam_diaria[contam_diaria["variable"] == variable]

                if df_var.empty:
                    continue

                # Datos durante el evento
                mask_ev = df_var["fecha"].dt.date.isin(event_dates)
                datos_ev = df_var[mask_ev]
                media_evento_val = (
                    datos_ev["valor_medio"].mean(
                    ) if not datos_ev.empty else np.nan
                )
                n_dias_ev_real = datos_ev["fecha"].nunique(
                ) if not datos_ev.empty else 0

                # Baseline
                mask_bl = _build_baseline_mask(
                    df_var["fecha"],
                    evento,
                    all_event_dates,
                    meteo_diaria,
                    logger,
                )
                datos_bl = df_var[mask_bl]
                media_baseline_val = (
                    datos_bl["valor_medio"].mean(
                    ) if not datos_bl.empty else np.nan
                )
                n_dias_bl = datos_bl["fecha"].nunique(
                ) if not datos_bl.empty else 0

                # Calcular impacto
                impacto_pct = np.nan
                if (
                    not np.isnan(media_evento_val)
                    and not np.isnan(media_baseline_val)
                    and media_baseline_val > 0
                ):
                    impacto_pct = (
                        (media_evento_val - media_baseline_val)
                        / media_baseline_val * 100
                    )

                results.append({
                    "evento_id": evento["evento_id"],
                    "nombre_evento": evento["nombre"],
                    "tipo_evento": evento["tipo_evento"],
                    "impacto_esperado": evento["impacto_esperado"],
                    "fecha_inicio": start.strftime("%Y-%m-%d"),
                    "fecha_fin": end.strftime("%Y-%m-%d"),
                    "variable": variable,
                    "media_evento": round(media_evento_val, 2) if not np.isnan(media_evento_val) else np.nan,
                    "media_baseline": round(media_baseline_val, 2) if not np.isnan(media_baseline_val) else np.nan,
                    "impacto_pct": round(impacto_pct, 2) if not np.isnan(impacto_pct) else np.nan,
                    "n_dias_evento": n_dias_ev_real,
                    "n_dias_baseline": n_dias_bl,
                    "media_temp_evento": round(media_temp_evento, 1) if not np.isnan(media_temp_evento) else np.nan,
                    "media_temp_baseline": round(media_temp_baseline, 1) if not np.isnan(media_temp_baseline) else np.nan,
                    "media_precip_evento": round(media_precip_evento, 2) if not np.isnan(media_precip_evento) else np.nan,
                    "media_precip_baseline": round(media_precip_baseline, 2) if not np.isnan(media_precip_baseline) else np.nan,
                    "impacto_trafico_pct": round(impacto_trafico_pct, 2) if not np.isnan(impacto_trafico_pct) else np.nan,
                })
                tiene_datos = True

            if tiene_datos:
                eventos_procesados += 1
            else:
                eventos_saltados += 1
                logger.debug(
                    f"    → Saltado: sin datos de contaminación para sus fechas")

        else:
            # Sin datos de contaminación: generar fila solo con tráfico
            results.append({
                "evento_id": evento["evento_id"],
                "nombre_evento": evento["nombre"],
                "tipo_evento": evento["tipo_evento"],
                "impacto_esperado": evento["impacto_esperado"],
                "fecha_inicio": start.strftime("%Y-%m-%d"),
                "fecha_fin": end.strftime("%Y-%m-%d"),
                "variable": "sin_datos",
                "media_evento": np.nan,
                "media_baseline": np.nan,
                "impacto_pct": np.nan,
                "n_dias_evento": n_dias_evento,
                "n_dias_baseline": 0,
                "media_temp_evento": round(media_temp_evento, 1) if not np.isnan(media_temp_evento) else np.nan,
                "media_temp_baseline": round(media_temp_baseline, 1) if not np.isnan(media_temp_baseline) else np.nan,
                "media_precip_evento": round(media_precip_evento, 2) if not np.isnan(media_precip_evento) else np.nan,
                "media_precip_baseline": round(media_precip_baseline, 2) if not np.isnan(media_precip_baseline) else np.nan,
                "impacto_trafico_pct": round(impacto_trafico_pct, 2) if not np.isnan(impacto_trafico_pct) else np.nan,
            })

            if not np.isnan(impacto_trafico_pct):
                eventos_procesados += 1
            else:
                eventos_saltados += 1

    logger.info(f"  Eventos procesados con datos: {eventos_procesados}")
    logger.info(f"  Eventos saltados (sin datos): {eventos_saltados}")
    logger.info(f"  Filas de resultado generadas: {len(results)}")

    if not results:
        return pd.DataFrame()

    df_results = pd.DataFrame(results)

    # Asegurar orden de columnas
    columnas_output = [
        "evento_id",
        "nombre_evento",
        "tipo_evento",
        "impacto_esperado",
        "fecha_inicio",
        "fecha_fin",
        "variable",
        "media_evento",
        "media_baseline",
        "impacto_pct",
        "n_dias_evento",
        "n_dias_baseline",
        "media_temp_evento",
        "media_temp_baseline",
        "media_precip_evento",
        "media_precip_baseline",
        "impacto_trafico_pct",
    ]
    for col in columnas_output:
        if col not in df_results.columns:
            df_results[col] = np.nan

    df_results = df_results[columnas_output]

    return df_results


# ==============================================================================
# GUARDADO DE RESULTADOS
# ==============================================================================

def save_results(
    df: pd.DataFrame,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Guarda el DataFrame de resultados como CSV.

    Returns:
        Path al archivo guardado, o None si falla.
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("PASO 5: Guardado de resultados")
    logger.info("─" * 40)

    if df.empty:
        logger.warning("  DataFrame vacío - no se guarda nada")
        return None

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
        logger.info(f"  ✓ Guardado: {OUTPUT_FILE}")
        logger.info(f"    Filas:    {len(df):,}")
        logger.info(f"    Columnas: {list(df.columns)}")
        logger.info(
            f"    Tamaño:   {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")
        return OUTPUT_FILE
    except Exception as e:
        logger.error(f"  ✘ Error al guardar: {e}")
        return None


# ==============================================================================
# RESUMEN FINAL
# ==============================================================================

def print_summary(
    df: pd.DataFrame,
    n_events_input: int,
    n_events_parsed: int,
    logger: logging.Logger,
) -> None:
    """
    Imprime un resumen del análisis para el log y la consola.
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("RESUMEN FINAL - CORRELACIÓN EVENTOS ↔ CONTAMINACIÓN/TRÁFICO")
    logger.info("=" * 70)

    logger.info(f"  Eventos en entrada (JSON):  {n_events_input}")
    logger.info(f"  Eventos parseados válidos:  {n_events_parsed}")

    if df.empty:
        logger.info("  Sin comparaciones generadas (dataset vacío)")
        logger.info("=" * 70)
        return

    n_eventos_unicos = df["evento_id"].nunique()
    n_comparaciones = len(df)
    logger.info(f"  Eventos con datos:          {n_eventos_unicos}")
    logger.info(f"  Total comparaciones:        {n_comparaciones}")

    # Desglose por tipo de evento
    logger.info("")
    logger.info("  Por tipo de evento:")
    for tipo, grupo in df.groupby("tipo_evento"):
        n_ev = grupo["evento_id"].nunique()
        logger.info(f"    {tipo:>12}: {n_ev} eventos")

    # Desglose por impacto esperado
    logger.info("")
    logger.info("  Por impacto esperado:")
    for impacto, grupo in df.groupby("impacto_esperado"):
        n_ev = grupo["evento_id"].nunique()
        logger.info(f"    {impacto:>12}: {n_ev} eventos")

    # Variables analizadas
    logger.info("")
    logger.info("  Variables analizadas:")
    for var in sorted(df["variable"].unique()):
        subset = df[df["variable"] == var]
        n_valid = subset["impacto_pct"].notna().sum()
        if n_valid > 0:
            media_impacto = subset["impacto_pct"].mean()
            logger.info(
                f"    {var:>10}: {n_valid} comparaciones válidas | "
                f"impacto medio = {media_impacto:+.1f}%"
            )
        else:
            logger.info(f"    {var:>10}: sin comparaciones válidas")

    # Tráfico
    trafico_valid = df["impacto_trafico_pct"].notna()
    n_traf = trafico_valid.sum()
    if n_traf > 0:
        # Solo 1 fila por evento para no duplicar (agrupamos por evento)
        traf_por_evento = (
            df[trafico_valid]
            .drop_duplicates(subset=["evento_id"])
            ["impacto_trafico_pct"]
        )
        logger.info("")
        logger.info(
            f"  Tráfico: {len(traf_por_evento)} eventos con comparación válida | "
            f"impacto medio = {traf_por_evento.mean():+.1f}%"
        )

    # Top 5 eventos con mayor impacto medio (contaminación)
    valid_impacto = df[df["impacto_pct"].notna()]
    if not valid_impacto.empty:
        logger.info("")
        logger.info("  Top 5 eventos con mayor impacto medio (contaminación):")
        top = (
            valid_impacto
            .groupby(["evento_id", "nombre_evento"], as_index=False)
            .agg(impacto_medio=("impacto_pct", "mean"))
            .nlargest(5, "impacto_medio")
        )
        for _, row in top.iterrows():
            logger.info(
                f"    {row['nombre_evento'][:45]:>45}: "
                f"{row['impacto_medio']:+.1f}%"
            )

    logger.info("=" * 70)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta el pipeline completo de correlación eventos ↔ datos urbanos.

    Flujo:
        1. Cargar datos (contaminación, tráfico, meteorología, eventos)
        2. Agregar a nivel diario
        3. Parsear y deduplicar eventos
        4. Para cada evento: calcular impacto vs baseline
        5. Guardar CSV de resultados
        6. Imprimir resumen
    """
    logger = setup_logging()

    logger.info("=" * 70)
    logger.info("FASE 5.5: CORRELACIÓN EVENTOS ↔ CONTAMINACIÓN / TRÁFICO")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Proyecto raíz: {PROJECT_ROOT}")
    logger.info("")

    # ------------------------------------------------------------------
    # 1. Cargar datos
    # ------------------------------------------------------------------
    df_contam, df_trafico, df_meteo, eventos_raw = load_data(logger)

    if eventos_raw is None or len(eventos_raw) == 0:
        logger.error("Sin eventos para analizar. Abortando.")
        print("\n❌ ERROR: No se encontraron eventos. Verifica eventos_clasificados.json")
        return

    n_events_input = len(eventos_raw)

    # Verificar que al menos tenemos contaminación O tráfico
    if df_contam is None and df_trafico is None:
        logger.error(
            "Sin datos de contaminación NI tráfico. "
            "Se requiere al menos una fuente. Abortando."
        )
        print("\n❌ ERROR: Se necesita al menos contaminación o tráfico.")
        return

    # ------------------------------------------------------------------
    # 2. Agregaciones diarias
    # ------------------------------------------------------------------
    contam_diaria, trafico_diario, meteo_diaria = build_daily_aggregations(
        df_contam, df_trafico, df_meteo, logger
    )

    # ------------------------------------------------------------------
    # 3. Parsear y deduplicar eventos
    # ------------------------------------------------------------------
    events = parse_and_deduplicate_events(eventos_raw, logger)
    n_events_parsed = len(events)

    if not events:
        logger.error("Todos los eventos fueron descartados. Abortando.")
        print("\n❌ ERROR: Ningún evento pudo ser parseado correctamente.")
        return

    # ------------------------------------------------------------------
    # 4. Calcular impacto
    # ------------------------------------------------------------------
    df_results = compute_event_impact(
        events, contam_diaria, trafico_diario, meteo_diaria, logger
    )

    # ------------------------------------------------------------------
    # 5. Guardar
    # ------------------------------------------------------------------
    output_path = save_results(df_results, logger)

    # ------------------------------------------------------------------
    # 6. Resumen
    # ------------------------------------------------------------------
    print_summary(df_results, n_events_input, n_events_parsed, logger)

    # Mensaje final para consola
    if output_path:
        print(f"\n✅ CORRELACIÓN COMPLETA: {len(df_results):,} comparaciones")
        print(f"   Eventos analizados: {df_results['evento_id'].nunique()}")
        print(f"   → CSV: {output_path}")
    else:
        print("\n⚠️  Correlación completada pero sin resultados. Revisa los logs.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
