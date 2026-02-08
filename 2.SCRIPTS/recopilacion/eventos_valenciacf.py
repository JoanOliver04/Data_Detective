# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 4.3: Integracion de Eventos Masivos - Partidos Valencia CF (Mestalla)
==============================================================================

Descripcion:
    Captura el calendario de partidos del Valencia CF para correlacionar
    eventos deportivos masivos con picos de contaminacion y trafico en
    la ciudad. Los partidos en Mestalla (~55.000 espectadores) generan
    un impacto significativo en movilidad y calidad del aire.

    ESTRATEGIA DE OBTENCION DE DATOS:
    -----------------------------------
    Se investigo la existencia de feeds .ics publicos para el Valencia CF.

    FUENTE ELEGIDA: fixtur.es (feed .ics publico y auto-actualizable)
        URL: https://ics.fixtur.es/v2/valencia.ics
        - Cubre: LaLiga + Copa del Rey + competiciones europeas
        - Auto-actualiza: resultados y cambios de horario
        - Formato estandar iCalendar (.ics)
        - Sin autenticacion requerida
        - Servicio gratuito y estable

    METODO: icalevents (libreria Python para parsear .ics)
        - Descarga y parsea el feed .ics en una sola operacion
        - Extrae: SUMMARY (equipos), DTSTART (fecha/hora), LOCATION
        - Se determina local/visitante analizando el orden en SUMMARY

    FUENTES DESCARTADAS:
        - valenciacf.com/es/partidos -> requiere Selenium (JS rendering)
        - LaLiga.com -> API interna no documentada
        - ESPN -> scraping complejo, sin .ics

    FALLBACK: Si icalevents falla, se intenta descarga directa del .ics
    con requests y parsing manual como ultimo recurso.

Fuente de datos:
    URL:       https://ics.fixtur.es/v2/valencia.ics
    Metodo:    icalevents (parsing .ics) + requests (fallback)
    Auth:      No requiere
    Formato:   iCalendar (.ics) estandar RFC 5545
    Licencia:  Datos publicos de calendario deportivo

Scraping etico:
    - User-Agent transparente (proyecto academico)
    - Una sola peticion por ejecucion
    - Feed publico disenado para suscripcion de calendarios
    - Se respeta cualquier bloqueo HTTP (403, 429, etc.)

Uso:
    python eventos_valenciacf.py

Salida:
    - 1.DATOS_EN_CRUDO/eventos/valenciacf.json
    - JSON RAW con metadatos de captura y lista de partidos

Ruta esperada del script:
    2.SCRIPTS/recopilacion/eventos_valenciacf.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import io
import os
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sys

# ==============================================================================
# FIX ENCODING WINDOWS (CRITICO para PowerShell / cmd)
# ==============================================================================
# PROBLEMA: Windows tiene DOS capas de encoding en la consola:
#
#   Capa 1 - Python stdout: encoding que Python usa para convertir
#            str -> bytes al escribir en consola (default: cp1252)
#   Capa 2 - Console Code Page: encoding que la terminal usa para
#            interpretar los bytes recibidos (default: 850 o 1252)
#
# Si solo arreglamos la Capa 1 (reconfigure), Python envia UTF-8 pero
# PowerShell sigue leyendo cp1252 -> mojibake (ej: "Metodo" se corrompe)
#
# SOLUCION: arreglar AMBAS capas:
#   1) SetConsoleOutputCP(65001) -> consola interpreta bytes como UTF-8
#   2) reconfigure(encoding=utf-8) -> Python emite bytes UTF-8
# ==============================================================================

if sys.platform == "win32":
    try:
        import ctypes
        # Capa 2: decirle a la consola de Windows que use UTF-8
        # Equivalente a ejecutar "chcp 65001" pero via Win32 API
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleOutputCP(65001)  # Output: lo que Python escribe
        kernel32.SetConsoleCP(65001)        # Input: lo que el usuario escribe
    except Exception:
        pass  # Si falla ctypes, continuamos igualmente

    # Capa 1: Python emite UTF-8 en vez de cp1252
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    else:
        # Fallback para Python < 3.7
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer, encoding="utf-8", errors="replace"
        )

# zoneinfo: modulo estandar desde Python 3.9 para zonas horarias
# En Windows puede requerir: pip install tzdata
try:
    from zoneinfo import ZoneInfo
    ZONEINFO_DISPONIBLE = True
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
        ZONEINFO_DISPONIBLE = True
    except ImportError:
        ZONEINFO_DISPONIBLE = False

# icalevents: libreria para parsear feeds .ics
# Instalar con: pip install icalevents
try:
    from icalevents.icalevents import events as ical_events
    ICALEVENTS_DISPONIBLE = True
except ImportError:
    ICALEVENTS_DISPONIBLE = False

# ==============================================================================
# CONFIGURACION
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "eventos"
LOG_DIR = PROJECT_ROOT / "logs"

# Feed .ics publico de fixtur.es para Valencia CF
# Incluye: LaLiga, Copa del Rey, competiciones europeas
# Se auto-actualiza con resultados y cambios de horario
VALENCIACF_ICS_URL = "https://ics.fixtur.es/v2/valencia.ics"

# Nombre del equipo tal como aparece en el feed .ics
# Usado para determinar local/visitante
EQUIPO_NOMBRE = "Valencia"
EQUIPO_NOMBRE_COMPLETO = "Valencia CF"
ESTADIO_LOCAL = "Mestalla"
CIUDAD = "Valencia"

# Rango de fechas para filtrar partidos
# Por defecto: 6 meses hacia atras + 6 meses hacia adelante
# Asi capturamos resultados recientes Y proximos partidos
MESES_ATRAS = 6
MESES_ADELANTE = 6

# Zona horaria de Espana (Europe/Madrid)
# CET (UTC+1) en invierno, CEST (UTC+2) en verano
# El feed .ics de fixtur.es sirve horas en UTC (sufijo Z)
# Convertimos SIEMPRE a hora local para correlacionar con trafico/contaminacion
SPAIN_TZ = ZoneInfo("Europe/Madrid") if ZONEINFO_DISPONIBLE else None
UTC_TZ = timezone.utc

# Configuracion de peticiones HTTP (para fallback con requests)
REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": (
        "DataDetective/1.0 "
        "(Proyecto academico universitario; Valencia; "
        "scraping etico con delays y limite de peticiones)"
    ),
    "Accept": "text/calendar, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


# ==============================================================================
# CONFIGURACION DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging para el script.

    Escribe en:
    - Consola: nivel INFO (mensajes de estado)
    - Archivo: nivel DEBUG (detalle completo para debug)

    Returns:
        logging.Logger: Instancia del logger configurado
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "eventos_valenciacf.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Eventos_ValenciaCF")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Handler para archivo (detalle completo)
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Handler para consola (resumen)
    # Usa sys.stdout que ya fue reconfigurado a UTF-8 arriba
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ==============================================================================
# FUNCIONES DE CONVERSION HORARIA
# ==============================================================================

def convert_utc_to_madrid(
    dt_utc: datetime,
    logger: logging.Logger,
) -> datetime:
    """
    Convierte un datetime UTC a hora local de Espana (Europe/Madrid).

    Espana usa CET (UTC+1) en invierno y CEST (UTC+2) en verano.
    El cambio horario se aplica automaticamente con zoneinfo.

    Ejemplos:
        20260208T200000Z (UTC) -> 08/02/2026 21:00 (CET, invierno)
        20260615T180000Z (UTC) -> 15/06/2026 20:00 (CEST, verano)

    Args:
        dt_utc: Datetime en UTC (puede ser naive o aware)
        logger: Logger para registrar eventos

    Returns:
        Datetime convertido a Europe/Madrid
    """
    if SPAIN_TZ is None:
        logger.debug("    zoneinfo no disponible, hora se mantiene en UTC")
        return dt_utc

    # Si el datetime es naive (sin tzinfo), asumimos UTC
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=UTC_TZ)

    # Convertir a Europe/Madrid
    dt_madrid = dt_utc.astimezone(SPAIN_TZ)

    offset_seconds = dt_madrid.utcoffset().total_seconds()
    tz_label = "CEST" if offset_seconds == 7200 else "CET"

    logger.debug(
        f"    Hora: {dt_utc.strftime('%H:%M')} UTC -> "
        f"{dt_madrid.strftime('%H:%M')} Madrid ({tz_label})"
    )

    return dt_madrid


# ==============================================================================
# FUNCIONES DE ANALISIS DE PARTIDOS
# ==============================================================================

def parse_match_summary(
    summary: str,
    logger: logging.Logger,
) -> Tuple[str, str, str]:
    """
    Analiza el SUMMARY de un evento .ics para extraer equipos y condicion.

    El formato tipico de fixtur.es es:
        "Valencia - Real Madrid (2-1)"     -> local
        "FC Barcelona - Valencia (3-0)"    -> visitante
        "Valencia - Athletic de Bilbao"    -> local (sin resultado aun)

    Args:
        summary: Campo SUMMARY del evento iCal
        logger: Logger para registrar eventos

    Returns:
        Tuple (rival, local_visitante, resultado_raw)
    """
    rival = ""
    local_visitante = ""
    resultado_raw = ""

    if not summary:
        return rival, local_visitante, resultado_raw

    clean_summary = summary.strip()

    # Extraer resultado si existe: "(2-1)"
    resultado_raw = ""
    if "(" in clean_summary and ")" in clean_summary:
        paren_start = clean_summary.rfind("(")
        paren_end = clean_summary.rfind(")")
        if paren_start < paren_end:
            resultado_raw = clean_summary[paren_start + 1:paren_end].strip()
            clean_summary = clean_summary[:paren_start].strip()

    # Separar equipos por " - " o guion largo (en-dash, em-dash)
    separator = None
    for sep in (" - ", " \u2013 ", " \u2014 ", " vs ", " vs. "):
        if sep in clean_summary:
            separator = sep
            break

    if separator is None:
        logger.debug(f"    No se pudo parsear SUMMARY: '{summary}'")
        return summary, "", resultado_raw

    parts = clean_summary.split(separator, 1)
    if len(parts) != 2:
        return summary, "", resultado_raw

    equipo_1 = parts[0].strip()
    equipo_2 = parts[1].strip()

    # Determinar local/visitante
    # En fixtur.es el formato es: "Local - Visitante"
    equipo_nombre_lower = EQUIPO_NOMBRE.lower()

    if equipo_1.lower().startswith(equipo_nombre_lower):
        local_visitante = "home"
        rival = equipo_2
    elif equipo_2.lower().startswith(equipo_nombre_lower):
        local_visitante = "away"
        rival = equipo_1
    else:
        logger.debug(f"    '{EQUIPO_NOMBRE}' no encontrado en: '{summary}'")
        rival = f"{equipo_1} / {equipo_2}"
        local_visitante = "unknown"

    logger.debug(
        f"    Partido: vs {rival} ({local_visitante}) "
        f"{'[' + resultado_raw + ']' if resultado_raw else '[pendiente]'}"
    )
    return rival, local_visitante, resultado_raw


def detect_competition(
    summary: str,
    description: str,
    logger: logging.Logger,
) -> str:
    """
    Intenta detectar la competicion del partido.

    Analiza el SUMMARY y DESCRIPTION del evento .ics buscando
    indicadores de competicion (Copa del Rey, Europa League, etc.).
    Si no se detecta, asume LaLiga como competicion por defecto.

    Args:
        summary: Campo SUMMARY del evento
        description: Campo DESCRIPTION del evento
        logger: Logger para registrar eventos

    Returns:
        Nombre de la competicion detectada (RAW)
    """
    text = f"{summary} {description}".lower()

    competition_indicators = [
        (["copa del rey", "copa rey"], "Copa del Rey"),
        (["champions league", "ucl", "champions"], "Champions League"),
        (["europa league", "uel"], "Europa League"),
        (["conference league", "uecl"], "Conference League"),
        (["supercopa", "super copa"], "Supercopa"),
        (["amistoso", "friendly", "pretemporada"], "Amistoso"),
    ]

    for indicators, competition_name in competition_indicators:
        for indicator in indicators:
            if indicator in text:
                logger.debug(f"    Competicion detectada: {competition_name}")
                return competition_name

    return "LaLiga"


# ==============================================================================
# FUNCION DE CAPTURA PRINCIPAL (icalevents)
# ==============================================================================

def capture_via_icalevents(
    logger: logging.Logger,
) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Captura partidos del Valencia CF usando la libreria icalevents.

    Descarga y parsea el feed .ics de fixtur.es en una sola operacion.
    Extrae todos los campos relevantes de cada evento iCal.

    Args:
        logger: Logger para registrar eventos

    Returns:
        Tuple (lista_partidos, metodo_usado)
    """
    if not ICALEVENTS_DISPONIBLE:
        logger.warning(
            "icalevents no esta instalado. "
            "Instalar con: pip install icalevents"
        )
        return None, "icalevents no disponible"

    logger.info("Metodo principal: icalevents (parsing .ics)")
    logger.info(f"  Feed: {VALENCIACF_ICS_URL}")

    # Calcular rango de fechas
    ahora = datetime.now()
    fecha_inicio = ahora - timedelta(days=MESES_ATRAS * 30)
    fecha_fin = ahora + timedelta(days=MESES_ADELANTE * 30)

    logger.info(
        f"  Rango: {fecha_inicio.strftime('%d/%m/%Y')} -> "
        f"{fecha_fin.strftime('%d/%m/%Y')}"
    )

    try:
        # icalevents descarga y parsea el .ics automaticamente
        eventos_ical = ical_events(
            url=VALENCIACF_ICS_URL,
            start=fecha_inicio,
            end=fecha_fin,
        )

        logger.info(f"  [OK] Feed descargado: {len(eventos_ical)} eventos en rango")

        if not eventos_ical:
            logger.warning("  Feed descargado pero sin eventos en el rango")
            return [], "icalevents (feed vacio en rango)"

        # Convertir eventos ical a diccionarios RAW
        partidos = []
        seen_keys = set()

        for idx, ev in enumerate(eventos_ical):
            summary = str(ev.summary) if ev.summary else ""
            description = str(ev.description) if ev.description else ""
            location = str(ev.location) if ev.location else ""

            logger.debug(f"  [#{idx}] SUMMARY: {summary}")

            # Extraer fecha y hora (convertir UTC -> Europe/Madrid)
            dt_start = ev.start
            if dt_start:
                dt_local = convert_utc_to_madrid(dt_start, logger)
                fecha = dt_local.strftime("%d/%m/%Y")
                hora = dt_local.strftime("%H:%M")
            else:
                fecha = ""
                hora = ""

            # Analizar SUMMARY para rival y condicion
            rival, local_visitante, resultado_raw = parse_match_summary(
                summary, logger
            )

            # Detectar competicion
            competicion = detect_competition(summary, description, logger)

            # Determinar estadio
            if local_visitante == "home":
                estadio = location if location else ESTADIO_LOCAL
            else:
                estadio = location if location else ""

            # Deduplicacion por fecha + rival
            dedup_key = f"{fecha}|{rival.lower()}"
            if dedup_key in seen_keys and dedup_key != "|":
                logger.debug(f"  [#{idx}] Duplicado, saltando...")
                continue
            seen_keys.add(dedup_key)

            partido = {
                "fecha": fecha,
                "hora": hora,
                "rival": rival,
                "competicion": competicion,
                "estadio": estadio,
                "local_visitante": local_visitante,
                "ciudad": CIUDAD,
                "equipo": EQUIPO_NOMBRE_COMPLETO,
                "resultado_raw": resultado_raw,
                "summary_raw": summary,
                "url_evento": "",
            }

            partidos.append(partido)

        logger.info(f"  [OK] {len(partidos)} partidos procesados")
        return partidos, "icalevents (feed .ics fixtur.es)"

    except requests.exceptions.Timeout:
        logger.error("  [ERROR] Timeout al descargar feed .ics")
        return None, "icalevents timeout"

    except requests.exceptions.ConnectionError as e:
        logger.error(f"  [ERROR] Error de conexion: {e}")
        return None, "icalevents error conexion"

    except Exception as e:
        logger.error(f"  [ERROR] Error inesperado con icalevents: {type(e).__name__}: {e}")
        return None, f"icalevents error: {type(e).__name__}"


# ==============================================================================
# FUNCION DE CAPTURA FALLBACK (requests directo)
# ==============================================================================

def capture_via_requests_raw(
    logger: logging.Logger,
) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Fallback: descarga el .ics con requests y parsea manualmente.

    Si icalevents no esta disponible o falla, se intenta descargar
    el archivo .ics directamente y hacer un parsing basico linea
    a linea del formato iCalendar.

    Args:
        logger: Logger para registrar eventos

    Returns:
        Tuple (lista_partidos, metodo_usado)
    """
    logger.info("Metodo fallback: requests + parsing manual del .ics")
    logger.info(f"  URL: {VALENCIACF_ICS_URL}")

    try:
        response = requests.get(
            VALENCIACF_ICS_URL,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )

        logger.debug(f"  HTTP {response.status_code} ({len(response.text)} bytes)")

        if response.status_code != 200:
            logger.error(f"  [ERROR] HTTP {response.status_code}")
            return None, f"requests HTTP {response.status_code}"

        logger.info(
            f"  [OK] Feed descargado ({len(response.text):,} bytes). "
            f"Parseando manualmente..."
        )

        # Parsing manual basico del formato iCalendar
        ics_text = response.text
        partidos = []
        seen_keys = set()

        # Separar por VEVENT
        vevent_blocks = ics_text.split("BEGIN:VEVENT")

        for block_idx, block in enumerate(vevent_blocks[1:], start=1):
            # Extraer campos del VEVENT
            fields = {}
            for line in block.split("\n"):
                line = line.strip()
                if ":" in line and not line.startswith(" "):
                    key, _, value = line.partition(":")
                    # Manejar propiedades con parametros (ej: DTSTART;VALUE=DATE:20260301)
                    key_base = key.split(";")[0]
                    fields[key_base] = value.strip()

            summary = fields.get("SUMMARY", "")
            dtstart_raw = fields.get("DTSTART", "")
            location = fields.get("LOCATION", "")
            description = fields.get("DESCRIPTION", "")

            if not summary:
                continue

            logger.debug(f"  [#{block_idx}] SUMMARY: {summary}")

            # Parsear fecha/hora del DTSTART y convertir UTC -> Europe/Madrid
            fecha = ""
            hora = ""
            if dtstart_raw:
                try:
                    # Formato: 20260208T210000Z o 20260208T210000
                    is_utc = dtstart_raw.strip().endswith("Z")
                    clean_dt = dtstart_raw.replace("Z", "").strip()
                    if "T" in clean_dt:
                        dt = datetime.strptime(clean_dt[:15], "%Y%m%dT%H%M%S")
                        # Si tenia Z, es UTC -> convertir a Madrid
                        if is_utc:
                            dt = convert_utc_to_madrid(dt, logger)
                        fecha = dt.strftime("%d/%m/%Y")
                        hora = dt.strftime("%H:%M")
                    else:
                        dt = datetime.strptime(clean_dt[:8], "%Y%m%d")
                        fecha = dt.strftime("%d/%m/%Y")
                except ValueError:
                    fecha = dtstart_raw
                    logger.debug(f"    No se pudo parsear DTSTART: {dtstart_raw}")

            # Filtrar por rango de fechas
            if fecha:
                try:
                    fecha_dt = datetime.strptime(fecha, "%d/%m/%Y")
                    ahora = datetime.now()
                    limite_pasado = ahora - timedelta(days=MESES_ATRAS * 30)
                    limite_futuro = ahora + timedelta(days=MESES_ADELANTE * 30)
                    if fecha_dt < limite_pasado or fecha_dt > limite_futuro:
                        continue
                except ValueError:
                    pass  # Si no se puede verificar, incluir igualmente

            # Analizar SUMMARY
            rival, local_visitante, resultado_raw = parse_match_summary(
                summary, logger
            )

            # Detectar competicion
            competicion = detect_competition(summary, description, logger)

            # Estadio
            if local_visitante == "home":
                estadio = location if location else ESTADIO_LOCAL
            else:
                estadio = location if location else ""

            # Deduplicacion
            dedup_key = f"{fecha}|{rival.lower()}"
            if dedup_key in seen_keys and dedup_key != "|":
                continue
            seen_keys.add(dedup_key)

            partido = {
                "fecha": fecha,
                "hora": hora,
                "rival": rival,
                "competicion": competicion,
                "estadio": estadio,
                "local_visitante": local_visitante,
                "ciudad": CIUDAD,
                "equipo": EQUIPO_NOMBRE_COMPLETO,
                "resultado_raw": resultado_raw,
                "summary_raw": summary,
                "url_evento": "",
            }

            partidos.append(partido)

        logger.info(f"  [OK] {len(partidos)} partidos parseados (fallback manual)")
        return partidos, "requests + parsing manual .ics"

    except requests.exceptions.Timeout:
        logger.error(f"  [ERROR] Timeout tras {REQUEST_TIMEOUT}s")
        return None, "requests timeout"

    except requests.exceptions.ConnectionError as e:
        logger.error(f"  [ERROR] Error de conexion: {e}")
        return None, "requests error conexion"

    except Exception as e:
        logger.error(f"  [ERROR] Error inesperado: {type(e).__name__}: {e}")
        return None, f"requests error: {type(e).__name__}"


# ==============================================================================
# FUNCION DE CAPTURA ORQUESTADORA
# ==============================================================================

def capture_valenciacf_matches(
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Orquesta la captura completa de partidos del Valencia CF.

    Flujo:
    1. Intenta captura con icalevents (metodo principal)
    2. Si falla, intenta con requests + parsing manual (fallback)
    3. Construye JSON con metadatos y datos RAW

    Args:
        logger: Logger para registrar eventos

    Returns:
        Diccionario con datos capturados y metadatos
    """
    capture_timestamp = datetime.now()
    ahora = capture_timestamp
    fecha_inicio = ahora - timedelta(days=MESES_ATRAS * 30)
    fecha_fin = ahora + timedelta(days=MESES_ADELANTE * 30)

    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "4.3 - Partidos Valencia CF",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "fixtur.es - Calendario digital Valencia CF (.ics)",
            "url_feed": VALENCIACF_ICS_URL,
            "metodo": None,
            "rango_busqueda": {
                "fecha_inicio": fecha_inicio.strftime("%d-%m-%Y"),
                "fecha_fin": fecha_fin.strftime("%d-%m-%Y"),
                "meses_atras": MESES_ATRAS,
                "meses_adelante": MESES_ADELANTE,
            },
            "estado_fuente": "desconocido",
            "registros_extraidos": 0,
            "partidos_en_mestalla": 0,
            "notas_eticas": [
                "User-Agent transparente (proyecto academico)",
                "Una sola peticion por ejecucion",
                "Feed publico disenado para suscripcion de calendarios",
                "Se respeta cualquier bloqueo HTTP",
                "Datos publicos de calendario deportivo",
                "Horas convertidas de UTC a Europe/Madrid (CET/CEST)",
            ],
            "zona_horaria": "Europe/Madrid",
        },
        "partidos": None,
    }

    # -- Paso 1: Intentar con icalevents (metodo principal) --
    partidos, metodo = capture_via_icalevents(logger)

    # -- Paso 2: Fallback con requests si icalevents falla --
    if partidos is None:
        logger.info("")
        logger.info("Intentando metodo fallback...")
        partidos, metodo = capture_via_requests_raw(logger)

    # -- Paso 3: Construir resultado --
    captured_data["_metadata"]["metodo"] = metodo

    if partidos is not None and len(partidos) > 0:
        captured_data["partidos"] = partidos
        captured_data["_metadata"]["registros_extraidos"] = len(partidos)
        captured_data["_metadata"]["estado_fuente"] = "operativa"

        # Contar partidos en Mestalla (home)
        en_mestalla = sum(
            1 for p in partidos if p.get("local_visitante") == "home"
        )
        captured_data["_metadata"]["partidos_en_mestalla"] = en_mestalla

        logger.info(f"[OK] {len(partidos)} partidos capturados ({en_mestalla} en Mestalla)")

    elif partidos is not None and len(partidos) == 0:
        captured_data["partidos"] = []
        captured_data["_metadata"]["estado_fuente"] = "sin_partidos"
        captured_data["_metadata"]["nota_estado"] = (
            "El feed .ics se descargo correctamente pero no se encontraron "
            "partidos en el rango de fechas configurado."
        )
        logger.warning("Feed descargado pero sin partidos en el rango")

    else:
        captured_data["partidos"] = None
        captured_data["_metadata"]["estado_fuente"] = "no_disponible"
        captured_data["_metadata"]["nota_estado"] = (
            "No se pudo obtener el calendario del Valencia CF. "
            "Posibles causas: feed .ics caido, bloqueo de red, "
            "o icalevents no instalado (pip install icalevents)."
        )
        logger.error("No se pudo obtener datos del Valencia CF")

    return captured_data


# ==============================================================================
# FUNCIONES DE GUARDADO
# ==============================================================================

def save_capture(
    data: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Guarda los datos capturados en un archivo JSON.

    Los partidos del Valencia CF se guardan en un archivo unico
    (valenciacf.json) que se sobreescribe en cada ejecucion,
    representando el estado actual del calendario.

    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos

    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    filename = "valenciacf.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )

        logger.info(f"[OK] Archivo guardado: {filename} ({size_str})")
        logger.debug(f"  Ruta completa: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Error guardando {filename}: {e}")
        return None


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def main():
    """
    Funcion principal que orquesta la captura de partidos del Valencia CF.

    Flujo:
    1. Configura logging
    2. Verifica disponibilidad de icalevents y zoneinfo
    3. Intenta captura (.ics -> icalevents -> fallback requests)
    4. Guarda resultado (exito o documentacion del fallo)
    5. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA DE EVENTOS: Partidos Valencia CF (Feed .ics)")
    logger.info("=" * 70)

    # Informar sobre disponibilidad de icalevents
    if ICALEVENTS_DISPONIBLE:
        logger.info("icalevents: [OK] disponible")
    else:
        logger.warning(
            "icalevents: [X] NO instalado. Se usara fallback con requests. "
            "Para mejor resultado: pip install icalevents"
        )

    # Informar sobre zona horaria
    if ZONEINFO_DISPONIBLE:
        logger.info("Zona horaria: Europe/Madrid (UTC -> hora local)")
    else:
        logger.warning(
            "zoneinfo no disponible. Horas en UTC. "
            "En Windows: pip install tzdata"
        )

    # Capturar partidos
    captured_data = capture_valenciacf_matches(logger)

    meta = captured_data["_metadata"]
    estado = meta["estado_fuente"]
    num_partidos = meta["registros_extraidos"]
    en_mestalla = meta["partidos_en_mestalla"]

    # Guardar siempre (incluso si fallo, para registro)
    output_path = save_capture(captured_data, logger)

    if output_path is None:
        print("\n[ERROR] No se pudo guardar el archivo.")
        return

    # Resumen final
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estado fuente:     {estado}")
    logger.info(f"  Metodo:            {meta['metodo']}")
    logger.info(f"  Partidos totales:  {num_partidos}")
    logger.info(f"  En Mestalla:       {en_mestalla}")
    logger.info(f"  Rango busqueda:    {meta['rango_busqueda']['fecha_inicio']} -> "
                f"{meta['rango_busqueda']['fecha_fin']}")
    logger.info(f"  Archivo:           {output_path.name}")
    logger.info(f"  Ubicacion:         {OUTPUT_DIR}")
    logger.info(f"  Timestamp:         {meta['timestamp_captura']}")

    if meta.get("nota_estado"):
        logger.info(f"  Nota: {meta['nota_estado']}")

    logger.info("")

    # Mensaje claro en consola
    if estado == "operativa" and num_partidos > 0:
        print(
            f"\n>>> CAPTURA CORRECTA: {num_partidos} partidos "
            f"({en_mestalla} en Mestalla) -> {output_path.name}"
        )

        # Mostrar preview de proximos partidos
        partidos = captured_data.get("partidos", [])
        if partidos:
            # Filtrar proximos partidos (sin resultado aun)
            proximos = [
                p for p in partidos
                if not p.get("resultado_raw")
            ]
            if proximos:
                print(f"\nProximos partidos (hasta {min(5, len(proximos))}):")
                print("-" * 60)
                for p in proximos[:5]:
                    sede = "[HOME]" if p["local_visitante"] == "home" else "[AWAY]"
                    print(
                        f"  {sede} {p['fecha']} {p['hora']} "
                        f"vs {p['rival']} [{p['competicion']}]"
                    )
                if len(proximos) > 5:
                    print(f"  ... y {len(proximos) - 5} partidos mas")

            # Mostrar ultimos resultados
            resultados = [
                p for p in partidos
                if p.get("resultado_raw")
            ]
            if resultados:
                print(f"\nUltimos resultados (hasta {min(5, len(resultados))}):")
                print("-" * 60)
                for p in resultados[-5:]:
                    sede = "[HOME]" if p["local_visitante"] == "home" else "[AWAY]"
                    print(
                        f"  {sede} {p['fecha']} vs {p['rival']} "
                        f"({p['resultado_raw']}) [{p['competicion']}]"
                    )

    elif estado == "sin_partidos":
        print(
            f"\n[!] SIN PARTIDOS: el feed se descargo pero no hay partidos "
            f"en el rango.\n"
            f"    Estado documentado en -> {output_path.name}"
        )
    elif estado == "no_disponible":
        print(
            f"\n[!] FUENTE NO DISPONIBLE: no se pudo obtener el calendario.\n"
            f"    Estado documentado en -> {output_path.name}\n"
            f"    Verificar: pip install icalevents"
        )
    else:
        print(f"\n[!] CAPTURA PARCIAL: estado={estado} -> {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
