# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.4: Captura de Datos en Tiempo Real - Tráfico (DGT · DATEX II v3.6)
==============================================================================

Descripción:
    Este script captura datos de incidencias de tráfico en TIEMPO REAL desde
    el NAP (National Access Point) de la DGT en formato DATEX II versión 3.6.
    
    El XML contiene incidencias activas en TODA España (obras, cortes,
    accidentes, congestión, etc.) con coordenadas GPS, carretera, municipio,
    provincia y comunidad autónoma. El filtrado por Valencia se realizará
    en la Fase 5 (ETL).
    
    NOTA SOBRE VELOCIDAD/INTENSIDAD:
    ─────────────────────────────────
    Este endpoint publica INCIDENCIAS (SituationPublication).
    NO contiene datos de velocidad media ni intensidad de tráfico.
    La DGT publica esos datos en endpoints separados (TrafficStatus,
    MeasuredDataPublication) que requieren acceso diferente.
    El script detecta e informa de esta situación sin fallar.

Fuente de datos:
    NAP DGT:   https://nap.dgt.es
    Endpoint:  /datex2/v3/dgt/SituationPublication/datex2_v36.xml
    Método:    GET
    Auth:      No requiere
    Formato:   XML (DATEX II v3.6 - estándar europeo)
    Contenido: Incidencias de tráfico activas en carreteras españolas
    Frecuencia: Se actualiza cada ~5 minutos

Namespaces DATEX II v3.6:
    d2  = http://levelC/schema/3/d2Payload
    sit = http://levelC/schema/3/situation
    com = http://levelC/schema/3/common
    loc = http://levelC/schema/3/locationReferencing
    lse = http://levelC/schema/3/locationReferencingSpanishExtension

Uso:
    python streaming_dgt.py
    
Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/trafico/dgt_YYYYMMDD_HHMMSS.json
    - Datos parseados + metadatos de captura

Ruta esperada del script:
    2.SCRIPTS/recopilacion/streaming_dgt.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from lxml import etree
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "trafico"
LOG_DIR = PROJECT_ROOT / "logs"

# Endpoint NAP DGT (DATEX II v3.6 - Incidencias)
DGT_URL = (
    "https://nap.dgt.es/datex2/v3/dgt/"
    "SituationPublication/datex2_v36.xml"
)

# Namespaces DATEX II v3.6 (mapeados desde el XML real)
NS = {
    "d2":  "http://levelC/schema/3/d2Payload",
    "sit": "http://levelC/schema/3/situation",
    "com": "http://levelC/schema/3/common",
    "loc": "http://levelC/schema/3/locationReferencing",
    "lse": "http://levelC/schema/3/locationReferencingSpanishExtension",
    "cse": "http://levelC/schema/3/commonSpanishExtension",
    "sse": "http://levelC/schema/3/situationSpanishExtension",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

# Configuración de peticiones HTTP
REQUEST_TIMEOUT = 60  # XML grande, dar más tiempo
REQUEST_HEADERS = {
    "User-Agent": (
        "DataDetective/1.0 "
        "(Proyecto académico universitario; Valencia; "
        "captura DATEX II para análisis de tráfico)"
    ),
    "Accept": "application/xml, text/xml, */*;q=0.8",
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
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

    log_file = LOG_DIR / "streaming_dgt.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Streaming_DGT")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Handler para archivo (detalle completo)
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Handler para consola (resumen)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ==============================================================================
# FUNCIONES DE DESCARGA
# ==============================================================================

def fetch_datex_xml(logger: logging.Logger) -> Optional[bytes]:
    """
    Descarga el XML DATEX II v3.6 desde el NAP de la DGT.
    
    El archivo XML puede ser grande (varios MB) ya que contiene
    todas las incidencias activas en España.
    
    Args:
        logger: Logger para registrar eventos
    
    Returns:
        Contenido XML como bytes, o None si hay error
    """
    logger.info(f"Descargando XML DATEX II desde NAP DGT...")
    logger.debug(f"URL: {DGT_URL}")

    try:
        response = requests.get(
            DGT_URL,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            content_length = len(response.content)
            size_str = (
                f"{content_length / 1024:.1f} KB"
                if content_length >= 1024
                else f"{content_length} B"
            )

            logger.info(f"✔ XML descargado: {size_str} (Content-Type: {content_type})")
            return response.content

        elif response.status_code == 403:
            logger.error(
                "Acceso denegado (HTTP 403). "
                "El NAP DGT puede requerir condiciones especiales."
            )
            return None

        elif response.status_code == 404:
            logger.error(
                "Endpoint no encontrado (HTTP 404). "
                "La URL del NAP puede haber cambiado."
            )
            return None

        elif response.status_code == 429:
            logger.error(
                "Rate limit (HTTP 429). "
                "Reducir frecuencia de peticiones."
            )
            return None

        elif response.status_code >= 500:
            logger.error(
                f"Error del servidor DGT (HTTP {response.status_code}). "
                f"Reintentar más tarde."
            )
            return None

        else:
            logger.warning(
                f"Respuesta inesperada (HTTP {response.status_code})"
            )
            return None

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout después de {REQUEST_TIMEOUT}s. "
            f"El XML puede ser muy grande o hay problemas de red."
        )
        return None

    except requests.exceptions.ConnectionError:
        logger.error("Error de conexión. Verifica tu conexión a internet.")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Error inesperado: {e}")
        return None


# ==============================================================================
# FUNCIONES DE PARSING (lxml)
# ==============================================================================

def get_text(element: etree._Element, xpath: str) -> Optional[str]:
    """
    Extrae texto de un elemento XML usando XPath con namespaces.
    
    Helper para simplificar la extracción de valores del XML DATEX II,
    evitando repetir try/except en cada campo.
    
    Args:
        element: Elemento XML padre
        xpath: Expresión XPath relativa
    
    Returns:
        Texto del elemento encontrado, o None
    """
    try:
        result = element.find(xpath, NS)
        if result is not None and result.text:
            return result.text.strip()
    except Exception:
        pass
    return None


def parse_location(location_el: etree._Element, logger: logging.Logger) -> Dict[str, Any]:
    """
    Parsea el bloque de localización de una incidencia DATEX II.
    
    Extrae carretera, coordenadas GPS, municipio, provincia y
    comunidad autónoma desde la estructura anidada de locationReference.
    
    Estructura esperada:
    <sit:locationReference xsi:type="loc:SingleRoadLinearLocation">
      <loc:supplementaryPositionalDescription>
        <loc:roadInformation><loc:roadName>V-31</loc:roadName></loc:roadInformation>
      </loc:supplementaryPositionalDescription>
      <loc:tpegLinearLocation>
        <loc:from>
          <loc:pointCoordinates>
            <loc:latitude>39.45</loc:latitude>
            <loc:longitude>-0.38</loc:longitude>
          </loc:pointCoordinates>
          <loc:_tpegNonJunctionPointExtension>
            <loc:extendedTpegNonJunctionPoint>
              <lse:province>Valencia/València</lse:province>
    
    Args:
        location_el: Elemento XML locationReference
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con datos de localización extraídos
    """
    loc_data = {}

    # Carretera
    road_name = get_text(
        location_el,
        ".//loc:supplementaryPositionalDescription/loc:roadInformation/loc:roadName"
    )
    if road_name:
        loc_data["carretera"] = road_name

    # Carril / calzada
    lane_usage = get_text(
        location_el,
        ".//loc:supplementaryPositionalDescription/loc:carriageway/loc:lane/loc:laneUsage"
    )
    if lane_usage:
        loc_data["carril"] = lane_usage

    # Puntos geográficos (from / to)
    for point_name in ("from", "to"):
        point_el = location_el.find(
            f".//loc:tpegLinearLocation/loc:{point_name}",
            NS
        )
        if point_el is None:
            continue

        point_data = {}

        # Coordenadas GPS
        lat = get_text(point_el, ".//loc:pointCoordinates/loc:latitude")
        lon = get_text(point_el, ".//loc:pointCoordinates/loc:longitude")
        if lat and lon:
            try:
                point_data["latitud"] = float(lat)
                point_data["longitud"] = float(lon)
            except ValueError:
                point_data["latitud_raw"] = lat
                point_data["longitud_raw"] = lon

        # Extensión española (municipio, provincia, comunidad, PK)
        ext_el = point_el.find(
            ".//loc:_tpegNonJunctionPointExtension/loc:extendedTpegNonJunctionPoint",
            NS
        )
        if ext_el is not None:
            comunidad = get_text(ext_el, "lse:autonomousCommunity")
            if comunidad:
                point_data["comunidad_autonoma"] = comunidad

            provincia = get_text(ext_el, "lse:province")
            if provincia:
                point_data["provincia"] = provincia

            municipio = get_text(ext_el, "lse:municipality")
            if municipio:
                point_data["municipio"] = municipio

            km_point = get_text(ext_el, "lse:kilometerPoint")
            if km_point:
                try:
                    point_data["punto_kilometrico"] = float(km_point)
                except ValueError:
                    point_data["punto_kilometrico_raw"] = km_point

        if point_data:
            loc_data[f"punto_{point_name}"] = point_data

    return loc_data


def parse_situation_record(
    record_el: etree._Element,
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Parsea un situationRecord individual del XML DATEX II.
    
    Cada situationRecord representa una incidencia con su tipo,
    severidad, causa, localización y vigencia.
    
    Args:
        record_el: Elemento XML situationRecord
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con datos de la incidencia
    """
    record_data = {}

    # Atributos del elemento
    record_data["id"] = record_el.get("id")
    record_data["version"] = record_el.get("version")

    # Tipo del registro (xsi:type indica la clase DATEX II)
    xsi_type = record_el.get(f"{{{NS['xsi']}}}type")
    if xsi_type:
        record_data["tipo_datex"] = xsi_type

    # Timestamps
    creation_time = get_text(record_el, "sit:situationRecordCreationTime")
    if creation_time:
        record_data["fecha_creacion"] = creation_time

    version_time = get_text(record_el, "sit:situationRecordVersionTime")
    if version_time:
        record_data["fecha_version"] = version_time

    # Probabilidad y severidad
    probability = get_text(record_el, "sit:probabilityOfOccurrence")
    if probability:
        record_data["probabilidad"] = probability

    severity = get_text(record_el, "sit:severity")
    if severity:
        record_data["severidad"] = severity

    # Fuente
    source = get_text(record_el, "sit:source/com:sourceIdentification")
    if source:
        record_data["fuente"] = source

    # Vigencia
    validity_status = get_text(
        record_el, "sit:validity/com:validityStatus"
    )
    if validity_status:
        record_data["estado_vigencia"] = validity_status

    start_time = get_text(
        record_el,
        "sit:validity/com:validityTimeSpecification/com:overallStartTime"
    )
    if start_time:
        record_data["fecha_inicio"] = start_time

    end_time = get_text(
        record_el,
        "sit:validity/com:validityTimeSpecification/com:overallEndTime"
    )
    if end_time:
        record_data["fecha_fin"] = end_time

    # Causa
    cause_type = get_text(record_el, "sit:cause/sit:causeType")
    if cause_type:
        record_data["causa_tipo"] = cause_type

    # Causa detallada (roadMaintenanceType, accidentType, etc.)
    detailed_cause_el = record_el.find("sit:cause/sit:detailedCauseType", NS)
    if detailed_cause_el is not None:
        # Iterar sobre los hijos para capturar cualquier subtipo
        for child in detailed_cause_el:
            tag_local = etree.QName(child.tag).localname
            if child.text:
                record_data[f"causa_detalle_{tag_local}"] = child.text.strip()

    # Tipo de gestión (roadClosed, laneClosures, etc.)
    # El tag varía según el xsi:type del record
    management_tags = [
        "sit:roadOrCarriagewayOrLaneManagementType",
        "sit:networkManagementType",
        "sit:reroutingManagementType",
        "sit:speedManagementType",
    ]
    for tag in management_tags:
        val = get_text(record_el, tag)
        if val:
            record_data["tipo_gestion"] = val
            break

    # Vehículos afectados
    vehicle_type = get_text(
        record_el, "sit:forVehiclesWithCharacteristicsOf/com:vehicleType"
    )
    if vehicle_type:
        record_data["vehiculos_afectados"] = vehicle_type

    # Opción de cumplimiento
    compliance = get_text(record_el, "sit:complianceOption")
    if compliance:
        record_data["cumplimiento"] = compliance

    # Localización
    loc_el = record_el.find("sit:locationReference", NS)
    if loc_el is not None:
        loc_data = parse_location(loc_el, logger)
        if loc_data:
            record_data["localizacion"] = loc_data

    return record_data


def parse_datex_xml(
    xml_bytes: bytes,
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Parsea el XML DATEX II v3.6 completo y extrae todas las incidencias.
    
    Estructura del XML:
    <d2:payload>
      <com:publicationTime>...</com:publicationTime>
      <sit:situation id="...">
        <sit:overallSeverity>...</sit:overallSeverity>
        <sit:situationRecord>...</sit:situationRecord>
      </sit:situation>
      ...
    </d2:payload>
    
    Args:
        xml_bytes: XML como bytes
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con:
        - publicacion: metadatos del feed
        - incidencias: lista de incidencias parseadas
        - estadisticas: conteos por tipo/severidad
    """
    result = {
        "publicacion": {},
        "incidencias": [],
        "estadisticas": {},
    }

    # Parsear XML
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as e:
        logger.error(f"Error parseando XML: {e}")
        return result

    logger.debug(f"XML parseado. Tag raíz: {root.tag}")

    # Metadatos de la publicación
    pub_time = get_text(root, "com:publicationTime")
    if pub_time:
        result["publicacion"]["timestamp_publicacion"] = pub_time
        logger.info(f"  Publicación DGT: {pub_time}")

    feed_desc = get_text(root, "com:feedDescription/com:values/com:value")
    if feed_desc:
        result["publicacion"]["descripcion"] = feed_desc

    country = get_text(root, "com:publicationCreator/com:country")
    nat_id = get_text(root, "com:publicationCreator/com:nationalIdentifier")
    if country or nat_id:
        result["publicacion"]["creador"] = {
            "pais": country,
            "identificador": nat_id,
        }

    # Parsear todas las situaciones (incidencias)
    situations = root.findall("sit:situation", NS)
    logger.info(f"  Situaciones encontradas: {len(situations)}")

    # Contadores para estadísticas
    por_severidad = {}
    por_tipo_causa = {}
    por_tipo_gestion = {}

    for sit_el in situations:
        sit_id = sit_el.get("id")
        overall_severity = get_text(sit_el, "sit:overallSeverity")
        info_status = get_text(
            sit_el, "sit:headerInformation/com:informationStatus"
        )

        # Cada situación puede tener múltiples situationRecord
        records = sit_el.findall("sit:situationRecord", NS)

        for record_el in records:
            record_data = parse_situation_record(record_el, logger)

            # Añadir datos a nivel de situación
            record_data["situacion_id"] = sit_id
            if overall_severity:
                record_data["severidad_global"] = overall_severity
            if info_status:
                record_data["estado_informacion"] = info_status

            result["incidencias"].append(record_data)

            # Acumular estadísticas
            sev = record_data.get("severidad", "desconocida")
            por_severidad[sev] = por_severidad.get(sev, 0) + 1

            causa = record_data.get("causa_tipo", "desconocida")
            por_tipo_causa[causa] = por_tipo_causa.get(causa, 0) + 1

            gestion = record_data.get("tipo_gestion", "no_especificado")
            por_tipo_gestion[gestion] = por_tipo_gestion.get(gestion, 0) + 1

    # Estadísticas
    result["estadisticas"] = {
        "total_incidencias": len(result["incidencias"]),
        "por_severidad": por_severidad,
        "por_tipo_causa": por_tipo_causa,
        "por_tipo_gestion": por_tipo_gestion,
    }

    logger.info(f"  Incidencias parseadas: {len(result['incidencias'])}")
    logger.debug(f"  Por severidad: {por_severidad}")
    logger.debug(f"  Por causa: {por_tipo_causa}")

    # Advertencia sobre velocidad/intensidad
    logger.warning(
        "Este endpoint (SituationPublication) NO contiene datos de "
        "velocidad media ni intensidad de tráfico. Solo incidencias. "
        "Los datos de flujo requieren endpoints TrafficStatus o "
        "MeasuredDataPublication del NAP."
    )

    return result


# ==============================================================================
# FUNCIÓN DE CAPTURA PRINCIPAL
# ==============================================================================

def capture_dgt_data(
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Orquesta la captura completa de datos de tráfico de la DGT.
    
    Flujo:
    1. Descarga XML DATEX II desde NAP
    2. Parsea con lxml
    3. Extrae incidencias, localización, severidad
    4. Construye JSON con metadatos
    
    Args:
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con datos capturados y metadatos
    """
    capture_timestamp = datetime.now()

    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.4 - Streaming DGT (Tráfico DATEX II)",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "DGT - NAP (National Access Point)",
            "url": DGT_URL,
            "formato_origen": "XML DATEX II v3.6",
            "tipo_publicacion": "SituationPublication (Incidencias)",
            "cobertura": "España completa (sin filtrar)",
            "nota_velocidad_intensidad": (
                "Este endpoint solo contiene incidencias. "
                "No incluye velocidad media ni intensidad de tráfico. "
                "Esos datos requieren endpoints diferentes del NAP."
            ),
            "estado_captura": "desconocido",
            "total_incidencias": 0,
        },
        "publicacion": None,
        "incidencias": None,
        "estadisticas": None,
    }

    # Descargar XML
    xml_bytes = fetch_datex_xml(logger)

    if xml_bytes is None:
        captured_data["_metadata"]["estado_captura"] = "descarga_fallida"
        return captured_data

    captured_data["_metadata"]["xml_bytes_descargados"] = len(xml_bytes)

    # Parsear XML
    logger.info("Parseando XML DATEX II v3.6...")
    parsed = parse_datex_xml(xml_bytes, logger)

    if not parsed["incidencias"]:
        captured_data["_metadata"]["estado_captura"] = "sin_incidencias"
        captured_data["publicacion"] = parsed.get("publicacion")
        return captured_data

    # Poblar datos capturados
    captured_data["publicacion"] = parsed["publicacion"]
    captured_data["incidencias"] = parsed["incidencias"]
    captured_data["estadisticas"] = parsed["estadisticas"]
    captured_data["_metadata"]["estado_captura"] = "exitosa"
    captured_data["_metadata"]["total_incidencias"] = len(parsed["incidencias"])
    captured_data["_metadata"]["timestamp_publicacion_dgt"] = (
        parsed["publicacion"].get("timestamp_publicacion")
    )

    return captured_data


# ==============================================================================
# FUNCIONES DE GUARDADO
# ==============================================================================

def save_capture(
    data: Dict[str, Any],
    logger: logging.Logger
) -> Optional[Path]:
    """
    Guarda los datos capturados en un archivo JSON.
    
    Formato nombre: dgt_YYYYMMDD_HHMMSS.json
    
    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos
    
    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dgt_{timestamp_str}.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        if file_size >= 1024 * 1024:
            size_str = f"{file_size / (1024 * 1024):.1f} MB"
        elif file_size >= 1024:
            size_str = f"{file_size / 1024:.1f} KB"
        else:
            size_str = f"{file_size} B"

        logger.info(f"✔ Archivo guardado: {filename} ({size_str})")
        logger.debug(f"  Ruta completa: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Error guardando {filename}: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal que orquesta la captura de tráfico DGT.
    
    Flujo:
    1. Descarga XML DATEX II v3.6 del NAP
    2. Parsea incidencias con lxml
    3. Guarda JSON con metadatos y datos parseados
    4. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: Tráfico (DGT · DATEX II v3.6)")
    logger.info("=" * 70)

    # Capturar datos
    captured_data = capture_dgt_data(logger)

    meta = captured_data["_metadata"]
    estado = meta["estado_captura"]
    total = meta["total_incidencias"]

    # Guardar
    output_path = save_capture(captured_data, logger)

    if output_path is None:
        print("\n❌ ERROR: no se pudo guardar el archivo.")
        return

    # Resumen final
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estado: {estado}")
    logger.info(f"  Incidencias totales (España): {total}")
    logger.info(f"  Archivo: {output_path.name}")
    logger.info(f"  Ubicación: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {meta['timestamp_captura']}")

    if meta.get("timestamp_publicacion_dgt"):
        logger.info(f"  Publicación DGT: {meta['timestamp_publicacion_dgt']}")

    # Mostrar estadísticas si hay datos
    stats = captured_data.get("estadisticas")
    if stats:
        logger.info(f"  --- Desglose por severidad ---")
        for sev, count in sorted(
            stats.get("por_severidad", {}).items(),
            key=lambda x: x[1],
            reverse=True
        ):
            logger.info(f"    {sev}: {count}")

        logger.info(f"  --- Desglose por tipo de causa ---")
        for causa, count in sorted(
            stats.get("por_tipo_causa", {}).items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]:
            logger.info(f"    {causa}: {count}")

    logger.info("")

    # Mensaje claro en consola
    if estado == "exitosa":
        print(f"\n✅ CAPTURA CORRECTA: {total} incidencias (España) → {output_path.name}")
    elif estado == "descarga_fallida":
        print(f"\n❌ CAPTURA FALLIDA: no se pudo descargar el XML del NAP DGT.")
    elif estado == "sin_incidencias":
        print(f"\n⚠️  XML descargado pero sin incidencias parseables → {output_path.name}")
    else:
        print(f"\n⚠️  Estado: {estado} → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
