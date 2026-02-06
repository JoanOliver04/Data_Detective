# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 3.3: Captura de Datos en Tiempo Real - Precipitaciones (AVAMET)
==============================================================================

Descripción:
    Este script intenta capturar datos de precipitación en TIEMPO REAL desde
    la web de AVAMET (Associació Valenciana de Meteorologia) mediante web
    scraping ético.
    
    ESTADO ACTUAL DE LA FUENTE (febrero 2026):
    ─────────────────────────────────────────
    AVAMET bloquea peticiones automatizadas con HTTP 403 en TODAS sus
    páginas (incluida la portada). No existe API pública documentada.
    
    datos.gob.es referencia un endpoint GeoJSON y servicio WFS de AVAMET,
    pero las URLs exactas no son accesibles sin sesión autenticada.
    
    El script implementa la lógica completa de scraping y se ejecutará
    correctamente cuando/si AVAMET restablece el acceso. Mientras tanto,
    documenta el estado de la fuente sin errores fatales.
    
    ALTERNATIVAS FUNCIONALES ya implementadas en el proyecto:
    - streaming_openweather.py → precipitación actual y pronóstico 5 días
    - descargar_aemet_historico.py → precipitación histórica vía API

Fuente de datos:
    URL principal: https://www.avamet.org/mx-meteoxarxa.php
    URL precip:    https://www.avamet.org/mxo-mxo-prec.php
    Método:        GET + BeautifulSoup4 (web scraping)
    Auth:          No requiere (pero bloquea bots)
    Licencia:      CC BY-NC-ND (Reconocimiento-NoComercial-SinObraDerivada)

Scraping ético:
    - Se verifica robots.txt antes de cualquier petición
    - User-Agent transparente (proyecto académico)
    - Máximo 2 peticiones por ejecución
    - Delay mínimo de 2 segundos entre peticiones
    - Se respeta cualquier bloqueo HTTP (403, 429, etc.)

Uso:
    python scraping_avamet.py
    
Salida:
    - 1.DATOS_EN_CRUDO/dinamicos/precipitaciones/avamet_YYYYMMDD_HHMMSS.json
    - Si la fuente está caída: JSON con _metadata documentando el estado

Ruta esperada del script:
    2.SCRIPTS/recopilacion/scraping_avamet.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import time
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from bs4 import BeautifulSoup
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "dinamicos" / "precipitaciones"
LOG_DIR = PROJECT_ROOT / "logs"

# URLs de AVAMET a consultar (por orden de prioridad)
AVAMET_URLS = {
    "precipitacion": "https://www.avamet.org/mxo-mxo-prec.php",
    "meteoxarxa": "https://www.avamet.org/mx-meteoxarxa.php",
}

# URL de robots.txt para verificación ética
AVAMET_ROBOTS_URL = "https://www.avamet.org/robots.txt"
AVAMET_DOMAIN = "www.avamet.org"

# Configuración de peticiones HTTP
REQUEST_TIMEOUT = 30
DELAY_ENTRE_PETICIONES = 2  # Segundos mínimos entre peticiones (ético)
REQUEST_HEADERS = {
    "User-Agent": (
        "DataDetective/1.0 "
        "(Proyecto académico universitario; Valencia; "
        "scraping ético con delays y límite de peticiones)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
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

    log_file = LOG_DIR / "scraping_avamet.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Scraping_AVAMET")
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
# FUNCIONES DE SCRAPING ÉTICO
# ==============================================================================

def check_robots_txt(logger: logging.Logger) -> Tuple[bool, str]:
    """
    Verifica robots.txt de AVAMET antes de hacer scraping.
    
    Parte fundamental del scraping ético: respetamos las directivas
    del sitio web antes de realizar cualquier petición de datos.
    
    Args:
        logger: Logger para registrar eventos
    
    Returns:
        Tuple (permitido: bool, motivo: str)
        - permitido: True si el scraping parece permitido
        - motivo: Descripción del resultado de la verificación
    """
    logger.info("Verificando robots.txt de AVAMET...")
    
    try:
        response = requests.get(
            AVAMET_ROBOTS_URL,
            headers={"User-Agent": REQUEST_HEADERS["User-Agent"]},
            timeout=15
        )
        
        if response.status_code == 200:
            robots_content = response.text.lower()
            logger.debug(f"robots.txt obtenido ({len(robots_content)} bytes)")
            
            # Verificar si hay Disallow general para todos los bots
            lines = robots_content.split("\n")
            current_agent_applies = False
            
            for line in lines:
                line = line.strip()
                
                if line.startswith("user-agent:"):
                    agent = line.split(":", 1)[1].strip()
                    current_agent_applies = (agent == "*")
                
                elif line.startswith("disallow:") and current_agent_applies:
                    path = line.split(":", 1)[1].strip()
                    # Si bloquea la raíz completa
                    if path == "/":
                        logger.warning(
                            "robots.txt BLOQUEA todo el scraping (Disallow: /)"
                        )
                        return False, "robots.txt prohíbe scraping (Disallow: /)"
            
            logger.info("robots.txt verificado: sin bloqueo explícito")
            return True, "robots.txt no prohíbe las rutas solicitadas"
            
        elif response.status_code == 403:
            logger.warning(
                "robots.txt devuelve 403. El servidor bloquea acceso automatizado."
            )
            return False, "Servidor bloquea acceso automatizado (HTTP 403)"
            
        elif response.status_code == 404:
            # Sin robots.txt = no hay restricciones explícitas
            logger.info("robots.txt no encontrado (404). Sin restricciones explícitas.")
            return True, "robots.txt no existe (sin restricciones)"
            
        else:
            logger.warning(f"robots.txt devuelve HTTP {response.status_code}")
            return True, f"robots.txt no determinante (HTTP {response.status_code})"
    
    except requests.exceptions.Timeout:
        logger.warning("Timeout al verificar robots.txt")
        return True, "robots.txt no verificable (timeout)"
        
    except requests.exceptions.ConnectionError:
        logger.error("Error de conexión al verificar robots.txt")
        return False, "Sin conexión al servidor AVAMET"
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error verificando robots.txt: {e}")
        return True, f"robots.txt no verificable ({e})"


def fetch_page(
    url: str,
    page_name: str,
    logger: logging.Logger
) -> Optional[str]:
    """
    Descarga el HTML de una página de AVAMET.
    
    Implementa manejo detallado de errores HTTP, especialmente
    el 403 que AVAMET devuelve actualmente a bots.
    
    Args:
        url: URL completa de la página
        page_name: Nombre descriptivo para logs
        logger: Logger para registrar eventos
    
    Returns:
        Contenido HTML como string, o None si hay error
    """
    logger.debug(f"Solicitando página '{page_name}': {url}")
    
    try:
        response = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                logger.warning(
                    f"Página '{page_name}': tipo inesperado: {content_type}"
                )
            
            logger.debug(
                f"Página '{page_name}': descargada OK "
                f"({len(response.text)} bytes, encoding={response.encoding})"
            )
            return response.text
        
        elif response.status_code == 403:
            logger.warning(
                f"Página '{page_name}': ACCESO BLOQUEADO (HTTP 403). "
                f"AVAMET bloquea peticiones automatizadas. "
                f"Esto es esperado en el estado actual de la fuente."
            )
            return None
        
        elif response.status_code == 404:
            logger.warning(
                f"Página '{page_name}': no encontrada (HTTP 404). "
                f"La URL puede haber cambiado."
            )
            return None
        
        elif response.status_code == 429:
            logger.error(
                f"Página '{page_name}': rate limit (HTTP 429). "
                f"Demasiadas peticiones. Respetar límites."
            )
            return None
        
        elif response.status_code >= 500:
            logger.error(
                f"Página '{page_name}': error del servidor "
                f"(HTTP {response.status_code})"
            )
            return None
        
        else:
            logger.warning(
                f"Página '{page_name}': respuesta inesperada "
                f"(HTTP {response.status_code})"
            )
            return None
    
    except requests.exceptions.Timeout:
        logger.error(
            f"Página '{page_name}': timeout después de {REQUEST_TIMEOUT}s"
        )
        return None
    
    except requests.exceptions.ConnectionError:
        logger.error(
            f"Página '{page_name}': error de conexión. "
            f"Verifica tu conexión a internet."
        )
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Página '{page_name}': error inesperado: {e}")
        return None


# ==============================================================================
# FUNCIONES DE PARSING (BeautifulSoup4)
# ==============================================================================

def parse_precipitation_table(
    html: str,
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Extrae datos de precipitación de las tablas HTML de AVAMET.
    
    AVAMET organiza los datos en tablas HTML con estaciones
    agrupadas por comarca. Cada fila contiene:
    - Nombre de estación
    - Precipitación acumulada (mm)
    - Otros datos meteorológicos
    
    NOTA: La estructura exacta del HTML puede cambiar sin aviso.
    El parser intenta ser resiliente a cambios menores.
    
    Args:
        html: Contenido HTML de la página
        logger: Logger para registrar eventos
    
    Returns:
        Lista de diccionarios con datos extraídos
    """
    soup = BeautifulSoup(html, "html.parser")
    registros = []
    
    # Buscar todas las tablas de datos
    tables = soup.find_all("table")
    
    if not tables:
        logger.warning(
            "No se encontraron tablas HTML. "
            "La estructura de la página puede haber cambiado."
        )
        return registros
    
    logger.debug(f"Encontradas {len(tables)} tablas en la página")
    
    for table_idx, table in enumerate(tables):
        rows = table.find_all("tr")
        
        if not rows:
            continue
        
        # Intentar identificar las columnas del header
        header_row = rows[0]
        headers = []
        for th in header_row.find_all(["th", "td"]):
            header_text = th.get_text(strip=True).lower()
            headers.append(header_text)
        
        logger.debug(f"Tabla {table_idx}: headers = {headers}")
        
        # Buscar índices de columnas relevantes
        # (nombres en valenciano/castellano que AVAMET podría usar)
        precip_keywords = ["prec", "pluja", "lluvia", "precipit", "mm", "acum"]
        station_keywords = ["estació", "estacion", "nom", "nombre", "station"]
        
        idx_station = None
        idx_precip = None
        
        for i, h in enumerate(headers):
            if any(kw in h for kw in station_keywords) and idx_station is None:
                idx_station = i
            if any(kw in h for kw in precip_keywords) and idx_precip is None:
                idx_precip = i
        
        # Si no encontramos headers claros, asumir primera columna = estación
        if idx_station is None and len(headers) > 0:
            idx_station = 0
        
        # Parsear filas de datos (saltar header)
        for row_idx, row in enumerate(rows[1:], start=1):
            cells = row.find_all(["td", "th"])
            
            if not cells or len(cells) < 2:
                continue
            
            # Extraer valores como texto RAW
            cell_values = [cell.get_text(strip=True) for cell in cells]
            
            record = {
                "tabla_idx": table_idx,
                "fila_idx": row_idx,
                "celdas_raw": cell_values,
            }
            
            # Intentar extraer nombre de estación
            if idx_station is not None and idx_station < len(cell_values):
                record["estacion"] = cell_values[idx_station]
            
            # Intentar extraer precipitación
            if idx_precip is not None and idx_precip < len(cell_values):
                precip_raw = cell_values[idx_precip]
                record["precipitacion_raw"] = precip_raw
                
                # Intentar convertir a número (validación, no transformación)
                try:
                    precip_clean = precip_raw.replace(",", ".").replace("mm", "").strip()
                    if precip_clean and precip_clean not in ("-", "--", ""):
                        record["precipitacion_mm"] = float(precip_clean)
                except (ValueError, AttributeError):
                    pass  # Dejar como raw, se procesará en Fase 5
            
            registros.append(record)
    
    logger.info(f"Extraídos {len(registros)} registros de {len(tables)} tablas")
    return registros


def parse_general_meteo_table(
    html: str,
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Extrae datos meteorológicos generales de la MeteoXarxa de AVAMET.
    
    Fallback parser para la página general mx-meteoxarxa.php, que
    contiene datos de temperatura, viento, precipitación, etc.
    
    Args:
        html: Contenido HTML de la página
        logger: Logger para registrar eventos
    
    Returns:
        Lista de diccionarios con datos extraídos (raw)
    """
    soup = BeautifulSoup(html, "html.parser")
    registros = []
    
    tables = soup.find_all("table")
    
    if not tables:
        logger.warning("Sin tablas ni contenedores de datos encontrados")
        return registros
    
    logger.debug(f"Encontradas {len(tables)} tablas en MeteoXarxa")
    
    for table_idx, table in enumerate(tables):
        rows = table.find_all("tr")
        
        for row_idx, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            
            if not cells or len(cells) < 2:
                continue
            
            cell_values = [cell.get_text(strip=True) for cell in cells]
            
            record = {
                "tabla_idx": table_idx,
                "fila_idx": row_idx,
                "celdas_raw": cell_values,
                "es_header": row_idx == 0 or bool(row.find("th")),
            }
            
            registros.append(record)
    
    logger.info(f"Extraídos {len(registros)} registros de MeteoXarxa general")
    return registros


# ==============================================================================
# FUNCIÓN DE CAPTURA PRINCIPAL
# ==============================================================================

def capture_avamet_data(
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Orquesta la captura completa de datos de AVAMET.
    
    Flujo:
    1. Verifica robots.txt
    2. Intenta página de precipitación (prioridad)
    3. Si falla, intenta MeteoXarxa general (fallback)
    4. Parsea tablas HTML si hay respuesta
    5. Construye JSON con metadatos y datos raw
    
    Args:
        logger: Logger para registrar eventos
    
    Returns:
        Diccionario con datos capturados y metadatos
    """
    capture_timestamp = datetime.now()
    
    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "3.3 - Scraping AVAMET (Precipitaciones)",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "AVAMET - Associació Valenciana de Meteorologia",
            "metodo": "Web scraping ético (BeautifulSoup4)",
            "urls_intentadas": [],
            "robots_txt": None,
            "estado_fuente": "desconocido",
            "registros_extraidos": 0,
            "notas_eticas": [
                "Se verifica robots.txt antes de scraping",
                "User-Agent transparente (proyecto académico)",
                "Máximo 2 peticiones por ejecución",
                "Delay mínimo de 2s entre peticiones",
                "Se respeta cualquier bloqueo HTTP",
                "Licencia AVAMET: CC BY-NC-ND",
            ],
        },
        "datos": None,
    }
    
    # ── Paso 1: Verificar robots.txt ──
    robots_permitido, robots_motivo = check_robots_txt(logger)
    captured_data["_metadata"]["robots_txt"] = {
        "verificado": True,
        "permitido": robots_permitido,
        "motivo": robots_motivo,
    }
    
    if not robots_permitido:
        logger.warning(
            f"Scraping NO permitido: {robots_motivo}. "
            f"Respetando directivas del sitio."
        )
        captured_data["_metadata"]["estado_fuente"] = "bloqueado_por_robots_o_servidor"
        captured_data["_metadata"]["nota_estado"] = (
            "AVAMET bloquea acceso automatizado (HTTP 403). "
            "Alternativas activas: streaming_openweather.py, "
            "descargar_aemet_historico.py."
        )
        return captured_data
    
    # ── Paso 2: Intentar captura por cada URL ──
    logger.info("Intentando captura de datos de precipitación...")
    
    for url_name, url in AVAMET_URLS.items():
        logger.info(f"  Intentando: {url_name} ({url})")
        
        captured_data["_metadata"]["urls_intentadas"].append({
            "nombre": url_name,
            "url": url,
            "resultado": None,
        })
        
        html = fetch_page(url, url_name, logger)
        
        if html is not None:
            logger.info(f"  ✔ Página descargada. Parseando tablas...")
            
            if url_name == "precipitacion":
                registros = parse_precipitation_table(html, logger)
            else:
                registros = parse_general_meteo_table(html, logger)
            
            captured_data["_metadata"]["urls_intentadas"][-1]["resultado"] = "exitosa"
            
            if registros:
                captured_data["datos"] = registros
                captured_data["_metadata"]["registros_extraidos"] = len(registros)
                captured_data["_metadata"]["estado_fuente"] = "operativa"
                captured_data["_metadata"]["url_utilizada"] = url
                
                logger.info(f"  ✔ {len(registros)} registros extraídos")
                return captured_data
            else:
                logger.warning("  Página descargada pero sin datos parseables.")
                captured_data["_metadata"]["urls_intentadas"][-1]["resultado"] = "sin_datos"
        else:
            captured_data["_metadata"]["urls_intentadas"][-1]["resultado"] = "fallida"
        
        # Delay ético entre peticiones
        logger.debug(f"  Esperando {DELAY_ENTRE_PETICIONES}s (delay ético)...")
        time.sleep(DELAY_ENTRE_PETICIONES)
    
    # ── Ninguna URL funcionó ──
    captured_data["_metadata"]["estado_fuente"] = "no_disponible"
    captured_data["_metadata"]["nota_estado"] = (
        "Ninguna URL de AVAMET respondió con datos. "
        "Estado habitual: HTTP 403 (acceso bloqueado a bots). "
        "Precipitación cubierta por: streaming_openweather.py (OWM) "
        "y descargar_aemet_historico.py (AEMET)."
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
    
    Siempre guarda el archivo, incluso si la captura falló,
    para mantener un registro histórico de intentos.
    
    Formato nombre: avamet_YYYYMMDD_HHMMSS.json
    
    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos
    
    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"avamet_{timestamp_str}.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        size_str = f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"

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
    Función principal que orquesta el scraping de AVAMET.
    
    Flujo:
    1. Configura logging
    2. Verifica robots.txt
    3. Intenta capturar datos de AVAMET
    4. Guarda resultado (éxito o documentación del fallo)
    5. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA EN TIEMPO REAL: Precipitaciones (AVAMET - Web Scraping)")
    logger.info("=" * 70)
    
    # Capturar datos
    captured_data = capture_avamet_data(logger)
    
    meta = captured_data["_metadata"]
    estado = meta["estado_fuente"]
    registros = meta["registros_extraidos"]
    
    # Guardar siempre (incluso si falló, para registro histórico)
    output_path = save_capture(captured_data, logger)
    
    if output_path is None:
        print("\n❌ ERROR: no se pudo guardar el archivo.")
        return
    
    # Resumen final
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estado fuente: {estado}")
    logger.info(f"  Registros extraídos: {registros}")
    logger.info(f"  Archivo: {output_path.name}")
    logger.info(f"  Ubicación: {OUTPUT_DIR}")
    logger.info(f"  Timestamp: {meta['timestamp_captura']}")
    
    if meta.get("nota_estado"):
        logger.info(f"  Nota: {meta['nota_estado']}")
    
    logger.info("")
    
    # Mensaje claro en consola
    if estado == "operativa" and registros > 0:
        print(f"\n✅ CAPTURA CORRECTA: {registros} registros → {output_path.name}")
    elif estado == "bloqueado_por_robots_o_servidor":
        print(
            f"\n⚠️  FUENTE BLOQUEADA: AVAMET bloquea acceso automatizado (HTTP 403).\n"
            f"   Estado documentado en → {output_path.name}\n"
            f"   Alternativa activa: streaming_openweather.py (precipitación vía OWM)"
        )
    elif estado == "no_disponible":
        print(
            f"\n⚠️  FUENTE NO DISPONIBLE: sin datos de AVAMET.\n"
            f"   Intento documentado en → {output_path.name}\n"
            f"   Alternativa activa: streaming_openweather.py (precipitación vía OWM)"
        )
    else:
        print(f"\n⚠️  CAPTURA PARCIAL: estado={estado} → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
