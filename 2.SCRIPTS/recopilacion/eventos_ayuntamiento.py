# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 4.2: Integración de Eventos Masivos - Agenda Ayuntamiento Valencia
==============================================================================

Descripción:
    Captura eventos de la agenda cultural oficial del Ayuntamiento de
    Valencia mediante web scraping ético. Los eventos municipales (ferias,
    exposiciones, festivales, actos institucionales) complementan la
    agenda turística de Visit Valencia para correlacionar actividad
    urbana con picos de contaminación y tráfico.

    ESTRATEGIA DE SCRAPING:
    ─────────────────────────
    La página del Ayuntamiento NO ofrece feed .ics público accesible.
    Se realiza scraping directo de la página de agenda cultural.

    Cada evento está dentro de un <div class="journal-content-article">.
    Dentro de cada tarjeta se extraen:
        - Nombre:    <p class="label-title-agenda">
        - Fecha raw: <p class="label-fecha-actualidad">
                     Ejemplo: "21/01/2026 - 19/04/2026"
        - Categoría: <p class="label-categoria-actualidad">
        - URL:       <a href="/cas/agenda-de-la-ciudad/-/content/...">

Fuente de datos:
    URL:       https://www.valencia.es/cas/cultura/agenda
    Método:    GET + BeautifulSoup4 (web scraping)
    Auth:      No requiere
    Formato:   HTML server-side rendered
    Licencia:  Información pública del Ayuntamiento de Valencia

Scraping ético:
    - User-Agent transparente (proyecto académico)
    - Una sola petición por ejecución
    - Delay configurable entre peticiones futuras
    - Se respeta cualquier bloqueo HTTP (403, 429, etc.)

Uso:
    python eventos_ayuntamiento.py

Salida:
    - 1.DATOS_EN_CRUDO/eventos/ayuntamiento.json
    - JSON RAW con metadatos de captura y lista de eventos

Ruta esperada del script:
    2.SCRIPTS/recopilacion/eventos_ayuntamiento.py

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
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "eventos"
LOG_DIR = PROJECT_ROOT / "logs"

# URL de la agenda cultural del Ayuntamiento de Valencia
AYUNTAMIENTO_AGENDA_URL = "https://www.valencia.es/cas/cultura/agenda"
AYUNTAMIENTO_DOMAIN = "www.valencia.es"
AYUNTAMIENTO_BASE_URL = "https://www.valencia.es"

# Configuración de peticiones HTTP
REQUEST_TIMEOUT = 30
DELAY_ENTRE_PETICIONES = 3  # Segundos mínimos entre peticiones (ético)
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

    log_file = LOG_DIR / "eventos_ayuntamiento.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Eventos_Ayuntamiento")
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

def fetch_page(
    url: str,
    logger: logging.Logger,
) -> Optional[str]:
    """
    Descarga una página web de forma ética.

    Incluye manejo robusto de errores HTTP y de red,
    con logging detallado para facilitar debug.

    Args:
        url: URL a descargar
        logger: Logger para registrar eventos

    Returns:
        Contenido HTML como string, o None si falla
    """
    logger.info("Descargando página de la Agenda del Ayuntamiento...")
    logger.debug(f"  URL: {url}")

    try:
        response = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )

        logger.debug(f"  HTTP {response.status_code} ({len(response.text)} bytes)")

        if response.status_code == 200:
            logger.info(
                f"  ✓ Página descargada correctamente "
                f"({len(response.text):,} bytes)"
            )
            return response.text

        elif response.status_code == 403:
            logger.warning(
                "  ✗ HTTP 403: Acceso bloqueado por el servidor."
            )
            return None

        elif response.status_code == 404:
            logger.warning(
                "  ✗ HTTP 404: Página no encontrada. "
                "La URL puede haber cambiado."
            )
            return None

        elif response.status_code == 429:
            logger.warning(
                "  ✗ HTTP 429: Demasiadas peticiones. Respetando rate limit."
            )
            return None

        else:
            logger.warning(f"  ✗ HTTP {response.status_code}: respuesta inesperada.")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"  ✗ Timeout tras {REQUEST_TIMEOUT}s")
        return None

    except requests.exceptions.ConnectionError as e:
        logger.error(f"  ✗ Error de conexión: {e}")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"  ✗ Error HTTP genérico: {e}")
        return None


# ==============================================================================
# FUNCIONES DE PARSING
# ==============================================================================

def extract_event_name(
    article: BeautifulSoup,
    logger: logging.Logger,
) -> str:
    """
    Extrae el nombre del evento desde el artículo.

    Busca el patrón principal <p class="label-title-agenda">,
    con fallbacks a otros elementos de heading.

    Args:
        article: Elemento BeautifulSoup del artículo
        logger: Logger para registrar eventos

    Returns:
        Nombre del evento o cadena vacía si no se encuentra
    """
    # Patrón principal: <p class="label-title-agenda">
    title_elem = article.find("p", class_="label-title-agenda")

    if title_elem is None:
        # Fallback: buscar clase que contenga "title" y "agenda"
        title_elem = article.find(
            "p",
            class_=lambda c: c and "title" in c.lower() and "agenda" in c.lower(),
        )

    if title_elem is None:
        # Fallback: cualquier h2, h3 o h4 dentro del artículo
        for tag in ("h3", "h2", "h4"):
            title_elem = article.find(tag)
            if title_elem:
                break

    if title_elem:
        # El nombre puede estar directamente o dentro de un <a>
        link = title_elem.find("a")
        if link:
            name = link.get_text(strip=True)
        else:
            name = title_elem.get_text(strip=True)

        logger.debug(f"    Nombre: {name}")
        return name

    logger.debug("    ⚠ No se encontró nombre del evento")
    return ""


def extract_event_dates(
    article: BeautifulSoup,
    logger: logging.Logger,
) -> Tuple[str, str]:
    """
    Extrae las fechas de inicio y fin del evento en formato RAW.

    El patrón típico del Ayuntamiento es:
        <p class="label-fecha-actualidad">21/01/2026 - 19/04/2026</p>

    También puede aparecer una sola fecha (evento de un día):
        <p class="label-fecha-actualidad">15/03/2026</p>

    NO se normalizan las fechas (datos RAW para Fase 5).

    Args:
        article: Elemento BeautifulSoup del artículo
        logger: Logger para registrar eventos

    Returns:
        Tuple (fecha_inicio_raw, fecha_fin_raw)
        Ambas como strings sin normalizar
    """
    fecha_inicio = ""
    fecha_fin = ""

    # Patrón principal: <p class="label-fecha-actualidad">
    date_elem = article.find("p", class_="label-fecha-actualidad")

    if date_elem is None:
        # Fallback: clase que contenga "fecha"
        date_elem = article.find(
            "p",
            class_=lambda c: c and "fecha" in c.lower(),
        )

    if date_elem is None:
        logger.debug("    ⚠ No se encontró bloque de fechas")
        return fecha_inicio, fecha_fin

    date_text = date_elem.get_text(strip=True)
    logger.debug(f"    Fecha raw: '{date_text}'")

    if not date_text:
        return fecha_inicio, fecha_fin

    # Intentar separar por " - " (patrón "21/01/2026 - 19/04/2026")
    if " - " in date_text:
        parts = date_text.split(" - ", 1)
        fecha_inicio = parts[0].strip()
        fecha_fin = parts[1].strip()
    elif " al " in date_text.lower():
        # Alternativa: "Del 21/01/2026 al 19/04/2026"
        parts = date_text.lower().split(" al ", 1)
        fecha_inicio = parts[0].strip()
        fecha_fin = parts[1].strip()
        # Limpiar posible "Del " o "del " al inicio
        for prefix in ("del ", "desde "):
            if fecha_inicio.lower().startswith(prefix):
                fecha_inicio = fecha_inicio[len(prefix):].strip()
    else:
        # Evento de un solo día
        fecha_inicio = date_text
        fecha_fin = date_text

    logger.debug(f"    Fechas: {fecha_inicio} → {fecha_fin}")
    return fecha_inicio, fecha_fin


def extract_event_category(
    article: BeautifulSoup,
    logger: logging.Logger,
) -> str:
    """
    Extrae la categoría del evento.

    Busca <p class="label-categoria-actualidad"> que contiene
    la clasificación del Ayuntamiento (ej: "Exposiciones",
    "Música", "Teatro", etc.).

    Args:
        article: Elemento BeautifulSoup del artículo
        logger: Logger para registrar eventos

    Returns:
        Categoría como string o cadena vacía
    """
    # Patrón principal: <p class="label-categoria-actualidad">
    cat_elem = article.find("p", class_="label-categoria-actualidad")

    if cat_elem is None:
        # Fallback: clase que contenga "categoria"
        cat_elem = article.find(
            "p",
            class_=lambda c: c and "categoria" in c.lower(),
        )

    if cat_elem:
        category = cat_elem.get_text(strip=True)
        logger.debug(f"    Categoría: {category}")
        return category

    logger.debug("    ⚠ No se encontró categoría")
    return ""


def extract_event_url(
    article: BeautifulSoup,
    logger: logging.Logger,
) -> str:
    """
    Extrae la URL de detalle del evento.

    Busca enlaces que apunten a páginas de la agenda del Ayuntamiento.
    Las URLs suelen tener el patrón:
        /cas/agenda-de-la-ciudad/-/content/...

    Se devuelve como URL absoluta (añadiendo dominio base si es relativa).

    Args:
        article: Elemento BeautifulSoup del artículo
        logger: Logger para registrar eventos

    Returns:
        URL absoluta del evento o cadena vacía
    """
    event_url = ""

    # Buscar todos los enlaces dentro del artículo
    links = article.find_all("a", href=True)

    for link in links:
        href = link["href"]

        # Priorizar enlaces que parezcan de la agenda
        if "/agenda" in href or "/content/" in href or "/cultura/" in href:
            event_url = href
            break

    # Si no se encontró enlace de agenda, tomar el primer enlace útil
    if not event_url and links:
        for link in links:
            href = link["href"]
            # Descartar anclas, javascript y enlaces externos genéricos
            if (
                href
                and not href.startswith("#")
                and not href.startswith("javascript:")
                and "valencia.es" in href or href.startswith("/")
            ):
                event_url = href
                break

    # Convertir a URL absoluta si es relativa
    if event_url and not event_url.startswith("http"):
        event_url = f"{AYUNTAMIENTO_BASE_URL}{event_url}"

    if event_url:
        logger.debug(f"    URL: {event_url}")
    else:
        logger.debug("    ⚠ No se encontró URL del evento")

    return event_url


def parse_event_articles(
    html: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Parsea todos los artículos de eventos del HTML descargado.

    Identifica eventos dentro de <div class="journal-content-article">.
    Deduplica por combinación de nombre + url para evitar repeticiones.

    Args:
        html: Contenido HTML de la página
        logger: Logger para registrar eventos

    Returns:
        Lista de diccionarios con datos RAW de cada evento
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []
    seen_keys = set()

    # Buscar todos los artículos de eventos
    articles = soup.find_all("div", class_="journal-content-article")

    if not articles:
        logger.warning(
            "No se encontraron artículos con clase 'journal-content-article'"
        )
        logger.debug("  Intentando búsqueda alternativa...")

        # Fallback 1: clase que contenga "journal-content"
        articles = soup.find_all(
            "div",
            class_=lambda c: c and "journal-content" in c if isinstance(c, str) else False,
        )
        logger.debug(f"  Fallback 'journal-content': {len(articles)} encontrados")

        # Fallback 2: artículos con clase que contenga "agenda"
        if not articles:
            articles = soup.find_all(
                "div",
                class_=lambda c: c and "agenda" in c.lower() if isinstance(c, str) else False,
            )
            logger.debug(f"  Fallback 'agenda': {len(articles)} encontrados")

        # Fallback 3: elementos <article>
        if not articles:
            articles = soup.find_all("article")
            logger.debug(f"  Fallback '<article>': {len(articles)} encontrados")

    logger.info(f"Artículos de eventos encontrados: {len(articles)}")

    for idx, article in enumerate(articles):
        logger.debug(f"  [#{idx}] Procesando artículo...")

        # ── Extraer campos ──
        nombre = extract_event_name(article, logger)
        fecha_inicio, fecha_fin = extract_event_dates(article, logger)
        categoria = extract_event_category(article, logger)
        url_evento = extract_event_url(article, logger)

        # ── Deduplicación por nombre + url ──
        dedup_key = f"{nombre.lower().strip()}|{url_evento.lower().strip()}"
        if dedup_key in seen_keys and dedup_key != "|":
            logger.debug(f"  [#{idx}] Duplicado, saltando...")
            continue
        seen_keys.add(dedup_key)

        # Saltar artículos completamente vacíos
        if not nombre and not fecha_inicio and not url_evento:
            logger.debug(f"  [#{idx}] Artículo vacío, saltando...")
            continue

        evento = {
            "nombre": nombre,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "categoria": categoria,
            "url_evento": url_evento,
        }

        eventos.append(evento)

    logger.info(f"Eventos únicos extraídos: {len(eventos)}")
    return eventos


# ==============================================================================
# FUNCIÓN DE CAPTURA PRINCIPAL
# ==============================================================================

def capture_ayuntamiento_events(
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Orquesta la captura completa de eventos del Ayuntamiento de Valencia.

    Flujo:
    1. Descarga HTML de la agenda cultural
    2. Parsea artículos de eventos (journal-content-article)
    3. Extrae campos RAW: nombre, fechas, categoría, URL
    4. Deduplica por nombre + url
    5. Construye JSON con metadatos y datos RAW

    Args:
        logger: Logger para registrar eventos

    Returns:
        Diccionario con datos capturados y metadatos
    """
    capture_timestamp = datetime.now()

    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "4.2 - Eventos Agenda Ayuntamiento Valencia",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fuente": "Ayuntamiento de Valencia - Agenda Cultural",
            "metodo": "Web scraping ético (BeautifulSoup4)",
            "url_consultada": AYUNTAMIENTO_AGENDA_URL,
            "estado_fuente": "desconocido",
            "eventos_extraidos": 0,
            "notas_eticas": [
                "User-Agent transparente (proyecto académico)",
                "Una sola petición por ejecución",
                "Delay configurable entre peticiones",
                "Se respeta cualquier bloqueo HTTP",
                "Información pública del Ayuntamiento de Valencia",
            ],
        },
        "eventos": None,
    }

    # ── Paso 1: Descargar página ──
    html = fetch_page(AYUNTAMIENTO_AGENDA_URL, logger)

    if html is None:
        captured_data["_metadata"]["estado_fuente"] = "no_disponible"
        captured_data["_metadata"]["nota_estado"] = (
            "No se pudo acceder a la agenda del Ayuntamiento de Valencia. "
            "Posibles causas: bloqueo de bots, servidor caído, "
            "o cambio en la URL de la agenda."
        )
        return captured_data

    # ── Paso 2: Parsear eventos ──
    logger.info("Parseando artículos de eventos...")
    eventos = parse_event_articles(html, logger)

    if eventos:
        captured_data["eventos"] = eventos
        captured_data["_metadata"]["eventos_extraidos"] = len(eventos)
        captured_data["_metadata"]["estado_fuente"] = "operativa"
        logger.info(f"✓ {len(eventos)} eventos extraídos correctamente")
    else:
        captured_data["eventos"] = []
        captured_data["_metadata"]["estado_fuente"] = "sin_eventos"
        captured_data["_metadata"]["nota_estado"] = (
            "La página se descargó correctamente pero no se encontraron "
            "artículos de eventos. Posibles causas: la estructura HTML "
            "ha cambiado, o no hay eventos publicados actualmente. "
            "Verificar con F12 si los selectores CSS siguen vigentes."
        )
        logger.warning("Página descargada pero sin eventos encontrados")

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

    Los eventos del Ayuntamiento se guardan en un archivo único
    (ayuntamiento.json) que se sobreescribe en cada ejecución,
    ya que representa el estado actual de la agenda.

    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos

    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Archivo único (no timestamped) según especificación del proyecto
    filename = "ayuntamiento.json"
    output_path = OUTPUT_DIR / filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )

        logger.info(f"✓ Archivo guardado: {filename} ({size_str})")
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
    Función principal que orquesta el scraping del Ayuntamiento.

    Flujo:
    1. Configura logging
    2. Descarga la página de la agenda cultural
    3. Parsea artículos de eventos
    4. Guarda resultado (éxito o documentación del fallo)
    5. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA DE EVENTOS: Agenda Ayuntamiento Valencia (Web Scraping)")
    logger.info("=" * 70)

    # Capturar eventos
    captured_data = capture_ayuntamiento_events(logger)

    meta = captured_data["_metadata"]
    estado = meta["estado_fuente"]
    num_eventos = meta["eventos_extraidos"]

    # Guardar siempre (incluso si falló, para registro)
    output_path = save_capture(captured_data, logger)

    if output_path is None:
        print("\n✗ ERROR: no se pudo guardar el archivo.")
        return

    # Resumen final
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CAPTURA")
    logger.info("-" * 70)
    logger.info(f"  Estado fuente:    {estado}")
    logger.info(f"  Eventos extraídos: {num_eventos}")
    logger.info(f"  URL consultada:   {AYUNTAMIENTO_AGENDA_URL}")
    logger.info(f"  Archivo:          {output_path.name}")
    logger.info(f"  Ubicación:        {OUTPUT_DIR}")
    logger.info(f"  Timestamp:        {meta['timestamp_captura']}")

    if meta.get("nota_estado"):
        logger.info(f"  Nota: {meta['nota_estado']}")

    logger.info("")

    # Mensaje claro en consola
    if estado == "operativa" and num_eventos > 0:
        print(f"\n✅ CAPTURA CORRECTA: {num_eventos} eventos → {output_path.name}")

        # Mostrar preview de los primeros 5 eventos
        eventos = captured_data.get("eventos", [])
        if eventos:
            print(f"\n📋 Preview (primeros {min(5, len(eventos))} eventos):")
            print("-" * 60)
            for ev in eventos[:5]:
                nombre = ev.get("nombre", "Sin nombre")[:50]
                fechas = f"{ev.get('fecha_inicio', '?')} → {ev.get('fecha_fin', '?')}"
                cat = ev.get("categoria", "")
                print(f"  • {nombre}")
                print(f"    {fechas}")
                if cat:
                    print(f"    [{cat}]")
            if len(eventos) > 5:
                print(f"  ... y {len(eventos) - 5} eventos más")

    elif estado == "no_disponible":
        print(
            f"\n⚠️  FUENTE NO DISPONIBLE: no se pudo acceder al Ayuntamiento.\n"
            f"   Estado documentado en → {output_path.name}"
        )
    elif estado == "sin_eventos":
        print(
            f"\n⚠️  SIN EVENTOS: la página se descargó pero no se encontraron eventos.\n"
            f"   La estructura HTML puede haber cambiado (verificar con F12).\n"
            f"   Estado documentado en → {output_path.name}"
        )
    else:
        print(f"\n⚠️  CAPTURA PARCIAL: estado={estado} → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
