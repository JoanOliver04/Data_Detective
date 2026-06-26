# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 4.1: Integración de Eventos Masivos - Visit Valencia
==============================================================================

Descripción:
    Captura eventos de la agenda pública de Visit Valencia mediante web
    scraping ético. Los eventos masivos (Fallas, conciertos, ferias) son
    fundamentales para correlacionar picos de contaminación y tráfico
    con la actividad cultural y turística de la ciudad.

    ESTRATEGIA DE SCRAPING:
    ─────────────────────────
    La página principal /en/events-valencia NO muestra eventos sin búsqueda.
    Se utiliza la URL real de la agenda (/en/agenda-convention-bureau) con
    parámetros de fecha para obtener resultados server-side rendered.

    Cada evento se identifica por un <div> con atributo data-history-node-id.
    Dentro de cada tarjeta se extraen: nombre, fechas, ubicación y URL.

Fuente de datos:
    URL base:  https://www.visitvalencia.com/en/agenda-convention-bureau
    Método:    GET + BeautifulSoup4 (web scraping)
    Auth:      No requiere
    Formato:   HTML server-side rendered
    Licencia:  Contenido público de promoción turística

Scraping ético:
    - User-Agent transparente (proyecto académico)
    - Una sola petición por ejecución
    - Delay configurable entre peticiones futuras
    - Se respeta cualquier bloqueo HTTP (403, 429, etc.)

Uso:
    python eventos_visitvalencia.py

Salida:
    - 1.DATOS_EN_CRUDO/eventos/visitvalencia.json
    - JSON RAW con metadatos de captura y lista de eventos

Ruta esperada del script:
    2.SCRIPTS/recopilacion/eventos_visitvalencia.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from bs4 import BeautifulSoup
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "eventos"
LOG_DIR = PROJECT_ROOT / "logs"

# URL base de la agenda (la que realmente devuelve eventos)
VISITVALENCIA_BASE_URL = "https://www.visitvalencia.com/en/agenda-convention-bureau"
VISITVALENCIA_DOMAIN = "www.visitvalencia.com"

# Rango de fechas por defecto: hoy → hoy + 90 días
# Se puede ajustar según las necesidades del análisis
DIAS_RANGO_BUSQUEDA = 90

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
    "Accept-Language": "en-GB,en;q=0.9,es-ES;q=0.8,es;q=0.7",
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

    log_file = LOG_DIR / "eventos_visitvalencia.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Eventos_VisitValencia")
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
# FUNCIONES DE CONSTRUCCIÓN DE URL
# ==============================================================================


def build_agenda_url(
    fecha_inicio: Optional[datetime] = None,
    fecha_fin: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> str:
    """
    Construye la URL de búsqueda de la agenda con rango de fechas.

    La agenda de Visit Valencia requiere parámetros de fecha en formato
    DD-MM-YYYY para devolver resultados. Sin estos parámetros, la página
    no muestra ningún evento.

    Args:
        fecha_inicio: Fecha de inicio del rango (default: hoy)
        fecha_fin: Fecha fin del rango (default: hoy + DIAS_RANGO_BUSQUEDA)
        logger: Logger para registrar eventos

    Returns:
        URL completa con parámetros de búsqueda
    """
    if fecha_inicio is None:
        fecha_inicio = datetime.now()
    if fecha_fin is None:
        fecha_fin = fecha_inicio + timedelta(days=DIAS_RANGO_BUSQUEDA)

    # Formato DD-MM-YYYY requerido por Visit Valencia
    min_str = fecha_inicio.strftime("%d-%m-%Y")
    max_str = fecha_fin.strftime("%d-%m-%Y")

    url = (
        f"{VISITVALENCIA_BASE_URL}"
        f"?field_event_category_target_id=All"
        f"&field_event_dates_value[min]={min_str}"
        f"&field_event_dates_value[max]={max_str}"
    )

    if logger:
        logger.info(f"Rango de búsqueda: {min_str} → {max_str}")
        logger.debug(f"URL construida: {url}")

    return url


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
    logger.info("Descargando página de Visit Valencia...")
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
            logger.warning("  ✗ HTTP 403: Acceso bloqueado por el servidor.")
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


def extract_event_name(card: BeautifulSoup, logger: logging.Logger) -> str:
    """
    Extrae el nombre del evento desde el heading de la tarjeta.

    Busca primero h3 (más común), luego h2 como fallback.

    Args:
        card: Elemento BeautifulSoup de la tarjeta
        logger: Logger para registrar eventos

    Returns:
        Nombre del evento o cadena vacía si no se encuentra
    """
    # Buscar h3 con clase card__heading (patrón principal)
    heading = card.find("h3", class_="card__heading")

    # Fallback: cualquier h3 dentro de la tarjeta
    if heading is None:
        heading = card.find("h3")

    # Fallback: cualquier h2 dentro de la tarjeta
    if heading is None:
        heading = card.find("h2")

    if heading:
        name = heading.get_text(strip=True)
        logger.debug(f"    Nombre: {name}")
        return name

    logger.debug("    ⚠ No se encontró nombre del evento")
    return ""


def extract_event_dates(
    card: BeautifulSoup,
    logger: logging.Logger,
) -> Tuple[str, str]:
    """
    Reconstruye las fechas de inicio y fin del evento.

    Las fechas en Visit Valencia están fragmentadas en múltiples
    elementos <span class="card__date-text"> dentro de un
    <div class="card__date">. El patrón típico es:
        "From" | "01/03/2026" | "to" | "19/03/2026"

    NO hay un nodo de texto literal "DATE:"; hay un <p> con
    class="card__date-label".

    Args:
        card: Elemento BeautifulSoup de la tarjeta
        logger: Logger para registrar eventos

    Returns:
        Tuple (fecha_inicio_raw, fecha_fin_raw)
        Ambas como strings sin normalizar
    """
    fecha_inicio = ""
    fecha_fin = ""

    date_block = card.find("div", class_="card__date")

    if date_block is None:
        # Fallback: buscar cualquier div que contenga "date" en su clase
        date_block = card.find("div", class_=lambda c: c and "date" in c.lower())

    if date_block is None:
        logger.debug("    ⚠ No se encontró bloque de fechas")
        return fecha_inicio, fecha_fin

    # Extraer todos los spans de fecha
    date_spans = date_block.find_all("span", class_="card__date-text")

    if not date_spans:
        # Fallback: todos los spans dentro del bloque
        date_spans = date_block.find_all("span")

    # Reconstruir texto completo de fechas
    date_texts = [span.get_text(strip=True) for span in date_spans]
    logger.debug(f"    Fragmentos de fecha: {date_texts}")

    # Estrategia: buscar tokens que parezcan fechas (contienen /)
    # y separar por los conectores "From" / "to"
    date_values = []
    for text in date_texts:
        text_lower = text.lower()
        # Ignorar conectores, quedarnos con valores de fecha
        if text_lower in ("from", "to", "desde", "hasta", "date:", "date"):
            continue
        if text.strip():
            date_values.append(text.strip())

    if len(date_values) >= 2:
        fecha_inicio = date_values[0]
        fecha_fin = date_values[1]
    elif len(date_values) == 1:
        # Evento de un solo día
        fecha_inicio = date_values[0]
        fecha_fin = date_values[0]

    logger.debug(f"    Fechas: {fecha_inicio} → {fecha_fin}")
    return fecha_inicio, fecha_fin


def extract_event_location(
    card: BeautifulSoup,
    logger: logging.Logger,
) -> str:
    """
    Extrae la ubicación del evento desde el bloque card__place.

    Args:
        card: Elemento BeautifulSoup de la tarjeta
        logger: Logger para registrar eventos

    Returns:
        Ubicación como string o cadena vacía
    """
    place_block = card.find("div", class_="card__place")

    if place_block is None:
        # Fallback: buscar cualquier div con "place" o "location"
        place_block = card.find(
            "div",
            class_=lambda c: c and ("place" in c.lower() or "location" in c.lower()),
        )

    if place_block:
        location = place_block.get_text(strip=True)
        # Limpiar prefijos comunes
        for prefix in ("Place:", "Lugar:", "Location:"):
            if location.startswith(prefix):
                location = location[len(prefix) :].strip()
        logger.debug(f"    Ubicación: {location}")
        return location

    logger.debug("    ⚠ No se encontró ubicación")
    return ""


def extract_event_url(
    card: BeautifulSoup,
    logger: logging.Logger,
) -> str:
    """
    Extrae la URL del evento desde la tarjeta.

    Busca la URL en dos lugares (por orden de prioridad):
    1. Atributo 'about' del propio div de la tarjeta
    2. Enlace <a class="card__link"> (visually-hidden)
    3. Enlace <a> que contenga "SEE MORE"

    Args:
        card: Elemento BeautifulSoup de la tarjeta
        logger: Logger para registrar eventos

    Returns:
        URL absoluta del evento o cadena vacía
    """
    base_url = "https://www.visitvalencia.com"
    event_url = ""

    # Opción 1: atributo 'about' en el div de la tarjeta
    about = card.get("about", "")
    if about:
        event_url = about if about.startswith("http") else f"{base_url}{about}"
        logger.debug(f"    URL (about): {event_url}")
        return event_url

    # Opción 2: enlace con clase card__link
    link = card.find("a", class_="card__link")
    if link and link.get("href"):
        href = link["href"]
        event_url = href if href.startswith("http") else f"{base_url}{href}"
        logger.debug(f"    URL (card__link): {event_url}")
        return event_url

    # Opción 3: enlace que contenga "SEE MORE" en su texto
    all_links = card.find_all("a")
    for a_tag in all_links:
        if "see more" in a_tag.get_text(strip=True).lower():
            href = a_tag.get("href", "")
            if href:
                event_url = href if href.startswith("http") else f"{base_url}{href}"
                logger.debug(f"    URL (see more): {event_url}")
                return event_url

    # Opción 4: cualquier enlace con clase 'button'
    button_link = card.find("a", class_="button")
    if button_link and button_link.get("href"):
        href = button_link["href"]
        event_url = href if href.startswith("http") else f"{base_url}{href}"
        logger.debug(f"    URL (button): {event_url}")
        return event_url

    logger.debug("    ⚠ No se encontró URL del evento")
    return event_url


def parse_event_cards(
    html: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Parsea todas las tarjetas de eventos del HTML descargado.

    Identifica eventos por el atributo data-history-node-id en los
    elementos <div>. Deduplica por este ID para evitar repeticiones.

    Args:
        html: Contenido HTML de la página
        logger: Logger para registrar eventos

    Returns:
        Lista de diccionarios con datos RAW de cada evento
    """
    soup = BeautifulSoup(html, "html.parser")
    eventos = []
    seen_ids = set()

    # Buscar todas las tarjetas de eventos por data-history-node-id
    cards = soup.find_all("div", attrs={"data-history-node-id": True})

    if not cards:
        logger.warning("No se encontraron tarjetas de eventos (data-history-node-id)")
        logger.debug("  Intentando búsqueda alternativa por clase 'card'...")

        # Fallback: buscar divs con clase que contenga "card"
        cards = soup.find_all(
            "div",
            class_=lambda c: (
                c and "card" in " ".join(c)
                if isinstance(c, list)
                else c and "card" in c
            ),
        )
        logger.debug(f"  Encontrados {len(cards)} divs con clase 'card'")

    logger.info(f"Tarjetas de eventos encontradas: {len(cards)}")

    for idx, card in enumerate(cards):
        node_id = card.get("data-history-node-id", f"unknown_{idx}")

        # ── Deduplicación por node_id ──
        if node_id in seen_ids:
            logger.debug(f"  [#{idx}] Duplicado (node_id={node_id}), saltando...")
            continue
        seen_ids.add(node_id)

        logger.debug(f"  [#{idx}] Procesando evento (node_id={node_id})...")

        # ── Extraer campos ──
        nombre = extract_event_name(card, logger)
        fecha_inicio, fecha_fin = extract_event_dates(card, logger)
        ubicacion = extract_event_location(card, logger)
        url_evento = extract_event_url(card, logger)

        evento = {
            "node_id": node_id,
            "nombre": nombre,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "ubicacion": ubicacion,
            "url_evento": url_evento,
        }

        eventos.append(evento)

    logger.info(f"Eventos únicos extraídos: {len(eventos)}")
    return eventos


# ==============================================================================
# FUNCIÓN DE CAPTURA PRINCIPAL
# ==============================================================================


def capture_visitvalencia_events(
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Orquesta la captura completa de eventos de Visit Valencia.

    Flujo:
    1. Construye URL con rango de fechas configurable
    2. Descarga HTML de la agenda
    3. Parsea tarjetas de eventos
    4. Deduplica por data-history-node-id
    5. Construye JSON con metadatos y datos RAW

    Args:
        logger: Logger para registrar eventos

    Returns:
        Diccionario con datos capturados y metadatos
    """
    capture_timestamp = datetime.now()
    fecha_inicio = capture_timestamp
    fecha_fin = capture_timestamp + timedelta(days=DIAS_RANGO_BUSQUEDA)

    captured_data = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "4.1 - Eventos Visit Valencia",
            "timestamp_captura": capture_timestamp.isoformat(),
            "timestamp_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "fuente": "Visit Valencia - Agenda Convention Bureau",
            "metodo": "Web scraping ético (BeautifulSoup4)",
            "url_consultada": None,
            "rango_busqueda": {
                "fecha_inicio": fecha_inicio.strftime("%d-%m-%Y"),
                "fecha_fin": fecha_fin.strftime("%d-%m-%Y"),
                "dias": DIAS_RANGO_BUSQUEDA,
            },
            "estado_fuente": "desconocido",
            "eventos_extraidos": 0,
            "notas_eticas": [
                "User-Agent transparente (proyecto académico)",
                "Una sola petición por ejecución",
                "Delay configurable entre peticiones",
                "Se respeta cualquier bloqueo HTTP",
                "Contenido público de promoción turística",
            ],
        },
        "eventos": None,
    }

    # ── Paso 1: Construir URL con rango de fechas ──
    url = build_agenda_url(fecha_inicio, fecha_fin, logger)
    captured_data["_metadata"]["url_consultada"] = url

    # ── Paso 2: Descargar página ──
    html = fetch_page(url, logger)

    if html is None:
        captured_data["_metadata"]["estado_fuente"] = "no_disponible"
        captured_data["_metadata"]["nota_estado"] = (
            "No se pudo acceder a la agenda de Visit Valencia. "
            "Posibles causas: bloqueo de bots, servidor caído, "
            "o cambio en la estructura de la web."
        )
        return captured_data

    # ── Paso 3: Parsear eventos ──
    logger.info("Parseando tarjetas de eventos...")
    eventos = parse_event_cards(html, logger)

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
            "tarjetas de eventos. Posibles causas: no hay eventos en el "
            "rango de fechas, o la estructura HTML ha cambiado."
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

    A diferencia de los scripts de streaming (que generan archivos
    con timestamp), los eventos se guardan en un archivo único
    (visitvalencia.json) que se sobreescribe en cada ejecución,
    ya que representa el estado actual de la agenda.

    Args:
        data: Diccionario con datos capturados
        logger: Logger para registrar eventos

    Returns:
        Path al archivo guardado o None si hay error
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Archivo único (no timestamped) según especificación del proyecto
    filename = "visitvalencia.json"
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
    Función principal que orquesta el scraping de Visit Valencia.

    Flujo:
    1. Configura logging
    2. Construye URL con rango de fechas
    3. Descarga y parsea la página de eventos
    4. Guarda resultado (éxito o documentación del fallo)
    5. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("CAPTURA DE EVENTOS: Visit Valencia (Web Scraping)")
    logger.info("=" * 70)

    # Capturar eventos
    captured_data = capture_visitvalencia_events(logger)

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
    logger.info(
        f"  Rango búsqueda:   {meta['rango_busqueda']['fecha_inicio']} → "
        f"{meta['rango_busqueda']['fecha_fin']}"
    )
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
                print(f"  • {nombre}")
                print(f"    {fechas}")
            if len(eventos) > 5:
                print(f"  ... y {len(eventos) - 5} eventos más")

    elif estado == "no_disponible":
        print(
            f"\n⚠️  FUENTE NO DISPONIBLE: no se pudo acceder a Visit Valencia.\n"
            f"   Estado documentado en → {output_path.name}"
        )
    elif estado == "sin_eventos":
        print(
            f"\n⚠️  SIN EVENTOS: la página se descargó pero no se encontraron eventos.\n"
            f"   Puede que no haya eventos en el rango de fechas configurado.\n"
            f"   Estado documentado en → {output_path.name}"
        )
    else:
        print(f"\n⚠️  CAPTURA PARCIAL: estado={estado} → {output_path.name}")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
