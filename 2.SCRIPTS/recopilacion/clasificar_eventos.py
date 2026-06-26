# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 4.4: Clasificación de Eventos
==============================================================================

Descripción:
    Unifica los tres JSON de eventos (Visit Valencia, Ayuntamiento, Valencia CF)
    en un único archivo clasificado. Para cada evento se añaden dos campos
    heurísticos:
        - tipo:             "puntual" | "dilatado"
        - impacto_esperado: "alto" | "medio" | "bajo"

    Estas etiquetas permiten en fases posteriores correlacionar picos de
    contaminación y tráfico con la naturaleza e intensidad de cada evento.

    NO se normalizan fechas ni se eliminan eventos.
    NO se usa pandas: solo json + listas/dicts.

Archivos de entrada (ya existentes):
    1.DATOS_EN_CRUDO/eventos/visitvalencia.json
    1.DATOS_EN_CRUDO/eventos/ayuntamiento.json
    1.DATOS_EN_CRUDO/eventos/valenciacf.json

Archivo de salida:
    1.DATOS_EN_CRUDO/eventos/eventos_clasificados.json

Ruta esperada del script:
    2.SCRIPTS/procesamiento/clasificar_eventos.py

Uso:
    python clasificar_eventos.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EVENTOS_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "eventos"
LOG_DIR = PROJECT_ROOT / "logs"

# Archivos de entrada
INPUT_FILES = {
    "visitvalencia": EVENTOS_DIR / "visitvalencia.json",
    "ayuntamiento": EVENTOS_DIR / "ayuntamiento.json",
    "valenciacf": EVENTOS_DIR / "valenciacf.json",
}

# Archivo de salida
OUTPUT_FILE = EVENTOS_DIR / "eventos_clasificados.json"

# --- Palabras clave para heurísticas de clasificación ---

# Impacto ALTO: eventos masivos que generan grandes desplazamientos
KEYWORDS_IMPACTO_ALTO = [
    "fallas",
    "falla",
    "ninot",
    "mascletà",
    "mascletá",
    "cremà",
    "cremá",
    "ofrenda",
    "plantà",
    "plantá",
    "nit del foc",
    "roig arena",
    "concierto",
    "concert",
    "festival",
    "maratón",
    "maraton",
    "marathon",
    "gran fira",
    "cabalgata",
    "desfile",
    "procesión",
    "procesion",
    "feria",
    "año nuevo",
    "nochevieja",
    "fin de año",
]

# Impacto BAJO: eventos pequeños o de nicho
KEYWORDS_IMPACTO_BAJO = [
    "charla",
    "conferencia",
    "taller",
    "workshop",
    "seminario",
    "coloquio",
    "mesa redonda",
    "debate",
    "presentación libro",
    "presentacion libro",
    "lectura",
    "recital poético",
    "recital poetico",
    "visita guiada",
    "ruta",
    "jornada técnica",
    "jornada tecnica",
    "reunión",
    "reunion",
    "asamblea",
]

# Categorías del Ayuntamiento que indican impacto medio
CATEGORIAS_IMPACTO_MEDIO = [
    "exposiciones",
    "exposición",
    "exposicion",
    "música",
    "musica",
    "teatro",
    "danza",
    "cine",
    "cinema",
    "artes escénicas",
    "artes escenicas",
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================


def setup_logging() -> logging.Logger:
    """
    Configura logging dual: consola (INFO) + archivo (DEBUG).

    Returns:
        logging.Logger configurado
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "clasificar_eventos.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Clasificar_Eventos")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Handler archivo (DEBUG completo)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    # Handler consola (INFO resumen)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# FUNCIONES DE CARGA
# ==============================================================================


def load_json_file(
    filepath: Path,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    Carga un archivo JSON con manejo robusto de errores.

    Args:
        filepath: Ruta al archivo JSON
        logger: Logger para registrar eventos

    Returns:
        Diccionario con el contenido del JSON, o None si falla
    """
    if not filepath.exists():
        logger.warning(f"Archivo no encontrado: {filepath.name}")
        logger.debug(f"  Ruta completa: {filepath}")
        return None

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        file_size = filepath.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )
        logger.info(f"  ✔ Cargado: {filepath.name} ({size_str})")
        return data

    except json.JSONDecodeError as e:
        logger.error(f"  ✗ JSON inválido en {filepath.name}: {e}")
        return None

    except Exception as e:
        logger.error(f"  ✗ Error leyendo {filepath.name}: {e}")
        return None


def extract_events_from_source(
    data: Dict[str, Any],
    fuente: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Extrae la lista de eventos/partidos de un JSON cargado.

    Cada fuente tiene una estructura diferente:
    - visitvalencia: clave "eventos" → lista de dicts
    - ayuntamiento:  clave "eventos" → lista de dicts
    - valenciacf:    clave "partidos" → lista de dicts

    Se añade el campo "fuente" a cada evento para trazabilidad.

    Args:
        data: Diccionario cargado del JSON
        fuente: Identificador de la fuente ("visitvalencia", etc.)
        logger: Logger

    Returns:
        Lista de eventos con campo "fuente" añadido
    """
    # Valencia CF usa "partidos", las demás usan "eventos"
    if fuente == "valenciacf":
        raw_list = data.get("partidos", None)
    else:
        raw_list = data.get("eventos", None)

    if raw_list is None:
        logger.warning(f"  ⚠ '{fuente}': no se encontró la lista de eventos/partidos")
        return []

    if not isinstance(raw_list, list):
        logger.warning(f"  ⚠ '{fuente}': el campo de eventos no es una lista")
        return []

    # Añadir fuente a cada evento (sin modificar el original)
    eventos = []
    for evento in raw_list:
        evento_con_fuente = dict(evento)  # Copia superficial
        evento_con_fuente["fuente"] = fuente
        eventos.append(evento_con_fuente)

    logger.info(f"  → {fuente}: {len(eventos)} eventos extraídos")
    return eventos


# ==============================================================================
# FUNCIONES DE CLASIFICACIÓN (HEURÍSTICAS)
# ==============================================================================


def _text_contains_any(text: str, keywords: List[str]) -> bool:
    """
    Comprueba si un texto contiene alguna de las palabras clave.

    Comparación case-insensitive.

    Args:
        text: Texto a analizar
        keywords: Lista de palabras clave

    Returns:
        True si alguna keyword está contenida en el texto
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def _is_fallas_event(evento: Dict[str, Any]) -> bool:
    """
    Detecta si un evento está relacionado con las Fallas.

    Comprueba:
    - Palabras clave en nombre/categoría/ubicación
    - Fechas en marzo (mes típico de Fallas)

    Args:
        evento: Diccionario del evento

    Returns:
        True si parece un evento de Fallas
    """
    fallas_keywords = [
        "fallas",
        "falla",
        "ninot",
        "mascletà",
        "mascletá",
        "cremà",
        "cremá",
        "ofrenda",
        "plantà",
        "plantá",
        "nit del foc",
    ]

    # Buscar en campos de texto
    campos_texto = [
        evento.get("nombre", ""),
        evento.get("categoria", ""),
        evento.get("ubicacion", ""),
        evento.get("summary_raw", ""),
    ]

    for campo in campos_texto:
        if _text_contains_any(campo, fallas_keywords):
            return True

    # Comprobar si las fechas caen en marzo
    for campo_fecha in ["fecha_inicio", "fecha"]:
        fecha_str = evento.get(campo_fecha, "")
        if "/03/" in fecha_str or "-03-" in fecha_str:
            # Podría ser Fallas (1-19 marzo), pero no es suficiente solo
            # Si el nombre también sugiere algo festivo, se marca
            nombre = evento.get("nombre", "").lower()
            if any(w in nombre for w in ["valencia", "fest", "fire", "fuego"]):
                return True

    return False


def _is_large_venue_event(evento: Dict[str, Any]) -> bool:
    """
    Detecta si un evento se celebra en un recinto de gran aforo.

    Recintos conocidos en Valencia con aforo masivo:
    - Roig Arena (~18.600)
    - Mestalla (~55.000)
    - Ciudad de las Artes y las Ciencias
    - Jardín del Turia (eventos al aire libre masivos)

    Args:
        evento: Diccionario del evento

    Returns:
        True si el evento es en un gran recinto
    """
    grandes_recintos = [
        "roig arena",
        "mestalla",
        "ciudad de las artes",
        "ciutat de les arts",
        "palau de la música",
        "palau de la musica",
        "plaza de toros",
        "plaça de bous",
    ]

    campos_texto = [
        evento.get("nombre", ""),
        evento.get("ubicacion", ""),
        evento.get("estadio", ""),
    ]

    for campo in campos_texto:
        if _text_contains_any(campo, grandes_recintos):
            return True

    return False


def _determine_tipo(evento: Dict[str, Any]) -> str:
    """
    Determina si un evento es "puntual" o "dilatado".

    Reglas:
    - Valencia CF → siempre "puntual" (un partido = unas horas)
    - fecha_inicio == fecha_fin → "puntual"
    - fecha_inicio != fecha_fin (rango > 1 día) → "dilatado"
    - Sin fechas claras → "puntual" por defecto

    Args:
        evento: Diccionario del evento

    Returns:
        "puntual" o "dilatado"
    """
    fuente = evento.get("fuente", "")

    # --- Valencia CF: siempre puntual ---
    if fuente == "valenciacf":
        return "puntual"

    # --- Comparar fecha_inicio vs fecha_fin ---
    fecha_inicio = evento.get("fecha_inicio", "").strip()
    fecha_fin = evento.get("fecha_fin", "").strip()

    # Si no hay fechas, asumir puntual
    if not fecha_inicio:
        return "puntual"

    # Si solo hay inicio o son iguales → puntual
    if not fecha_fin or fecha_inicio == fecha_fin:
        return "puntual"

    # Si son diferentes → dilatado (rango de varios días)
    return "dilatado"


def _determine_impacto(evento: Dict[str, Any]) -> str:
    """
    Determina el impacto esperado de un evento: "alto", "medio" o "bajo".

    Reglas (por prioridad):
    1. Valencia CF → ALTO (55.000 espectadores, cortes tráfico)
    2. Fallas → ALTO (evento masivo por excelencia)
    3. Grandes recintos (Roig Arena, etc.) → ALTO
    4. Keywords de impacto alto (conciertos, maratón...) → ALTO
    5. Keywords de impacto bajo (charlas, talleres...) → BAJO
    6. Categorías culturales del Ayuntamiento → MEDIO
    7. Evento dilatado (exposición larga) → MEDIO
    8. Default → MEDIO

    Args:
        evento: Diccionario del evento

    Returns:
        "alto", "medio" o "bajo"
    """
    fuente = evento.get("fuente", "")

    # --- Regla 1: Valencia CF siempre alto ---
    if fuente == "valenciacf":
        return "alto"

    # --- Construir texto combinado para búsqueda de keywords ---
    nombre = evento.get("nombre", "")
    categoria = evento.get("categoria", "")
    ubicacion = evento.get("ubicacion", "")
    summary = evento.get("summary_raw", "")
    texto_combinado = f"{nombre} {categoria} {ubicacion} {summary}"

    # --- Regla 2: Fallas → alto ---
    if _is_fallas_event(evento):
        return "alto"

    # --- Regla 3: Grandes recintos → alto ---
    if _is_large_venue_event(evento):
        return "alto"

    # --- Regla 4: Keywords de impacto alto → alto ---
    if _text_contains_any(texto_combinado, KEYWORDS_IMPACTO_ALTO):
        return "alto"

    # --- Regla 5: Keywords de impacto bajo → bajo ---
    if _text_contains_any(texto_combinado, KEYWORDS_IMPACTO_BAJO):
        return "bajo"

    # --- Regla 6: Categorías culturales del Ayuntamiento → medio ---
    if categoria and _text_contains_any(categoria, CATEGORIAS_IMPACTO_MEDIO):
        return "medio"

    # --- Regla 7: Exposiciones largas (dilatado) → medio ---
    # (se evalúa después de keywords para no sobreescribir alto/bajo)
    tipo = evento.get("tipo", "")
    if tipo == "dilatado":
        return "medio"

    # --- Default → medio ---
    return "medio"


def classify_event(
    evento: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Aplica clasificación heurística a un evento individual.

    Añade los campos "tipo" e "impacto_esperado" al evento.
    No modifica ni elimina ningún campo original.

    Args:
        evento: Diccionario del evento (ya con campo "fuente")
        logger: Logger

    Returns:
        Evento con campos de clasificación añadidos
    """
    # Primero asignar tipo (se usa luego para determinar impacto)
    tipo = _determine_tipo(evento)
    evento["tipo"] = tipo

    # Después asignar impacto (puede depender del tipo)
    impacto = _determine_impacto(evento)
    evento["impacto_esperado"] = impacto

    # Log de debug con resumen
    nombre = evento.get("nombre", evento.get("rival", "?"))[:50]
    logger.debug(
        f"    [{evento.get('fuente', '?')}] "
        f"{nombre} → tipo={tipo}, impacto={impacto}"
    )

    return evento


# ==============================================================================
# FUNCIÓN DE CLASIFICACIÓN MASIVA
# ==============================================================================


def classify_all_events(
    eventos: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Clasifica todos los eventos de la lista unificada.

    Args:
        eventos: Lista de eventos (ya con campo "fuente")
        logger: Logger

    Returns:
        Misma lista con campos de clasificación añadidos
    """
    logger.info(f"Clasificando {len(eventos)} eventos...")

    for evento in eventos:
        classify_event(evento, logger)

    # Resumen de clasificación
    conteo_tipo = {"puntual": 0, "dilatado": 0}
    conteo_impacto = {"alto": 0, "medio": 0, "bajo": 0}

    for evento in eventos:
        tipo = evento.get("tipo", "desconocido")
        impacto = evento.get("impacto_esperado", "desconocido")
        conteo_tipo[tipo] = conteo_tipo.get(tipo, 0) + 1
        conteo_impacto[impacto] = conteo_impacto.get(impacto, 0) + 1

    logger.info(f"  Tipo:    {conteo_tipo}")
    logger.info(f"  Impacto: {conteo_impacto}")

    return eventos


# ==============================================================================
# FUNCIÓN DE GUARDADO
# ==============================================================================


def build_output(
    eventos: List[Dict[str, Any]],
    eventos_por_fuente: Dict[str, int],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Construye el JSON de salida con metadatos del proceso.

    Args:
        eventos: Lista de eventos clasificados
        eventos_por_fuente: Conteo de eventos por fuente
        logger: Logger

    Returns:
        Diccionario listo para serializar a JSON
    """
    # Conteos de clasificación para metadata
    conteo_tipo = {}
    conteo_impacto = {}
    for evento in eventos:
        tipo = evento.get("tipo", "desconocido")
        impacto = evento.get("impacto_esperado", "desconocido")
        conteo_tipo[tipo] = conteo_tipo.get(tipo, 0) + 1
        conteo_impacto[impacto] = conteo_impacto.get(impacto, 0) + 1

    output = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "4.4 - Clasificación de eventos",
            "timestamp_captura": datetime.now().isoformat(),
            "timestamp_utc": (
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            ),
            "descripcion": (
                "Eventos unificados de 3 fuentes con clasificación "
                "heurística de tipo (puntual/dilatado) e impacto "
                "(alto/medio/bajo) para correlación con contaminación "
                "y tráfico."
            ),
            "total_eventos": len(eventos),
            "eventos_por_fuente": eventos_por_fuente,
            "clasificacion": {
                "por_tipo": conteo_tipo,
                "por_impacto": conteo_impacto,
            },
            "archivos_entrada": [
                "visitvalencia.json",
                "ayuntamiento.json",
                "valenciacf.json",
            ],
            "archivo_salida": "eventos_clasificados.json",
            "reglas_clasificacion": {
                "tipo": {
                    "puntual": "Evento de un solo día o unas horas (partidos, conciertos)",
                    "dilatado": "Evento que abarca varios días (exposiciones, ferias)",
                },
                "impacto_esperado": {
                    "alto": "Valencia CF, Fallas, grandes recintos, conciertos masivos",
                    "medio": "Eventos culturales, exposiciones, festivales medianos",
                    "bajo": "Charlas, conferencias, talleres, visitas guiadas",
                },
            },
        },
        "eventos": eventos,
    }

    return output


def save_output(
    data: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Guarda el JSON clasificado en disco.

    Args:
        data: Diccionario con eventos clasificados y metadatos
        logger: Logger

    Returns:
        Path al archivo guardado, o None si falla
    """
    EVENTOS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        file_size = OUTPUT_FILE.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )
        logger.info(f"✔ Archivo guardado: {OUTPUT_FILE.name} ({size_str})")
        logger.debug(f"  Ruta completa: {OUTPUT_FILE}")
        return OUTPUT_FILE

    except Exception as e:
        logger.error(f"✗ Error guardando {OUTPUT_FILE.name}: {e}")
        return None


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================


def main():
    """
    Función principal: carga → unifica → clasifica → guarda.

    Flujo:
    1. Carga los 3 JSON de entrada
    2. Extrae y unifica eventos en una sola lista
    3. Clasifica cada evento (tipo + impacto)
    4. Genera JSON de salida con metadatos
    5. Muestra resumen en consola
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("FASE 4.4: CLASIFICACIÓN DE EVENTOS")
    logger.info("=" * 70)

    # ── Paso 1: Cargar archivos de entrada ──
    logger.info("")
    logger.info("Paso 1: Cargando archivos de entrada...")

    todos_los_eventos = []
    eventos_por_fuente = {}
    fuentes_ok = 0
    fuentes_fail = 0

    for fuente, filepath in INPUT_FILES.items():
        data = load_json_file(filepath, logger)

        if data is None:
            eventos_por_fuente[fuente] = 0
            fuentes_fail += 1
            continue

        eventos = extract_events_from_source(data, fuente, logger)
        eventos_por_fuente[fuente] = len(eventos)
        todos_los_eventos.extend(eventos)
        fuentes_ok += 1

    logger.info(
        f"  Fuentes cargadas: {fuentes_ok}/{len(INPUT_FILES)} "
        f"({fuentes_fail} no disponibles)"
    )
    logger.info(f"  Total eventos unificados: {len(todos_los_eventos)}")

    # Comprobar que hay al menos algo para clasificar
    if len(todos_los_eventos) == 0:
        logger.error("No se encontraron eventos en ninguna fuente.")
        logger.error("Verifica que los JSON de entrada existen y tienen datos.")
        print("\n✗ ERROR: No hay eventos para clasificar.")
        print("  Ejecuta primero los scripts de captura:")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_visitvalencia.py")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_ayuntamiento.py")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_valenciacf.py")
        return

    # ── Paso 2: Clasificar eventos ──
    logger.info("")
    logger.info("Paso 2: Clasificando eventos...")
    classify_all_events(todos_los_eventos, logger)

    # ── Paso 3: Construir y guardar salida ──
    logger.info("")
    logger.info("Paso 3: Guardando resultado...")
    output_data = build_output(todos_los_eventos, eventos_por_fuente, logger)
    output_path = save_output(output_data, logger)

    if output_path is None:
        print("\n✗ ERROR: No se pudo guardar el archivo de salida.")
        return

    # ── Resumen final ──
    meta = output_data["_metadata"]
    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CLASIFICACIÓN")
    logger.info("-" * 70)
    logger.info(f"  Total eventos:     {meta['total_eventos']}")
    logger.info(f"  Por fuente:        {meta['eventos_por_fuente']}")
    logger.info(f"  Por tipo:          {meta['clasificacion']['por_tipo']}")
    logger.info(f"  Por impacto:       {meta['clasificacion']['por_impacto']}")
    logger.info(f"  Archivo:           {output_path.name}")
    logger.info(f"  Ubicación:         {EVENTOS_DIR}")
    logger.info(f"  Timestamp:         {meta['timestamp_captura']}")
    logger.info("")

    # Mensaje en consola
    print(f"\n✅ CLASIFICACIÓN COMPLETA: {meta['total_eventos']} eventos")
    print(f"   → {output_path.name}")
    print("\n📊 Desglose:")
    for fuente, count in meta["eventos_por_fuente"].items():
        print(f"   {fuente}: {count} eventos")
    print("\n📋 Tipo:")
    for tipo, count in meta["clasificacion"]["por_tipo"].items():
        print(f"   {tipo}: {count}")
    print("\n⚡ Impacto esperado:")
    for impacto, count in meta["clasificacion"]["por_impacto"].items():
        print(f"   {impacto}: {count}")

    # Preview de los primeros 5 eventos clasificados
    print("\n🔍 Preview (primeros 5 eventos):")
    print("-" * 60)
    for ev in todos_los_eventos[:5]:
        nombre = ev.get("nombre", ev.get("rival", "?"))[:45]
        fuente = ev.get("fuente", "?")
        tipo = ev.get("tipo", "?")
        impacto = ev.get("impacto_esperado", "?")
        print(f"  [{fuente}] {nombre}")
        print(f"    tipo={tipo}  impacto={impacto}")
    if len(todos_los_eventos) > 5:
        print(f"  ... y {len(todos_los_eventos) - 5} eventos más")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
