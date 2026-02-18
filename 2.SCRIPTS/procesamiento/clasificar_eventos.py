# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 4.4: Clasificacion de Eventos - Taxonomia Multidimensional (v2)
==============================================================================

Descripcion:
    Unifica los tres JSON de eventos (Visit Valencia, Ayuntamiento, Valencia CF)
    en un unico archivo clasificado con taxonomia multidimensional.

    Para cada evento se asignan heuristicamente 6 campos:

    ┌──────────────────────┬────────────────────────────────────────────┐
    │ Campo                │ Valores posibles                          │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ categoria_evento     │ festivo, deportivo, musical, cultural,    │
    │                      │ institucional, comercial, religioso, otro │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ subcategoria_evento  │ fallas, partido_futbol, concierto_masivo, │
    │                      │ maraton, exposicion, teatro, procesion... │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ duracion_tipo        │ puntual, multi_dia, temporada             │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ impacto_esperado     │ bajo, medio, alto, muy_alto               │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ impacto_score        │ 1, 2, 3, 4                                │
    ├──────────────────────┼────────────────────────────────────────────┤
    │ tipo (COMPAT)        │ = categoria_evento (backward compat)      │
    └──────────────────────┴────────────────────────────────────────────┘

    Compatibilidad hacia atras:
      - campo "tipo" = categoria_evento
        (correlacion_eventos.py lee evento.get("tipo") -> tipo_evento)
      - campo "impacto_esperado" se mantiene (ahora con "muy_alto" extra)

Archivos de entrada:
    1.DATOS_EN_CRUDO/eventos/visitvalencia.json
    1.DATOS_EN_CRUDO/eventos/ayuntamiento.json
    1.DATOS_EN_CRUDO/eventos/valenciacf.json

Archivo de salida:
    1.DATOS_EN_CRUDO/eventos/eventos_clasificados.json

Ruta: 2.SCRIPTS/procesamiento/clasificar_eventos.py
Uso:  python clasificar_eventos.py
Autor: Joan | Fecha: 2026
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


# ==============================================================================
# CONFIGURACION
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EVENTOS_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "eventos"
LOG_DIR = PROJECT_ROOT / "logs"

INPUT_FILES = {
    "visitvalencia": EVENTOS_DIR / "visitvalencia.json",
    "ayuntamiento":  EVENTOS_DIR / "ayuntamiento.json",
    "valenciacf":    EVENTOS_DIR / "valenciacf.json",
}

OUTPUT_FILE = EVENTOS_DIR / "eventos_clasificados.json"


# ==============================================================================
# TAXONOMIA: REGLAS DE CLASIFICACION
# ==============================================================================

# --- Keywords para detectar CATEGORIA + SUBCATEGORIA ---
# Cada tupla: (categoria, subcategoria, [keywords])
# Se evaluan en orden de prioridad (primera coincidencia gana)

REGLAS_SUBCATEGORIA = [
    # ── FESTIVO ──
    ("festivo", "fallas", [
        "fallas", "falla", "ninot", "mascletà", "mascletá",
        "cremà", "cremá", "ofrenda", "plantà", "plantá",
        "nit del foc", "despertà", "despertá",
    ]),
    ("festivo", "navidad", [
        "navidad", "nadal", "nochevieja", "nit de cap d'any",
        "reyes magos", "reis mags", "cabalgata de reyes",
        "belen", "belén", "pessebre",
    ]),
    ("festivo", "semana_santa", [
        "semana santa", "setmana santa", "procesión semana",
        "procesion semana", "pascua", "pasqua",
    ]),
    ("festivo", "ano_nuevo", [
        "año nuevo", "any nou", "fin de año", "fin de any",
        "nochevieja", "nit de cap",
    ]),
    ("festivo", "fiesta_local", [
        "9 d'octubre", "9 de octubre", "dia de la comunitat",
        "san vicente", "sant vicent", "corpus", "gran fira",
        "feria de julio", "fira de juliol", "san juan", "sant joan",
    ]),

    # ── DEPORTIVO ──
    ("deportivo", "partido_futbol", [
        "valencia cf", "valenciacf", "mestalla", "liga ",
        "laliga", "copa del rey", "champions", "europa league",
        "conference league",
    ]),
    ("deportivo", "maraton", [
        "maratón", "maraton", "marathon", "media maratón",
        "media maraton", "10k", "15k", "carrera popular",
    ]),
    ("deportivo", "ciclismo", [
        "vuelta ciclista", "volta ciclista", "ciclismo",
        "etapa ciclista",
    ]),
    ("deportivo", "otro_deporte", [
        "triatlón", "triatlon", "regata", "vela",
        "atletismo", "tenis", "open de tenis",
        "baloncesto", "basket", "balonmano", "handball",
    ]),

    # ── MUSICAL ──
    ("musical", "concierto_masivo", [
        "roig arena", "concierto", "concert", "gira ",
        "live ", "en directo", "recital musical",
    ]),
    ("musical", "festival_musica", [
        "festival", "fest ", "festivalpark",
        "les arts ", "berklee", "jazz",
    ]),
    ("musical", "opera", [
        "ópera", "opera", "palau de les arts",
        "zarzuela", "lírica", "lirica",
    ]),

    # ── CULTURAL ──
    ("cultural", "exposicion", [
        "exposición", "exposicion", "exposició", "exhibición",
        "muestra", "mostra",
    ]),
    ("cultural", "teatro", [
        "teatro", "teatre", "obra teatral", "comedia",
        "drama ", "monólogo", "monologo", "improv",
    ]),
    ("cultural", "danza", [
        "danza", "dansa", "ballet", "flamenco",
        "coreografía", "coreografia",
    ]),
    ("cultural", "cine", [
        "cine", "cinema", "película", "pelicula",
        "documental", "cortometraje", "filmoteca",
    ]),
    ("cultural", "museo", [
        "museo", "museu", "ivam", "muvim", "bombas gens",
        "centre del carme", "galería", "galeria",
    ]),
    ("cultural", "literatura", [
        "feria del libro", "fira del llibre", "presentación libro",
        "presentacion libro", "firma de libros", "lectura",
        "recital poético", "recital poetico", "poesía", "poesia",
    ]),

    # ── INSTITUCIONAL ──
    ("institucional", "conferencia", [
        "conferencia", "congreso", "simposio", "jornada técnica",
        "jornada tecnica", "seminario", "foro ",
    ]),
    ("institucional", "taller", [
        "taller", "workshop", "formación", "formacion",
        "curso ", "masterclass", "hackathon",
    ]),
    ("institucional", "acto_oficial", [
        "acto oficial", "acte oficial", "inauguración",
        "inauguracion", "homenaje", "entrega de premios",
        "pleno municipal",
    ]),

    # ── COMERCIAL ──
    ("comercial", "feria_comercial", [
        "feria", "fira", "salón del", "salon del",
        "mercado", "mercat", "mercadillo",
    ]),
    ("comercial", "gastronomia", [
        "gastronomía", "gastronomia", "tapa", "paella",
        "gastro", "showcooking", "cata de",
    ]),

    # ── RELIGIOSO ──
    ("religioso", "procesion", [
        "procesión", "procesion", "processó", "romería",
        "romeria", "peregrinación", "peregrinacion",
        "virgen de los desamparados", "mare de déu",
    ]),
    ("religioso", "misa_especial", [
        "misa solemne", "missa", "tedeum", "te deum",
    ]),
]

# --- Grandes recintos que elevan impacto ---
GRANDES_RECINTOS = [
    "roig arena", "mestalla", "ciudad de las artes",
    "ciutat de les arts", "palau de la música",
    "palau de la musica", "plaza de toros",
    "plaça de bous", "jardín del turia",
    "marina real", "oceanogràfic", "oceanografic",
]

# --- Mapeo impacto_esperado -> impacto_score ---
IMPACTO_A_SCORE = {
    "bajo": 1,
    "medio": 2,
    "alto": 3,
    "muy_alto": 4,
}


# ==============================================================================
# CONFIGURACION DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """Configura logging dual: consola (INFO) + archivo (DEBUG)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "clasificar_eventos.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Clasificar_Eventos")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

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
    """Carga un archivo JSON con manejo robusto de errores."""
    if not filepath.exists():
        logger.warning(f"Archivo no encontrado: {filepath.name}")
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        file_size = filepath.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )
        logger.info(f"  Cargado: {filepath.name} ({size_str})")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"  JSON invalido en {filepath.name}: {e}")
        return None
    except Exception as e:
        logger.error(f"  Error leyendo {filepath.name}: {e}")
        return None


def extract_events_from_source(
    data: Dict[str, Any],
    fuente: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Extrae la lista de eventos/partidos de un JSON cargado.
    Valencia CF usa "partidos", las demas usan "eventos".
    Anade campo "fuente" a cada evento.
    """
    if fuente == "valenciacf":
        raw_list = data.get("partidos", None)
    else:
        raw_list = data.get("eventos", None)

    if raw_list is None or not isinstance(raw_list, list):
        logger.warning(f"  '{fuente}': sin lista de eventos valida")
        return []

    eventos = []
    for evento in raw_list:
        evento_con_fuente = dict(evento)
        evento_con_fuente["fuente"] = fuente
        eventos.append(evento_con_fuente)

    logger.info(f"  -> {fuente}: {len(eventos)} eventos extraidos")
    return eventos


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def _text_contains_any(text: str, keywords: List[str]) -> bool:
    """Comprueba si un texto contiene alguna keyword (case-insensitive)."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def _get_texto_combinado(evento: Dict[str, Any]) -> str:
    """Concatena todos los campos de texto relevantes para busqueda."""
    campos = [
        evento.get("nombre", ""),
        evento.get("categoria", ""),
        evento.get("ubicacion", ""),
        evento.get("summary_raw", ""),
        evento.get("descripcion", ""),
        evento.get("rival", ""),
        evento.get("competicion", ""),
        evento.get("estadio", ""),
    ]
    return " ".join(str(c) for c in campos if c)


def _calcular_dias_evento(evento: Dict[str, Any]) -> Optional[int]:
    """
    Calcula la duracion en dias entre fecha_inicio y fecha_fin.

    Soporta formatos:
      - "DD/MM/YYYY"
      - "YYYY-MM-DD"
      - "YYYY-MM-DDTHH:MM:SS"

    Returns:
        Numero de dias, o None si no se pueden parsear las fechas.
    """
    fecha_inicio_str = evento.get("fecha_inicio", "").strip()
    fecha_fin_str = evento.get("fecha_fin", "").strip()

    if not fecha_inicio_str:
        return None

    # Si no hay fecha fin, es 1 dia
    if not fecha_fin_str or fecha_fin_str == fecha_inicio_str:
        return 1

    # Intentar parsear ambas fechas
    formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"]

    fecha_inicio = None
    fecha_fin = None

    for fmt in formatos:
        try:
            fecha_inicio = datetime.strptime(fecha_inicio_str[:len("YYYY-MM-DD")], fmt[:len(fmt)])
            break
        except ValueError:
            continue

    # Segundo intento con la fecha completa
    if fecha_inicio is None:
        for fmt in formatos:
            try:
                fecha_inicio = datetime.strptime(fecha_inicio_str, fmt)
                break
            except ValueError:
                continue

    for fmt in formatos:
        try:
            fecha_fin = datetime.strptime(fecha_fin_str[:len("YYYY-MM-DD")], fmt[:len(fmt)])
            break
        except ValueError:
            continue

    if fecha_fin is None:
        for fmt in formatos:
            try:
                fecha_fin = datetime.strptime(fecha_fin_str, fmt)
                break
            except ValueError:
                continue

    if fecha_inicio is None or fecha_fin is None:
        return None

    delta = (fecha_fin - fecha_inicio).days
    return max(1, abs(delta) + 1)  # Minimo 1 dia


# ==============================================================================
# CLASIFICACION MULTIDIMENSIONAL
# ==============================================================================

def _determine_categoria_y_subcategoria(
    evento: Dict[str, Any],
) -> tuple:
    """
    Determina categoria_evento y subcategoria_evento.

    Logica de prioridad:
      1. Valencia CF (fuente) -> deportivo / partido_futbol
      2. Busqueda por keywords en REGLAS_SUBCATEGORIA (primera coincidencia)
      3. Fallback: "otro" / "sin_clasificar"

    Returns:
        Tupla (categoria, subcategoria).
    """
    fuente = evento.get("fuente", "")

    # --- Regla 1: Valencia CF siempre deportivo/partido_futbol ---
    if fuente == "valenciacf":
        return ("deportivo", "partido_futbol")

    # --- Regla 2: Buscar por keywords ---
    texto = _get_texto_combinado(evento)

    for categoria, subcategoria, keywords in REGLAS_SUBCATEGORIA:
        if _text_contains_any(texto, keywords):
            return (categoria, subcategoria)

    # --- Fallback ---
    return ("otro", "sin_clasificar")


def _determine_duracion_tipo(evento: Dict[str, Any]) -> str:
    """
    Determina duracion_tipo: "puntual" | "multi_dia" | "temporada".

    Reglas:
      - Valencia CF -> siempre "puntual" (un partido dura horas)
      - 1 dia       -> "puntual"
      - 2-7 dias    -> "multi_dia"
      - >7 dias     -> "temporada"
      - Sin fechas  -> "puntual" (default seguro)
    """
    fuente = evento.get("fuente", "")
    if fuente == "valenciacf":
        return "puntual"

    n_dias = _calcular_dias_evento(evento)

    if n_dias is None or n_dias <= 1:
        return "puntual"
    elif n_dias <= 7:
        return "multi_dia"
    else:
        return "temporada"


def _determine_impacto(
    evento: Dict[str, Any],
    categoria: str,
    subcategoria: str,
) -> str:
    """
    Determina impacto_esperado: "bajo" | "medio" | "alto" | "muy_alto".

    Reglas por prioridad:
      1. Fallas                       -> muy_alto (4)
      2. Valencia CF                   -> alto (3)
      3. Gran recinto                  -> alto (3)
      4. Concierto masivo / festival   -> alto (3)
      5. Maraton / evento deportivo    -> alto (3)
      6. Navidad / Semana Santa        -> alto (3)
      7. Procesion / cabalgata         -> medio (2)
      8. Exposicion / teatro / cine    -> medio (2)
      9. Conferencia / taller          -> bajo (1)
      10. Default                      -> medio (2)
    """
    # Regla 1: Fallas -> muy_alto
    if subcategoria == "fallas":
        return "muy_alto"

    # Regla 2: Valencia CF -> alto
    if subcategoria == "partido_futbol":
        return "alto"

    # Regla 3: Gran recinto -> alto
    texto = _get_texto_combinado(evento)
    if _text_contains_any(texto, GRANDES_RECINTOS):
        return "alto"

    # Reglas por subcategoria
    MAPA_SUBCATEGORIA_IMPACTO = {
        # muy_alto
        "fallas": "muy_alto",
        # alto
        "partido_futbol": "alto",
        "concierto_masivo": "alto",
        "festival_musica": "alto",
        "maraton": "alto",
        "navidad": "alto",
        "ano_nuevo": "alto",
        "semana_santa": "alto",
        "fiesta_local": "alto",
        # medio
        "exposicion": "medio",
        "teatro": "medio",
        "danza": "medio",
        "cine": "medio",
        "museo": "medio",
        "opera": "medio",
        "procesion": "medio",
        "feria_comercial": "medio",
        "gastronomia": "medio",
        "literatura": "medio",
        "otro_deporte": "medio",
        "ciclismo": "medio",
        "misa_especial": "medio",
        "acto_oficial": "medio",
        # bajo
        "conferencia": "bajo",
        "taller": "bajo",
    }

    impacto = MAPA_SUBCATEGORIA_IMPACTO.get(subcategoria, None)
    if impacto:
        return impacto

    # Default por categoria
    MAPA_CATEGORIA_IMPACTO = {
        "festivo": "alto",
        "deportivo": "alto",
        "musical": "alto",
        "cultural": "medio",
        "comercial": "medio",
        "religioso": "medio",
        "institucional": "bajo",
        "otro": "medio",
    }

    return MAPA_CATEGORIA_IMPACTO.get(categoria, "medio")


# ==============================================================================
# FUNCION PRINCIPAL DE CLASIFICACION (por evento)
# ==============================================================================

def classify_event(
    evento: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Aplica clasificacion multidimensional a un evento individual.

    Anade 6 campos sin modificar ni eliminar ningun campo original:
      - categoria_evento      (festivo, deportivo, musical, ...)
      - subcategoria_evento   (fallas, partido_futbol, concierto_masivo, ...)
      - duracion_tipo         (puntual, multi_dia, temporada)
      - impacto_esperado      (bajo, medio, alto, muy_alto)
      - impacto_score         (1-4)
      - tipo                  (= categoria_evento, backward compat)

    Args:
        evento: Diccionario del evento (ya con campo "fuente").
        logger: Logger para debug.

    Returns:
        Evento con campos de clasificacion anadidos.
    """
    # 1. Categoria + Subcategoria
    categoria, subcategoria = _determine_categoria_y_subcategoria(evento)
    evento["categoria_evento"] = categoria
    evento["subcategoria_evento"] = subcategoria

    # 2. Duracion
    duracion = _determine_duracion_tipo(evento)
    evento["duracion_tipo"] = duracion

    # 3. Impacto
    impacto = _determine_impacto(evento, categoria, subcategoria)
    evento["impacto_esperado"] = impacto
    evento["impacto_score"] = IMPACTO_A_SCORE.get(impacto, 2)

    # 4. Backward compat: tipo = categoria_evento
    #    correlacion_eventos.py lee evento.get("tipo") -> tipo_evento
    evento["tipo"] = categoria

    # Log de debug
    nombre = evento.get("nombre", evento.get("rival", "?"))[:50]
    logger.debug(
        f"    [{evento.get('fuente', '?')}] {nombre} -> "
        f"cat={categoria}, sub={subcategoria}, "
        f"dur={duracion}, imp={impacto}({evento['impacto_score']})"
    )

    return evento


# ==============================================================================
# CLASIFICACION MASIVA
# ==============================================================================

def classify_all_events(
    eventos: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Clasifica todos los eventos y muestra resumen."""
    logger.info(f"Clasificando {len(eventos)} eventos...")

    for evento in eventos:
        classify_event(evento, logger)

    # Resumen
    conteos = {
        "por_categoria": {},
        "por_subcategoria": {},
        "por_duracion": {},
        "por_impacto": {},
    }

    for ev in eventos:
        cat = ev.get("categoria_evento", "?")
        sub = ev.get("subcategoria_evento", "?")
        dur = ev.get("duracion_tipo", "?")
        imp = ev.get("impacto_esperado", "?")

        conteos["por_categoria"][cat] = conteos["por_categoria"].get(cat, 0) + 1
        conteos["por_subcategoria"][sub] = conteos["por_subcategoria"].get(sub, 0) + 1
        conteos["por_duracion"][dur] = conteos["por_duracion"].get(dur, 0) + 1
        conteos["por_impacto"][imp] = conteos["por_impacto"].get(imp, 0) + 1

    logger.info(f"  Categoria:    {conteos['por_categoria']}")
    logger.info(f"  Subcategoria: {conteos['por_subcategoria']}")
    logger.info(f"  Duracion:     {conteos['por_duracion']}")
    logger.info(f"  Impacto:      {conteos['por_impacto']}")

    return eventos


# ==============================================================================
# CONSTRUCCION DEL OUTPUT
# ==============================================================================

def build_output(
    eventos: List[Dict[str, Any]],
    eventos_por_fuente: Dict[str, int],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Construye el JSON de salida con metadatos del proceso."""
    conteos = {
        "por_categoria": {},
        "por_subcategoria": {},
        "por_duracion": {},
        "por_impacto": {},
    }
    for ev in eventos:
        for campo, clave in [
            ("categoria_evento", "por_categoria"),
            ("subcategoria_evento", "por_subcategoria"),
            ("duracion_tipo", "por_duracion"),
            ("impacto_esperado", "por_impacto"),
        ]:
            val = ev.get(campo, "desconocido")
            conteos[clave][val] = conteos[clave].get(val, 0) + 1

    output = {
        "_metadata": {
            "proyecto": "Data Detective Valencia",
            "fase": "4.4 - Clasificacion de eventos (taxonomia multidimensional v2)",
            "timestamp_captura": datetime.now().isoformat(),
            "timestamp_utc": (
                datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            ),
            "descripcion": (
                "Eventos unificados de 3 fuentes con clasificacion "
                "multidimensional heuristica: categoria, subcategoria, "
                "duracion, impacto esperado y score numerico."
            ),
            "total_eventos": len(eventos),
            "eventos_por_fuente": eventos_por_fuente,
            "clasificacion": conteos,
            "archivos_entrada": [
                "visitvalencia.json",
                "ayuntamiento.json",
                "valenciacf.json",
            ],
            "archivo_salida": "eventos_clasificados.json",
            "taxonomia": {
                "categoria_evento": {
                    "festivo": "Fiestas populares y festividades (Fallas, Navidad, etc.)",
                    "deportivo": "Eventos deportivos (Valencia CF, maratones, etc.)",
                    "musical": "Conciertos, festivales y opera",
                    "cultural": "Exposiciones, teatro, cine, museos, literatura",
                    "institucional": "Conferencias, talleres, actos oficiales",
                    "comercial": "Ferias, mercados, gastronomia",
                    "religioso": "Procesiones, romerias, actos liturgicos",
                    "otro": "Eventos no clasificados en categorias anteriores",
                },
                "duracion_tipo": {
                    "puntual": "Evento de un solo dia o unas horas",
                    "multi_dia": "Evento de 2 a 7 dias",
                    "temporada": "Evento de mas de 7 dias",
                },
                "impacto_esperado": {
                    "muy_alto": "Fallas (score 4)",
                    "alto": "Valencia CF, conciertos masivos, festivales (score 3)",
                    "medio": "Eventos culturales, exposiciones, ferias (score 2)",
                    "bajo": "Charlas, talleres, conferencias (score 1)",
                },
                "backward_compat": (
                    "campo 'tipo' = categoria_evento para compatibilidad "
                    "con correlacion_eventos.py (tipo_evento en CSV)"
                ),
            },
        },
        "eventos": eventos,
    }

    return output


def save_output(
    data: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Path]:
    """Guarda el JSON clasificado en disco."""
    EVENTOS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        file_size = OUTPUT_FILE.stat().st_size
        size_str = (
            f"{file_size / 1024:.1f} KB" if file_size >= 1024 else f"{file_size} B"
        )
        logger.info(f"Archivo guardado: {OUTPUT_FILE.name} ({size_str})")
        return OUTPUT_FILE
    except Exception as e:
        logger.error(f"Error guardando {OUTPUT_FILE.name}: {e}")
        return None


# ==============================================================================
# FUNCION PRINCIPAL
# ==============================================================================

def main():
    """Flujo: carga -> unifica -> clasifica multidimensional -> guarda."""
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("FASE 4.4: CLASIFICACION DE EVENTOS (TAXONOMIA v2)")
    logger.info("=" * 70)

    # -- Paso 1: Cargar --
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

    if not todos_los_eventos:
        logger.error("No se encontraron eventos en ninguna fuente.")
        print("\nERROR: No hay eventos para clasificar.")
        print("  Ejecuta primero los scripts de captura:")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_visitvalencia.py")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_ayuntamiento.py")
        print("    py 2.SCRIPTS\\recopilacion\\eventos_valenciacf.py")
        return

    # -- Paso 2: Clasificar --
    logger.info("")
    logger.info("Paso 2: Clasificando eventos (taxonomia multidimensional)...")
    classify_all_events(todos_los_eventos, logger)

    # -- Paso 3: Guardar --
    logger.info("")
    logger.info("Paso 3: Guardando resultado...")
    output_data = build_output(todos_los_eventos, eventos_por_fuente, logger)
    output_path = save_output(output_data, logger)

    if output_path is None:
        print("\nERROR: No se pudo guardar el archivo de salida.")
        return

    # -- Resumen --
    meta = output_data["_metadata"]
    clf = meta["clasificacion"]

    logger.info("")
    logger.info("-" * 70)
    logger.info("RESUMEN DE CLASIFICACION")
    logger.info("-" * 70)
    logger.info(f"  Total eventos:     {meta['total_eventos']}")
    logger.info(f"  Por fuente:        {meta['eventos_por_fuente']}")
    logger.info(f"  Por categoria:     {clf['por_categoria']}")
    logger.info(f"  Por subcategoria:  {clf['por_subcategoria']}")
    logger.info(f"  Por duracion:      {clf['por_duracion']}")
    logger.info(f"  Por impacto:       {clf['por_impacto']}")
    logger.info(f"  Archivo:           {output_path.name}")

    print(f"\nCLASIFICACION COMPLETA: {meta['total_eventos']} eventos")
    print(f"   -> {output_path.name}")

    print(f"\nCategoria:")
    for cat, count in sorted(clf["por_categoria"].items(), key=lambda x: -x[1]):
        print(f"   {cat:>15}: {count}")

    print(f"\nSubcategoria:")
    for sub, count in sorted(clf["por_subcategoria"].items(), key=lambda x: -x[1]):
        print(f"   {sub:>20}: {count}")

    print(f"\nDuracion:")
    for dur, count in sorted(clf["por_duracion"].items(), key=lambda x: -x[1]):
        print(f"   {dur:>12}: {count}")

    print(f"\nImpacto:")
    for imp, count in sorted(clf["por_impacto"].items(), key=lambda x: -x[1]):
        print(f"   {imp:>10}: {count} (score {IMPACTO_A_SCORE.get(imp, '?')})")

    # Preview
    print(f"\nPreview (primeros 5):")
    print("-" * 70)
    for ev in todos_los_eventos[:5]:
        nombre = ev.get("nombre", ev.get("rival", "?"))[:42]
        cat = ev.get("categoria_evento", "?")
        sub = ev.get("subcategoria_evento", "?")
        dur = ev.get("duracion_tipo", "?")
        imp = ev.get("impacto_esperado", "?")
        score = ev.get("impacto_score", "?")
        print(f"  [{ev.get('fuente', '?')}] {nombre}")
        print(f"    {cat}/{sub} | {dur} | {imp} (score={score})")
    if len(todos_los_eventos) > 5:
        print(f"  ... y {len(todos_los_eventos) - 5} eventos mas")


if __name__ == "__main__":
    main()
