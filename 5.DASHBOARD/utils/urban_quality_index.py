# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utils: Indice de Calidad Urbana (0-10)
==============================================================================
Ruta: 5.DASHBOARD/utils/urban_quality_index.py
Autor: Joan | Fecha: 2026

Calcula un indice sintetico de calidad urbana que fusiona tres ejes:
contaminacion (50%), meteorologia (25%) y trafico (25%).

Escala: 0 (pesimo) → 10 (excelente)

Cuando faltan datos de algun eje, los pesos se redistribuyen
proporcionalmente entre los ejes disponibles.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ============================================================================
# PESOS POR EJE
# ============================================================================

PESO_CONTAMINACION: float = 0.50
PESO_METEOROLOGIA: float = 0.25
PESO_TRAFICO: float = 0.25

# ============================================================================
# NIVELES DEL INDICE URBANO
# ============================================================================

_NIVELES_URBANO: List[Tuple[float, str, str, str]] = [
    (8.5, "Excelente",  "#2ca02c", "🟢"),
    (7.0, "Bueno",      "#27ae60", "🟢"),
    (5.0, "Aceptable",  "#f39c12", "🟡"),
    (3.0, "Deficiente", "#e67e22", "🟠"),
    (1.0, "Malo",       "#e74c3c", "🔴"),
    (0.0, "Crítico",    "#8b0000", "⛔"),
]


def _nivel_desde_score_urbano(score: float) -> Tuple[str, str, str]:
    """
    Convierte un score numerico en nivel, color y emoji del indice urbano.

    Args:
        score: Valor entre 0.0 y 10.0.

    Returns:
        Tupla (nivel, color_hex, emoji).
    """
    for umbral, nivel, color, emoji in _NIVELES_URBANO:
        if score >= umbral:
            return nivel, color, emoji
    return "Crítico", "#8b0000", "⛔"


# ============================================================================
# SCORE DE CONTAMINACION (delega en quality_index.py)
# ============================================================================

def _score_contaminacion(contam_rt: Optional[dict]) -> Optional[Dict[str, Any]]:
    """
    Calcula el score de contaminacion (0-10) a partir de datos RT de AQICN.

    Reutiliza la logica de quality_index.py convirtiendo los datos RT a un
    DataFrame compatible con calcular_indice_calidad().

    Args:
        contam_rt: Diccionario RT con clave 'estaciones' (lista de dicts
            con no2, o3, pm10, pm25, so2, co).

    Returns:
        Diccionario con 'score' (float 0-10) y 'detalle' (dict del indice),
        o None si no hay datos.
    """
    if contam_rt is None:
        return None

    estaciones = contam_rt.get("estaciones", [])
    if not estaciones:
        return None

    try:
        import pandas as pd
        from utils.quality_index import calcular_indice_calidad
        from config import UMBRALES_OMS

        # Convertir datos RT a formato DataFrame para quality_index
        registros = []
        key_map = {
            "no2": "NO2", "o3": "O3", "pm10": "PM10",
            "pm25": "PM2.5", "so2": "SO2", "co": "CO",
        }
        for est in estaciones:
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

        df = pd.DataFrame(registros)
        indice = calcular_indice_calidad(df, UMBRALES_OMS)
        if indice is None:
            return None

        return {"score": indice["score"], "detalle": indice}
    except Exception as e:
        logger.error(f"[IndiceUrbano] Error calculando score contaminacion: {e}")
        return None


# ============================================================================
# SCORE DE METEOROLOGIA
# ============================================================================

def _score_temperatura(temp: float) -> float:
    """
    Score 0-10 basado en temperatura.

    10 si esta en el rango ideal 18-25°C, baja linealmente fuera.

    Args:
        temp: Temperatura en grados Celsius.

    Returns:
        Score entre 0.0 y 10.0.
    """
    if 18.0 <= temp <= 25.0:
        return 10.0
    elif temp < 18.0:
        # Baja linealmente: 0°C → 0, 18°C → 10
        return max(0.0, 10.0 * temp / 18.0)
    else:
        # Baja linealmente: 25°C → 10, 45°C → 0
        return max(0.0, 10.0 * (1.0 - (temp - 25.0) / 20.0))


def _score_humedad(humedad: float) -> float:
    """
    Score 0-10 basado en humedad relativa.

    10 si esta en el rango ideal 40-70%, baja linealmente fuera.

    Args:
        humedad: Humedad relativa en porcentaje (0-100).

    Returns:
        Score entre 0.0 y 10.0.
    """
    if 40.0 <= humedad <= 70.0:
        return 10.0
    elif humedad < 40.0:
        # 0% → 0, 40% → 10
        return max(0.0, 10.0 * humedad / 40.0)
    else:
        # 70% → 10, 100% → 0
        return max(0.0, 10.0 * (1.0 - (humedad - 70.0) / 30.0))


def _score_lluvia(rain_mm: float) -> float:
    """
    Score basado en precipitacion.

    Args:
        rain_mm: Precipitacion en mm.

    Returns:
        10 si <1mm, 5 si 1-10mm, 2 si >10mm.
    """
    if rain_mm < 1.0:
        return 10.0
    elif rain_mm <= 10.0:
        return 5.0
    else:
        return 2.0


def _score_meteorologia(meteo_rt: Optional[dict]) -> Optional[Dict[str, Any]]:
    """
    Calcula el score meteorologico (0-10) a partir de datos RT de OpenWeather.

    Combina sub-scores de temperatura, humedad y lluvia con media simple
    de los componentes disponibles.

    Args:
        meteo_rt: Diccionario RT con claves 'temp', 'humedad', y
            opcionalmente 'rain_mm' o 'lluvia_mm'.

    Returns:
        Diccionario con 'score' (float 0-10) y 'detalle' (sub-scores),
        o None si no hay datos suficientes.
    """
    if meteo_rt is None:
        return None

    sub_scores = {}

    temp = meteo_rt.get("temp")
    if temp is not None:
        sub_scores["temperatura"] = round(_score_temperatura(float(temp)), 2)

    humedad = meteo_rt.get("humedad")
    if humedad is not None:
        sub_scores["humedad"] = round(_score_humedad(float(humedad)), 2)

    # Lluvia: buscar en varias claves posibles
    rain = meteo_rt.get("rain_mm") or meteo_rt.get("lluvia_mm") or 0.0
    sub_scores["lluvia"] = round(_score_lluvia(float(rain)), 2)

    if not sub_scores:
        return None

    score = round(sum(sub_scores.values()) / len(sub_scores), 2)
    return {"score": score, "detalle": sub_scores}


# ============================================================================
# SCORE DE TRAFICO
# ============================================================================

def _score_trafico(trafico_rt: Optional[dict]) -> Optional[Dict[str, Any]]:
    """
    Calcula el score de trafico (0-10) basado en incidencias en Valencia.

    Criterios:
        - Sin incidencias: 10
        - 1-3 incidencias leves: 7
        - 4-10 incidencias o alguna grave (high/highest): 4
        - >10 incidencias o varias graves: 1

    Args:
        trafico_rt: Diccionario RT con clave 'incidencias_valencia' (lista).

    Returns:
        Diccionario con 'score' (float 0-10) y 'detalle' (info incidencias),
        o None si no hay datos.
    """
    if trafico_rt is None:
        return None

    incidencias = trafico_rt.get("incidencias_valencia", [])
    total = len(incidencias)

    # Contar incidencias graves (high/highest)
    graves = sum(
        1 for inc in incidencias
        if inc.get("severidad") in ("high", "highest")
    )

    if total == 0:
        score = 10.0
    elif total <= 3 and graves == 0:
        score = 7.0
    elif total > 10 or graves >= 2:
        score = 1.0
    else:
        # 4-10 incidencias o alguna grave
        score = 4.0

    return {
        "score": score,
        "detalle": {
            "total_incidencias": total,
            "incidencias_graves": graves,
        },
    }


# ============================================================================
# FUNCION PRINCIPAL: INDICE DE CALIDAD URBANA
# ============================================================================

def calcular_indice_urbano(
    contam_rt: Optional[dict] = None,
    meteo_rt: Optional[dict] = None,
    trafico_rt: Optional[dict] = None,
) -> Optional[Dict[str, Any]]:
    """
    Calcula el Indice de Calidad Urbana (0-10) fusionando tres ejes.

    Algoritmo:
        1. Calcula score individual de cada eje con datos disponibles.
        2. Score global = sum(score_eje * peso_eje) / sum(pesos_con_datos).
           Los pesos se redistribuyen si faltan ejes.

    Pesos base:
        - Contaminacion: 50%
        - Meteorologia: 25%
        - Trafico: 25%

    Args:
        contam_rt: Datos RT de contaminacion (de cargar_contaminacion_realtime).
        meteo_rt: Datos RT de meteorologia (de cargar_meteo_realtime).
        trafico_rt: Datos RT de trafico (de cargar_trafico_realtime).

    Returns:
        Diccionario con claves:
            - "score": float 0-10
            - "nivel": str (Excelente/Bueno/Aceptable/Deficiente/Malo/Critico)
            - "color": str (hex)
            - "emoji": str
            - "ejes": dict con score y detalle por eje presente
            - "ejes_disponibles": int (cuantos ejes tenian datos)
            - "ejes_totales": int (siempre 3)
        Retorna None si ningun eje tiene datos.
    """
    # Calcular cada eje
    ejes: Dict[str, Dict[str, Any]] = {}

    resultado_contam = _score_contaminacion(contam_rt)
    if resultado_contam is not None:
        ejes["contaminacion"] = {
            "peso": PESO_CONTAMINACION,
            "score": resultado_contam["score"],
            "detalle": resultado_contam["detalle"],
        }

    resultado_meteo = _score_meteorologia(meteo_rt)
    if resultado_meteo is not None:
        ejes["meteorologia"] = {
            "peso": PESO_METEOROLOGIA,
            "score": resultado_meteo["score"],
            "detalle": resultado_meteo["detalle"],
        }

    resultado_trafico = _score_trafico(trafico_rt)
    if resultado_trafico is not None:
        ejes["trafico"] = {
            "peso": PESO_TRAFICO,
            "score": resultado_trafico["score"],
            "detalle": resultado_trafico["detalle"],
        }

    if not ejes:
        logger.warning("[IndiceUrbano] Sin datos en ningun eje.")
        return None

    # Redistribuir pesos entre ejes con datos
    peso_total = sum(e["peso"] for e in ejes.values())
    score_global = sum(
        e["score"] * e["peso"] for e in ejes.values()
    ) / peso_total

    score_global = round(min(10.0, max(0.0, score_global)), 2)
    nivel, color, emoji = _nivel_desde_score_urbano(score_global)

    logger.info(
        f"[IndiceUrbano] score={score_global} nivel={nivel} "
        f"ejes={list(ejes.keys())}"
    )

    return {
        "score": score_global,
        "nivel": nivel,
        "color": color,
        "emoji": emoji,
        "ejes": ejes,
        "ejes_disponibles": len(ejes),
        "ejes_totales": 3,
    }


# ============================================================================
# TESTS BASICOS
# ============================================================================

if __name__ == "__main__":
    import sys

    def test_scores_temperatura():
        """Verifica rangos del score de temperatura."""
        assert _score_temperatura(20.0) == 10.0, "20°C debe ser 10"
        assert _score_temperatura(18.0) == 10.0, "18°C debe ser 10"
        assert _score_temperatura(25.0) == 10.0, "25°C debe ser 10"
        assert _score_temperatura(0.0) == 0.0, "0°C debe ser 0"
        assert _score_temperatura(45.0) == 0.0, "45°C debe ser 0"
        assert 4.0 < _score_temperatura(9.0) < 6.0, "9°C debe ser ~5"
        assert 4.0 < _score_temperatura(35.0) < 6.0, "35°C debe ser ~5"
        print("  [OK] test_scores_temperatura")

    def test_scores_humedad():
        """Verifica rangos del score de humedad."""
        assert _score_humedad(55.0) == 10.0, "55% debe ser 10"
        assert _score_humedad(40.0) == 10.0, "40% debe ser 10"
        assert _score_humedad(70.0) == 10.0, "70% debe ser 10"
        assert _score_humedad(0.0) == 0.0, "0% debe ser 0"
        assert _score_humedad(100.0) == 0.0, "100% debe ser 0"
        print("  [OK] test_scores_humedad")

    def test_scores_lluvia():
        """Verifica rangos del score de lluvia."""
        assert _score_lluvia(0.0) == 10.0, "0mm debe ser 10"
        assert _score_lluvia(0.5) == 10.0, "0.5mm debe ser 10"
        assert _score_lluvia(5.0) == 5.0, "5mm debe ser 5"
        assert _score_lluvia(15.0) == 2.0, "15mm debe ser 2"
        print("  [OK] test_scores_lluvia")

    def test_score_trafico():
        """Verifica logica de incidencias de trafico."""
        assert _score_trafico(None) is None
        assert _score_trafico({"incidencias_valencia": []})["score"] == 10.0
        assert _score_trafico({
            "incidencias_valencia": [
                {"severidad": "low"}, {"severidad": "low"},
            ]
        })["score"] == 7.0
        assert _score_trafico({
            "incidencias_valencia": [
                {"severidad": "high"}, {"severidad": "low"},
            ]
        })["score"] == 4.0
        assert _score_trafico({
            "incidencias_valencia": [{"severidad": "low"}] * 12
        })["score"] == 1.0
        assert _score_trafico({
            "incidencias_valencia": [
                {"severidad": "highest"}, {"severidad": "high"},
            ]
        })["score"] == 1.0
        print("  [OK] test_score_trafico")

    def test_score_meteorologia():
        """Verifica fusion de sub-scores meteorologicos."""
        result = _score_meteorologia({"temp": 22.0, "humedad": 55.0})
        assert result is not None
        assert result["score"] == 10.0, f"Condiciones ideales deben dar 10, got {result['score']}"

        result_none = _score_meteorologia(None)
        assert result_none is None
        print("  [OK] test_score_meteorologia")

    def test_indice_urbano_parcial():
        """Verifica que funciona con datos parciales."""
        # Solo trafico
        result = calcular_indice_urbano(trafico_rt={"incidencias_valencia": []})
        assert result is not None
        assert result["score"] == 10.0
        assert result["ejes_disponibles"] == 1

        # Meteo + trafico
        result2 = calcular_indice_urbano(
            meteo_rt={"temp": 22.0, "humedad": 55.0},
            trafico_rt={"incidencias_valencia": []},
        )
        assert result2 is not None
        assert result2["score"] == 10.0
        assert result2["ejes_disponibles"] == 2

        # Nada
        result_none = calcular_indice_urbano()
        assert result_none is None
        print("  [OK] test_indice_urbano_parcial")

    def test_niveles():
        """Verifica asignacion de niveles."""
        assert _nivel_desde_score_urbano(9.0) == ("Excelente", "#2ca02c", "🟢")
        assert _nivel_desde_score_urbano(7.5) == ("Bueno", "#27ae60", "🟢")
        assert _nivel_desde_score_urbano(6.0) == ("Aceptable", "#f39c12", "🟡")
        assert _nivel_desde_score_urbano(3.5) == ("Deficiente", "#e67e22", "🟠")
        assert _nivel_desde_score_urbano(2.0) == ("Malo", "#e74c3c", "🔴")
        assert _nivel_desde_score_urbano(0.5) == ("Crítico", "#8b0000", "⛔")
        print("  [OK] test_niveles")

    # Ejecutar todos los tests
    print("=" * 50)
    print("Tests del Indice de Calidad Urbana")
    print("=" * 50)

    tests = [
        test_scores_temperatura,
        test_scores_humedad,
        test_scores_lluvia,
        test_score_trafico,
        test_score_meteorologia,
        test_indice_urbano_parcial,
        test_niveles,
    ]

    errores = 0
    for test_fn in tests:
        try:
            test_fn()
        except AssertionError as e:
            print(f"  [FAIL] {test_fn.__name__}: {e}")
            errores += 1
        except Exception as e:
            print(f"  [ERROR] {test_fn.__name__}: {e}")
            errores += 1

    print("=" * 50)
    if errores == 0:
        print(f"Todos los tests pasaron ({len(tests)}/{len(tests)})")
    else:
        print(f"FALLOS: {errores}/{len(tests)}")
        sys.exit(1)
