# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Utils: Pronostico Estadistico de Contaminacion
==============================================================================
Ruta: 5.DASHBOARD/utils/pronostico_estadistico.py
Autor: Joan | Fecha: 2026

Genera un pronostico heuristico de contaminacion para manana basado en:
  - Media historica para el mismo mes + dia de la semana
  - Correccion por tendencia interanual (ultimos 3 anios)
  - Ponderacion con datos RT recientes (si disponibles)

Escala de confianza:
  - Alta:  >= 50 registros historicos para ese mes+dia_semana
  - Media: >= 10 registros
  - Baja:  < 10 registros (fallback a media mensual general)
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Variables principales para el pronostico
_VARIABLES_PRONOSTICO: List[str] = ["NO2", "O3", "PM10", "PM2.5"]

# Umbrales de confianza (numero de registros historicos)
_UMBRAL_CONFIANZA_ALTA = 50
_UMBRAL_CONFIANZA_MEDIA = 10

# Pesos para fusion historico / RT
_PESO_HISTORICO = 0.60
_PESO_RT = 0.40

# Anios para calcular tendencia
_ANIOS_TENDENCIA = 3


def pronosticar_contaminacion_manana(
    df_contaminacion: Optional[pd.DataFrame],
    contam_rt: Optional[dict] = None,
    fecha_objetivo: Optional[datetime] = None,
) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Genera pronostico de contaminacion para manana.

    Algoritmo:
        a) Filtra datos historicos por mes y dia de la semana de manana.
        b) Calcula media y rango (percentiles 25-75).
        c) Ajusta por tendencia interanual si hay >= 3 anios de datos.
        d) Pondera 60% historico + 40% RT si hay datos recientes.

    Args:
        df_contaminacion: DataFrame de contaminacion normalizada con columnas
            [fecha_utc, variable, valor, calidad_dato].
        contam_rt: Diccionario RT de AQICN (con clave 'estaciones'),
            tal como devuelve cargar_contaminacion_realtime().
        fecha_objetivo: Fecha para la que pronosticar. Si None, usa manana.

    Returns:
        Diccionario {variable: {prediccion, rango_min, rango_max, confianza,
        tendencia, n_registros, metodo}} para cada variable con datos.
        None si no hay datos historicos.
    """
    if df_contaminacion is None or df_contaminacion.empty:
        logger.warning("[PronosticoEstadistico] Sin datos de contaminacion.")
        return None

    if "fecha_utc" not in df_contaminacion.columns:
        logger.warning("[PronosticoEstadistico] Falta columna fecha_utc.")
        return None

    # Fecha objetivo: manana por defecto
    if fecha_objetivo is None:
        fecha_objetivo = datetime.now() + timedelta(days=1)

    mes_objetivo = fecha_objetivo.month
    dia_semana_objetivo = fecha_objetivo.weekday()  # 0=lunes

    # Preparar datos
    df = df_contaminacion.copy()
    df["fecha_dt"] = pd.to_datetime(df["fecha_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["fecha_dt"])
    if "calidad_dato" in df.columns:
        df = df[df["calidad_dato"] == "ok"]

    df["mes"] = df["fecha_dt"].dt.month
    df["dia_semana"] = df["fecha_dt"].dt.weekday
    df["anio"] = df["fecha_dt"].dt.year

    # Extraer medias RT si disponibles
    medias_rt = _extraer_medias_rt(contam_rt)

    resultados: Dict[str, Dict[str, Any]] = {}

    for variable in _VARIABLES_PRONOSTICO:
        df_var = df[df["variable"] == variable]["valor"].dropna()
        if df_var.empty:
            continue

        df_var_full = df[df["variable"] == variable].copy()

        resultado = _pronosticar_variable(
            df_var_full, variable, mes_objetivo, dia_semana_objetivo,
            medias_rt.get(variable),
        )
        if resultado is not None:
            resultados[variable] = resultado

    if not resultados:
        logger.warning("[PronosticoEstadistico] Sin resultados para ninguna variable.")
        return None

    logger.info(
        "[PronosticoEstadistico] Pronostico generado para %d variables: %s",
        len(resultados), list(resultados.keys()),
    )
    return resultados


def _pronosticar_variable(
    df_var: pd.DataFrame,
    variable: str,
    mes: int,
    dia_semana: int,
    valor_rt: Optional[float],
) -> Optional[Dict[str, Any]]:
    """
    Genera pronostico para una variable individual.

    Args:
        df_var: DataFrame filtrado a una variable, con columnas
            [valor, mes, dia_semana, anio].
        variable: Nombre de la variable.
        mes: Mes objetivo (1-12).
        dia_semana: Dia de la semana objetivo (0=lunes).
        valor_rt: Media RT reciente (o None).

    Returns:
        Diccionario con prediccion, rango, confianza, tendencia.
    """
    # Filtrar por mes + dia de semana
    mask_especifico = (df_var["mes"] == mes) & (df_var["dia_semana"] == dia_semana)
    df_especifico = df_var[mask_especifico]["valor"].dropna()

    # Fallback: solo mes (si pocos registros especificos)
    mask_mes = df_var["mes"] == mes
    df_mes = df_var[mask_mes]["valor"].dropna()

    if len(df_especifico) >= _UMBRAL_CONFIANZA_ALTA:
        confianza = "Alta"
        serie = df_especifico
        metodo = "mes+día semana"
    elif len(df_especifico) >= _UMBRAL_CONFIANZA_MEDIA:
        confianza = "Media"
        serie = df_especifico
        metodo = "mes+día semana"
    elif len(df_mes) >= _UMBRAL_CONFIANZA_MEDIA:
        confianza = "Baja"
        serie = df_mes
        metodo = "media mensual"
    else:
        # Muy pocos datos incluso mensuales
        if df_mes.empty:
            return None
        confianza = "Baja"
        serie = df_mes
        metodo = "media mensual (pocos datos)"

    media_hist = float(serie.mean())
    p25 = float(serie.quantile(0.25))
    p75 = float(serie.quantile(0.75))

    # --- Correccion por tendencia interanual ---
    tendencia_str, factor_tendencia = _calcular_tendencia(df_var, variable, mes)
    prediccion_base = media_hist * factor_tendencia

    # --- Fusion con RT ---
    if valor_rt is not None:
        prediccion = _PESO_HISTORICO * prediccion_base + _PESO_RT * valor_rt
        metodo += " + RT"
    else:
        prediccion = prediccion_base

    prediccion = max(0.0, prediccion)

    return {
        "prediccion": round(prediccion, 1),
        "rango_min": round(max(0.0, p25 * factor_tendencia), 1),
        "rango_max": round(p75 * factor_tendencia, 1),
        "confianza": confianza,
        "tendencia": tendencia_str,
        "n_registros": len(serie),
        "metodo": metodo,
    }


def _calcular_tendencia(
    df_var: pd.DataFrame,
    variable: str,
    mes: int,
) -> tuple:
    """
    Calcula la tendencia interanual para los ultimos N anios.

    Compara la media de los ultimos _ANIOS_TENDENCIA anios con la media
    de los anios anteriores, para el mismo mes.

    Args:
        df_var: DataFrame con columnas [valor, anio, mes].
        variable: Nombre de la variable (para logging).
        mes: Mes de referencia.

    Returns:
        Tupla (tendencia_str, factor_float).
        factor > 1.0 = tendencia al alza, < 1.0 = a la baja.
    """
    df_mes = df_var[df_var["mes"] == mes].copy()
    if df_mes.empty:
        return "Sin datos", 1.0

    medias_anuales = df_mes.groupby("anio")["valor"].mean().sort_index()
    if len(medias_anuales) < 3:
        return "Insuficiente", 1.0

    anios = medias_anuales.index.tolist()
    corte = max(0, len(anios) - _ANIOS_TENDENCIA)
    recientes = medias_anuales.iloc[corte:]
    anteriores = medias_anuales.iloc[:corte]

    if anteriores.empty or recientes.empty:
        return "Insuficiente", 1.0

    media_reciente = float(recientes.mean())
    media_anterior = float(anteriores.mean())

    if media_anterior == 0:
        return "Estable", 1.0

    ratio = media_reciente / media_anterior

    # Limitar el factor de ajuste para evitar extrapolaciones extremas
    factor = max(0.5, min(1.5, ratio))

    if factor > 1.05:
        tendencia = f"Al alza ({(factor - 1) * 100:+.0f}%)"
    elif factor < 0.95:
        tendencia = f"A la baja ({(factor - 1) * 100:+.0f}%)"
    else:
        tendencia = "Estable"

    return tendencia, factor


def _extraer_medias_rt(contam_rt: Optional[dict]) -> Dict[str, float]:
    """
    Extrae medias de contaminantes de los datos RT de AQICN.

    Args:
        contam_rt: Diccionario RT con clave 'estaciones'.

    Returns:
        Diccionario {variable: media} con las variables disponibles.
    """
    if contam_rt is None:
        return {}

    estaciones = contam_rt.get("estaciones", [])
    if not estaciones:
        return {}

    key_map = {
        "no2": "NO2", "o3": "O3", "pm10": "PM10", "pm25": "PM2.5",
    }

    medias: Dict[str, float] = {}
    for key_rt, variable in key_map.items():
        vals = [e[key_rt] for e in estaciones if e.get(key_rt) is not None]
        if vals:
            medias[variable] = sum(vals) / len(vals)

    return medias


def obtener_distribucion_mensual(
    df_contaminacion: Optional[pd.DataFrame],
    variable: str,
    mes: int,
) -> Optional[pd.Series]:
    """
    Obtiene la distribucion de valores de una variable para un mes dado.

    Util para generar boxplots de contexto historico.

    Args:
        df_contaminacion: DataFrame de contaminacion.
        variable: Nombre de la variable (NO2, O3, etc.).
        mes: Mes (1-12).

    Returns:
        Serie de valores, o None si no hay datos.
    """
    if df_contaminacion is None or df_contaminacion.empty:
        return None

    df = df_contaminacion.copy()
    df["fecha_dt"] = pd.to_datetime(df["fecha_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["fecha_dt"])

    if "calidad_dato" in df.columns:
        df = df[df["calidad_dato"] == "ok"]

    mask = (df["variable"] == variable) & (df["fecha_dt"].dt.month == mes)
    serie = df[mask]["valor"].dropna()

    return serie if not serie.empty else None


# ============================================================================
# TESTS BASICOS
# ============================================================================

if __name__ == "__main__":
    import sys

    def test_calcular_tendencia():
        """Verifica calculo de tendencia con datos sinteticos."""
        # Crear DataFrame simulado con tendencia a la baja
        registros = []
        for anio in range(2018, 2026):
            for i in range(20):
                registros.append({
                    "valor": 30.0 - (anio - 2018) * 1.5 + np.random.normal(0, 2),
                    "anio": anio,
                    "mes": 4,
                    "dia_semana": 2,
                })
        df = pd.DataFrame(registros)
        tendencia, factor = _calcular_tendencia(df, "NO2", 4)
        assert factor < 1.0, f"Tendencia a la baja esperada, got factor={factor}"
        assert "baja" in tendencia.lower(), f"Expected 'baja' in '{tendencia}'"
        print("  [OK] test_calcular_tendencia")

    def test_extraer_medias_rt():
        """Verifica extraccion de medias RT."""
        rt = {
            "estaciones": [
                {"no2": 20.0, "o3": 40.0, "pm10": None, "pm25": 10.0},
                {"no2": 30.0, "o3": 50.0, "pm10": 25.0, "pm25": 15.0},
            ]
        }
        medias = _extraer_medias_rt(rt)
        assert medias["NO2"] == 25.0
        assert medias["O3"] == 45.0
        assert medias["PM10"] == 25.0
        assert medias["PM2.5"] == 12.5
        assert _extraer_medias_rt(None) == {}
        print("  [OK] test_extraer_medias_rt")

    def test_pronosticar_variable_sintetico():
        """Verifica pronostico con datos sinteticos suficientes."""
        registros = []
        for anio in range(2020, 2026):
            for dia in range(1, 29):
                dt = datetime(anio, 4, dia)
                registros.append({
                    "valor": 25.0 + np.random.normal(0, 5),
                    "anio": anio,
                    "mes": 4,
                    "dia_semana": dt.weekday(),
                    "fecha_utc": pd.Timestamp(dt, tz="UTC"),
                })
        df = pd.DataFrame(registros)
        df["variable"] = "NO2"

        resultado = _pronosticar_variable(df, "NO2", 4, 2, None)
        assert resultado is not None
        assert 10 < resultado["prediccion"] < 50, f"Prediccion fuera de rango: {resultado['prediccion']}"
        assert resultado["confianza"] in ("Alta", "Media", "Baja")
        assert resultado["rango_min"] <= resultado["prediccion"] <= resultado["rango_max"] + 5
        print("  [OK] test_pronosticar_variable_sintetico")

    def test_pronosticar_con_rt():
        """Verifica que RT pondera correctamente."""
        registros = []
        for anio in range(2020, 2026):
            for dia in range(1, 29):
                dt = datetime(anio, 4, dia)
                registros.append({
                    "valor": 20.0,
                    "anio": anio,
                    "mes": 4,
                    "dia_semana": dt.weekday(),
                    "fecha_utc": pd.Timestamp(dt, tz="UTC"),
                })
        df = pd.DataFrame(registros)
        df["variable"] = "NO2"

        # Sin RT
        r1 = _pronosticar_variable(df, "NO2", 4, 2, None)
        # Con RT alto
        r2 = _pronosticar_variable(df, "NO2", 4, 2, 40.0)

        assert r2["prediccion"] > r1["prediccion"], "RT alto debe subir prediccion"
        print("  [OK] test_pronosticar_con_rt")

    def test_datos_insuficientes():
        """Verifica fallback con pocos datos."""
        df = pd.DataFrame({
            "valor": [10.0, 15.0, 12.0],
            "anio": [2024, 2024, 2024],
            "mes": [4, 4, 5],
            "dia_semana": [2, 3, 2],
            "fecha_utc": pd.to_datetime(["2024-04-10", "2024-04-11", "2024-05-08"]).tz_localize("UTC"),
            "variable": ["NO2"] * 3,
        })
        resultado = _pronosticar_variable(df, "NO2", 4, 2, None)
        # Pocos datos pero al menos devuelve algo
        if resultado is not None:
            assert resultado["confianza"] == "Baja"
        print("  [OK] test_datos_insuficientes")

    # Ejecutar tests
    print("=" * 50)
    print("Tests del Pronostico Estadistico")
    print("=" * 50)

    tests = [
        test_calcular_tendencia,
        test_extraer_medias_rt,
        test_pronosticar_variable_sintetico,
        test_pronosticar_con_rt,
        test_datos_insuficientes,
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
