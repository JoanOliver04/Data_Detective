# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Tests basicos del dashboard
==============================================================================
Ruta: tests/test_dashboard_basico.py

Verifica que los componentes principales funcionan correctamente sin necesitar
datos reales en disco ni conexion a internet.

Ejecutar:
    pytest tests/test_dashboard_basico.py -v
    python tests/run_tests.py
"""

import pytest
import pandas as pd

# ---------------------------------------------------------------------------
# Importaciones del proyecto
# (conftest.py ya anadio 5.DASHBOARD/ al sys.path y parcheó st.cache_data)
# ---------------------------------------------------------------------------
from utils.quality_index import calcular_indice_calidad, nivel_desde_score, _NIVELES
from config import UMBRALES_OMS, ESTACION_BARRIO_MAP
import data_loader

# ==============================================================================
# FIXTURES COMPARTIDOS
# ==============================================================================


@pytest.fixture
def df_contaminacion_mock():
    """
    DataFrame minimo con la estructura esperada por calcular_indice_calidad.
    Contiene registros 'ok' de NO2 y PM2.5 con valores por debajo del umbral OMS.
    """
    return pd.DataFrame(
        {
            "variable": ["NO2", "NO2", "PM2.5", "PM2.5", "PM10", "O3"],
            "valor": [8.0, 12.0, 3.0, 4.5, 10.0, 45.0],
            "calidad_dato": ["ok", "ok", "ok", "ok", "ok", "ok"],
        }
    )


@pytest.fixture
def df_contaminacion_alto_mock():
    """DataFrame con valores muy por encima del umbral (scores bajos esperados)."""
    return pd.DataFrame(
        {
            "variable": ["NO2", "PM2.5", "PM10"],
            "valor": [200.0, 80.0, 120.0],
            "calidad_dato": ["ok", "ok", "ok"],
        }
    )


# ==============================================================================
# TEST 1: calcular_indice_calidad con datos normales
# ==============================================================================


class TestQualityIndexCalculo:

    def test_retorna_dict(self, df_contaminacion_mock):
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        assert isinstance(resultado, dict), "Debe devolver un diccionario"

    def test_claves_presentes(self, df_contaminacion_mock):
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        for clave in ("score", "nivel", "color", "emoji", "detalle_variables"):
            assert clave in resultado, f"Clave '{clave}' ausente en el resultado"

    def test_score_en_rango(self, df_contaminacion_mock):
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        score = resultado["score"]
        assert 0.0 <= score <= 10.0, f"Score fuera de rango: {score}"

    def test_nivel_valido(self, df_contaminacion_mock):
        niveles_esperados = {n for _, n, _, _ in _NIVELES}
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        assert (
            resultado["nivel"] in niveles_esperados
        ), f"Nivel '{resultado['nivel']}' no esta en {niveles_esperados}"

    def test_color_es_hex(self, df_contaminacion_mock):
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        color = resultado["color"]
        assert color.startswith("#") and len(color) in (
            7,
            9,
        ), f"Color no es un hex valido: {color}"

    def test_detalle_variables_no_vacio(self, df_contaminacion_mock):
        resultado = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        assert resultado["detalle_variables"], "detalle_variables no debe estar vacio"

    def test_datos_contaminados_score_mas_bajo(
        self, df_contaminacion_mock, df_contaminacion_alto_mock
    ):
        """Datos muy por encima del umbral deben producir un score mas bajo."""
        r_normal = calcular_indice_calidad(df_contaminacion_mock, UMBRALES_OMS)
        r_alto = calcular_indice_calidad(df_contaminacion_alto_mock, UMBRALES_OMS)
        assert (
            r_alto["score"] < r_normal["score"]
        ), "Score con valores altos debe ser menor que con valores normales"

    def test_registros_sospechosos_ignorados(self):
        """Registros con calidad_dato != 'ok' deben ser excluidos."""
        df = pd.DataFrame(
            {
                "variable": ["NO2", "NO2"],
                "valor": [500.0, 8.0],
                "calidad_dato": ["sospechoso", "ok"],
            }
        )
        resultado = calcular_indice_calidad(df, UMBRALES_OMS)
        assert resultado is not None
        # Solo el registro 'ok' (8.0) debe contribuir; score debe ser bueno
        media_no2 = resultado["detalle_variables"]["NO2"]["media"]
        assert media_no2 == pytest.approx(
            8.0
        ), f"Solo el registro 'ok' deberia contar; media esperada 8.0, obtenida {media_no2}"


# ==============================================================================
# TEST 2: calcular_indice_calidad con datos vacios
# ==============================================================================


class TestQualityIndexSinDatos:

    def test_dataframe_vacio_retorna_none(self):
        df_vacio = pd.DataFrame(columns=["variable", "valor", "calidad_dato"])
        assert calcular_indice_calidad(df_vacio, UMBRALES_OMS) is None

    def test_none_retorna_none(self):
        assert calcular_indice_calidad(None, UMBRALES_OMS) is None

    def test_sin_variables_conocidas_retorna_none(self):
        """DataFrame con variables no reconocidas debe retornar None."""
        df = pd.DataFrame(
            {
                "variable": ["VARIABLE_INEXISTENTE"],
                "valor": [10.0],
                "calidad_dato": ["ok"],
            }
        )
        assert calcular_indice_calidad(df, UMBRALES_OMS) is None

    def test_todos_registros_sospechosos_retorna_none(self):
        """Si todos los registros son sospechosos, no hay datos validos."""
        df = pd.DataFrame(
            {
                "variable": ["NO2", "PM2.5"],
                "valor": [50.0, 20.0],
                "calidad_dato": ["sospechoso", "sospechoso"],
            }
        )
        assert calcular_indice_calidad(df, UMBRALES_OMS) is None


# ==============================================================================
# TEST 3: nivel_desde_score
# ==============================================================================


class TestNivelDesdeScore:

    @pytest.mark.parametrize(
        "score,nivel_esperado",
        [
            (9.5, "Excelente"),
            (8.0, "Bueno"),
            (6.0, "Aceptable"),
            (4.0, "Deficiente"),
            (2.0, "Malo"),
            (0.5, "Peligroso"),
            (0.0, "Peligroso"),
            (10.0, "Excelente"),
        ],
    )
    def test_niveles_correctos(self, score, nivel_esperado):
        nivel, color, emoji = nivel_desde_score(score)
        assert (
            nivel == nivel_esperado
        ), f"Score {score}: esperado '{nivel_esperado}', obtenido '{nivel}'"

    def test_retorna_tres_valores(self):
        resultado = nivel_desde_score(7.5)
        assert len(resultado) == 3


# ==============================================================================
# TEST 4: UMBRALES_OMS
# ==============================================================================


class TestUmbralesOms:

    def test_variables_minimas_presentes(self):
        requeridas = {"NO2", "O3", "PM10", "PM2.5"}
        faltantes = requeridas - set(UMBRALES_OMS.keys())
        assert not faltantes, f"Variables ausentes en UMBRALES_OMS: {faltantes}"

    def test_todos_los_valores_positivos(self):
        for variable, valor in UMBRALES_OMS.items():
            assert valor > 0, f"Umbral de {variable} debe ser positivo, es {valor}"

    def test_tipos_correctos(self):
        for variable, valor in UMBRALES_OMS.items():
            assert isinstance(variable, str), f"Clave debe ser str: {variable!r}"
            assert isinstance(
                valor, (int, float)
            ), f"Valor debe ser numerico: {variable}={valor!r}"

    def test_no2_menor_que_ue(self):
        """Umbral OMS de NO2 (10) debe ser menor que el de la UE (40)."""
        from config import UMBRALES_UE

        assert (
            UMBRALES_OMS["NO2"] < UMBRALES_UE["NO2"]
        ), "OMS debe ser mas estricto que UE para NO2"


# ==============================================================================
# TEST 5: ESTACION_BARRIO_MAP
# ==============================================================================


class TestEstacionBarrioMap:

    def test_no_esta_vacio(self):
        assert ESTACION_BARRIO_MAP, "ESTACION_BARRIO_MAP no debe estar vacio"

    def test_todas_las_claves_son_strings(self):
        for clave in ESTACION_BARRIO_MAP:
            assert (
                isinstance(clave, str) and clave
            ), f"Clave invalida en ESTACION_BARRIO_MAP: {clave!r}"

    def test_todos_los_valores_son_strings_no_vacios(self):
        for clave, barrio in ESTACION_BARRIO_MAP.items():
            assert (
                isinstance(barrio, str) and barrio.strip()
            ), f"Valor vacio o no-string para estacion '{clave}': {barrio!r}"

    def test_contiene_estaciones_conocidas(self):
        """Las estaciones GVA principales deben estar presentes."""
        estaciones_gva = {"46250001", "46250004", "46250030", "46250047", "46250050"}
        presentes = estaciones_gva & set(ESTACION_BARRIO_MAP.keys())
        assert presentes, (
            f"Ninguna estacion GVA encontrada en ESTACION_BARRIO_MAP. "
            f"Claves actuales: {list(ESTACION_BARRIO_MAP.keys())}"
        )


# ==============================================================================
# TEST 6: data_loader no lanza excepcion sin datos en disco
# ==============================================================================


class TestDataLoaderNoCrash:
    """
    Verifica que cada funcion cargar_* retorna None (o un valor) sin
    lanzar ninguna excepcion, incluso cuando los archivos de datos no existen.
    """

    _FUNCIONES_HISTORICAS = [
        ("cargar_contaminacion", data_loader.cargar_contaminacion),
        ("cargar_meteorologia", data_loader.cargar_meteorologia),
        ("cargar_trafico", data_loader.cargar_trafico),
        ("cargar_impacto_eventos", data_loader.cargar_impacto_eventos),
        ("cargar_contam_anual_barrio", data_loader.cargar_contam_anual_barrio),
        ("cargar_precip_mensual", data_loader.cargar_precip_mensual),
        ("cargar_tendencias", data_loader.cargar_tendencias),
        ("cargar_pronostico_72h", data_loader.cargar_pronostico_72h),
    ]

    _FUNCIONES_RT = [
        ("cargar_contaminacion_realtime", data_loader.cargar_contaminacion_realtime),
        ("cargar_meteo_realtime", data_loader.cargar_meteo_realtime),
        ("cargar_trafico_realtime", data_loader.cargar_trafico_realtime),
    ]

    @pytest.mark.parametrize("nombre,func", _FUNCIONES_HISTORICAS)
    def test_historicas_no_crashean(self, nombre, func):
        try:
            resultado = func()
        except Exception as exc:
            pytest.fail(
                f"{nombre}() lanzo una excepcion inesperada: "
                f"{type(exc).__name__}: {exc}"
            )
        # Sin archivos, debe devolver None o un DataFrame/dict vacio
        assert resultado is None or isinstance(
            resultado, (pd.DataFrame, dict)
        ), f"{nombre}() devolvio un tipo inesperado: {type(resultado)}"

    @pytest.mark.parametrize("nombre,func", _FUNCIONES_RT)
    def test_rt_no_crashean(self, nombre, func):
        try:
            resultado = func()
        except Exception as exc:
            pytest.fail(
                f"{nombre}() lanzo una excepcion inesperada: "
                f"{type(exc).__name__}: {exc}"
            )
        assert resultado is None or isinstance(
            resultado, (dict, pd.DataFrame)
        ), f"{nombre}() devolvio un tipo inesperado: {type(resultado)}"
