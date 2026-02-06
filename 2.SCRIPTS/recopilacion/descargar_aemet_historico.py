# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 2.3: Descarga de Datos Meteorológicos Históricos (AEMET OpenData)
==============================================================================

Descripción:
    Este script descarga datos meteorológicos históricos desde la API de AEMET.
    
    AEMET usa un sistema de DOBLE PETICIÓN:
    1) Primera petición: obtiene URL temporal con los datos
    2) Segunda petición: descarga los datos reales desde esa URL
    
    La URL temporal caduca aproximadamente en 1 hora.

API Documentation:
    https://opendata.aemet.es/dist/index.html
    
Endpoints utilizados:
    - /api/valores/climatologicos/diarios/datos/fechaini/{}/fechafin/{}/estacion/{}
    - /api/valores/climatologicos/inventarioestaciones/todasestaciones

Uso:
    1. Obtener API Key en: https://opendata.aemet.es/centrodedescargas/altaUsuario
    2. Guardar en .env: AEMET_API_KEY=tu_clave_aqui
    3. Ejecutar: python descargar_aemet_historico.py
    
Salida:
    - 1.DATOS_EN_CRUDO/estaticos/meteorologia/aemet_valencia_historico.csv
    - Formato: [fecha, estacion, variable, valor]

Limitaciones conocidas:
    - La API tiene límites de rango de fechas por petición (~31 días)
    - No todos los datos históricos están disponibles vía API
    - Rate limiting: respetar tiempos entre peticiones

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import os
import json
import time
import requests
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Cargar variables de entorno
load_dotenv()

# Rutas base
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "meteorologia"
LOG_DIR = PROJECT_ROOT / "logs"

# API AEMET
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
AEMET_API_KEY = os.getenv("AEMET_API_KEY")

# Estaciones meteorológicas de Valencia (respaldo si falla inventario)
ESTACIONES_VALENCIA_FALLBACK = {
    "8416Y": "Valencia Aeropuerto",
    "8414A": "Valencia",
    "8293X": "Valencia-Manises",
}

# Modo LITE: buscar en inventario pero limitar a N estaciones
USAR_SOLO_ESTACIONES_PREDEFINIDAS = False
MAX_ESTACIONES_LITE = 3  # Solo procesar las primeras 3 estaciones encontradas

# Variables meteorológicas de interés y su mapeo
# Nombres de campos en respuesta AEMET -> nombres estándar
VARIABLES_MAPPING = {
    "prec": "precipitacion",      # Precipitación diaria (mm)
    "tmed": "temperatura_media",   # Temperatura media (°C)
    "tmax": "temperatura_max",     # Temperatura máxima (°C)
    "tmin": "temperatura_min",     # Temperatura mínima (°C)
    "hrMedia": "humedad_media",    # Humedad relativa media (%)
    "hrMax": "humedad_max",        # Humedad máxima (%)
    "hrMin": "humedad_min",        # Humedad mínima (%)
    "velmedia": "viento_velocidad",  # Velocidad media del viento (m/s)
    "racha": "viento_racha",       # Racha máxima (m/s)
    "dir": "viento_direccion",     # Dirección del viento (grados)
    "presMax": "presion_max",      # Presión máxima (hPa)
    "presMin": "presion_min",      # Presión mínima (hPa)
    "sol": "horas_sol",            # Horas de sol
}

# Configuración de descarga
DIAS_POR_PETICION = 30  # AEMET limita a ~31 días por petición
# Segundos entre peticiones (aumentado para evitar rate limit)
DELAY_ENTRE_PETICIONES = 3.0
MAX_REINTENTOS = 3
AÑOS_HISTORICO = 1  # Modo LITE: solo 1 año de datos


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """Configura el sistema de logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "aemet_historico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("AEMET_Historico")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ==============================================================================
# FUNCIONES DE LA API AEMET (DOBLE PETICIÓN)
# ==============================================================================

def aemet_request(endpoint: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Realiza una petición a la API de AEMET.

    Primera parte del sistema de doble petición: obtiene la URL temporal.

    Args:
        endpoint: Endpoint relativo de la API
        logger: Logger

    Returns:
        Diccionario con la respuesta o None si hay error
    """
    if not AEMET_API_KEY:
        logger.error("AEMET_API_KEY no configurada en .env")
        return None

    url = f"{AEMET_BASE_URL}{endpoint}"
    headers = {
        "api_key": AEMET_API_KEY,
        "Accept": "application/json"
    }

    for intento in range(MAX_REINTENTOS):
        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.error("API Key inválida o no autorizada")
                return None
            elif response.status_code == 429:
                logger.warning("Rate limit alcanzado, esperando...")
                time.sleep(60)  # Esperar 1 minuto
                continue
            elif response.status_code == 404:
                logger.debug(f"No hay datos disponibles para: {endpoint}")
                return None
            else:
                logger.warning(
                    f"Error HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            logger.warning(
                f"Timeout en intento {intento + 1}/{MAX_REINTENTOS}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {str(e)}")

        if intento < MAX_REINTENTOS - 1:
            time.sleep(DELAY_ENTRE_PETICIONES * (intento + 1))

    return None


def aemet_fetch_data(data_url: str, logger: logging.Logger) -> Optional[List[Dict]]:
    """
    Segunda parte del sistema de doble petición: descarga los datos reales.

    Args:
        data_url: URL temporal proporcionada por la primera petición
        logger: Logger

    Returns:
        Lista de diccionarios con los datos o None si hay error
    """
    for intento in range(MAX_REINTENTOS):
        try:
            response = requests.get(data_url, timeout=60)

            if response.status_code == 200:
                # Los datos vienen como JSON
                return response.json()
            elif response.status_code == 404:
                logger.warning("URL de datos caducada o no disponible")
                return None
            else:
                logger.warning(
                    f"Error descargando datos: HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout descargando datos, intento {intento + 1}")
        except json.JSONDecodeError:
            logger.error("Error decodificando JSON de datos")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {str(e)}")

        if intento < MAX_REINTENTOS - 1:
            time.sleep(DELAY_ENTRE_PETICIONES)

    return None


def get_climatologia_diaria(
    estacion: str,
    fecha_inicio: datetime,
    fecha_fin: datetime,
    logger: logging.Logger
) -> Optional[List[Dict]]:
    """
    Obtiene datos climatológicos diarios para una estación y rango de fechas.

    Implementa el sistema de DOBLE PETICIÓN de AEMET.

    Args:
        estacion: Código de la estación (idema)
        fecha_inicio: Fecha de inicio
        fecha_fin: Fecha de fin
        logger: Logger

    Returns:
        Lista de registros diarios o None
    """
    # Formato de fecha para AEMET: AAAA-MM-DDTHH:MM:SSUTC
    fecha_ini_str = fecha_inicio.strftime("%Y-%m-%dT00:00:00UTC")
    fecha_fin_str = fecha_fin.strftime("%Y-%m-%dT23:59:59UTC")

    endpoint = f"/valores/climatologicos/diarios/datos/fechaini/{fecha_ini_str}/fechafin/{fecha_fin_str}/estacion/{estacion}"

    logger.debug(
        f"Solicitando datos: {estacion} ({fecha_inicio.date()} - {fecha_fin.date()})")

    # Primera petición: obtener URL de datos
    response = aemet_request(endpoint, logger)

    if not response:
        return None

    # Verificar estado de la respuesta
    estado = response.get("estado", 0)
    if estado != 200:
        descripcion = response.get("descripcion", "Error desconocido")
        logger.debug(f"AEMET respuesta: {descripcion}")
        return None

    # Obtener URL de datos
    data_url = response.get("datos")
    if not data_url:
        logger.warning("No se recibió URL de datos")
        return None

    # Pequeña pausa antes de la segunda petición
    time.sleep(0.5)

    # Segunda petición: descargar datos reales
    datos = aemet_fetch_data(data_url, logger)

    return datos


def get_inventario_estaciones(logger: logging.Logger) -> Optional[List[Dict]]:
    """
    Obtiene el inventario de todas las estaciones climatológicas.

    Útil para descubrir estaciones disponibles en Valencia.

    Args:
        logger: Logger

    Returns:
        Lista de estaciones o None
    """
    endpoint = "/valores/climatologicos/inventarioestaciones/todasestaciones"

    response = aemet_request(endpoint, logger)

    if not response or response.get("estado") != 200:
        return None

    data_url = response.get("datos")
    if not data_url:
        return None

    time.sleep(0.5)
    return aemet_fetch_data(data_url, logger)


# ==============================================================================
# FUNCIONES DE PROCESAMIENTO
# ==============================================================================

def buscar_estaciones_valencia(logger: logging.Logger) -> Dict[str, str]:
    """
    Busca estaciones climatológicas en la provincia de Valencia.

    En modo LITE, limita el número de estaciones a MAX_ESTACIONES_LITE.

    Args:
        logger: Logger

    Returns:
        Diccionario {codigo: nombre} de estaciones
    """
    if USAR_SOLO_ESTACIONES_PREDEFINIDAS:
        logger.info(
            f"Modo LITE: usando {len(ESTACIONES_VALENCIA_FALLBACK)} estaciones predefinidas")
        for codigo, nombre in ESTACIONES_VALENCIA_FALLBACK.items():
            logger.info(f"  - {codigo}: {nombre}")
        return ESTACIONES_VALENCIA_FALLBACK

    logger.info("Buscando estaciones de Valencia en inventario AEMET...")

    inventario = get_inventario_estaciones(logger)

    if not inventario:
        logger.warning(
            "No se pudo obtener inventario, usando estaciones predefinidas")
        return ESTACIONES_VALENCIA_FALLBACK

    estaciones_valencia = {}

    for estacion in inventario:
        provincia = estacion.get("provincia", "").upper()

        # Filtrar por provincia de Valencia
        if "VALENCIA" in provincia or provincia == "VALENCIA/VALÈNCIA":
            indicativo = estacion.get("indicativo", "")
            nombre = estacion.get("nombre", "Desconocida")

            if indicativo:
                estaciones_valencia[indicativo] = nombre

    if estaciones_valencia:
        total_encontradas = len(estaciones_valencia)
        logger.info(f"Encontradas {total_encontradas} estaciones en Valencia")

        # Modo LITE: limitar número de estaciones
        if MAX_ESTACIONES_LITE and len(estaciones_valencia) > MAX_ESTACIONES_LITE:
            # Tomar solo las primeras N estaciones
            estaciones_limitadas = dict(
                list(estaciones_valencia.items())[:MAX_ESTACIONES_LITE])
            logger.info(
                f"Modo LITE: limitando a {MAX_ESTACIONES_LITE} estaciones")
            estaciones_valencia = estaciones_limitadas

        for codigo, nombre in estaciones_valencia.items():
            logger.info(f"  - {codigo}: {nombre}")
    else:
        logger.warning(
            "No se encontraron estaciones de Valencia, usando predefinidas")
        estaciones_valencia = ESTACIONES_VALENCIA_FALLBACK

    return estaciones_valencia


def transform_to_long_format(
    datos: List[Dict],
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Transforma los datos de AEMET al formato largo normalizado.

    De:
        {fecha, indicativo, nombre, prec, tmed, tmax, ...}

    A:
        fecha | estacion | variable | valor

    Args:
        datos: Lista de registros de AEMET
        logger: Logger

    Returns:
        DataFrame en formato largo
    """
    if not datos:
        return pd.DataFrame(columns=["fecha", "estacion", "variable", "valor"])

    records = []

    for registro in datos:
        fecha_str = registro.get("fecha")
        estacion = registro.get("indicativo", "")

        if not fecha_str or not estacion:
            continue

        # Parsear fecha
        try:
            fecha = pd.to_datetime(fecha_str)
        except Exception:
            continue

        # Extraer cada variable de interés
        for campo_aemet, variable_std in VARIABLES_MAPPING.items():
            valor_str = registro.get(campo_aemet)

            if valor_str is not None and valor_str != "":
                try:
                    # AEMET usa coma como decimal en algunos casos
                    if isinstance(valor_str, str):
                        valor_str = valor_str.replace(",", ".")
                        # Manejar valores especiales como "Ip" (inapreciable)
                        if valor_str.lower() in ["ip", "acum", "varias"]:
                            valor = 0.0  # Precipitación inapreciable
                        else:
                            valor = float(valor_str)
                    else:
                        valor = float(valor_str)

                    records.append({
                        "fecha": fecha,
                        "estacion": estacion,
                        "variable": variable_std,
                        "valor": valor
                    })
                except (ValueError, TypeError):
                    continue

    return pd.DataFrame(records)


def generar_rangos_fechas(
    fecha_inicio: datetime,
    fecha_fin: datetime,
    dias_por_rango: int = DIAS_POR_PETICION
) -> List[tuple]:
    """
    Genera rangos de fechas para las peticiones (máx 31 días por petición).

    Args:
        fecha_inicio: Fecha inicial
        fecha_fin: Fecha final
        dias_por_rango: Días máximos por petición

    Returns:
        Lista de tuplas (fecha_inicio, fecha_fin)
    """
    rangos = []
    current = fecha_inicio

    while current < fecha_fin:
        fin_rango = min(current + timedelta(days=dias_por_rango), fecha_fin)
        rangos.append((current, fin_rango))
        current = fin_rango + timedelta(days=1)

    return rangos


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """Función principal que orquesta la descarga de datos AEMET."""

    # Configurar logging
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("INICIO: Descarga de datos meteorológicos AEMET")
    logger.info("=" * 70)

    # Verificar API Key
    if not AEMET_API_KEY:
        logger.error("AEMET_API_KEY no encontrada en .env")
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║  CONFIGURACIÓN REQUERIDA                                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  1. Obtén tu API Key en: https://opendata.aemet.es/centrodedescargas/altaUsuario
║  2. Añade a tu archivo .env:                                                 ║
║     AEMET_API_KEY=tu_clave_aqui                                              ║
║  3. Vuelve a ejecutar este script                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
        return

    logger.info("API Key configurada correctamente")

    # Crear directorio de salida
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Buscar estaciones de Valencia
    estaciones = buscar_estaciones_valencia(logger)

    if not estaciones:
        logger.error("No se encontraron estaciones para descargar")
        return

    # Configurar rango de fechas
    # Modo LITE: solo el último año de datos
    fecha_fin = datetime.now() - timedelta(days=1)  # Ayer
    fecha_inicio = fecha_fin - timedelta(days=365 * AÑOS_HISTORICO)

    logger.info(f"Modo LITE: descargando {AÑOS_HISTORICO} año(s) de datos")
    logger.info(f"Rango de fechas: {fecha_inicio.date()} → {fecha_fin.date()}")
    logger.info(f"Estaciones a procesar: {len(estaciones)}")

    # Generar rangos de fechas
    rangos = generar_rangos_fechas(fecha_inicio, fecha_fin)
    logger.info(f"Peticiones necesarias por estación: {len(rangos)}")

    # Recopilar datos
    all_data = []
    estaciones_con_datos = 0
    estaciones_sin_datos = []

    for codigo, nombre in estaciones.items():
        logger.info(f"\nProcesando estación: {codigo} - {nombre}")

        datos_estacion = []
        rangos_con_datos = 0

        for i, (fecha_ini, fecha_fin_rango) in enumerate(rangos):
            # Mostrar progreso
            if (i + 1) % 10 == 0 or i == 0:
                logger.info(
                    f"  Rango {i + 1}/{len(rangos)}: {fecha_ini.date()} - {fecha_fin_rango.date()}")

            # Obtener datos
            datos = get_climatologia_diaria(
                codigo, fecha_ini, fecha_fin_rango, logger)

            if datos:
                datos_estacion.extend(datos)
                rangos_con_datos += 1

            # Respetar rate limit
            time.sleep(DELAY_ENTRE_PETICIONES)

        if datos_estacion:
            # Transformar al formato estándar
            df_estacion = transform_to_long_format(datos_estacion, logger)

            if not df_estacion.empty:
                all_data.append(df_estacion)
                estaciones_con_datos += 1
                logger.info(f"  ✓ {len(df_estacion)} registros obtenidos")
        else:
            estaciones_sin_datos.append(f"{codigo} ({nombre})")
            logger.warning(f"  ✗ Sin datos disponibles para esta estación")

    # Verificar si hay datos
    if not all_data:
        logger.error("No se obtuvieron datos de ninguna estación")
        logger.info("Posibles causas:")
        logger.info("  - Las estaciones no tienen datos en el rango de fechas")
        logger.info("  - Los datos históricos no están disponibles vía API")
        logger.info("  - Problemas de conexión o rate limiting")
        return

    # Combinar todos los datos
    logger.info("\n" + "-" * 70)
    logger.info("GENERANDO ARCHIVO DE SALIDA")
    logger.info("-" * 70)

    combined = pd.concat(all_data, ignore_index=True)

    # Eliminar duplicados y ordenar
    combined = combined.drop_duplicates(
        subset=["fecha", "estacion", "variable"])
    combined = combined.sort_values(
        ["fecha", "estacion", "variable"]).reset_index(drop=True)

    # Guardar CSV
    output_file = OUTPUT_DIR / "aemet_valencia_historico.csv"
    combined.to_csv(output_file, index=False, encoding="utf-8")

    # Estadísticas finales
    fecha_min = combined["fecha"].min().strftime("%Y-%m-%d")
    fecha_max = combined["fecha"].max().strftime("%Y-%m-%d")
    estaciones_unicas = combined["estacion"].nunique()
    variables_unicas = combined["variable"].unique().tolist()

    logger.info("")
    logger.info("=" * 70)
    logger.info("DESCARGA COMPLETADA")
    logger.info("=" * 70)
    logger.info(f"✓ Guardado: {output_file.name}")
    logger.info(f"  Registros totales: {len(combined):,}")
    logger.info(f"  Estaciones con datos: {estaciones_con_datos}")
    logger.info(f"  Variables: {', '.join(variables_unicas[:5])}...")
    logger.info(f"  Periodo: {fecha_min} → {fecha_max}")
    logger.info(f"  Ubicación: {output_file}")

    if estaciones_sin_datos:
        logger.info("")
        logger.info("Estaciones sin datos disponibles:")
        for est in estaciones_sin_datos[:5]:
            logger.info(f"  - {est}")
        if len(estaciones_sin_datos) > 5:
            logger.info(f"  ... y {len(estaciones_sin_datos) - 5} más")

    logger.info("")
    logger.info("NOTA: Si necesitas datos históricos más antiguos,")
    logger.info("      es posible que debas solicitarlos directamente a AEMET.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
