# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 2.2: Procesamiento de Datos Históricos EEA (European Environment Agency)
==============================================================================

Descripción:
    Este script procesa archivos Parquet descargados manualmente desde:
    https://eeadmz1-downloads-webapp.azurewebsites.net/
    
    Los datos corresponden a "Verified data - E1a" en formato Parquet.
    Un archivo por sampling point y contaminante.

Formato de archivos EEA:
    - Extensión: .parquet
    - Nombre: SP_[codigo_estacion]_[pollutant_id]_[sufijo].parquet
    - Columnas: Samplingpoint, Pollutant, Start, End, Value, Unit, AggType, etc.
    
Uso:
    1. Descargar archivos Parquet desde el portal EEA
    2. Colocarlos en: 1.DATOS_EN_CRUDO/estaticos/eea/raw/
    3. Ejecutar: python procesar_eea_historico.py
    
Salida:
    - 1.DATOS_EN_CRUDO/estaticos/eea/eea_valencia_filtrado.csv
    - Formato: [fecha, estacion, variable, valor]

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Optional, Generator
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas base (relativas al directorio raíz del proyecto)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "eea" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "eea"
OUTPUT_FILE = OUTPUT_DIR / "eea_valencia_filtrado.csv"
LOG_DIR = PROJECT_ROOT / "logs"

# Códigos de contaminantes EEA (según especificación E1a)
# Referencia: https://dd.eionet.europa.eu/vocabulary/aq/pollutant
POLLUTANT_CODES = {
    1: "SO2",      # Sulphur dioxide
    5: "PM10",     # Particulate Matter < 10 µm
    6001: "PM2.5", # Particulate Matter < 2.5 µm
    7: "O3",       # Ozone
    8: "NO2",      # Nitrogen dioxide
    10: "CO",      # Carbon monoxide
    # Códigos alternativos que pueden aparecer
    38: "NO2",     # NO2 (código alternativo)
    9: "NOx",      # Nitrogen oxides
}

# Contaminantes de interés para el proyecto
POLLUTANTS_OF_INTEREST = ["NO2", "O3", "PM10", "PM2.5"]

# Prefijos de código de estación para Valencia (Comunidad Valenciana)
# Formato Samplingpoint: ES/SP_462XXXXX_... (462 = Valencia provincia)
# También puede ser 460, 461, 462, 463 para diferentes zonas de la CV
VALENCIA_STATION_PREFIXES = [
    "ES/SP_460",  # Castellón
    "ES/SP_461",  # Castellón
    "ES/SP_462",  # Valencia
    "ES/SP_463",  # Alicante
    "ES/SP_030",  # Alicante (código alternativo)
    "ES/SP_120",  # Castellón (código alternativo)
]

# Para filtrar SOLO Valencia ciudad y área metropolitana
VALENCIA_CITY_PREFIXES = [
    "ES/SP_4625",  # Valencia ciudad y área metropolitana
]


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging para el script.
    
    Returns:
        logging.Logger: Instancia del logger configurado
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file = LOG_DIR / "eea_historico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    logger = logging.getLogger("EEA_Historico")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# ==============================================================================
# FUNCIONES DE UTILIDAD
# ==============================================================================

def get_parquet_files(input_dir: Path) -> Generator[Path, None, None]:
    """
    Genera rutas a todos los archivos Parquet en el directorio (recursivo).
    
    Args:
        input_dir: Directorio raíz donde buscar
    
    Yields:
        Path: Ruta a cada archivo .parquet encontrado
    """
    for parquet_file in input_dir.rglob("*.parquet"):
        yield parquet_file


def extract_station_code(samplingpoint: str) -> str:
    """
    Extrae el código de estación del Samplingpoint.
    
    Formato: ES/SP_46250030_10_M -> 46250030
    
    Args:
        samplingpoint: String completo del samplingpoint
    
    Returns:
        Código de estación (8 dígitos)
    """
    if not samplingpoint or not isinstance(samplingpoint, str):
        return "unknown"
    
    # Formato típico: ES/SP_XXXXXXXX_YY_Z
    parts = samplingpoint.split("_")
    if len(parts) >= 2:
        # El código de estación está después de ES/SP_
        station = parts[0].replace("ES/SP_", "")
        if station.isdigit():
            return station
        # Si el primer split no funcionó, intentar con el segundo elemento
        if len(parts) > 1 and parts[1].isdigit():
            return parts[1][:8]  # Primeros 8 dígitos
    
    return samplingpoint


def is_valencia_station(samplingpoint: str, city_only: bool = False) -> bool:
    """
    Verifica si el samplingpoint corresponde a Valencia.
    
    Args:
        samplingpoint: String del samplingpoint
        city_only: Si True, filtra solo Valencia ciudad
    
    Returns:
        True si es una estación de Valencia
    """
    if not samplingpoint:
        return False
    
    prefixes = VALENCIA_CITY_PREFIXES if city_only else VALENCIA_STATION_PREFIXES
    return any(samplingpoint.startswith(prefix) for prefix in prefixes)


def get_pollutant_name(pollutant_code: int) -> Optional[str]:
    """
    Convierte código numérico de contaminante a nombre estándar.
    
    Args:
        pollutant_code: Código numérico del contaminante
    
    Returns:
        Nombre del contaminante o None si no está en la lista
    """
    return POLLUTANT_CODES.get(pollutant_code)


def is_pollutant_of_interest(pollutant_code: int) -> bool:
    """
    Verifica si el contaminante es de los que nos interesan.
    
    Args:
        pollutant_code: Código numérico del contaminante
    
    Returns:
        True si es un contaminante de interés
    """
    name = get_pollutant_name(pollutant_code)
    return name in POLLUTANTS_OF_INTEREST if name else False


# ==============================================================================
# FUNCIONES DE PROCESAMIENTO
# ==============================================================================

def process_parquet_file(
    file_path: Path, 
    logger: logging.Logger,
    city_only: bool = False
) -> Optional[pd.DataFrame]:
    """
    Procesa un archivo Parquet individual y extrae datos de Valencia.
    
    Args:
        file_path: Ruta al archivo .parquet
        logger: Logger
        city_only: Si True, filtra solo Valencia ciudad
    
    Returns:
        DataFrame con datos filtrados o None si no hay datos relevantes
    """
    try:
        # Leer el archivo Parquet
        df = pd.read_parquet(file_path)
        
        if df.empty:
            logger.debug(f"Archivo vacío: {file_path.name}")
            return None
        
        logger.debug(f"Leyendo {file_path.name}: {len(df)} filas")
        
        # Verificar columnas requeridas
        required_cols = ["Samplingpoint", "Pollutant", "Start", "Value"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"Columnas faltantes en {file_path.name}: {missing_cols}")
            return None
        
        # Filtrar por estaciones de Valencia
        df_valencia = df[df["Samplingpoint"].apply(
            lambda x: is_valencia_station(x, city_only)
        )]
        
        if df_valencia.empty:
            logger.debug(f"Sin datos de Valencia en: {file_path.name}")
            return None
        
        # Filtrar por contaminantes de interés
        df_filtered = df_valencia[df_valencia["Pollutant"].apply(is_pollutant_of_interest)]
        
        if df_filtered.empty:
            logger.debug(f"Sin contaminantes de interés en: {file_path.name}")
            return None
        
        logger.info(f"  ✓ {file_path.name}: {len(df_filtered)} registros de Valencia")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error procesando {file_path.name}: {str(e)}")
        return None


def transform_to_standard_format(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Transforma el DataFrame EEA al formato estándar del proyecto.
    
    De:
        Samplingpoint | Pollutant | Start | Value | ...
        ES/SP_46250030_10_M | 5 | 2014-01-01 | 4 | ...
    
    A:
        fecha | estacion | variable | valor
        2014-01-01 | 46250030 | PM10 | 4.0
    
    Args:
        df: DataFrame con datos EEA
        logger: Logger
    
    Returns:
        DataFrame en formato estándar
    """
    if df.empty:
        return pd.DataFrame(columns=["fecha", "estacion", "variable", "valor"])
    
    # Crear DataFrame transformado
    transformed = pd.DataFrame({
        "fecha": pd.to_datetime(df["Start"], errors="coerce"),
        "estacion": df["Samplingpoint"].apply(extract_station_code),
        "variable": df["Pollutant"].apply(get_pollutant_name),
        "valor": pd.to_numeric(df["Value"], errors="coerce")
    })
    
    # Eliminar filas con valores nulos
    transformed = transformed.dropna()
    
    # Filtrar solo contaminantes de interés (por si acaso)
    transformed = transformed[transformed["variable"].isin(POLLUTANTS_OF_INTEREST)]
    
    # Extraer solo la fecha (sin hora) para consistencia con GVA
    transformed["fecha"] = transformed["fecha"].dt.date
    transformed["fecha"] = pd.to_datetime(transformed["fecha"])
    
    logger.debug(f"Transformación: {len(transformed)} registros")
    
    return transformed


def clean_and_validate(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Limpia y valida los datos procesados.
    
    Args:
        df: DataFrame a limpiar
        logger: Logger
    
    Returns:
        DataFrame limpio y validado
    """
    if df.empty:
        return df
    
    original_count = len(df)
    
    # Eliminar valores negativos
    df = df[df["valor"] >= 0]
    
    # Eliminar outliers extremos (> 1000 µg/m³)
    df = df[df["valor"] < 1000]
    
    # Eliminar duplicados (misma fecha, estación, variable)
    df = df.drop_duplicates(subset=["fecha", "estacion", "variable"])
    
    # Ordenar por fecha, estación y variable
    df = df.sort_values(["fecha", "estacion", "variable"]).reset_index(drop=True)
    
    final_count = len(df)
    removed = original_count - final_count
    
    if removed > 0:
        logger.info(f"Limpieza: {removed} registros eliminados ({final_count} válidos)")
    
    return df


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Función principal que orquesta el procesamiento de datos EEA.
    """
    # Configurar logging
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("INICIO: Procesamiento de datos históricos EEA")
    logger.info("=" * 70)
    
    # Verificar directorio de entrada
    if not INPUT_DIR.exists():
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.warning(f"Directorio de entrada creado: {INPUT_DIR}")
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  INSTRUCCIONES PARA DESCARGAR DATOS EEA                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  1. Visita: https://eeadmz1-downloads-webapp.azurewebsites.net/              ║
║  2. Selecciona "Verified data - E1a"                                         ║
║  3. Filtra por país: Spain (ES)                                              ║
║  4. Descarga los archivos .parquet                                           ║
║  5. Colócalos en:                                                            ║
║     {str(INPUT_DIR):<66} ║
║  6. Vuelve a ejecutar este script                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
        return
    
    # Crear directorio de salida
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Buscar archivos Parquet
    parquet_files = list(get_parquet_files(INPUT_DIR))
    
    if not parquet_files:
        logger.warning(f"No se encontraron archivos .parquet en: {INPUT_DIR}")
        logger.info("Descarga los datos desde el portal EEA.")
        return
    
    logger.info(f"Archivos Parquet encontrados: {len(parquet_files)}")
    
    # Procesar archivos de forma iterativa (sin cargar todo en memoria)
    all_data: List[pd.DataFrame] = []
    files_processed = 0
    files_with_data = 0
    
    for file_path in parquet_files:
        logger.info(f"Procesando: {file_path.name}")
        
        # Procesar archivo individual
        df_filtered = process_parquet_file(file_path, logger, city_only=False)
        
        files_processed += 1
        
        if df_filtered is not None and not df_filtered.empty:
            # Transformar al formato estándar
            df_standard = transform_to_standard_format(df_filtered, logger)
            
            if not df_standard.empty:
                all_data.append(df_standard)
                files_with_data += 1
    
    # Verificar si hay datos
    if not all_data:
        logger.error("No se encontraron datos de Valencia en ningún archivo.")
        logger.info("Verifica que los archivos Parquet contienen estaciones de Valencia (código 462XXXXX)")
        return
    
    # Combinar todos los datos
    logger.info("-" * 70)
    logger.info("COMBINANDO Y LIMPIANDO DATOS")
    logger.info("-" * 70)
    
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total combinado: {len(combined)} registros")
    
    # Limpiar y validar
    cleaned = clean_and_validate(combined, logger)
    
    if cleaned.empty:
        logger.error("No quedaron datos válidos después de la limpieza.")
        return
    
    # Guardar CSV
    cleaned.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    
    # Estadísticas finales
    fecha_min = cleaned["fecha"].min().strftime("%Y-%m-%d")
    fecha_max = cleaned["fecha"].max().strftime("%Y-%m-%d")
    estaciones = cleaned["estacion"].nunique()
    variables = cleaned["variable"].unique().tolist()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("PROCESAMIENTO COMPLETADO")
    logger.info("=" * 70)
    logger.info(f"Archivos procesados: {files_processed}")
    logger.info(f"Archivos con datos de Valencia: {files_with_data}")
    logger.info(f"")
    logger.info(f"✓ Guardado: {OUTPUT_FILE.name}")
    logger.info(f"  Registros totales: {len(cleaned):,}")
    logger.info(f"  Estaciones únicas: {estaciones}")
    logger.info(f"  Variables: {', '.join(variables)}")
    logger.info(f"  Periodo: {fecha_min} → {fecha_max}")
    logger.info(f"  Ubicación: {OUTPUT_FILE}")
    logger.info("")
    
    # Mostrar resumen por estación
    logger.info("Registros por estación:")
    station_counts = cleaned.groupby("estacion").size().sort_values(ascending=False)
    for station, count in station_counts.head(10).items():
        logger.info(f"  {station}: {count:,} registros")
    
    if len(station_counts) > 10:
        logger.info(f"  ... y {len(station_counts) - 10} estaciones más")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
