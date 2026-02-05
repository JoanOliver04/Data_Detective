# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 2.1: Procesamiento de Datos Históricos GVA (Calidad del Aire)
==============================================================================

Descripción:
    Este script procesa archivos TXT descargados manualmente desde:
    https://mediambient.gva.es/es/web/calidad-ambiental/datos-historicos
    
    La GVA NO ofrece un endpoint API REST para descarga automática masiva.
    Los datos deben descargarse manualmente desde el portal web.

Formato de archivos GVA:
    - Extensión: .txt
    - Separador: tabulador
    - Decimales: coma (formato europeo)
    - Cabecera: 3-4 líneas con metadatos (Red, Estación)
    - Nombre archivo: MDEST[codigo_estacion][año].txt
    
Uso:
    1. Descargar archivos históricos desde la web de GVA
    2. Colocarlos en: 1.DATOS_EN_CRUDO/estaticos/contaminacion/raw/
    3. Ejecutar: python descargar_gva_historico.py
    
Salida:
    - 1.DATOS_EN_CRUDO/estaticos/contaminacion/gva_[estacion]_historico.csv
    - Formato: [fecha, estacion, variable, valor]

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import pandas as pd
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas base (relativas al directorio raíz del proyecto)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "contaminacion" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "contaminacion"
LOG_DIR = PROJECT_ROOT / "logs"

# Estaciones de Valencia a filtrar (actualizado con códigos reales)
# Formato: código -> nombre descriptivo
ESTACIONES_VALENCIA = {
    "46250001": "València - Centro (Avd. Francia)",
    "46250030": "València - Pista de Silla",
    "46250047": "València - Politècnic",
    "46250050": "València - Molí del Sol",
    # Añadir más estaciones según se descubran en los archivos
}

# Variables de contaminación a extraer (las que nos interesan)
VARIABLES_INTERES = ["NO2", "SO2", "O3", "PM10", "PM2.5", "CO"]

# Mapeo de nombres de columnas en archivos GVA a nombres estándar
COLUMN_NORMALIZE = {
    "PM2.5": "PM2.5",
    "PM25": "PM2.5",
    "PM10": "PM10",
    "NO2": "NO2",
    "NO": "NO",      # Óxido nítrico (no es NO2, pero puede ser útil)
    "NOX": "NOx",    # Óxidos de nitrógeno totales
    "NOx": "NOx",
    "O3": "O3",
    "SO2": "SO2",
    "CO": "CO",
    "C6H6": "C6H6",  # Benceno
    "C7H8": "C7H8",  # Tolueno
    "C8H10": "C8H10" # Xileno
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura el sistema de logging para el script.
    
    Returns:
        logging.Logger: Instancia del logger configurado
    """
    # Crear directorio de logs si no existe
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Nombre del archivo de log
    log_file = LOG_DIR / "gva_historico.log"
    
    # Configurar formato
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Crear logger
    logger = logging.getLogger("GVA_Historico")
    logger.setLevel(logging.DEBUG)
    
    # Limpiar handlers existentes (evita duplicados en re-ejecuciones)
    logger.handlers.clear()
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Añadir handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# ==============================================================================
# FUNCIONES DE PARSING ESPECÍFICAS PARA FORMATO GVA
# ==============================================================================

def extract_station_from_filename(filename: str) -> Optional[str]:
    """
    Extrae el código de estación del nombre del archivo GVA.
    
    Formato esperado: MDEST[codigo][año].txt
    Ejemplo: MDEST462500302024.txt -> 46250030
    
    Args:
        filename: Nombre del archivo
    
    Returns:
        Código de estación (8 dígitos) o None
    """
    # Patrón: MDEST seguido de 8 dígitos (código estación) y 4 dígitos (año)
    match = re.search(r'MDEST(\d{8})(\d{4})', filename)
    if match:
        return match.group(1)
    
    # Patrón alternativo: buscar secuencia de 8 dígitos que empiece por 462
    match = re.search(r'(462\d{5})', filename)
    if match:
        return match.group(1)
    
    return None


def extract_station_from_header(lines: List[str]) -> Optional[str]:
    """
    Extrae el código de estación de la cabecera del archivo.
    
    Busca línea como: "Estación: 46250030-València - Pista de Silla"
    
    Args:
        lines: Primeras líneas del archivo
    
    Returns:
        Código de estación o None
    """
    for line in lines[:10]:  # Solo revisar primeras 10 líneas
        if "estación" in line.lower() or "estacion" in line.lower():
            # Buscar patrón de código: 8 dígitos empezando por 462
            match = re.search(r'(462\d{5})', line)
            if match:
                return match.group(1)
    return None


def extract_station_name_from_header(lines: List[str]) -> Optional[str]:
    """
    Extrae el nombre descriptivo de la estación de la cabecera.
    
    Args:
        lines: Primeras líneas del archivo
    
    Returns:
        Nombre de la estación o None
    """
    for line in lines[:10]:
        if "estación" in line.lower() or "estacion" in line.lower():
            # Formato: "Estación: 46250030-València - Pista de Silla"
            match = re.search(r'\d+-(.+)$', line.strip())
            if match:
                return match.group(1).strip()
    return None


def find_data_start_line(lines: List[str]) -> int:
    """
    Encuentra la línea donde empiezan los datos (después de cabecera).
    
    Busca la línea que contiene "FECHA" como indicador del header de datos.
    
    Args:
        lines: Todas las líneas del archivo
    
    Returns:
        Índice de la línea con los headers de columnas
    """
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("FECHA"):
            return i
    return 0  # Si no encuentra, asumir que empieza desde el principio


def parse_gva_file(file_path: Path, logger: logging.Logger) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """
    Parsea un archivo TXT de GVA con su formato específico.
    
    Args:
        file_path: Ruta al archivo .txt
        logger: Logger
    
    Returns:
        Tuple de (DataFrame, código_estación, nombre_estación)
    """
    logger.debug(f"Parseando archivo: {file_path.name}")
    
    # Intentar diferentes codificaciones
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    lines = None
    used_encoding = None
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                lines = f.readlines()
            used_encoding = encoding
            break
        except UnicodeDecodeError:
            continue
    
    if lines is None:
        logger.error(f"No se pudo leer el archivo con ninguna codificación: {file_path.name}")
        return None, None, None
    
    logger.debug(f"Archivo leído con codificación: {used_encoding}")
    
    # Extraer información de estación
    station_code = extract_station_from_filename(file_path.name)
    if not station_code:
        station_code = extract_station_from_header(lines)
    
    station_name = extract_station_name_from_header(lines)
    
    if not station_code:
        logger.warning(f"No se pudo determinar código de estación: {file_path.name}")
        return None, None, None
    
    logger.debug(f"Estación detectada: {station_code} - {station_name}")
    
    # Encontrar línea de inicio de datos
    header_line_idx = find_data_start_line(lines)
    
    # La fila de unidades está justo después del header (la saltamos)
    # Formato:
    #   FECHA  PM2.5  NO2  ...   <- header_line_idx
    #          µg/m³  µg/m³ ...  <- unidades (skip)
    #   01/01/2024  6  25  ...   <- datos
    
    # Leer solo las líneas de datos (saltando cabecera de metadatos y unidades)
    try:
        df = pd.read_csv(
            file_path,
            sep='\t',
            skiprows=header_line_idx,
            encoding=used_encoding,
            decimal=',',  # Decimales con coma (formato europeo)
            na_values=['', ' ', '-', '--', 'N/D', 'n/d'],
            skipinitialspace=True
        )
    except Exception as e:
        logger.error(f"Error leyendo datos de {file_path.name}: {e}")
        return None, None, None
    
    # Verificar si la primera fila son unidades (µg/m³, mg/m³, etc.)
    if len(df) > 0:
        first_row = df.iloc[0]
        # Detectar si la primera fila contiene unidades
        is_units_row = any(
            'µg' in str(val) or 'mg' in str(val) or 'm³' in str(val)
            for val in first_row.values if pd.notna(val)
        )
        if is_units_row:
            logger.debug("Detectada fila de unidades, eliminando...")
            df = df.iloc[1:].reset_index(drop=True)
    
    # Limpiar nombres de columnas
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    
    logger.info(f"Archivo parseado: {file_path.name} -> {len(df)} filas, {len(df.columns)} columnas")
    logger.debug(f"Columnas encontradas: {list(df.columns)}")
    
    return df, station_code, station_name


# ==============================================================================
# FUNCIONES DE TRANSFORMACIÓN
# ==============================================================================

def transform_to_long_format(
    df: pd.DataFrame, 
    station_code: str, 
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Transforma el DataFrame de formato ancho a formato largo normalizado.
    
    De:
        FECHA | PM2.5 | NO2 | O3 | ...
        01/01/2024 | 6 | 25 | 29 | ...
    
    A:
        fecha | estacion | variable | valor
        2024-01-01 | 46250030 | PM2.5 | 6.0
        2024-01-01 | 46250030 | NO2 | 25.0
    
    Args:
        df: DataFrame en formato ancho
        station_code: Código de la estación
        logger: Logger
    
    Returns:
        DataFrame en formato largo [fecha, estacion, variable, valor]
    """
    if df.empty:
        return pd.DataFrame(columns=["fecha", "estacion", "variable", "valor"])
    
    # Identificar columna de fecha
    fecha_col = None
    for col in df.columns:
        if col.upper().strip() == "FECHA":
            fecha_col = col
            break
    
    if fecha_col is None:
        logger.error("No se encontró columna FECHA")
        return pd.DataFrame(columns=["fecha", "estacion", "variable", "valor"])
    
    # Identificar columnas de variables (todas excepto FECHA)
    variable_columns = [col for col in df.columns if col != fecha_col]
    
    # Filtrar solo las variables de interés
    variables_to_process = []
    for col in variable_columns:
        col_upper = col.upper().strip()
        # Verificar si está en nuestra lista de interés
        if col_upper in [v.upper() for v in VARIABLES_INTERES]:
            variables_to_process.append(col)
        elif col_upper in COLUMN_NORMALIZE:
            normalized = COLUMN_NORMALIZE[col_upper]
            if normalized in VARIABLES_INTERES:
                variables_to_process.append(col)
    
    if not variables_to_process:
        logger.warning(f"No se encontraron variables de interés. Columnas disponibles: {variable_columns}")
        # Procesar todas las variables numéricas si no hay específicas
        variables_to_process = variable_columns
    
    logger.debug(f"Variables a procesar: {variables_to_process}")
    
    # Transformar a formato largo (melt)
    records = []
    
    for _, row in df.iterrows():
        fecha_raw = row[fecha_col]
        
        # Parsear fecha (formato DD/MM/YYYY)
        try:
            if isinstance(fecha_raw, str):
                fecha = pd.to_datetime(fecha_raw, format='%d/%m/%Y', errors='coerce')
            else:
                fecha = pd.to_datetime(fecha_raw, errors='coerce')
        except Exception:
            continue
        
        if pd.isna(fecha):
            continue
        
        for var_col in variables_to_process:
            valor = row[var_col]
            
            # Convertir valor a numérico
            if pd.notna(valor):
                try:
                    # Manejar valores con coma como decimal
                    if isinstance(valor, str):
                        valor = float(valor.replace(',', '.'))
                    else:
                        valor = float(valor)
                except (ValueError, TypeError):
                    continue
                
                # Normalizar nombre de variable
                var_name = var_col.upper().strip()
                if var_name in COLUMN_NORMALIZE:
                    var_name = COLUMN_NORMALIZE[var_name]
                
                # Solo incluir si es una variable de interés
                if var_name in VARIABLES_INTERES:
                    records.append({
                        "fecha": fecha,
                        "estacion": station_code,
                        "variable": var_name,
                        "valor": valor
                    })
    
    result = pd.DataFrame(records)
    logger.debug(f"Transformación: {len(result)} registros generados")
    
    return result


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
    
    # Asegurar que fecha es datetime
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    
    # Eliminar filas con fechas inválidas
    df = df.dropna(subset=["fecha"])
    
    # Convertir valor a numérico
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    
    # Eliminar valores nulos
    df = df.dropna(subset=["valor"])
    
    # Eliminar valores negativos (no tiene sentido para concentraciones)
    df = df[df["valor"] >= 0]
    
    # Eliminar outliers extremos (valores > 1000 µg/m³ son sospechosos)
    # Nota: ajustar según umbrales realistas por variable si es necesario
    df = df[df["valor"] < 1000]
    
    # Eliminar duplicados
    df = df.drop_duplicates(subset=["fecha", "estacion", "variable"])
    
    # Ordenar por fecha y variable
    df = df.sort_values(["fecha", "variable"]).reset_index(drop=True)
    
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
    Función principal que orquesta el procesamiento de datos GVA.
    """
    # Configurar logging
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("INICIO: Procesamiento de datos históricos GVA (Calidad del Aire)")
    logger.info("=" * 70)
    
    # Verificar directorio de entrada
    if not INPUT_DIR.exists():
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.warning(f"Directorio de entrada creado: {INPUT_DIR}")
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  INSTRUCCIONES PARA DESCARGAR DATOS GVA                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  1. Visita: https://mediambient.gva.es/es/web/calidad-ambiental/datos-historicos
║  2. Selecciona la estación y el año deseado                                  ║
║  3. Descarga los archivos .txt                                               ║
║  4. Colócalos en:                                                            ║
║     {str(INPUT_DIR):<66} ║
║  5. Vuelve a ejecutar este script                                            ║
║                                                                              ║
║  Formato esperado: MDEST[estacion][año].txt                                  ║
║  Ejemplo: MDEST462500302024.txt                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
        return
    
    # Crear directorio de salida
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Buscar archivos de datos
    input_files: List[Path] = list(INPUT_DIR.glob("*.txt"))
    
    # También buscar CSV/Excel por si acaso
    input_files.extend(INPUT_DIR.glob("*.csv"))
    input_files.extend(INPUT_DIR.glob("*.xlsx"))
    
    if not input_files:
        logger.warning(f"No se encontraron archivos en: {INPUT_DIR}")
        logger.info("Descarga los archivos .txt desde la web de GVA.")
        return
    
    logger.info(f"Archivos encontrados: {len(input_files)}")
    for f in input_files:
        logger.debug(f"  - {f.name}")
    
    # Diccionario para acumular datos por estación
    data_by_station: Dict[str, List[pd.DataFrame]] = {}
    station_names: Dict[str, str] = {}
    
    # Procesar cada archivo
    for file_path in input_files:
        logger.info(f"Procesando: {file_path.name}")
        
        # Parsear archivo
        df_raw, station_code, station_name = parse_gva_file(file_path, logger)
        
        if df_raw is None or df_raw.empty:
            logger.warning(f"No se obtuvieron datos de: {file_path.name}")
            continue
        
        # Guardar nombre de estación si se encontró
        if station_name and station_code:
            station_names[station_code] = station_name
        
        # Transformar a formato largo
        df_long = transform_to_long_format(df_raw, station_code, logger)
        
        if df_long.empty:
            logger.warning(f"No se generaron registros de: {file_path.name}")
            continue
        
        # Acumular por estación
        if station_code not in data_by_station:
            data_by_station[station_code] = []
        data_by_station[station_code].append(df_long)
        
        logger.info(f"  ✓ {len(df_long)} registros extraídos (estación {station_code})")
    
    # Procesar y guardar por estación
    if not data_by_station:
        logger.error("No se procesaron datos de ningún archivo.")
        return
    
    logger.info("-" * 70)
    logger.info("GENERANDO ARCHIVOS DE SALIDA")
    logger.info("-" * 70)
    
    total_records = 0
    
    for station_code, dfs in data_by_station.items():
        # Combinar todos los DataFrames de esta estación
        combined = pd.concat(dfs, ignore_index=True)
        
        # Limpiar y validar
        cleaned = clean_and_validate(combined, logger)
        
        if cleaned.empty:
            logger.warning(f"Sin datos válidos para estación {station_code}")
            continue
        
        # Nombre del archivo de salida
        output_file = OUTPUT_DIR / f"gva_{station_code}_historico.csv"
        
        # Guardar CSV
        cleaned.to_csv(output_file, index=False, encoding="utf-8")
        
        # Obtener nombre descriptivo
        nombre = station_names.get(station_code, ESTACIONES_VALENCIA.get(station_code, "Desconocida"))
        
        # Estadísticas
        fecha_min = cleaned["fecha"].min().strftime("%Y-%m-%d")
        fecha_max = cleaned["fecha"].max().strftime("%Y-%m-%d")
        variables = cleaned["variable"].unique().tolist()
        
        logger.info(f"")
        logger.info(f"✓ Guardado: {output_file.name}")
        logger.info(f"  Estación: {station_code} - {nombre}")
        logger.info(f"  Registros: {len(cleaned):,}")
        logger.info(f"  Variables: {', '.join(variables)}")
        logger.info(f"  Periodo: {fecha_min} → {fecha_max}")
        
        total_records += len(cleaned)
    
    # Resumen final
    logger.info("")
    logger.info("=" * 70)
    logger.info("PROCESAMIENTO COMPLETADO")
    logger.info("=" * 70)
    logger.info(f"Archivos procesados: {len(input_files)}")
    logger.info(f"Estaciones procesadas: {len(data_by_station)}")
    logger.info(f"Registros totales: {total_records:,}")
    logger.info(f"Salida en: {OUTPUT_DIR}")
    logger.info("")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
