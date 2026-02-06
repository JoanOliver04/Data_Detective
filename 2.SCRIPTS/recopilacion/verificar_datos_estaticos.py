# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 2.5: Verificación de Datos Estáticos
==============================================================================

Descripción:
    Este script verifica todos los datos estáticos obtenidos durante la Fase 2.
    Genera un informe completo con estadísticas y limitaciones documentadas.

Fuentes verificadas:
    - GVA (Contaminación atmosférica)
    - EEA (European Environment Agency)
    - AEMET (Meteorología)
    - DGT (Tráfico)

Uso:
    python verificar_datos_estaticos.py
    
Salida:
    - logs/informe_fase2.md (informe completo)
    - Resumen en consola

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATOS_ESTATICOS_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos"
LOG_DIR = PROJECT_ROOT / "logs"

# Estructura esperada de carpetas
FUENTES_ESPERADAS = {
    "contaminacion": {
        "nombre": "GVA - Calidad del Aire",
        "descripcion": "Datos históricos de contaminación de la Generalitat Valenciana",
        "variables": ["NO2", "SO2", "O3", "PM10", "PM2.5", "CO"],
    },
    "eea": {
        "nombre": "EEA - European Environment Agency",
        "descripcion": "Datos europeos de calidad del aire",
        "variables": ["NO2", "O3", "PM10", "PM2.5"],
    },
    "meteorologia": {
        "nombre": "AEMET - Meteorología",
        "descripcion": "Datos meteorológicos históricos",
        "variables": ["precipitacion", "temperatura", "humedad", "viento"],
    },
    "trafico": {
        "nombre": "DGT - Tráfico",
        "descripcion": "Datos de tráfico de la red estatal",
        "variables": ["intensidad", "velocidad", "incidencias"],
    },
}


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """Configura el sistema de logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file = LOG_DIR / "verificacion_fase2.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    logger = logging.getLogger("Verificacion_Fase2")
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
# FUNCIONES DE ANÁLISIS
# ==============================================================================

def analizar_csv(file_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza un archivo CSV y extrae estadísticas.
    
    Args:
        file_path: Ruta al archivo CSV
        logger: Logger
    
    Returns:
        Diccionario con estadísticas del archivo
    """
    stats = {
        "tipo": "CSV",
        "tamaño_bytes": file_path.stat().st_size,
        "registros": 0,
        "columnas": [],
        "fecha_min": None,
        "fecha_max": None,
        "variables": [],
        "estaciones": [],
        "error": None,
    }
    
    try:
        # Leer solo las primeras filas para obtener estructura
        df_sample = pd.read_csv(file_path, nrows=5)
        stats["columnas"] = list(df_sample.columns)
        
        # Contar registros totales (sin cargar todo en memoria)
        with open(file_path, 'r', encoding='utf-8') as f:
            stats["registros"] = sum(1 for _ in f) - 1  # -1 por header
        
        # Si tiene columnas esperadas, extraer más info
        if "fecha" in df_sample.columns:
            # Leer solo columna fecha para obtener rango
            df_fechas = pd.read_csv(file_path, usecols=["fecha"], parse_dates=["fecha"])
            stats["fecha_min"] = df_fechas["fecha"].min().strftime("%Y-%m-%d")
            stats["fecha_max"] = df_fechas["fecha"].max().strftime("%Y-%m-%d")
        
        if "variable" in df_sample.columns:
            df_vars = pd.read_csv(file_path, usecols=["variable"])
            stats["variables"] = df_vars["variable"].unique().tolist()
        
        if "estacion" in df_sample.columns:
            df_est = pd.read_csv(file_path, usecols=["estacion"])
            stats["estaciones"] = df_est["estacion"].unique().tolist()
            
    except Exception as e:
        stats["error"] = str(e)
        logger.warning(f"  Error analizando {file_path.name}: {e}")
    
    return stats


def analizar_parquet(file_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza un archivo Parquet y extrae estadísticas.
    
    Args:
        file_path: Ruta al archivo Parquet
        logger: Logger
    
    Returns:
        Diccionario con estadísticas del archivo
    """
    stats = {
        "tipo": "Parquet",
        "tamaño_bytes": file_path.stat().st_size,
        "registros": 0,
        "columnas": [],
        "fecha_min": None,
        "fecha_max": None,
        "error": None,
    }
    
    try:
        df = pd.read_parquet(file_path)
        stats["registros"] = len(df)
        stats["columnas"] = list(df.columns)
        
        # Buscar columnas de fecha
        for col in ["Start", "fecha", "date", "datetime"]:
            if col in df.columns:
                stats["fecha_min"] = df[col].min()
                stats["fecha_max"] = df[col].max()
                if hasattr(stats["fecha_min"], "strftime"):
                    stats["fecha_min"] = stats["fecha_min"].strftime("%Y-%m-%d")
                    stats["fecha_max"] = stats["fecha_max"].strftime("%Y-%m-%d")
                break
                
    except Exception as e:
        stats["error"] = str(e)
        logger.warning(f"  Error analizando {file_path.name}: {e}")
    
    return stats


def analizar_xml(file_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza un archivo XML (muestra de DGT).
    
    Args:
        file_path: Ruta al archivo XML
        logger: Logger
    
    Returns:
        Diccionario con estadísticas del archivo
    """
    stats = {
        "tipo": "XML",
        "tamaño_bytes": file_path.stat().st_size,
        "es_muestra": "muestra" in file_path.name.lower(),
        "error": None,
    }
    
    return stats


def analizar_markdown(file_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza un archivo Markdown (documentación).
    
    Args:
        file_path: Ruta al archivo Markdown
        logger: Logger
    
    Returns:
        Diccionario con estadísticas del archivo
    """
    stats = {
        "tipo": "Documentación",
        "tamaño_bytes": file_path.stat().st_size,
        "es_readme": "readme" in file_path.name.lower(),
        "error": None,
    }
    
    return stats


def analizar_directorio(dir_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza un directorio de fuente de datos.
    
    Args:
        dir_path: Ruta al directorio
        logger: Logger
    
    Returns:
        Diccionario con análisis completo del directorio
    """
    resultado = {
        "existe": dir_path.exists(),
        "archivos": [],
        "total_archivos": 0,
        "total_registros": 0,
        "total_bytes": 0,
        "fecha_min_global": None,
        "fecha_max_global": None,
        "tiene_datos": False,
        "tiene_documentacion": False,
        "archivos_vacios": [],
        "errores": [],
    }
    
    if not dir_path.exists():
        return resultado
    
    # Buscar todos los archivos (incluyendo subdirectorios)
    all_files = list(dir_path.rglob("*"))
    archivos = [f for f in all_files if f.is_file()]
    
    resultado["total_archivos"] = len(archivos)
    
    fechas_min = []
    fechas_max = []
    
    for archivo in archivos:
        file_info = {
            "nombre": archivo.name,
            "ruta_relativa": str(archivo.relative_to(dir_path)),
            "extension": archivo.suffix.lower(),
        }
        
        # Detectar archivos vacíos
        if archivo.stat().st_size == 0:
            resultado["archivos_vacios"].append(archivo.name)
            file_info["vacio"] = True
            resultado["archivos"].append(file_info)
            continue
        
        # Analizar según tipo
        if archivo.suffix.lower() == ".csv":
            stats = analizar_csv(archivo, logger)
            file_info.update(stats)
            resultado["tiene_datos"] = True
            resultado["total_registros"] += stats.get("registros", 0)
            
            if stats.get("fecha_min"):
                fechas_min.append(stats["fecha_min"])
            if stats.get("fecha_max"):
                fechas_max.append(stats["fecha_max"])
                
        elif archivo.suffix.lower() == ".parquet":
            stats = analizar_parquet(archivo, logger)
            file_info.update(stats)
            resultado["tiene_datos"] = True
            resultado["total_registros"] += stats.get("registros", 0)
            
            if stats.get("fecha_min"):
                fechas_min.append(stats["fecha_min"])
            if stats.get("fecha_max"):
                fechas_max.append(stats["fecha_max"])
                
        elif archivo.suffix.lower() == ".xml":
            stats = analizar_xml(archivo, logger)
            file_info.update(stats)
            resultado["tiene_datos"] = True
            
        elif archivo.suffix.lower() == ".md":
            stats = analizar_markdown(archivo, logger)
            file_info.update(stats)
            resultado["tiene_documentacion"] = True
            
        else:
            file_info["tipo"] = "Otro"
        
        file_info["tamaño_bytes"] = archivo.stat().st_size
        resultado["total_bytes"] += archivo.stat().st_size
        resultado["archivos"].append(file_info)
        
        if file_info.get("error"):
            resultado["errores"].append(f"{archivo.name}: {file_info['error']}")
    
    # Calcular rango temporal global
    if fechas_min:
        resultado["fecha_min_global"] = min(fechas_min)
    if fechas_max:
        resultado["fecha_max_global"] = max(fechas_max)
    
    return resultado


def formatear_bytes(bytes_val: int) -> str:
    """Formatea bytes a unidad legible."""
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024 * 1024 * 1024:
        return f"{bytes_val / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_val / (1024 * 1024 * 1024):.2f} GB"


# ==============================================================================
# GENERACIÓN DE INFORME
# ==============================================================================

def generar_informe(resultados: Dict[str, Dict], logger: logging.Logger) -> Path:
    """
    Genera el informe de verificación en formato Markdown.
    
    Args:
        resultados: Diccionario con resultados por fuente
        logger: Logger
    
    Returns:
        Ruta al archivo de informe generado
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    informe_path = LOG_DIR / "informe_fase2.md"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Calcular totales
    total_archivos = sum(r.get("total_archivos", 0) for r in resultados.values())
    total_registros = sum(r.get("total_registros", 0) for r in resultados.values())
    total_bytes = sum(r.get("total_bytes", 0) for r in resultados.values())
    
    content = f"""# 📊 Informe de Verificación - Fase 2: Datos Estáticos

**Proyecto**: Data Detective Valencia  
**Fecha de verificación**: {timestamp}  
**Directorio analizado**: `1.DATOS_EN_CRUDO/estaticos/`

---

## 📈 Resumen Ejecutivo

| Métrica | Valor |
|---------|-------|
| **Fuentes verificadas** | {len(resultados)} |
| **Total archivos** | {total_archivos} |
| **Total registros** | {total_registros:,} |
| **Tamaño total** | {formatear_bytes(total_bytes)} |

### Estado por Fuente

| Fuente | Datos | Documentación | Registros | Periodo |
|--------|:-----:|:-------------:|----------:|---------|
"""
    
    for fuente, resultado in resultados.items():
        info = FUENTES_ESPERADAS.get(fuente, {})
        nombre = info.get("nombre", fuente.upper())
        tiene_datos = "✅" if resultado.get("tiene_datos") else "❌"
        tiene_doc = "✅" if resultado.get("tiene_documentacion") else "➖"
        registros = f"{resultado.get('total_registros', 0):,}"
        
        if resultado.get("fecha_min_global") and resultado.get("fecha_max_global"):
            periodo = f"{resultado['fecha_min_global']} → {resultado['fecha_max_global']}"
        else:
            periodo = "N/A"
        
        content += f"| {nombre} | {tiene_datos} | {tiene_doc} | {registros} | {periodo} |\n"
    
    content += """
---

## 📁 Detalle por Fuente

"""
    
    for fuente, resultado in resultados.items():
        info = FUENTES_ESPERADAS.get(fuente, {})
        nombre = info.get("nombre", fuente.upper())
        descripcion = info.get("descripcion", "")
        
        content += f"""### {nombre}

**Descripción**: {descripcion}  
**Directorio**: `1.DATOS_EN_CRUDO/estaticos/{fuente}/`

"""
        
        if not resultado.get("existe"):
            content += "> ⚠️ **Directorio no encontrado**\n\n"
            continue
        
        if resultado.get("total_archivos", 0) == 0:
            content += "> ℹ️ **Directorio vacío**\n\n"
            continue
        
        # Estadísticas
        content += f"""**Estadísticas**:
- Archivos: {resultado.get('total_archivos', 0)}
- Registros totales: {resultado.get('total_registros', 0):,}
- Tamaño: {formatear_bytes(resultado.get('total_bytes', 0))}
"""
        
        if resultado.get("fecha_min_global"):
            content += f"- Periodo: {resultado['fecha_min_global']} → {resultado['fecha_max_global']}\n"
        
        content += "\n**Archivos**:\n\n"
        content += "| Archivo | Tipo | Registros | Tamaño |\n"
        content += "|---------|------|----------:|-------:|\n"
        
        for archivo in resultado.get("archivos", []):
            nombre_archivo = archivo.get("ruta_relativa", archivo.get("nombre", "?"))
            tipo = archivo.get("tipo", "?")
            registros = archivo.get("registros", "-")
            if isinstance(registros, int):
                registros = f"{registros:,}"
            tamaño = formatear_bytes(archivo.get("tamaño_bytes", 0))
            
            content += f"| `{nombre_archivo}` | {tipo} | {registros} | {tamaño} |\n"
        
        # Archivos vacíos
        if resultado.get("archivos_vacios"):
            content += f"\n> ⚠️ **Archivos vacíos**: {', '.join(resultado['archivos_vacios'])}\n"
        
        # Errores
        if resultado.get("errores"):
            content += "\n> ❌ **Errores encontrados**:\n"
            for error in resultado["errores"]:
                content += f"> - {error}\n"
        
        content += "\n"
    
    # Sección de limitaciones
    content += """---

## ⚠️ Limitaciones Documentadas

### DGT - Tráfico
- **Sin datos históricos públicos** vía API
- Los endpoints DATEX II solo ofrecen datos en tiempo real
- Los históricos se construirán por acumulación en Fase 3

### AEMET - Meteorología
- API con **rate limiting** estricto
- No todos los datos históricos disponibles vía API
- Datos anteriores a cierta fecha requieren solicitud directa a AEMET

### GVA - Contaminación
- Datos descargados **manualmente** desde portal web
- No existe API REST pública para descarga masiva

### EEA - Datos Europeos
- Archivos **muy grandes** (requieren procesamiento con chunks)
- Descarga manual desde portal

---

## ✅ Conclusiones

"""
    
    # Determinar conclusiones automáticas
    fuentes_con_datos = sum(1 for r in resultados.values() if r.get("tiene_datos"))
    fuentes_documentadas = sum(1 for r in resultados.values() if r.get("tiene_documentacion"))
    
    if fuentes_con_datos >= 3:
        content += "✅ **Fase 2 completada satisfactoriamente**\n\n"
    else:
        content += "⚠️ **Fase 2 parcialmente completada**\n\n"
    
    content += f"""- {fuentes_con_datos}/4 fuentes con datos recopilados
- {fuentes_documentadas}/4 fuentes con documentación
- Total de {total_registros:,} registros disponibles para análisis
- Tamaño total del dataset: {formatear_bytes(total_bytes)}

### Próximos pasos (Fase 3)
1. Implementar scripts de captura de datos dinámicos
2. Configurar Task Scheduler para automatización
3. Comenzar acumulación de históricos de tráfico DGT

---

*Informe generado automáticamente por Data Detective*  
*Verificación de Fase 2 - {timestamp}*
"""
    
    with open(informe_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    logger.info(f"✓ Informe generado: {informe_path}")
    return informe_path


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """Función principal de verificación."""
    
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("INICIO: Verificación de Datos Estáticos (Fase 2)")
    logger.info("=" * 70)
    
    # Verificar que existe el directorio base
    if not DATOS_ESTATICOS_DIR.exists():
        logger.error(f"Directorio no encontrado: {DATOS_ESTATICOS_DIR}")
        logger.info("Asegúrate de haber ejecutado los scripts de las fases 2.1 a 2.4")
        return
    
    logger.info(f"Directorio base: {DATOS_ESTATICOS_DIR}")
    
    # Analizar cada fuente
    resultados = {}
    
    for fuente in FUENTES_ESPERADAS.keys():
        logger.info(f"\n{'─' * 50}")
        logger.info(f"Verificando: {FUENTES_ESPERADAS[fuente]['nombre']}")
        logger.info(f"{'─' * 50}")
        
        fuente_dir = DATOS_ESTATICOS_DIR / fuente
        resultado = analizar_directorio(fuente_dir, logger)
        resultados[fuente] = resultado
        
        if resultado["existe"]:
            logger.info(f"  Archivos encontrados: {resultado['total_archivos']}")
            logger.info(f"  Registros totales: {resultado['total_registros']:,}")
            logger.info(f"  Tamaño: {formatear_bytes(resultado['total_bytes'])}")
            
            if resultado["fecha_min_global"]:
                logger.info(f"  Periodo: {resultado['fecha_min_global']} → {resultado['fecha_max_global']}")
            
            if resultado["archivos_vacios"]:
                logger.warning(f"  ⚠ Archivos vacíos: {len(resultado['archivos_vacios'])}")
        else:
            logger.warning(f"  ✗ Directorio no encontrado")
    
    # Generar informe
    logger.info(f"\n{'─' * 50}")
    logger.info("GENERANDO INFORME")
    logger.info(f"{'─' * 50}")
    
    informe_path = generar_informe(resultados, logger)
    
    # Resumen final
    total_registros = sum(r.get("total_registros", 0) for r in resultados.values())
    total_bytes = sum(r.get("total_bytes", 0) for r in resultados.values())
    fuentes_con_datos = sum(1 for r in resultados.values() if r.get("tiene_datos"))
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("VERIFICACIÓN COMPLETADA")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"  Fuentes con datos: {fuentes_con_datos}/4")
    logger.info(f"  Total registros: {total_registros:,}")
    logger.info(f"  Tamaño total: {formatear_bytes(total_bytes)}")
    logger.info("")
    logger.info(f"  📄 Informe completo: {informe_path}")
    logger.info("")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
