# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 2.4: Investigación de Datos de Tráfico DGT (DATEX II)
==============================================================================

Descripción:
    Este script investiga la disponibilidad de datos de tráfico de la DGT.
    
    CONCLUSIÓN IMPORTANTE:
    La DGT NO ofrece datos históricos públicos vía API.
    El endpoint DATEX II proporciona únicamente datos en TIEMPO REAL.
    
    Los datos históricos deben construirse mediante acumulación en la Fase 3
    (recopilación de datos dinámicos).

Endpoints investigados:
    - https://infocar.dgt.es/datex2/dgt/TrafficData (tiempo real)
    - https://infocar.dgt.es/datex2/dgt/SituationPublication (incidencias)
    
Formato: XML DATEX II (estándar europeo de intercambio de datos de tráfico)

Uso:
    python descargar_dgt_historico.py
    
    El script:
    1. Realiza una petición al endpoint de la DGT
    2. Analiza la estructura del XML
    3. Guarda una muestra del estado actual
    4. Documenta las limitaciones encontradas

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import requests
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "1.DATOS_EN_CRUDO" / "estaticos" / "trafico"
LOG_DIR = PROJECT_ROOT / "logs"

# Endpoints DGT DATEX II
DGT_ENDPOINTS = {
    "traffic_data": "https://infocar.dgt.es/datex2/dgt/TrafficData",
    "incidencias": "https://infocar.dgt.es/datex2/dgt/SituationPublication/all/content.xml",
    "camaras": "https://infocar.dgt.es/datex2/dgt/CCTVSiteTablePublication/all/content.xml",
}

# Headers para las peticiones
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DataDetective/1.0",
    "Accept": "application/xml, text/xml, */*",
}

# Timeout para las peticiones
REQUEST_TIMEOUT = 30


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """Configura el sistema de logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file = LOG_DIR / "dgt_historico.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    logger = logging.getLogger("DGT_Historico")
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
# FUNCIONES DE INVESTIGACIÓN
# ==============================================================================

def fetch_dgt_endpoint(url: str, logger: logging.Logger) -> Optional[str]:
    """
    Realiza una petición GET a un endpoint de la DGT.
    
    Args:
        url: URL del endpoint
        logger: Logger
    
    Returns:
        Contenido XML como string o None si hay error
    """
    try:
        logger.info(f"Consultando: {url}")
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            logger.info(f"  ✓ Respuesta OK ({len(response.content)} bytes)")
            return response.text
        else:
            logger.warning(f"  ✗ HTTP {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error(f"  ✗ Timeout después de {REQUEST_TIMEOUT}s")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"  ✗ Error de conexión: {str(e)}")
        return None


def analyze_xml_structure(xml_content: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Analiza la estructura del XML DATEX II.
    
    Args:
        xml_content: Contenido XML
        logger: Logger
    
    Returns:
        Diccionario con información sobre la estructura
    """
    analysis = {
        "tiene_datos": False,
        "tipo_publicacion": None,
        "fecha_publicacion": None,
        "num_elementos": 0,
        "elementos_ejemplo": [],
        "namespaces": [],
        "es_tiempo_real": True,  # Por defecto, asumimos tiempo real
        "tiene_historicos": False,
    }
    
    try:
        soup = BeautifulSoup(xml_content, "lxml-xml")
        
        # Buscar elemento raíz
        root = soup.find()
        if root:
            analysis["tipo_publicacion"] = root.name
            logger.debug(f"Tipo de publicación: {root.name}")
        
        # Buscar fecha de publicación
        pub_time = soup.find("publicationTime")
        if pub_time:
            analysis["fecha_publicacion"] = pub_time.text
            logger.info(f"  Fecha de publicación: {pub_time.text}")
        
        # Contar elementos de datos
        # En DATEX II, los datos suelen estar en elementos como:
        # - siteMeasurements (datos de tráfico)
        # - situation (incidencias)
        # - camera (cámaras)
        
        data_elements = []
        
        # Buscar mediciones de tráfico
        measurements = soup.find_all("siteMeasurements")
        if measurements:
            data_elements.extend(measurements)
            logger.info(f"  Mediciones de tráfico encontradas: {len(measurements)}")
        
        # Buscar incidencias
        situations = soup.find_all("situation")
        if situations:
            data_elements.extend(situations)
            logger.info(f"  Incidencias encontradas: {len(situations)}")
        
        # Buscar cámaras
        cameras = soup.find_all("cctvcamera") or soup.find_all("camera")
        if cameras:
            data_elements.extend(cameras)
            logger.info(f"  Cámaras encontradas: {len(cameras)}")
        
        analysis["num_elementos"] = len(data_elements)
        analysis["tiene_datos"] = len(data_elements) > 0
        
        # Obtener ejemplos de elementos (primeros 3)
        for elem in data_elements[:3]:
            # Extraer ID si existe
            elem_id = elem.get("id") or elem.find("id")
            if elem_id:
                if hasattr(elem_id, "text"):
                    analysis["elementos_ejemplo"].append(elem_id.text)
                else:
                    analysis["elementos_ejemplo"].append(str(elem_id))
        
        # Verificar si hay parámetros de fecha/históricos
        # (Normalmente no los hay en endpoints de tiempo real)
        historic_indicators = soup.find_all(["historicData", "archiveData", "dateRange"])
        if historic_indicators:
            analysis["tiene_historicos"] = True
            analysis["es_tiempo_real"] = False
            logger.info("  ⚠ Se encontraron indicadores de datos históricos")
        else:
            logger.info("  ℹ Solo datos en tiempo real (sin históricos)")
        
    except Exception as e:
        logger.error(f"Error analizando XML: {str(e)}")
    
    return analysis


def save_sample(xml_content: str, filename: str, logger: logging.Logger) -> Optional[Path]:
    """
    Guarda una muestra del XML actual.
    
    Args:
        xml_content: Contenido XML
        filename: Nombre del archivo
        logger: Logger
    
    Returns:
        Ruta al archivo guardado o None
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Añadir timestamp al nombre
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = OUTPUT_DIR / f"{filename}_{timestamp}.xml"
    
    try:
        # Guardar solo los primeros 50KB como muestra
        sample = xml_content[:50000]
        if len(xml_content) > 50000:
            sample += "\n\n<!-- ... contenido truncado (muestra de 50KB) ... -->"
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(sample)
        
        logger.info(f"  ✓ Muestra guardada: {output_file.name}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error guardando muestra: {str(e)}")
        return None


def generate_readme(analysis_results: Dict[str, Dict], logger: logging.Logger) -> Path:
    """
    Genera el archivo README documentando las limitaciones.
    
    Args:
        analysis_results: Resultados del análisis de cada endpoint
        logger: Logger
    
    Returns:
        Ruta al archivo README generado
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    readme_path = OUTPUT_DIR / "README_dgt_historico.md"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    content = f"""# Datos de Tráfico DGT - Documentación

## Investigación realizada: {timestamp}

---

## ⚠️ CONCLUSIÓN PRINCIPAL

**La DGT NO ofrece datos históricos de tráfico públicos vía API.**

Los endpoints DATEX II proporcionan únicamente **datos en tiempo real**.

---

## 📡 Endpoints Investigados

### 1. TrafficData (Datos de Tráfico)
- **URL**: `https://infocar.dgt.es/datex2/dgt/TrafficData`
- **Tipo**: Tiempo real
- **Formato**: XML DATEX II
- **Contenido**: Mediciones de intensidad, velocidad y ocupación de la red estatal
- **Actualización**: Cada pocos minutos
- **Históricos disponibles**: ❌ NO

### 2. SituationPublication (Incidencias)
- **URL**: `https://infocar.dgt.es/datex2/dgt/SituationPublication/all/content.xml`
- **Tipo**: Tiempo real
- **Formato**: XML DATEX II
- **Contenido**: Incidencias activas (obras, accidentes, retenciones)
- **Históricos disponibles**: ❌ NO

### 3. CCTVSiteTablePublication (Cámaras)
- **URL**: `https://infocar.dgt.es/datex2/dgt/CCTVSiteTablePublication/all/content.xml`
- **Tipo**: Tiempo real
- **Formato**: XML DATEX II
- **Contenido**: Ubicación y estado de cámaras de tráfico
- **Históricos disponibles**: ❌ NO

---

## 🔍 Resultados del Análisis

"""
    
    for endpoint_name, analysis in analysis_results.items():
        content += f"""### {endpoint_name}
- Datos encontrados: {"✓ Sí" if analysis.get("tiene_datos") else "✗ No"}
- Fecha de publicación: {analysis.get("fecha_publicacion", "N/A")}
- Número de elementos: {analysis.get("num_elementos", 0)}
- Es tiempo real: {"✓ Sí" if analysis.get("es_tiempo_real") else "No"}
- Tiene históricos: {"✓ Sí" if analysis.get("tiene_historicos") else "✗ No"}

"""
    
    content += """---

## 📋 Formato DATEX II

DATEX II es el estándar europeo para intercambio de datos de tráfico:

- **Especificación**: [docs.datex2.eu](https://docs.datex2.eu/)
- **Versiones**: La DGT usa v1.0 y v3.x según el endpoint
- **Estructura**: XML con namespaces específicos
- **Elementos principales**:
  - `siteMeasurements`: Mediciones de puntos de aforo
  - `situation`: Incidencias de tráfico
  - `cctvcamera`: Datos de cámaras

---

## 🚧 Limitaciones Identificadas

1. **Sin API de históricos**: No existe endpoint para consultar datos pasados
2. **Sin parámetros de fecha**: Los endpoints no aceptan rangos temporales
3. **Solo red estatal**: Excluye Cataluña y País Vasco
4. **Cobertura Valencia**: Solo carreteras estatales (A-3, A-7, V-30, etc.)

---

## ✅ Estrategia para Data Detective

### Fase 2 (Actual)
- ✓ Documentar la limitación (este archivo)
- ✓ Guardar muestra del formato XML actual
- ✓ No inventar datos históricos

### Fase 3 (Datos Dinámicos)
- Implementar script de captura periódica
- Programar con Task Scheduler (cada 5-10 minutos)
- Acumular datos en: `1.DATOS_EN_CRUDO/dinamicos/trafico/`
- Construir histórico propio por acumulación

### Formato de Acumulación Propuesto
```
fecha,hora,punto_medida,intensidad,velocidad,ocupacion
2026-02-06,14:30:00,PM_V30_KM5,1250,78,45
```

---

## 📚 Referencias

- [Portal DATEX II DGT](https://infocar.dgt.es/datex2/)
- [Guía de Utilización DATEX II](https://infocar.dgt.es/datex2/informacion_adicional/Guia%20de%20Utilizacion%20de%20DATEX%20II.pdf)
- [NAP - Punto de Acceso Nacional](https://nap.dgt.es/)
- [Especificación DATEX II](https://docs.datex2.eu/)

---

## 📁 Archivos en este directorio

- `README_dgt_historico.md` - Este archivo de documentación
- `muestra_traffic_*.xml` - Muestra del XML de tráfico en tiempo real
- `muestra_incidencias_*.xml` - Muestra del XML de incidencias (si disponible)

---

*Generado automáticamente por Data Detective - Fase 2.4*
"""
    
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    logger.info(f"✓ README generado: {readme_path.name}")
    return readme_path


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """Función principal que investiga los datos de la DGT."""
    
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("INICIO: Investigación de datos de tráfico DGT (DATEX II)")
    logger.info("=" * 70)
    
    # Crear directorio de salida
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    analysis_results = {}
    
    # Investigar cada endpoint
    for endpoint_name, url in DGT_ENDPOINTS.items():
        logger.info(f"\n{'─' * 50}")
        logger.info(f"Investigando: {endpoint_name}")
        logger.info(f"{'─' * 50}")
        
        # Obtener datos
        xml_content = fetch_dgt_endpoint(url, logger)
        
        if xml_content:
            # Analizar estructura
            analysis = analyze_xml_structure(xml_content, logger)
            analysis_results[endpoint_name] = analysis
            
            # Guardar muestra
            if analysis["tiene_datos"]:
                save_sample(xml_content, f"muestra_{endpoint_name}", logger)
        else:
            analysis_results[endpoint_name] = {
                "tiene_datos": False,
                "error": "No se pudo obtener respuesta"
            }
    
    # Generar documentación
    logger.info(f"\n{'─' * 50}")
    logger.info("GENERANDO DOCUMENTACIÓN")
    logger.info(f"{'─' * 50}")
    
    readme_path = generate_readme(analysis_results, logger)
    
    # Resumen final
    logger.info("")
    logger.info("=" * 70)
    logger.info("INVESTIGACIÓN COMPLETADA")
    logger.info("=" * 70)
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════════════════╗")
    logger.info("║  CONCLUSIÓN: La DGT NO ofrece datos históricos públicos             ║")
    logger.info("║                                                                      ║")
    logger.info("║  Los datos históricos de tráfico se construirán por ACUMULACIÓN     ║")
    logger.info("║  en la Fase 3 (Datos Dinámicos) mediante captura periódica.         ║")
    logger.info("╚══════════════════════════════════════════════════════════════════════╝")
    logger.info("")
    logger.info(f"Documentación generada: {readme_path}")
    logger.info(f"Muestras guardadas en: {OUTPUT_DIR}")
    logger.info("")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
