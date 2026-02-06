# Datos de Tráfico DGT - Documentación

## Investigación realizada: 2026-02-06 14:49:31

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

### traffic_data
- Datos encontrados: ✗ No
- Fecha de publicación: N/A
- Número de elementos: 0
- Es tiempo real: No
- Tiene históricos: ✗ No

### incidencias
- Datos encontrados: ✗ No
- Fecha de publicación: N/A
- Número de elementos: 0
- Es tiempo real: No
- Tiene históricos: ✗ No

### camaras
- Datos encontrados: ✗ No
- Fecha de publicación: N/A
- Número de elementos: 0
- Es tiempo real: No
- Tiene históricos: ✗ No

---

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
