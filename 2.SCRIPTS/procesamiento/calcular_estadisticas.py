# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Fase 5.4: Cálculo de Estadísticas Agregadas
==============================================================================

Descripción:
    Calcula estadísticas agregadas a partir de los datasets normalizados
    en las fases 5.1 (contaminación) y 5.2 (meteorología). Genera tres
    ficheros CSV con medias ponderadas por número de registros válidos.

Datos de entrada:
    1. 3.DATOS_LIMPIOS/contaminacion_normalizada.parquet
    2. 3.DATOS_LIMPIOS/meteorologia_limpio.csv

Datos de salida:
    1. 3.DATOS_LIMPIOS/estadisticas/contaminacion_media_anual_barrio.csv
       → Medias anuales de contaminación por barrio y variable
    2. 3.DATOS_LIMPIOS/estadisticas/precipitacion_media_mensual.csv
       → Medias mensuales de precipitación
    3. 3.DATOS_LIMPIOS/estadisticas/tendencias_historicas.csv
       → Tendencias históricas anuales (contaminación + meteorología)

Decisiones de diseño:
    - Solo se usan registros con calidad_dato == "ok" para contaminación.
    - Solo se usan registros con calidad_dato == "ok" para meteorología.
    - La media anual se calcula como sum(valor) / count(valor), que es
      equivalente a una media ponderada por nº de registros válidos cuando
      las estaciones tienen distinta cobertura temporal.
    - Se incluye n_registros en cada fila para que el dashboard pueda
      mostrar la fiabilidad estadística de cada media.
    - El mapeo estacion_id → barrio se define internamente (no depende
      de ficheros externos) para maximizar reproducibilidad.

Uso:
    python calcular_estadisticas.py

Autor: Joan
Fecha: 2026
Proyecto: Data Detective Valencia
"""

import importlib.util
import pandas as pd
import logging
import sys
from pathlib import Path
from typing import Optional

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATOS_LIMPIOS_DIR = PROJECT_ROOT / "3.DATOS_LIMPIOS"
STATS_DIR = DATOS_LIMPIOS_DIR / "estadisticas"
LOG_DIR = PROJECT_ROOT / "logs"

# Ficheros de entrada
CONTAMINACION_FILE = DATOS_LIMPIOS_DIR / "contaminacion_normalizada.parquet"
METEOROLOGIA_FILE = DATOS_LIMPIOS_DIR / "meteorologia_limpio.csv"

# Ficheros de salida
OUT_CONTAM_ANUAL = STATS_DIR / "contaminacion_media_anual_barrio.csv"
OUT_PRECIP_MENSUAL = STATS_DIR / "precipitacion_media_mensual.csv"
OUT_TENDENCIAS = STATS_DIR / "tendencias_historicas.csv"

# ==============================================================================
# MAPEO ESTACIÓN → BARRIO  (fuente única: 5.DASHBOARD/config.py)
# ==============================================================================
# Se carga dinámicamente con importlib.util para evitar añadir 5.DASHBOARD/
# al sys.path global (que colisionaría con los módulos de este directorio).

_CONFIG_PY = PROJECT_ROOT / "5.DASHBOARD" / "config.py"
_spec = importlib.util.spec_from_file_location("dashboard_config", _CONFIG_PY)
_dashboard_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dashboard_config)

ESTACION_BARRIO_MAP = _dashboard_config.ESTACION_BARRIO_MAP


# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """
    Configura logging dual (archivo + consola).
    Mismo patrón que normalizar_contaminacion.py (Fase 5.1).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "calcular_estadisticas.log"
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("Calcular_Estadisticas")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Archivo (todo)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, date_format))

    # Consola (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_format, date_format))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================

def cargar_contaminacion(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Carga el Parquet normalizado de contaminación (Fase 5.1).

    Esquema esperado:
        fecha_utc, estacion_id, estacion_nombre, fuente,
        variable, valor, unidad, calidad_dato

    Returns:
        DataFrame o None si el fichero no existe / está vacío.
    """
    if not CONTAMINACION_FILE.exists():
        logger.error(f"No se encuentra: {CONTAMINACION_FILE}")
        logger.error("Ejecuta primero normalizar_contaminacion.py (Fase 5.1)")
        return None

    logger.info(f"Cargando contaminación: {CONTAMINACION_FILE.name}")

    try:
        df = pd.read_parquet(CONTAMINACION_FILE)
    except Exception as e:
        logger.error(f"Error leyendo Parquet: {e}")
        return None

    if df.empty:
        logger.warning("Fichero de contaminación vacío")
        return None

    logger.info(f"  → {len(df):,} registros cargados")
    logger.info(f"  → Columnas: {list(df.columns)}")
    logger.info(
        f"  → Rango: {df['fecha_utc'].min()} → {df['fecha_utc'].max()}")

    return df


def cargar_meteorologia(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Carga el CSV normalizado de meteorología (Fase 5.2).

    Esquema esperado:
        fecha, hora, precipitacion_mm, temp_c, humedad_pct,
        fuente, calidad_dato

    Returns:
        DataFrame o None si el fichero no existe / está vacío.
    """
    if not METEOROLOGIA_FILE.exists():
        logger.error(f"No se encuentra: {METEOROLOGIA_FILE}")
        logger.error("Ejecuta primero limpiar_meteorologia.py (Fase 5.2)")
        return None

    logger.info(f"Cargando meteorología: {METEOROLOGIA_FILE.name}")

    try:
        # Cargar primero sin parsear fechas para inspeccionar
        df = pd.read_csv(METEOROLOGIA_FILE)

        # Intentar convertir fecha a datetime de forma robusta
        # errors='coerce' convertirá valores inválidos a NaT (Not a Time)
        df["fecha"] = pd.to_datetime(df["fecha"], errors='coerce', utc=True)

        # Verificar si hay fechas inválidas
        fechas_invalidas = df["fecha"].isna().sum()
        if fechas_invalidas > 0:
            logger.warning(
                f"  {fechas_invalidas:,} fechas inválidas encontradas y convertidas a NaT")
            # Opcional: mostrar algunos ejemplos de fechas inválidas
            muestra_invalidas = df[df["fecha"].isna()].head(3)
            if not muestra_invalidas.empty:
                logger.debug(
                    f"  Ejemplos de fechas inválidas: {muestra_invalidas.iloc[:, 0].tolist()}")

    except Exception as e:
        logger.error(f"Error leyendo CSV: {e}")
        return None

    if df.empty:
        logger.warning("Fichero de meteorología vacío")
        return None

    logger.info(f"  → {len(df):,} registros cargados")
    logger.info(f"  → Columnas: {list(df.columns)}")
    logger.info(f"  → Rango: {df['fecha'].min()} → {df['fecha'].max()}")

    return df

# ==============================================================================
# TAREA 1: MEDIAS ANUALES DE CONTAMINACIÓN POR BARRIO
# ==============================================================================


def calcular_contaminacion_anual_barrio(
    df: pd.DataFrame,
    logger: logging.Logger
) -> Optional[pd.DataFrame]:
    """
    Calcula medias anuales de contaminación por barrio y variable.

    Proceso:
        1. Filtra solo registros con calidad_dato == "ok"
        2. Extrae el año desde fecha_utc
        3. Mapea estacion_id → barrio usando ESTACION_BARRIO_MAP
        4. Agrupa por (año, barrio, variable)
        5. Calcula: media_anual = sum(valor) / count(valor)
        6. Añade n_registros para evaluar fiabilidad

    Args:
        df: DataFrame de contaminación normalizada
        logger: Logger configurado

    Returns:
        DataFrame con columnas:
            [año, barrio, variable, media_anual, n_registros, unidad]
    """
    logger.info("─" * 40)
    logger.info("TAREA 1: Medias anuales de contaminación por barrio")
    logger.info("─" * 40)

    # Paso 1: Filtrar solo datos válidos
    df_ok = df[df["calidad_dato"] == "ok"].copy()
    descartados = len(df) - len(df_ok)
    logger.info(f"  Registros válidos (ok): {len(df_ok):,} / {len(df):,}")
    if descartados > 0:
        logger.info(f"  Descartados (invalid/missing): {descartados:,}")

    if df_ok.empty:
        logger.warning("  Sin datos válidos para calcular estadísticas")
        return None

    # Paso 2: Extraer año
    # fecha_utc puede ser tz-aware (UTC) → extraemos .dt.year directamente
    df_ok["año"] = df_ok["fecha_utc"].dt.year

    # Paso 3: Mapear estacion_id → barrio
    df_ok["barrio"] = df_ok["estacion_id"].map(ESTACION_BARRIO_MAP)

    # Registrar estaciones sin mapeo (por si hay estaciones nuevas)
    sin_barrio = df_ok[df_ok["barrio"].isna()]["estacion_id"].unique()
    if len(sin_barrio) > 0:
        logger.warning(
            f"  Estaciones sin mapeo a barrio: {list(sin_barrio)}. "
            f"Se excluyen del cálculo. Actualiza ESTACION_BARRIO_MAP si es necesario."
        )
        df_ok = df_ok.dropna(subset=["barrio"])

    if df_ok.empty:
        logger.warning("  Sin datos tras mapeo de barrios")
        return None

    # Paso 4-5: Agrupar y calcular media
    # media_anual = sum(valor) / count(valor) → equivalente a .mean()
    # n_registros = count(valor) → para ponderar fiabilidad
    df_stats = (
        df_ok
        .groupby(["año", "barrio", "variable"], as_index=False)
        .agg(
            media_anual=("valor", "mean"),
            n_registros=("valor", "count"),
        )
    )

    # Añadir unidad (siempre µg/m³ para contaminación)
    df_stats["unidad"] = "µg/m³"

    # Redondear media a 2 decimales
    df_stats["media_anual"] = df_stats["media_anual"].round(2)

    # Ordenar para legibilidad
    df_stats = df_stats.sort_values(
        ["año", "barrio", "variable"]
    ).reset_index(drop=True)

    logger.info(f"  Resultado: {len(df_stats):,} filas")
    logger.info(
        f"  Años cubiertos: {df_stats['año'].min()} → {df_stats['año'].max()}")
    logger.info(f"  Barrios: {sorted(df_stats['barrio'].unique())}")
    logger.info(f"  Variables: {sorted(df_stats['variable'].unique())}")

    return df_stats


# ==============================================================================
# TAREA 2: MEDIAS MENSUALES DE PRECIPITACIÓN
# ==============================================================================

def calcular_precipitacion_mensual(
    df: pd.DataFrame,
    logger: logging.Logger
) -> Optional[pd.DataFrame]:
    """
    Calcula medias mensuales de precipitación.

    Proceso:
        1. Filtra solo registros con calidad_dato == "ok"
        2. Descarta filas donde precipitacion_mm es NaN o fecha es NaT
        3. Extrae año y mes desde fecha
        4. Agrupa por (año, mes)
        5. Calcula media mensual + n_registros
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("TAREA 2: Medias mensuales de precipitación")
    logger.info("─" * 40)

    # Paso 1: Filtrar datos válidos
    df_ok = df[df["calidad_dato"] == "ok"].copy()
    logger.info(f"  Registros válidos (ok): {len(df_ok):,} / {len(df):,}")

    # Paso 2: Descartar NaN en precipitación Y fechas inválidas (NaT)
    df_precip = df_ok.dropna(subset=["precipitacion_mm", "fecha"]).copy()
    logger.info(f"  Con precipitación y fecha válidas: {len(df_precip):,}")

    if df_precip.empty:
        logger.warning("  Sin datos de precipitación válidos")
        return None

    # Verificar que fecha es realmente datetime
    if not pd.api.types.is_datetime64_any_dtype(df_precip["fecha"]):
        logger.error(
            "  La columna 'fecha' no es de tipo datetime después de la limpieza")
        return None

    # Paso 3: Extraer año y mes
    df_precip["año"] = df_precip["fecha"].dt.year
    df_precip["mes"] = df_precip["fecha"].dt.month

    # Paso 4-5: Agrupar y calcular
    df_stats = (
        df_precip
        .groupby(["año", "mes"], as_index=False)
        .agg(
            precipitacion_media_mm=("precipitacion_mm", "mean"),
            n_registros=("precipitacion_mm", "count"),
        )
    )

    # Redondear
    df_stats["precipitacion_media_mm"] = df_stats["precipitacion_media_mm"].round(
        2)

    # Ordenar
    df_stats = df_stats.sort_values(["año", "mes"]).reset_index(drop=True)

    logger.info(f"  Resultado: {len(df_stats):,} filas (meses únicos)")
    if not df_stats.empty:
        logger.info(f"  Rango: {df_stats['año'].min()}/{df_stats['mes'].min():02d} "
                    f"→ {df_stats['año'].max()}/{df_stats['mes'].max():02d}")

    return df_stats

# ==============================================================================
# TAREA 3: TENDENCIAS HISTÓRICAS (1963-2026)
# ==============================================================================


def calcular_tendencias_historicas(
    df_contam: Optional[pd.DataFrame],
    df_meteo: Optional[pd.DataFrame],
    logger: logging.Logger
) -> Optional[pd.DataFrame]:
    """
    Calcula tendencias históricas anuales combinando contaminación y meteorología.

    Contaminación: media anual global ponderada por variable (sin desglose barrio).
    Meteorología: media anual de temp_c, precipitacion_mm y humedad_pct.

    Ambas fuentes se unen por año para tener una vista integrada de la
    evolución temporal de Valencia.

    Args:
        df_contam: DataFrame de contaminación normalizada (o None)
        df_meteo: DataFrame de meteorología normalizada (o None)
        logger: Logger configurado

    Returns:
        DataFrame con una fila por año y columnas para cada variable.
    """
    logger.info("")
    logger.info("─" * 40)
    logger.info("TAREA 3: Tendencias históricas (1963-2026)")
    logger.info("─" * 40)

    frames = []

    # --- 3A: Contaminación → media anual global por variable ---
    if df_contam is not None:
        logger.info("  3A: Procesando contaminación...")

        df_ok = df_contam[df_contam["calidad_dato"] == "ok"].copy()
        df_ok["año"] = df_ok["fecha_utc"].dt.year

        # Pivotar: una columna por variable (NO2, O3, PM10, etc.)
        # Para cada (año, variable): media de todos los valores válidos
        contam_anual = (
            df_ok
            .groupby(["año", "variable"], as_index=False)
            .agg(
                media=("valor", "mean"),
                n=("valor", "count"),
            )
        )

        # Pivotar a formato ancho: año | NO2_media | NO2_n | O3_media | ...
        pivot_media = contam_anual.pivot(
            index="año", columns="variable", values="media"
        )
        pivot_n = contam_anual.pivot(
            index="año", columns="variable", values="n"
        )

        # Renombrar columnas: NO2 → NO2_ugm3, para claridad
        pivot_media.columns = [f"{col}_ugm3" for col in pivot_media.columns]
        pivot_n.columns = [f"{col}_n_registros" for col in pivot_n.columns]

        # Unir media + n_registros
        contam_wide = pd.concat([pivot_media, pivot_n], axis=1)
        contam_wide = contam_wide.round(2)

        frames.append(contam_wide)
        logger.info(f"  → Contaminación: {len(contam_wide)} años")

    # --- 3B: Meteorología → media anual por variable ---
    if df_meteo is not None:
        logger.info("  3B: Procesando meteorología...")

        df_ok = df_meteo[df_meteo["calidad_dato"] == "ok"].copy()
        df_ok["año"] = df_ok["fecha"].dt.year

        # Calcular medias anuales para cada variable meteorológica
        meteo_stats = []

        for col, nombre_salida in [
            ("temp_c", "temp_media_c"),
            ("precipitacion_mm", "precipitacion_media_mm"),
            ("humedad_pct", "humedad_media_pct"),
        ]:
            if col not in df_ok.columns:
                continue

            serie = df_ok.dropna(subset=[col]).groupby("año")[col]
            media = serie.mean().rename(nombre_salida)
            n = serie.count().rename(
                f"{nombre_salida.replace('media_', '').replace('_media', '')}_n_registros")

            meteo_stats.extend([media, n])

        if meteo_stats:
            meteo_wide = pd.concat(meteo_stats, axis=1).round(2)
            frames.append(meteo_wide)
            logger.info(f"  → Meteorología: {len(meteo_wide)} años")

    # --- Unir ambas fuentes por año ---
    if not frames:
        logger.warning("  Sin datos para tendencias históricas")
        return None

    # outer join para no perder años que solo tienen una fuente
    df_tendencias = pd.concat(frames, axis=1)
    df_tendencias.index.name = "año"
    df_tendencias = df_tendencias.reset_index()

    # Ordenar por año
    df_tendencias = df_tendencias.sort_values("año").reset_index(drop=True)

    logger.info(f"  Resultado final: {len(df_tendencias)} años")
    logger.info(
        f"  Rango: {df_tendencias['año'].min()} → {df_tendencias['año'].max()}")
    logger.info(f"  Columnas: {list(df_tendencias.columns)}")

    return df_tendencias


# ==============================================================================
# GUARDADO
# ==============================================================================

def guardar_csv(
    df: pd.DataFrame,
    path: Path,
    logger: logging.Logger,
    descripcion: str
) -> bool:
    """
    Guarda un DataFrame como CSV con encoding UTF-8.

    Args:
        df: DataFrame a guardar
        path: Ruta de destino
        logger: Logger
        descripcion: Descripción para el log (ej: "contaminación anual")

    Returns:
        True si se guardó correctamente, False si hubo error.
    """
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"  ✔ Guardado [{descripcion}]: {path}")
        logger.info(f"    → {len(df):,} filas, {len(df.columns)} columnas")
        return True
    except Exception as e:
        logger.error(f"  ✘ Error guardando [{descripcion}]: {e}")
        return False


# ==============================================================================
# RESUMEN FINAL
# ==============================================================================

def imprimir_resumen(
    df_contam_barrio: Optional[pd.DataFrame],
    df_precip_mensual: Optional[pd.DataFrame],
    df_tendencias: Optional[pd.DataFrame],
    logger: logging.Logger
) -> None:
    """
    Imprime un resumen ejecutivo de todas las estadísticas calculadas.
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("RESUMEN FINAL - ESTADÍSTICAS AGREGADAS (Fase 5.4)")
    logger.info("=" * 70)

    # Tarea 1: Contaminación anual por barrio
    if df_contam_barrio is not None:
        logger.info("")
        logger.info("  📊 Contaminación media anual por barrio:")
        logger.info(f"     Filas: {len(df_contam_barrio):,}")
        logger.info(f"     Años: {df_contam_barrio['año'].min()} → "
                    f"{df_contam_barrio['año'].max()}")
        logger.info(
            f"     Barrios: {sorted(df_contam_barrio['barrio'].unique())}")
        logger.info(
            f"     Variables: {sorted(df_contam_barrio['variable'].unique())}")

        # Top 3 combinaciones con más registros
        top = df_contam_barrio.nlargest(3, "n_registros")
        for _, row in top.iterrows():
            logger.info(
                f"     Top: {row['barrio']}/{row['variable']} "
                f"({row['año']}): {row['media_anual']:.1f} µg/m³ "
                f"(n={row['n_registros']:,})"
            )
    else:
        logger.warning("  📊 Contaminación anual por barrio: NO GENERADO")

    # Tarea 2: Precipitación mensual
    if df_precip_mensual is not None:
        logger.info("")
        logger.info("  🌧️  Precipitación media mensual:")
        logger.info(f"     Filas: {len(df_precip_mensual):,}")
        logger.info(f"     Rango: {df_precip_mensual['año'].min()}/{df_precip_mensual['mes'].min():02d} → "
                    f"{df_precip_mensual['año'].max()}/{df_precip_mensual['mes'].max():02d}")

        # Mes más lluvioso global
        mes_max = df_precip_mensual.loc[
            df_precip_mensual["precipitacion_media_mm"].idxmax()
        ]
        logger.info(
            f"     Más lluvioso: {int(mes_max['año'])}/{int(mes_max['mes']):02d} "
            f"→ {mes_max['precipitacion_media_mm']:.1f} mm/día "
            f"(n={int(mes_max['n_registros']):,})"
        )
    else:
        logger.warning("  🌧️  Precipitación mensual: NO GENERADO")

    # Tarea 3: Tendencias históricas
    if df_tendencias is not None:
        logger.info("")
        logger.info("  📈 Tendencias históricas:")
        logger.info(f"     Filas: {len(df_tendencias):,}")
        logger.info(f"     Rango: {df_tendencias['año'].min()} → "
                    f"{df_tendencias['año'].max()}")
        logger.info(f"     Columnas: {len(df_tendencias.columns)}")
    else:
        logger.warning("  📈 Tendencias históricas: NO GENERADO")

    logger.info("")
    logger.info("=" * 70)


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta el cálculo completo de estadísticas agregadas.

    Flujo:
        1. Cargar datos normalizados
        2. Calcular medias anuales de contaminación por barrio
        3. Calcular medias mensuales de precipitación
        4. Calcular tendencias históricas
        5. Guardar todos los CSV
        6. Imprimir resumen
    """
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("DATA DETECTIVE - Fase 5.4: Estadísticas Agregadas")
    logger.info("=" * 70)

    # Crear carpeta de salida
    STATS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Carpeta de salida: {STATS_DIR}")

    # ------------------------------------------------------------------
    # PASO 1: Cargar datos
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("CARGA DE DATOS")
    logger.info("─" * 40)

    df_contam = cargar_contaminacion(logger)
    df_meteo = cargar_meteorologia(logger)

    if df_contam is None and df_meteo is None:
        logger.error("No hay datos de ninguna fuente. Abortando.")
        print("\n❌ ERROR: No se encontraron datos. Revisa las fases 5.1 y 5.2.")
        return

    # ------------------------------------------------------------------
    # PASO 2: Tarea 1 - Contaminación anual por barrio
    # ------------------------------------------------------------------
    df_contam_barrio = None
    if df_contam is not None:
        logger.info("")
        df_contam_barrio = calcular_contaminacion_anual_barrio(
            df_contam, logger)

    # ------------------------------------------------------------------
    # PASO 3: Tarea 2 - Precipitación mensual
    # ------------------------------------------------------------------
    df_precip_mensual = None
    if df_meteo is not None:
        df_precip_mensual = calcular_precipitacion_mensual(df_meteo, logger)

    # ------------------------------------------------------------------
    # PASO 4: Tarea 3 - Tendencias históricas
    # ------------------------------------------------------------------
    df_tendencias = calcular_tendencias_historicas(df_contam, df_meteo, logger)

    # ------------------------------------------------------------------
    # PASO 5: Guardar resultados
    # ------------------------------------------------------------------
    logger.info("")
    logger.info("─" * 40)
    logger.info("GUARDADO DE RESULTADOS")
    logger.info("─" * 40)

    guardados = 0

    if df_contam_barrio is not None:
        if guardar_csv(df_contam_barrio, OUT_CONTAM_ANUAL, logger,
                       "Contaminación anual por barrio"):
            guardados += 1

    if df_precip_mensual is not None:
        if guardar_csv(df_precip_mensual, OUT_PRECIP_MENSUAL, logger,
                       "Precipitación media mensual"):
            guardados += 1

    if df_tendencias is not None:
        if guardar_csv(df_tendencias, OUT_TENDENCIAS, logger,
                       "Tendencias históricas"):
            guardados += 1

    # ------------------------------------------------------------------
    # PASO 6: Resumen
    # ------------------------------------------------------------------
    imprimir_resumen(df_contam_barrio, df_precip_mensual,
                     df_tendencias, logger)

    # Mensaje final consola
    print(f"\n✅ ESTADÍSTICAS COMPLETADAS: {guardados}/3 ficheros generados")
    if df_contam_barrio is not None:
        print(f"   → {OUT_CONTAM_ANUAL}")
    if df_precip_mensual is not None:
        print(f"   → {OUT_PRECIP_MENSUAL}")
    if df_tendencias is not None:
        print(f"   → {OUT_TENDENCIAS}")

    print("\nCommit sugerido:")
    print("   git add 2.SCRIPTS/procesamiento/calcular_estadisticas.py")
    print("   git add 3.DATOS_LIMPIOS/estadisticas/")
    print('   git commit -m "feat: add Phase 5.4 aggregated statistics with weighted annual means"')


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    main()
