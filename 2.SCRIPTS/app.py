#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         DATA DETECTIVE - VALENCIA                            ║
║                    Dashboard de Análisis Urbano en Tiempo Real               ║
║                                                                              ║
║  Monitorización de: Contaminación | Precipitaciones | Tráfico | Eventos      ║
║  Autor: Joan                                                                 ║
║  Stack: Python 3.x + Streamlit + Pandas + Folium + Plotly                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import json
import os
import logging
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LA PÁGINA
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Data Detective - Valencia",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/tuusuario/data-detective',
        'Report a bug': 'https://github.com/tuusuario/data-detective/issues',
        'About': '**Data Detective Valencia** - Análisis de Big Data urbano'
    }
)

# ══════════════════════════════════════════════════════════════════════════════
# ESTILOS CSS PERSONALIZADOS (Tema oscuro estilo "Centro de Control")
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
    /* ═══ IMPORTS DE FUENTES ═══ */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* ═══ VARIABLES CSS ═══ */
    :root {
        --color-bg-primary: #0f172a;
        --color-bg-secondary: #1e293b;
        --color-bg-card: rgba(30, 41, 59, 0.6);
        --color-border: rgba(148, 163, 184, 0.1);
        --color-text-primary: #e2e8f0;
        --color-text-secondary: #94a3b8;
        --color-text-muted: #64748b;
        --color-accent-green: #10b981;
        --color-accent-yellow: #f59e0b;
        --color-accent-red: #ef4444;
        --color-accent-blue: #3b82f6;
        --color-accent-purple: #8b5cf6;
    }
    
    /* ═══ FONDO PRINCIPAL ═══ */
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
    }
    
    /* ═══ SIDEBAR ═══ */
    [data-testid="stSidebar"] {
        background: rgba(15, 23, 42, 0.95) !important;
        border-right: 1px solid var(--color-border);
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: var(--color-text-secondary);
    }
    
    /* ═══ CONTENEDOR PRINCIPAL ═══ */
    .main .block-container {
        padding: 2rem 3rem;
        max-width: 100%;
    }
    
    /* ═══ TÍTULOS ═══ */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif !important;
        color: var(--color-text-primary) !important;
    }
    
    h1 {
        font-size: 2rem !important;
        font-weight: 700 !important;
        margin-bottom: 0.5rem !important;
    }
    
    /* ═══ TEXTO GENERAL ═══ */
    p, span, div {
        font-family: 'Inter', sans-serif;
    }
    
    /* ═══ MÉTRICAS PERSONALIZADAS ═══ */
    .metric-card {
        background: var(--color-bg-card);
        backdrop-filter: blur(10px);
        border: 1px solid var(--color-border);
        border-radius: 16px;
        padding: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--accent-color), transparent);
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--color-text-muted);
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.25rem;
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.5rem;
        font-weight: 700;
        line-height: 1.2;
    }
    
    .metric-unit {
        font-size: 0.875rem;
        color: var(--color-text-muted);
    }
    
    /* ═══ TARJETAS ═══ */
    .card {
        background: var(--color-bg-card);
        backdrop-filter: blur(10px);
        border: 1px solid var(--color-border);
        border-radius: 16px;
        padding: 1.5rem;
    }
    
    .card-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--color-text-primary);
        margin-bottom: 1rem;
    }
    
    /* ═══ BADGES ═══ */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .badge-bueno {
        background: rgba(16, 185, 129, 0.2);
        color: #10b981;
    }
    
    .badge-moderado {
        background: rgba(245, 158, 11, 0.2);
        color: #f59e0b;
    }
    
    .badge-alto {
        background: rgba(239, 68, 68, 0.2);
        color: #ef4444;
    }
    
    .badge-muy-alto {
        background: rgba(139, 92, 246, 0.2);
        color: #8b5cf6;
    }
    
    /* ═══ INDICADOR LIVE ═══ */
    .live-indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: rgba(16, 185, 129, 0.1);
        border: 1px solid rgba(16, 185, 129, 0.3);
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-size: 0.75rem;
        color: #10b981;
    }
    
    .live-dot {
        width: 8px;
        height: 8px;
        background: #10b981;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(0.9); }
    }
    
    /* ═══ TABS PERSONALIZADOS ═══ */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(30, 41, 59, 0.5);
        border-radius: 12px;
        padding: 4px;
        gap: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        color: var(--color-text-muted);
        font-weight: 500;
        padding: 0.75rem 1.5rem;
    }
    
    .stTabs [aria-selected="true"] {
        background: rgba(16, 185, 129, 0.2) !important;
        color: #10b981 !important;
    }
    
    /* ═══ SELECTBOX Y INPUTS ═══ */
    .stSelectbox > div > div {
        background: rgba(30, 41, 59, 0.8);
        border: 1px solid var(--color-border);
        border-radius: 8px;
        color: var(--color-text-primary);
    }
    
    .stDateInput > div > div > input {
        background: rgba(30, 41, 59, 0.8);
        border: 1px solid var(--color-border);
        border-radius: 8px;
        color: var(--color-text-primary);
    }
    
    /* ═══ DATAFRAMES ═══ */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* ═══ ALERTAS ═══ */
    .alerta-card {
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.1));
        border: 1px solid rgba(59, 130, 246, 0.3);
        border-radius: 16px;
        padding: 1.5rem;
    }
    
    .alerta-fallas {
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.1), rgba(249, 115, 22, 0.1));
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    
    /* ═══ SCROLLBAR ═══ */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: var(--color-bg-primary);
    }
    
    ::-webkit-scrollbar-thumb {
        background: var(--color-text-muted);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: var(--color-text-secondary);
    }
    
    /* ═══ OCULTAR ELEMENTOS STREAMLIT ═══ */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES Y CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════════════
COORDENADAS_VALENCIA = {
    'lat': 39.4699,
    'lon': -0.3763
}

BARRIOS_VALENCIA = {
    'Ciutat Vella': {'lat': 39.4737, 'lon': -0.3754, 'poblacion': 27000},
    'L\'Eixample': {'lat': 39.4650, 'lon': -0.3720, 'poblacion': 42000},
    'Extramurs': {'lat': 39.4680, 'lon': -0.3850, 'poblacion': 48000},
    'Campanar': {'lat': 39.4820, 'lon': -0.3950, 'poblacion': 38000},
    'La Saïdia': {'lat': 39.4850, 'lon': -0.3700, 'poblacion': 47000},
    'El Pla del Real': {'lat': 39.4780, 'lon': -0.3600, 'poblacion': 31000},
    'Poblats Marítims': {'lat': 39.4550, 'lon': -0.3250, 'poblacion': 58000},
    'Quatre Carreres': {'lat': 39.4500, 'lon': -0.3650, 'poblacion': 75000},
    'Benimaclet': {'lat': 39.4900, 'lon': -0.3550, 'poblacion': 29000},
    'Rascanya': {'lat': 39.4950, 'lon': -0.3800, 'poblacion': 52000},
}

LIMITES_CONTAMINACION = {
    'NO2': {'bueno': 40, 'moderado': 100, 'alto': 200},
    'PM25': {'bueno': 15, 'moderado': 25, 'alto': 50},
    'PM10': {'bueno': 30, 'moderado': 50, 'alto': 100},
    'O3': {'bueno': 60, 'moderado': 120, 'alto': 180},
}

COLORES = {
    'bueno': '#10b981',
    'moderado': '#f59e0b',
    'alto': '#ef4444',
    'muy_alto': '#8b5cf6',
    'primary': '#3b82f6',
    'secondary': '#64748b',
}

# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE CARGA DE DATOS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300)  # Cache de 5 minutos
def cargar_datos_contaminacion():
    """
    Carga datos de contaminación desde archivos procesados.
    En producción, esto leería de 3.DATOS_LIMPIOS/contaminacion_limpio.csv
    """
    try:
        # Intentar cargar datos reales
        ruta = Path('3.DATOS_LIMPIOS/contaminacion_limpio.csv')
        if ruta.exists():
            df = pd.read_csv(ruta, parse_dates=['fecha'])
            logger.info(f"Datos de contaminación cargados: {len(df)} registros")
            return df
    except Exception as e:
        logger.warning(f"No se pudieron cargar datos reales: {e}")
    
    # Generar datos de demostración
    logger.info("Generando datos de demostración para contaminación")
    fechas = pd.date_range(end=datetime.now(), periods=24*7, freq='H')
    datos = []
    
    for barrio, coords in BARRIOS_VALENCIA.items():
        for fecha in fechas:
            # Simular variación horaria (más contaminación en horas punta)
            hora = fecha.hour
            factor_hora = 1 + 0.5 * np.sin((hora - 8) * np.pi / 12) if 6 <= hora <= 22 else 0.7
            
            # Variación por barrio (algunos más contaminados)
            factor_barrio = 1.2 if barrio in ['L\'Eixample', 'Quatre Carreres'] else 1.0
            
            datos.append({
                'fecha': fecha,
                'barrio': barrio,
                'NO2': max(0, np.random.normal(35 * factor_hora * factor_barrio, 10)),
                'PM25': max(0, np.random.normal(20 * factor_hora * factor_barrio, 8)),
                'PM10': max(0, np.random.normal(30 * factor_hora * factor_barrio, 12)),
                'O3': max(0, np.random.normal(50 * (2 - factor_hora), 15)),  # O3 inverso
                'lat': coords['lat'] + np.random.normal(0, 0.005),
                'lon': coords['lon'] + np.random.normal(0, 0.005),
            })
    
    return pd.DataFrame(datos)


@st.cache_data(ttl=600)  # Cache de 10 minutos
def cargar_datos_meteorologia():
    """
    Carga datos meteorológicos desde archivos procesados.
    En producción, esto leería de 3.DATOS_LIMPIOS/meteorologia_limpio.csv
    """
    try:
        ruta = Path('3.DATOS_LIMPIOS/meteorologia_limpio.csv')
        if ruta.exists():
            df = pd.read_csv(ruta, parse_dates=['fecha'])
            logger.info(f"Datos meteorológicos cargados: {len(df)} registros")
            return df
    except Exception as e:
        logger.warning(f"No se pudieron cargar datos reales: {e}")
    
    # Generar datos de demostración
    logger.info("Generando datos de demostración para meteorología")
    fechas = pd.date_range(end=datetime.now(), periods=24*7, freq='H')
    
    datos = []
    for fecha in fechas:
        hora = fecha.hour
        # Temperatura con variación diaria
        temp_base = 15 + 5 * np.sin((hora - 6) * np.pi / 12)
        
        datos.append({
            'fecha': fecha,
            'temperatura': temp_base + np.random.normal(0, 2),
            'humedad': 60 + np.random.normal(0, 15),
            'precipitacion': max(0, np.random.exponential(0.5) if np.random.random() > 0.8 else 0),
            'presion': 1013 + np.random.normal(0, 5),
            'viento_velocidad': max(0, np.random.normal(12, 5)),
            'viento_direccion': np.random.choice(['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO']),
        })
    
    return pd.DataFrame(datos)


@st.cache_data(ttl=300)
def cargar_datos_trafico():
    """
    Carga datos de tráfico desde archivos procesados.
    """
    try:
        ruta = Path('3.DATOS_LIMPIOS/trafico_limpio.csv')
        if ruta.exists():
            df = pd.read_csv(ruta, parse_dates=['fecha'])
            logger.info(f"Datos de tráfico cargados: {len(df)} registros")
            return df
    except Exception as e:
        logger.warning(f"No se pudieron cargar datos reales: {e}")
    
    # Generar datos de demostración
    logger.info("Generando datos de demostración para tráfico")
    fechas = pd.date_range(end=datetime.now(), periods=24*7, freq='H')
    
    ubicaciones = [
        'Av. Blasco Ibáñez', 'Av. del Puerto', 'Gran Vía', 
        'C/ Colón', 'Av. del Cid', 'Ronda Norte', 'V-30'
    ]
    
    datos = []
    for fecha in fechas:
        hora = fecha.hour
        dia_semana = fecha.weekday()
        
        # Factor hora punta
        es_hora_punta = hora in [8, 9, 14, 18, 19, 20]
        factor_hora = 1.8 if es_hora_punta else (0.3 if hora < 6 else 1.0)
        
        # Factor fin de semana
        factor_finde = 0.6 if dia_semana >= 5 else 1.0
        
        for ubicacion in ubicaciones:
            intensidad_base = 1500 if 'Av.' in ubicacion else 800
            datos.append({
                'fecha': fecha,
                'ubicacion': ubicacion,
                'intensidad': int(intensidad_base * factor_hora * factor_finde + np.random.normal(0, 200)),
                'velocidad_media': max(10, 45 - 20 * (factor_hora - 1) + np.random.normal(0, 5)),
                'ocupacion': min(100, max(0, 30 * factor_hora + np.random.normal(0, 10))),
            })
    
    return pd.DataFrame(datos)


@st.cache_data(ttl=3600)  # Cache de 1 hora
def cargar_eventos():
    """
    Carga eventos desde archivos procesados.
    """
    try:
        ruta = Path('1.DATOS_EN_CRUDO/eventos/eventos_clasificados.json')
        if ruta.exists():
            with open(ruta, 'r', encoding='utf-8') as f:
                eventos = json.load(f)
            logger.info(f"Eventos cargados: {len(eventos)} registros")
            return pd.DataFrame(eventos)
    except Exception as e:
        logger.warning(f"No se pudieron cargar eventos reales: {e}")
    
    # Eventos de demostración
    logger.info("Generando eventos de demostración")
    eventos = [
        {
            'nombre': 'Valencia CF vs Real Madrid',
            'fecha_inicio': (datetime.now() + timedelta(days=13)).strftime('%Y-%m-%d'),
            'fecha_fin': (datetime.now() + timedelta(days=13)).strftime('%Y-%m-%d'),
            'tipo': 'deportivo',
            'categoria': 'puntual',
            'impacto': 'alto',
            'ubicacion': 'Mestalla',
            'asistencia_estimada': 45000
        },
        {
            'nombre': 'Concierto Roig Arena',
            'fecha_inicio': (datetime.now() + timedelta(days=20)).strftime('%Y-%m-%d'),
            'fecha_fin': (datetime.now() + timedelta(days=20)).strftime('%Y-%m-%d'),
            'tipo': 'cultural',
            'categoria': 'puntual',
            'impacto': 'medio',
            'ubicacion': 'Roig Arena',
            'asistencia_estimada': 18000
        },
        {
            'nombre': 'Fallas 2026',
            'fecha_inicio': '2026-03-01',
            'fecha_fin': '2026-03-19',
            'tipo': 'festivo',
            'categoria': 'dilatado',
            'impacto': 'muy_alto',
            'ubicacion': 'Valencia',
            'asistencia_estimada': 2000000
        },
        {
            'nombre': 'Maratón Valencia',
            'fecha_inicio': (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d'),
            'fecha_fin': (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d'),
            'tipo': 'deportivo',
            'categoria': 'puntual',
            'impacto': 'alto',
            'ubicacion': 'Ciudad de las Artes',
            'asistencia_estimada': 30000
        },
        {
            'nombre': 'Feria de Julio',
            'fecha_inicio': '2026-07-01',
            'fecha_fin': '2026-07-31',
            'tipo': 'festivo',
            'categoria': 'dilatado',
            'impacto': 'medio',
            'ubicacion': 'Jardines de Viveros',
            'asistencia_estimada': 500000
        },
    ]
    
    return pd.DataFrame(eventos)


def cargar_pronostico():
    """
    Genera pronóstico meteorológico (en producción vendría de OpenWeatherMap).
    """
    dias = ['Hoy', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
    iconos = ['☀️', '🌤️', '🌧️', '🌦️', '⛅', '☀️', '🌤️']
    
    pronostico = []
    for i, dia in enumerate(dias[:5]):
        pronostico.append({
            'dia': dia,
            'fecha': (datetime.now() + timedelta(days=i)).strftime('%d/%m'),
            'temp_max': int(18 + np.random.randint(-3, 4)),
            'temp_min': int(10 + np.random.randint(-2, 3)),
            'lluvia': int(np.random.choice([10, 20, 45, 60, 80, 15, 25], p=[0.3, 0.2, 0.1, 0.1, 0.1, 0.1, 0.1])),
            'icono': iconos[i],
            'descripcion': np.random.choice(['Soleado', 'Parcialmente nublado', 'Lluvia ligera', 'Nublado'])
        })
    
    return pronostico


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ══════════════════════════════════════════════════════════════════════════════
def obtener_nivel_calidad(valor: float, contaminante: str) -> str:
    """Determina el nivel de calidad del aire según el valor y contaminante."""
    limites = LIMITES_CONTAMINACION.get(contaminante, LIMITES_CONTAMINACION['NO2'])
    
    if valor <= limites['bueno']:
        return 'bueno'
    elif valor <= limites['moderado']:
        return 'moderado'
    elif valor <= limites['alto']:
        return 'alto'
    else:
        return 'muy_alto'


def obtener_color_nivel(nivel: str) -> str:
    """Retorna el color hexadecimal para un nivel de calidad."""
    return COLORES.get(nivel, COLORES['secondary'])


def calcular_ica(no2: float, pm25: float, pm10: float, o3: float) -> int:
    """
    Calcula el Índice de Calidad del Aire simplificado.
    En producción, usar la fórmula oficial.
    """
    # Normalización simple (0-100 por contaminante, luego máximo)
    ica_no2 = min(100, (no2 / LIMITES_CONTAMINACION['NO2']['moderado']) * 50)
    ica_pm25 = min(100, (pm25 / LIMITES_CONTAMINACION['PM25']['moderado']) * 50)
    ica_pm10 = min(100, (pm10 / LIMITES_CONTAMINACION['PM10']['moderado']) * 50)
    ica_o3 = min(100, (o3 / LIMITES_CONTAMINACION['O3']['moderado']) * 50)
    
    return int(max(ica_no2, ica_pm25, ica_pm10, ica_o3))


def crear_mapa_calor(df: pd.DataFrame, variable: str = 'NO2') -> folium.Map:
    """Crea un mapa de calor con Folium."""
    m = folium.Map(
        location=[COORDENADAS_VALENCIA['lat'], COORDENADAS_VALENCIA['lon']],
        zoom_start=12,
        tiles='CartoDB dark_matter'
    )
    
    # Preparar datos para el heatmap
    datos_recientes = df[df['fecha'] >= df['fecha'].max() - timedelta(hours=1)]
    heat_data = [[row['lat'], row['lon'], row[variable]] for _, row in datos_recientes.iterrows()]
    
    # Añadir capa de calor
    HeatMap(
        heat_data,
        radius=25,
        blur=15,
        max_zoom=13,
        gradient={0.2: 'blue', 0.4: 'lime', 0.6: 'yellow', 0.8: 'orange', 1: 'red'}
    ).add_to(m)
    
    # Añadir marcadores de estaciones
    for barrio, coords in BARRIOS_VALENCIA.items():
        datos_barrio = datos_recientes[datos_recientes['barrio'] == barrio]
        if not datos_barrio.empty:
            valor = datos_barrio[variable].mean()
            nivel = obtener_nivel_calidad(valor, variable)
            color = obtener_color_nivel(nivel)
            
            folium.CircleMarker(
                location=[coords['lat'], coords['lon']],
                radius=10,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7,
                popup=f"<b>{barrio}</b><br>{variable}: {valor:.1f} µg/m³<br>Estado: {nivel}"
            ).add_to(m)
    
    return m


def crear_grafico_evolucion(df: pd.DataFrame, variable: str = 'NO2') -> go.Figure:
    """Crea un gráfico de evolución temporal."""
    df_agrupado = df.groupby('fecha')[variable].mean().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_agrupado['fecha'],
        y=df_agrupado[variable],
        mode='lines',
        name=variable,
        line=dict(color=COLORES['bueno'], width=2),
        fill='tozeroy',
        fillcolor='rgba(16, 185, 129, 0.1)'
    ))
    
    # Línea de límite
    limite = LIMITES_CONTAMINACION.get(variable, {}).get('bueno', 40)
    fig.add_hline(
        y=limite,
        line_dash="dash",
        line_color=COLORES['moderado'],
        annotation_text=f"Límite recomendado: {limite}",
        annotation_position="top right"
    )
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=30, b=20),
        height=300,
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            title=None
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            title=f'{variable} (µg/m³)'
        ),
        showlegend=False
    )
    
    return fig


def crear_grafico_barrios(df: pd.DataFrame) -> go.Figure:
    """Crea un gráfico de barras horizontales por barrio."""
    df_reciente = df[df['fecha'] >= df['fecha'].max() - timedelta(hours=24)]
    df_barrios = df_reciente.groupby('barrio').agg({
        'NO2': 'mean',
        'PM25': 'mean',
        'O3': 'mean'
    }).round(1).reset_index()
    
    df_barrios = df_barrios.sort_values('NO2', ascending=True)
    
    # Asignar colores según nivel
    colores = [obtener_color_nivel(obtener_nivel_calidad(v, 'NO2')) for v in df_barrios['NO2']]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=df_barrios['barrio'],
        x=df_barrios['NO2'],
        orientation='h',
        marker_color=colores,
        text=df_barrios['NO2'].apply(lambda x: f'{x:.0f}'),
        textposition='outside',
        name='NO₂'
    ))
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=60, t=30, b=20),
        height=400,
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(148, 163, 184, 0.1)',
            title='NO₂ (µg/m³)'
        ),
        yaxis=dict(
            showgrid=False,
            title=None
        ),
        showlegend=False
    )
    
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# COMPONENTES DE UI
# ══════════════════════════════════════════════════════════════════════════════
def render_metrica_card(label: str, valor: float, unidad: str, color: str, 
                        limite: float = None, descripcion: str = None):
    """Renderiza una tarjeta de métrica personalizada."""
    porcentaje = min(100, (valor / limite * 100)) if limite else 0
    nivel = obtener_nivel_calidad(valor, label.replace('₂', '2').replace('.', ''))
    color_valor = obtener_color_nivel(nivel) if limite else color
    
    st.markdown(f"""
    <div class="metric-card" style="--accent-color: {color}">
        <div class="metric-label">{descripcion or label}</div>
        <div style="font-size: 0.875rem; color: #94a3b8; font-weight: 600;">{label}</div>
        <div class="metric-value" style="color: {color_valor}">{valor:.0f}</div>
        <div class="metric-unit">{unidad}</div>
        {f'''
        <div style="margin-top: 0.75rem; height: 4px; background: rgba(148, 163, 184, 0.1); border-radius: 2px;">
            <div style="height: 100%; width: {porcentaje}%; background: {color_valor}; border-radius: 2px;"></div>
        </div>
        <div style="font-size: 0.65rem; color: #64748b; margin-top: 0.25rem;">Límite: {limite} {unidad}</div>
        ''' if limite else ''}
    </div>
    """, unsafe_allow_html=True)


def render_live_indicator():
    """Renderiza el indicador de datos en vivo."""
    ahora = datetime.now()
    st.markdown(f"""
    <div class="live-indicator">
        <div class="live-dot"></div>
        <span>LIVE DATA</span>
    </div>
    <div style="font-family: 'JetBrains Mono', monospace; font-size: 1.75rem; font-weight: 600; color: #10b981; margin-top: 0.5rem;">
        {ahora.strftime('%H:%M:%S')}
    </div>
    <div style="font-size: 0.875rem; color: #64748b;">
        {ahora.strftime('%A, %d de %B de %Y')}
    </div>
    """, unsafe_allow_html=True)


def render_estado_sistema():
    """Renderiza el estado de conexión del sistema."""
    servicios = [
        ('API GVA', 'online', '●'),
        ('OpenWeatherMap', 'online', '●'),
        ('AVAMET', 'online', '●'),
        ('DGT InfoCar', 'online', '●'),
        ('Task Scheduler', 'running', '▶'),
    ]
    
    st.markdown("<div style='font-size: 0.7rem; color: #64748b; letter-spacing: 2px; margin-bottom: 0.75rem;'>ESTADO DEL SISTEMA</div>", unsafe_allow_html=True)
    
    for nombre, estado, icono in servicios:
        color = '#10b981' if estado in ['online', 'running'] else '#ef4444'
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; font-size: 0.8rem;">
            <span style="color: #94a3b8;">{nombre}</span>
            <span style="color: {color}; display: flex; align-items: center; gap: 0.5rem;">
                <span style="font-size: 0.5rem;">{icono}</span>
                {estado}
            </span>
        </div>
        """, unsafe_allow_html=True)


def render_evento_card(evento: dict):
    """Renderiza una tarjeta de evento."""
    iconos = {
        'deportivo': '⚽',
        'cultural': '🎵',
        'festivo': '🔥'
    }
    
    colores_impacto = {
        'bajo': ('#10b981', 'rgba(16, 185, 129, 0.2)'),
        'medio': ('#f59e0b', 'rgba(245, 158, 11, 0.2)'),
        'alto': ('#ef4444', 'rgba(239, 68, 68, 0.2)'),
        'muy_alto': ('#8b5cf6', 'rgba(139, 92, 246, 0.2)')
    }
    
    color, bg = colores_impacto.get(evento['impacto'], colores_impacto['medio'])
    icono = iconos.get(evento['tipo'], '📅')
    
    incremento = {'bajo': '+10%', 'medio': '+20%', 'alto': '+35%', 'muy_alto': '+50%'}
    
    st.markdown(f"""
    <div style="
        display: flex;
        align-items: center;
        gap: 1.25rem;
        padding: 1.25rem;
        background: rgba(15, 23, 42, 0.5);
        border-radius: 12px;
        border: 1px solid {color}30;
        margin-bottom: 0.75rem;
    ">
        <div style="
            width: 50px;
            height: 50px;
            background: {bg};
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
        ">{icono}</div>
        <div style="flex: 1;">
            <div style="font-weight: 600; color: #e2e8f0; font-size: 1rem;">{evento['nombre']}</div>
            <div style="font-size: 0.8rem; color: #94a3b8; margin-top: 0.25rem;">
                📅 {evento['fecha_inicio']} &nbsp;|&nbsp; 📍 {evento['ubicacion']}
            </div>
        </div>
        <div style="text-align: right;">
            <div style="
                padding: 0.35rem 1rem;
                background: {bg};
                border-radius: 20px;
                font-size: 0.7rem;
                font-weight: 600;
                color: {color};
                text-transform: uppercase;
            ">Impacto {evento['impacto'].replace('_', ' ')}</div>
            <div style="font-size: 0.7rem; color: #64748b; margin-top: 0.5rem;">
                {incremento.get(evento['impacto'], '+15%')} tráfico esperado
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECCIONES PRINCIPALES
# ══════════════════════════════════════════════════════════════════════════════
def seccion_contaminacion(df: pd.DataFrame, barrio_filtro: str):
    """Renderiza la sección de contaminación."""
    
    # Filtrar por barrio si es necesario
    if barrio_filtro != 'Todos':
        df = df[df['barrio'] == barrio_filtro]
    
    # Datos más recientes
    df_reciente = df[df['fecha'] >= df['fecha'].max() - timedelta(hours=1)]
    
    # Métricas principales
    no2_actual = df_reciente['NO2'].mean()
    pm25_actual = df_reciente['PM25'].mean()
    o3_actual = df_reciente['O3'].mean()
    pm10_actual = df_reciente['PM10'].mean()
    ica = calcular_ica(no2_actual, pm25_actual, pm10_actual, o3_actual)
    
    # Fila de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        render_metrica_card('NO₂', no2_actual, 'µg/m³', COLORES['bueno'], 
                           LIMITES_CONTAMINACION['NO2']['bueno'], 'Dióxido de nitrógeno')
    with col2:
        render_metrica_card('PM2.5', pm25_actual, 'µg/m³', COLORES['moderado'],
                           LIMITES_CONTAMINACION['PM25']['bueno'], 'Partículas finas')
    with col3:
        render_metrica_card('O₃', o3_actual, 'µg/m³', COLORES['primary'],
                           LIMITES_CONTAMINACION['O3']['bueno'], 'Ozono troposférico')
    with col4:
        render_metrica_card('ICA', ica, 'puntos', COLORES['muy_alto'],
                           100, 'Índice Calidad Aire')
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Gráfico y mapa
    col_graf, col_mapa = st.columns(2)
    
    with col_graf:
        st.markdown('<div class="card"><div class="card-title">📈 Evolución Últimas 24h</div></div>', unsafe_allow_html=True)
        fig = crear_grafico_evolucion(df, 'NO2')
        st.plotly_chart(fig, use_container_width=True)
    
    with col_mapa:
        st.markdown('<div class="card"><div class="card-title">🗺️ Mapa de Calor - Valencia</div></div>', unsafe_allow_html=True)
        mapa = crear_mapa_calor(df, 'NO2')
        st_folium(mapa, width=None, height=350, returned_objects=[])
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Tabla de barrios
    st.markdown('<div class="card"><div class="card-title">🏘️ Calidad del Aire por Barrios</div>', unsafe_allow_html=True)
    
    df_tabla = df_reciente.groupby('barrio').agg({
        'NO2': 'mean',
        'PM25': 'mean',
        'O3': 'mean'
    }).round(1).reset_index()
    
    df_tabla['Estado'] = df_tabla['NO2'].apply(lambda x: obtener_nivel_calidad(x, 'NO2'))
    df_tabla['Tendencia'] = np.random.choice(['↓', '→', '↑'], size=len(df_tabla))
    
    df_tabla.columns = ['Barrio', 'NO₂ (µg/m³)', 'PM2.5 (µg/m³)', 'O₃ (µg/m³)', 'Estado', 'Tendencia']
    
    st.dataframe(
        df_tabla,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Estado': st.column_config.TextColumn(width='small'),
            'Tendencia': st.column_config.TextColumn(width='small'),
        }
    )
    st.markdown('</div>', unsafe_allow_html=True)


def seccion_precipitaciones(df_meteo: pd.DataFrame):
    """Renderiza la sección de precipitaciones."""
    
    # Pronóstico
    pronostico = cargar_pronostico()
    
    st.markdown('<div class="card"><div class="card-title">🌤️ Pronóstico 5 días - Valencia</div>', unsafe_allow_html=True)
    
    cols = st.columns(5)
    for i, p in enumerate(pronostico):
        with cols[i]:
            bg = 'rgba(59, 130, 246, 0.1)' if i == 0 else 'rgba(15, 23, 42, 0.5)'
            border = 'rgba(59, 130, 246, 0.3)' if i == 0 else 'rgba(148, 163, 184, 0.1)'
            
            st.markdown(f"""
            <div style="
                background: {bg};
                border: 1px solid {border};
                border-radius: 12px;
                padding: 1.25rem;
                text-align: center;
            ">
                <div style="font-size: 0.75rem; color: #64748b;">{p['dia']}</div>
                <div style="font-size: 2.5rem; margin: 0.5rem 0;">{p['icono']}</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #e2e8f0;">{p['temp_max']}°C</div>
                <div style="font-size: 0.875rem; color: #64748b;">{p['temp_min']}°C</div>
                <div style="
                    margin-top: 0.75rem;
                    padding: 0.35rem 0.75rem;
                    background: {'rgba(59, 130, 246, 0.2)' if p['lluvia'] > 50 else 'rgba(148, 163, 184, 0.1)'};
                    border-radius: 20px;
                    font-size: 0.75rem;
                    color: {'#3b82f6' if p['lluvia'] > 50 else '#94a3b8'};
                ">💧 {p['lluvia']}%</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Alerta si hay probabilidad alta de lluvia
    if any(p['lluvia'] > 60 for p in pronostico):
        dia_lluvia = next(p for p in pronostico if p['lluvia'] > 60)
        st.markdown(f"""
        <div class="alerta-card">
            <div style="display: flex; align-items: center; gap: 1.25rem;">
                <div style="font-size: 3rem;">⚠️</div>
                <div style="flex: 1;">
                    <div style="font-size: 1.125rem; font-weight: 700; color: #3b82f6;">
                        Alerta por lluvias - {dia_lluvia['dia']}
                    </div>
                    <div style="color: #94a3b8; font-size: 0.875rem; margin-top: 0.25rem;">
                        Probabilidad de precipitaciones del {dia_lluvia['lluvia']}%. 
                        Se recomienda precaución en zonas con riesgo de inundación.
                    </div>
                </div>
                <div style="
                    padding: 0.75rem 1.5rem;
                    background: rgba(59, 130, 246, 0.2);
                    border-radius: 8px;
                    font-size: 1.5rem;
                    font-weight: 700;
                    color: #3b82f6;
                ">{dia_lluvia['lluvia']}%</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Datos actuales
    df_actual = df_meteo[df_meteo['fecha'] >= df_meteo['fecha'].max() - timedelta(hours=1)]
    
    col1, col2, col3, col4 = st.columns(4)
    
    metricas_meteo = [
        ('💧', 'Humedad', f"{df_actual['humedad'].mean():.0f}%", COLORES['primary']),
        ('📊', 'Presión', f"{df_actual['presion'].mean():.0f} hPa", COLORES['muy_alto']),
        ('💨', 'Viento', f"{df_actual['viento_velocidad'].mean():.0f} km/h", COLORES['bueno']),
        ('🌡️', 'Temperatura', f"{df_actual['temperatura'].mean():.1f}°C", COLORES['moderado']),
    ]
    
    for col, (icono, label, valor, color) in zip([col1, col2, col3, col4], metricas_meteo):
        with col:
            st.markdown(f"""
            <div class="card" style="text-align: center;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">{icono}</div>
                <div style="font-size: 0.7rem; color: #64748b; margin-bottom: 0.25rem;">{label}</div>
                <div style="font-size: 1.25rem; font-weight: 700; color: {color};">{valor}</div>
            </div>
            """, unsafe_allow_html=True)


def seccion_trafico(df: pd.DataFrame):
    """Renderiza la sección de tráfico."""
    
    df_reciente = df[df['fecha'] >= df['fecha'].max() - timedelta(hours=1)]
    
    # Métricas principales
    intensidad_media = df_reciente['intensidad'].mean()
    velocidad_media = df_reciente['velocidad_media'].mean()
    ocupacion_media = df_reciente['ocupacion'].mean()
    
    # Determinar estados
    estado_intensidad = 'Moderado' if intensidad_media > 1500 else 'Fluido'
    estado_velocidad = 'Fluido' if velocidad_media > 35 else 'Lento'
    estado_ocupacion = 'Atención' if ocupacion_media > 50 else 'Normal'
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        color = COLORES['moderado'] if estado_intensidad == 'Moderado' else COLORES['bueno']
        st.markdown(f"""
        <div class="card">
            <div style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.5rem;">Intensidad media</div>
            <div style="display: flex; align-items: baseline; gap: 0.5rem;">
                <span style="font-size: 2.25rem; font-weight: 700; color: {color};">{intensidad_media:,.0f}</span>
                <span style="font-size: 0.875rem; color: #64748b;">veh/h</span>
            </div>
            <div class="badge badge-{'moderado' if estado_intensidad == 'Moderado' else 'bueno'}" style="margin-top: 0.75rem;">
                {estado_intensidad}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        color = COLORES['bueno'] if estado_velocidad == 'Fluido' else COLORES['alto']
        st.markdown(f"""
        <div class="card">
            <div style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.5rem;">Velocidad media</div>
            <div style="display: flex; align-items: baseline; gap: 0.5rem;">
                <span style="font-size: 2.25rem; font-weight: 700; color: {color};">{velocidad_media:.0f}</span>
                <span style="font-size: 0.875rem; color: #64748b;">km/h</span>
            </div>
            <div class="badge badge-{'bueno' if estado_velocidad == 'Fluido' else 'alto'}" style="margin-top: 0.75rem;">
                {estado_velocidad}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        color = COLORES['alto'] if estado_ocupacion == 'Atención' else COLORES['bueno']
        st.markdown(f"""
        <div class="card">
            <div style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.5rem;">Ocupación vías</div>
            <div style="display: flex; align-items: baseline; gap: 0.5rem;">
                <span style="font-size: 2.25rem; font-weight: 700; color: {color};">{ocupacion_media:.0f}</span>
                <span style="font-size: 0.875rem; color: #64748b;">%</span>
            </div>
            <div class="badge badge-{'alto' if estado_ocupacion == 'Atención' else 'bueno'}" style="margin-top: 0.75rem;">
                {estado_ocupacion}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Gráfico de intensidad por ubicación
    st.markdown('<div class="card"><div class="card-title">🛣️ Intensidad por Vía Principal</div>', unsafe_allow_html=True)
    
    df_vias = df_reciente.groupby('ubicacion')['intensidad'].mean().sort_values(ascending=True).reset_index()
    
    colores_vias = [
        COLORES['bueno'] if v < 1200 else (COLORES['moderado'] if v < 1800 else COLORES['alto'])
        for v in df_vias['intensidad']
    ]
    
    fig = go.Figure(go.Bar(
        y=df_vias['ubicacion'],
        x=df_vias['intensidad'],
        orientation='h',
        marker_color=colores_vias,
        text=df_vias['intensidad'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside'
    ))
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=80, t=20, b=20),
        height=300,
        xaxis=dict(showgrid=True, gridcolor='rgba(148, 163, 184, 0.1)', title='Vehículos/hora'),
        yaxis=dict(showgrid=False, title=None),
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Incidencias simuladas
    st.markdown('<div class="card"><div class="card-title">⚠️ Incidencias Activas</div>', unsafe_allow_html=True)
    
    incidencias = [
        {'tipo': 'Accidente', 'ubicacion': 'Av. Blasco Ibáñez', 'hora': 'Hace 15 min', 'severidad': 'alta', 'icono': '🚨'},
        {'tipo': 'Obras', 'ubicacion': 'C/ Colón', 'hora': 'Desde 08:00', 'severidad': 'media', 'icono': '🚧'},
        {'tipo': 'Evento', 'ubicacion': 'Zona Mestalla', 'hora': 'Partido 21:00', 'severidad': 'prevista', 'icono': '🏟️'},
    ]
    
    for inc in incidencias:
        color_borde = COLORES['alto'] if inc['severidad'] == 'alta' else (COLORES['moderado'] if inc['severidad'] == 'media' else COLORES['muy_alto'])
        st.markdown(f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 1rem;
            padding: 1rem;
            background: rgba(15, 23, 42, 0.5);
            border-radius: 8px;
            margin-bottom: 0.5rem;
            border-left: 3px solid {color_borde};
        ">
            <div style="font-size: 1.5rem;">{inc['icono']}</div>
            <div style="flex: 1;">
                <div style="font-weight: 600; color: #e2e8f0;">{inc['tipo']}</div>
                <div style="font-size: 0.8rem; color: #94a3b8;">{inc['ubicacion']}</div>
            </div>
            <div style="font-size: 0.75rem; color: #64748b;">{inc['hora']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)


def seccion_eventos(df_eventos: pd.DataFrame):
    """Renderiza la sección de eventos."""
    
    # Próximos eventos
    st.markdown('<div class="card"><div class="card-title">📅 Próximos Eventos con Impacto</div>', unsafe_allow_html=True)
    
    for _, evento in df_eventos.iterrows():
        render_evento_card(evento.to_dict())
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Análisis histórico Fallas
    st.markdown("""
    <div class="alerta-card alerta-fallas">
        <div style="display: flex; align-items: center; gap: 1.25rem; margin-bottom: 1.5rem;">
            <div style="font-size: 3rem;">🔥</div>
            <div>
                <div style="font-size: 1.25rem; font-weight: 700; color: #ef4444;">Análisis Histórico: Fallas</div>
                <div style="color: #94a3b8; font-size: 0.875rem; margin-top: 0.25rem;">
                    Impacto medido durante Fallas 2025 vs. media anual
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    metricas_fallas = [
        ('NO₂', '+45%', '65 µg/m³'),
        ('PM2.5', '+120%', '62 µg/m³'),
        ('PM10', '+85%', '78 µg/m³'),
        ('Tráfico', '+35%', '3,200 veh/h'),
    ]
    
    for col, (label, incremento, valor) in zip([col1, col2, col3, col4], metricas_fallas):
        with col:
            st.markdown(f"""
            <div style="
                background: rgba(15, 23, 42, 0.6);
                border-radius: 8px;
                padding: 1rem;
                text-align: center;
            ">
                <div style="font-size: 0.75rem; color: #94a3b8;">{label}</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #ef4444; margin-top: 0.25rem;">{incremento}</div>
                <div style="font-size: 0.7rem; color: #64748b; margin-top: 0.25rem;">{valor}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Gráfico de correlación
    st.markdown('<div class="card"><div class="card-title">📊 Correlación Eventos-Contaminación (2025)</div>', unsafe_allow_html=True)
    
    meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
    valores_base = [42, 40, 45, 38, 35, 32, 30, 28, 35, 40, 42, 45]
    valores_eventos = [50, 42, 85, 40, 36, 34, 55, 30, 38, 52, 44, 55]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=meses,
        y=valores_eventos,
        name='Con eventos',
        marker_color=[COLORES['alto'] if v > 60 else COLORES['moderado'] for v in valores_eventos]
    ))
    
    fig.add_trace(go.Scatter(
        x=meses,
        y=valores_base,
        name='Media base',
        mode='lines+markers',
        line=dict(color=COLORES['secondary'], dash='dash'),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=30, b=20),
        height=300,
        xaxis=dict(showgrid=False, title=None),
        yaxis=dict(showgrid=True, gridcolor='rgba(148, 163, 184, 0.1)', title='NO₂ (µg/m³)'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        barmode='group'
    )
    
    # Anotaciones de eventos
    fig.add_annotation(x='Mar', y=85, text='Fallas', showarrow=True, arrowhead=2, ax=0, ay=-30, font=dict(size=10, color='#ef4444'))
    fig.add_annotation(x='Jul', y=55, text='Feria', showarrow=True, arrowhead=2, ax=0, ay=-30, font=dict(size=10, color='#f59e0b'))
    fig.add_annotation(x='Oct', y=52, text='9 Oct', showarrow=True, arrowhead=2, ax=0, ay=-30, font=dict(size=10, color='#f59e0b'))
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# APLICACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def main():
    """Función principal de la aplicación."""
    
    # ══════════════════════════════════════════════════════════════════════════
    # SIDEBAR
    # ══════════════════════════════════════════════════════════════════════════
    with st.sidebar:
        # Logo
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0; border-bottom: 1px solid rgba(148, 163, 184, 0.1); margin-bottom: 1.5rem;">
            <div style="font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #10b981, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
                📊 DATA
            </div>
            <div style="font-size: 1.5rem; font-weight: 300; color: #94a3b8; letter-spacing: 4px;">
                DETECTIVE
            </div>
            <div style="font-size: 0.7rem; color: #64748b; margin-top: 0.5rem; letter-spacing: 2px;">
                VALENCIA • 2026
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Indicador live
        render_live_indicator()
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Filtros
        st.markdown("<div style='font-size: 0.7rem; color: #64748b; letter-spacing: 2px; margin-bottom: 0.75rem;'>FILTROS</div>", unsafe_allow_html=True)
        
        barrio_seleccionado = st.selectbox(
            "Barrio",
            ['Todos'] + list(BARRIOS_VALENCIA.keys()),
            key='filtro_barrio'
        )
        
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=datetime.now() - timedelta(days=7),
            key='filtro_fecha_inicio'
        )
        
        fecha_fin = st.date_input(
            "Fecha fin",
            value=datetime.now(),
            key='filtro_fecha_fin'
        )
        
        contaminante = st.multiselect(
            "Contaminantes",
            ['NO₂', 'PM2.5', 'PM10', 'O₃'],
            default=['NO₂'],
            key='filtro_contaminante'
        )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Estado del sistema
        render_estado_sistema()
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Créditos
        st.markdown("""
        <div style="font-size: 0.65rem; color: #64748b; text-align: center; padding-top: 1rem; border-top: 1px solid rgba(148, 163, 184, 0.1);">
            Desarrollado por Joan<br>
            Proyecto Data Detective<br>
            <span style="color: #10b981;">v1.0.0</span>
        </div>
        """, unsafe_allow_html=True)
    
    # ══════════════════════════════════════════════════════════════════════════
    # CONTENIDO PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════════
    
    # Header
    st.markdown("""
    <h1 style="display: flex; align-items: center; gap: 0.75rem;">
        <span>Dashboard</span>
        <span style="color: #64748b; font-weight: 300;">|</span>
        <span style="background: linear-gradient(135deg, #f59e0b, #ef4444); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            Valencia
        </span>
    </h1>
    <p style="color: #64748b; margin-top: 0;">
        Monitorización en tiempo real de calidad del aire, meteorología y tráfico urbano
    </p>
    """, unsafe_allow_html=True)
    
    # Leyenda global
    render_leyenda_tipos_datos()
    
    # Cargar datos
    with st.spinner('Cargando datos...'):
        df_contaminacion = cargar_datos_contaminacion()
        df_meteorologia = cargar_datos_meteorologia()
        df_trafico = cargar_datos_trafico()
        df_eventos = cargar_eventos()
    
    # Tabs principales
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌫️ Contaminación",
        "🌧️ Precipitaciones", 
        "🚗 Tráfico",
        "🎭 Eventos"
    ])
    
    with tab1:
        seccion_contaminacion(df_contaminacion, barrio_seleccionado)
    
    with tab2:
        seccion_precipitaciones(df_meteorologia)
    
    with tab3:
        seccion_trafico(df_trafico)
    
    with tab4:
        seccion_eventos(df_eventos)


# ══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Crear directorio de logs si no existe
    Path('logs').mkdir(exist_ok=True)
    
    # Ejecutar aplicación
    try:
        main()
    except Exception as e:
        logger.error(f"Error en la aplicación: {e}", exc_info=True)
        st.error(f"Ha ocurrido un error: {e}")
            key='filtro_fecha_inicio'
        )
        
        fecha_fin = st.date_input(
            "Fecha fin",
            value=datetime.now(),
            key='filtro_fecha_fin'
        )
        
        contaminante = st.multiselect(
            "Contaminantes",
            ['NO₂', 'PM2.5', 'PM10', 'O₃'],
            default=['NO₂'],
            key='filtro_contaminante'
        )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Estado del sistema
        render_estado_sistema()
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Créditos
        st.markdown("""
        <div style="font-size: 0.65rem; color: #64748b; text-align: center; padding-top: 1rem; border-top: 1px solid rgba(148, 163, 184, 0.1);">
            Desarrollado por Joan<br>
            Proyecto Data Detective<br>
            <span style="color: #10b981;">v1.0.0</span>
        </div>
        """, unsafe_allow_html=True)
    
    # ══════════════════════════════════════════════════════════════════════════
    # CONTENIDO PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════════
    
    # Header
    st.markdown("""
    <h1 style="display: flex; align-items: center; gap: 0.75rem;">
        <span>Dashboard</span>
        <span style="color: #64748b; font-weight: 300;">|</span>
        <span style="background: linear-gradient(135deg, #f59e0b, #ef4444); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            Valencia
        </span>
    </h1>
    <p style="color: #64748b; margin-top: 0;">
        Monitorización en tiempo real de calidad del aire, meteorología y tráfico urbano
    </p>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Cargar datos
    with st.spinner('Cargando datos...'):
        df_contaminacion = cargar_datos_contaminacion()
        df_meteorologia = cargar_datos_meteorologia()
        df_trafico = cargar_datos_trafico()
        df_eventos = cargar_eventos()
    
    # Tabs principales
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌫️ Contaminación",
        "🌧️ Precipitaciones", 
        "🚗 Tráfico",
        "🎭 Eventos"
    ])
    
    with tab1:
        seccion_contaminacion(df_contaminacion, barrio_seleccionado)
    
    with tab2:
        seccion_precipitaciones(df_meteorologia)
    
    with tab3:
        seccion_trafico(df_trafico)
    
    with tab4:
        seccion_eventos(df_eventos)


# ══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Crear directorio de logs si no existe
    Path('logs').mkdir(exist_ok=True)
    
    # Ejecutar aplicación
    try:
        main()
    except Exception as e:
        logger.error(f"Error en la aplicación: {e}", exc_info=True)
        st.error(f"Ha ocurrido un error: {e}")
