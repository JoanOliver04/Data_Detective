# -*- coding: utf-8 -*-
"""
==============================================================================
DATA DETECTIVE - VALENCIA
Modulo de Temas (Oscuro / Claro)
==============================================================================
Ruta: 5.DASHBOARD/theme.py
Autor: Joan | Fecha: 2026

Centraliza los tokens de color del dashboard para que todos los componentes
(Plotly, Folium, HTML inline) usen valores coherentes segun el tema activo.

Uso:
    from theme import get_theme
    t = get_theme()
    fig.update_layout(template=t["plotly_template"])
"""

import streamlit as st

# Clave de session_state donde el toggle almacena su valor.
# El valor es True (oscuro) / False (claro).
THEME_TOGGLE_KEY = "_dd_theme_toggle"

# ==============================================================================
# DEFINICIONES DE TEMAS
# ==============================================================================

THEMES = {
    "dark": {
        "name": "Oscuro",

        # --- Plotly y Folium ---
        "plotly_template": "plotly_dark",
        "folium_tiles":    "CartoDB dark_matter",

        # --- Fondos ---
        "app_bg":           "#0e1117",
        "app_text":         "#fafafa",
        "sidebar_bg":       "#262730",
        "bg_card":          "linear-gradient(135deg, #1e1e2e 0%, #252540 100%)",
        "bg_tabs":          "#0e0e1a",
        "bg_tab_selected":  "#1e1e2e",
        "bg_popup":         "#1a1a2e",
        "bg_table_header":  "#2a2a3a",
        "bg_highlight":     "#1e2a1e",
        "bg_legend":        "rgba(20,20,36,0.93)",
        "bg_score":         "#1e1e2e",

        # --- Texto ---
        "text":             "#ddd",
        "text_secondary":   "#ccc",
        "text_muted":       "#888",
        "text_faint":       "#555",
        "text_label":       "#aaa",
        "text_dim":         "#666",

        # --- Bordes ---
        "border":           "#333",
        "border_subtle":    "#2a2a3a",
        "border_hover":     "#1f77b4",
        "popup_hr":         "#333",
        "popup_hr_rt":      "#2a3a4a",
    },

    "light": {
        "name": "Claro",

        # --- Plotly y Folium ---
        "plotly_template": "plotly_white",
        "folium_tiles":    "CartoDB positron",

        # --- Fondos ---
        "app_bg":           "#ffffff",
        "app_text":         "#31333F",
        "sidebar_bg":       "#f0f2f6",
        "bg_card":          "linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%)",
        "bg_tabs":          "#e8e8f0",
        "bg_tab_selected":  "#ffffff",
        "bg_popup":         "#ffffff",
        "bg_table_header":  "#e0e0e8",
        "bg_highlight":     "#e8f5e9",
        "bg_legend":        "rgba(255,255,255,0.95)",
        "bg_score":         "#f0f0f8",

        # --- Texto ---
        "text":             "#333",
        "text_secondary":   "#555",
        "text_muted":       "#777",
        "text_faint":       "#999",
        "text_label":       "#666",
        "text_dim":         "#888",

        # --- Bordes ---
        "border":           "#ccc",
        "border_subtle":    "#e0e0e0",
        "border_hover":     "#1f77b4",
        "popup_hr":         "#ddd",
        "popup_hr_rt":      "#b0c4d4",
    },
}


def get_theme() -> dict:
    """
    Devuelve el diccionario de tokens del tema activo.

    Lee el valor del toggle en session_state. Si no existe aun
    (primer render), devuelve el tema oscuro por defecto.

    Returns:
        Dict con todos los tokens del tema activo.
    """
    is_dark = st.session_state.get(THEME_TOGGLE_KEY, True)
    return THEMES["dark"] if is_dark else THEMES["light"]


def get_theme_key() -> str:
    """Devuelve 'dark' o 'light' segun el tema activo."""
    is_dark = st.session_state.get(THEME_TOGGLE_KEY, True)
    return "dark" if is_dark else "light"
