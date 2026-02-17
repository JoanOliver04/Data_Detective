# -*- coding: utf-8 -*-
"""Funciones de formateo para el dashboard."""
from typing import Optional

def formato_numero(valor, decimales=1):
    if valor is None: return "-"
    if abs(valor) >= 1_000_000: return f"{valor/1_000_000:.{decimales}f}M"
    if abs(valor) >= 1_000: return f"{valor/1_000:.{decimales}f}K"
    return f"{valor:,.{decimales}f}"

def formato_porcentaje(valor, con_signo=True):
    if valor is None: return "-"
    return f"{valor:+.1f}%" if con_signo else f"{valor:.1f}%"

def formato_concentracion(valor, unidad="ug/m3"):
    if valor is None: return "-"
    return f"{valor:.1f} {unidad}"

def color_impacto(valor_pct):
    if valor_pct is None: return "#7f7f7f"
    if valor_pct > 20: return "#d62728"
    if valor_pct > 5: return "#ff7f0e"
    if valor_pct > -5: return "#ffbb33"
    return "#2ca02c"
