# Screenshots

Capturas del dashboard usadas por el `README.md` principal. Para regenerarlas:

```powershell
streamlit run 5.DASHBOARD/app.py
```

Abre `http://localhost:8501`, recorre las pestañas y exporta cada vista como PNG
(tema oscuro, ~1600px de ancho) con su nombre correspondiente.

| Archivo | Vista |
|---|---|
| `pantalla_principal.png` | Vista principal / Índice de Calidad Urbana (imagen hero) |
| `contaminacion.png` | Contaminación — índice compuesto + desglose por contaminante |
| `mapa-calidad-aire.png` | Mapa de sensores + heatmap de calidad del aire |
| `ranking-barrios-calidad-aire.png` | Ranking de los 19 distritos |
| `alertas-tiempo-real-contaminacion.png` | Alertas en tiempo real (umbrales OMS) |
| `evolucion-anual-indicador-contaminante.png` | Evolución anual por contaminante |
| `eventos-masivos.png` | Impacto de eventos masivos (Δ% vs baseline) |
| `timeline-eventos-masivos.png` | Timeline de eventos |
| `comparador.png` | Comparador histórico (periodo vs periodo) |
| `detective-de-patrones.png` | Pattern Detective |
| `deteccion-anomalias.png` | Detección de anomalías |
| `patrones-temporales.png` | Patrones temporales |
| `precipitaciones.png` | Precipitaciones |
| `climatologia-mensual.png` | Climatología mensual |
| `trafico.png` | Tráfico |
| `mapa-trafico-tiempo-real.png` | Mapa de tráfico en tiempo real |
| `pronostico-72h.png` | Pronóstico meteorológico 72h |
| `pronosticos-de-contaminacion.png` | Pronóstico estadístico de contaminación |
| `rutas-limpias.png` | Rutas pulmón limpio |
