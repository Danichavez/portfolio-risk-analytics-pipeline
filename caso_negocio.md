# Caso de Negocio — Pipeline de Métricas de Riesgo

## Resumen Ejecutivo

Pipeline que automatiza el cálculo diario de métricas de riesgo (TE, VaR, Beta) para un fondo de pensiones regulado, reemplazando un proceso manual de planillas con un sistema reproducible, auditable y serverless.

---

## Problema

| Antes (Manual) | Después (Pipeline) |
|----------------|-------------------|
| 4-6 horas de analista/día | 15 minutos automatizado |
| Errores de copy-paste y fórmulas | Resultados determinísticos |
| Sin trazabilidad | Lineage completo por registro |
| Punto único de falla (1 persona) | Serverless, auto-recuperable |
| Resultados al día siguiente | Mismo día 13:30 |
| Sin control de versiones | Git + CI/CD |
| No reproducible | Idempotente, cualquier fecha recalculable |

---

## Impacto Cuantificado

### Ahorro de tiempo

| Actividad | Antes | Después | Ahorro |
|-----------|-------|---------|--------|
| Cálculo diario TE | 4h | 0h | 4h/día |
| Validación mensual | 8h | 1h | 7h/mes |
| Preparación reporte regulatorio | 3h | 30min | 2.5h |
| Recálculo ad-hoc | 6h | 15min | 5.75h |

**Ahorro anual**: ~1,100 horas de analista senior

### Modelo de costos

| Concepto | Costo anual |
|----------|-------------|
| Tiempo analista ahorrado (1,100h × $80/h) | $88,000 |
| Corrección de errores evitada (4 incidentes × $5,000) | $20,000 |
| **Ahorro total cuantificable** | **$108,000/año** |

### Costo de operación

| Componente | Costo mensual |
|-----------|---------------|
| AWS Glue (4 jobs × 22 días) | $60 |
| S3 (~50 GB comprimido) | $5 |
| Redshift (nodo compartido) | $180 |
| Step Functions + Lambda | $1 |
| **Total** | **$246/mes** |

### ROI

```
Beneficio neto anual = $108,000 - $2,955 = $105,045
Costo desarrollo (una vez) = ~$40,000
Período de payback = 4.6 meses
ROI a 3 años = 688%
```

---

## Reducción de Riesgo

### Riesgo operacional

| Riesgo | Antes | Después |
|--------|-------|---------|
| Error de cálculo en producción | Alto | Muy bajo |
| Dependencia de persona clave | Crítico | Ninguno |
| Retraso en reportes | Frecuente | Raro |
| Metodología inconsistente | Posible | Imposible (código = metodología) |

### Riesgo regulatorio

| Requisito | Antes | Después |
|-----------|-------|---------|
| Reproducibilidad | No puede reproducir cálculos pasados | Cualquier fecha recalculable |
| Auditoría | Versiones de planilla (no confiable) | Git history + lineage |
| Documentación metodológica | Documento separado (puede divergir) | Código ES la documentación |
| Oportunidad | Entrega al día siguiente | Entrega mismo día |

---

## Industrias Objetivo

Esta solución aplica a cualquier organización que:
- Gestione portafolios contra benchmarks
- Necesite métricas diarias de riesgo
- Opere en entornos regulados con requisitos de auditoría
- Tenga múltiples tipos de fondo o estrategias

**Mercado primario**: Fondos de pensiones, gestoras de activos, aseguradoras, family offices institucionales.

---

## Métricas de Éxito

| Métrica | Objetivo |
|---------|----------|
| Tasa de éxito del pipeline | > 99% |
| Latencia end-to-end | < 30 min |
| Controles de calidad pasados | 9/9 |
| Tiempo analista liberado | > 4h/día |
| Hallazgos regulatorios | 0 por auditoría |
