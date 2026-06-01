# Arquitectura — Pipeline de Métricas de Riesgo

## Diagrama de Contenedores

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS (Producción)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  EventBridge ──► Step Functions (orquestador)                       │
│                       │                                             │
│                       ├── Lambda: ¿Es día hábil?                    │
│                       ├── Glue Job 1: Staging incremental           │
│                       ├── Glue Job 2: Historia real                 │
│                       ├── Glue Job 3: Imputación (9 niveles)        │
│                       ├── Glue Job 4: TE / Beta / VaR → Redshift   │
│                       └── Validación: Query a Redshift              │
│                                                                     │
│  ┌────────────┐    ┌────────────┐    ┌─────────────────┐           │
│  │ S3 Bucket  │    │ Redshift   │    │ Secrets Manager │           │
│  │ staging/   │    │ 12 tablas  │    │ Credenciales    │           │
│  │ history/   │    │ 5 vistas   │    │                 │           │
│  │ complete/  │    │            │    │                 │           │
│  └────────────┘    └────────────┘    └─────────────────┘           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Flujo de Datos

```
MySQL (fuente) → Job 1 → S3 staging/ (Parquet, particionado year/month)
                          │
                          ▼
                 Job 2 → S3 history/ (solo instrumentos activos al corte)
                          │
                          ▼
                 Job 3 → S3 complete/ (252 días × instrumento, imputado)
                          │
                          ▼
                 Job 4 → Redshift analytics.* (12 tablas + 5 vistas)
                          │
                          ▼
                 Dashboards / Reportes regulatorios / Validaciones
```

## Seguridad

| Componente | Acceso |
|-----------|--------|
| Glue Jobs | IAM Role: S3 (R/W bucket específico), Redshift, Secrets Manager |
| GitHub Actions | OIDC Role: S3 (solo glue-scripts/), Glue (UpdateJob) |
| Step Functions | IAM Role: Glue (StartJobRun), Lambda (Invoke), Redshift Data API |
| Credenciales | Secrets Manager (rotación automática) |

## Requisitos No Funcionales

| Requisito | Objetivo | Cómo se logra |
|-----------|----------|---------------|
| Disponibilidad | 99.9% | Serverless (Glue), retry en Step Functions |
| Idempotencia | Total | Dynamic partition overwrite + DELETE/INSERT por corte |
| Latencia | < 30 min | Lecturas paralelas, DataFrames cacheados |
| Auditabilidad | Completa | Columnas origen, jerarquía, método en cada registro |
| Costo | < $250/mes | Auto-scaling Glue, S3 lifecycle |
