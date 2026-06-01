# Modelo de Datos — Pipeline de Métricas de Riesgo

## Tablas en Redshift (schema `analytics`)

### Tablas de hechos (granularidad diaria)

| Tabla | Granularidad | Descripción |
|-------|-------------|-------------|
| `te_retornos_base` | activo × día × gestor × fondo | Retornos calculados por clase |
| `te_retornos_comparables` | activo × día × fondo | Retornos promedio de mercado (con derivados) |
| `te_retornos_comparables_sin_derivados` | activo × día × fondo | Retornos promedio (sin derivados) |
| `te_exante_diario` | día × fondo | Serie de r_diff (retorno diferencial) |

### Tablas de dimensión (granularidad al corte)

| Tabla | Granularidad | Descripción |
|-------|-------------|-------------|
| `te_pesos_base` | activo × gestor × fondo | Pesos brutos al día de corte |
| `te_pesos_consolidados` | activo × fondo × lado | Pesos normalizados (portfolio vs benchmark) |
| `te_pesos_comparables` | activo × fondo | Delta peso con derivados |
| `te_pesos_comparables_sin_derivados` | activo × fondo | Delta peso sin derivados |

### Tablas resumen (granularidad por fondo)

| Tabla | Contenido |
|-------|-----------|
| `te_exante_resumen` | TE anualizado por fondo |
| `te_beta_exante_resumen` | Beta por fondo |
| `te_var_exante_resumen` | VaR con derivados |
| `te_var_exante_resumen_sin_derivados` | VaR sin derivados |

### Vistas

| Vista | Propósito |
|-------|-----------|
| `v_te_exante_resumen_sin_derivados` | TE sin derivados (calculado desde tablas base) |
| `v_te_exante_diario_sin_derivados` | Serie diaria r_diff sin derivados |
| `v_te_beta_exante_resumen_sin_derivados` | Beta sin derivados |
| `v_te_desglose` | Descomposición del TE por activo (delta_peso × retorno) |

---

## Volúmenes (producción)

| Tabla | Registros/día | Estrategia |
|-------|--------------|------------|
| te_retornos_base | ~600K | DELETE + INSERT por corte |
| te_retornos_comparables | ~120K | DELETE + INSERT por corte |
| te_pesos_base | ~5K | DELETE + INSERT por corte |
| te_exante_diario | ~600 | DELETE + INSERT por corte |
| te_exante_resumen | 5 | DELETE + INSERT por corte |

**Almacenamiento total**: ~50 GB (S3 Parquet) + ~2 GB (Redshift)

---

## Relaciones clave

```
te_pesos_comparables.llave_comparable ←→ te_retornos_comparables.asset_key
te_exante_diario.r_diff = Σ(delta_peso × retorno_comparable)
te_exante_resumen.te_ex_ante = stddev(r_diff) × √252
```
