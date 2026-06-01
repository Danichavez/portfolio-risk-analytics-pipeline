# ADR-001: Dynamic Partition Overwrite para Staging Idempotente

## Estado
Aceptado

## Contexto

Job 1 extrae datos incrementales de MySQL a S3. La implementación original usaba `mode("append")` que duplicaba registros en re-ejecuciones, corrompiendo los cálculos downstream.

## Decisión

Usar `partitionOverwriteMode = "dynamic"` con `mode("overwrite")`:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
```

## Consecuencias

- **Idempotente**: N ejecuciones con los mismos datos producen el mismo resultado
- **Aislamiento**: Solo se reescriben particiones presentes en el DataFrame de salida
- **Sin duplicados**: Elimina el problema raíz sin agregar lógica de deduplicación
- **Compatible**: Schema Parquet y paths S3 no cambian — jobs downstream sin modificaciones

## Alternativas descartadas

| Alternativa | Motivo de rechazo |
|-------------|-------------------|
| append + dedup downstream | Agrega complejidad a Jobs 2-4 |
| Delta Lake MERGE | Over-engineering para este caso |
| Delete partición + write | No atómico, riesgo de pérdida |
