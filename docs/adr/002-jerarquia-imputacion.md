# ADR-002: Jerarquía de 9 Niveles para Imputación de Retornos

## Estado
Aceptado

## Contexto

El modelo requiere 252 días de historia por instrumento. No todos tienen dato real cada día (instrumentos nuevos, ilíquidos, gaps de datos). Necesitamos completar faltantes de forma sistemática, transparente y auditable.

## Decisión

Implementar 9 niveles de búsqueda jerárquica, del más específico al más general:

```
Nivel 1: tipo + fondo + clase + gestor + C1 + C2 + C3 + C4 + C5 (exacto)
Nivel 2: relaja C5 (plazo)
...
Nivel 9: solo tipo de instrumento (más general)
```

Cada registro imputado lleva:
- `origen = "EQUIVALENTE"`
- `nivel_jerarquia = N`

## Consecuencias

- **Cobertura completa**: Todo instrumento obtiene 252 días de historia
- **Gradiente de calidad**: La mayoría de imputaciones usan niveles 1-3
- **Auditable**: Cada registro muestra su fuente
- **Costo computacional**: 9 joins por instrumento × 252 días (aceptable en Glue)

## Alternativas descartadas

| Alternativa | Motivo de rechazo |
|-------------|-------------------|
| Rellenar con ceros | Sesga TE hacia abajo |
| Forward-fill | Introduce autocorrelación |
| Eliminar instrumentos con gaps | Reduce universo, sesga hacia líquidos |
