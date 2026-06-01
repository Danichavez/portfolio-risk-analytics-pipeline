# ADR-003: Normalización Independiente de Pesos por Lado

## Estado
Aceptado

## Contexto

Los pesos brutos del sistema fuente suman ~2.6 a 3.4 por fondo (no 1.0) debido a posiciones derivadas y doble conteo. Sin normalización, el delta_peso se amplifica artificialmente y todas las métricas quedan distorsionadas.

## Decisión

Normalizar pesos **independientemente** por cada lado:

```
peso_normalizado(i) = peso_bruto(i) / Σ peso_bruto(j)  por (corte, fondo, lado)
```

Resultado: `Σ peso = 1.0` por (corte, fondo, lado). Delta_peso total ≈ 0.

**Excepción**: VaR usa pesos brutos para capturar apalancamiento real de derivados.

## Consecuencias

- **TE y Beta correctos**: En escala interpretable (% anualizado)
- **Comparable entre fondos**: Todos tienen la misma escala de pesos
- **VaR captura leverage**: Pesos brutos reflejan exposición real
- **Trade-off**: Se pierde información de apalancamiento en TE/Beta (aceptable porque TE mide desviación relativa, no absoluta)

## Alternativas descartadas

| Alternativa | Motivo de rechazo |
|-------------|-------------------|
| Sin normalización | TE sin sentido (escala depende de suma de pesos brutos) |
| Denominador común | Introduce asimetría entre lados |
| Cap a 1.0 | Arbitrario, pierde información |
