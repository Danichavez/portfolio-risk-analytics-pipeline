# Reglas de Negocio — Pipeline de Métricas de Riesgo

## Parámetros del Modelo

| Parámetro | Valor | Descripción |
|-----------|-------|-------------|
| `TE_WINDOW_DAYS` | 120 | Ventana móvil en días hábiles |
| `RET_CAP` | ±20% | Cap de retornos comparables (winsorización) |
| `VAR_ALPHA` | 1.6449 | Cuantil normal 95% unilateral |
| `BENCHMARK_MODE` | AVG | Promedio de pesos entre gestores del sistema |

---

## Fórmulas

### Tracking Error Ex-Ante
```
r_diff(t) = Σᵢ [ delta_peso(i) × retorno_comparable(i, t) ]
TE = stddev_muestral(r_diff) × √252
```

### Beta Ex-Ante
```
Beta = Cov(R_portfolio, R_benchmark) / Var(R_benchmark)
```

### VaR 95% Paramétrico
```
σ²_p = w' × Σ × w    (matriz de covarianza completa)
VaR_1d = 1.6449 × σ_p
VaR_252d = 1.6449 × √252 × σ_p
```

---

## Reglas por Clase de Instrumento

| Clase | Agrupación | Retorno | Peso |
|-------|-----------|---------|------|
| **MON** | Por moneda (C1) | Promedio spot. Moneda local = 0 | Valorización económica |
| **SWP TASA** | Por bucket de plazo (C5) | aporte/peso_exposición. Plazo 0 = 0 | Exposición económica |
| **SWP no-TASA** | — | 0 (forzado) | 0 (forzado) |
| **ACT** | Por nemotécnico | aporte/peso_valorización | Valorización económica |
| **ALT** | Por región + tipo (C2, C3) | Tabla de parámetros | Valorización económica |
| **Otros** | Por nemotécnico | aporte/peso_exposición | Exposición económica |

---

## Exclusiones

Se excluyen antes de cualquier cálculo:
- FWD (forwards)
- MTM (mark-to-market)
- ACT + CAJA / MTM / CC2 / CC3

---

## Jerarquía de Imputación (9 niveles)

Cuando un instrumento no tiene retorno real en un día histórico:

| Nivel | Columnas de match | Ejemplo |
|-------|------------------|---------|
| 1 | tipo + fondo + clase + gestor + C1-C5 | Match exacto |
| 2 | tipo + fondo + clase + gestor + C1-C4 | Relaja plazo |
| ... | ... | ... |
| 9 | tipo | Solo tipo de instrumento |

Cada registro lleva: `origen` (REAL / EQUIVALENTE / NO_EQUIVALENTE) + `nivel_jerarquia`

---

## Normalización de Pesos

- **Para TE y Beta**: Cada lado (portfolio, benchmark) normalizado a suma = 1.0
- **Para VaR**: Pesos brutos (captura apalancamiento real de derivados)
- **Matching**: Full outer join entre portfolio y benchmark (peso = 0 si no existe en un lado)

---

## Con Derivados vs Sin Derivados

| Aspecto | Con Derivados | Sin Derivados |
|---------|--------------|---------------|
| Pesos portfolio | Todos | Excluye SWP |
| Pesos benchmark | Todos | Todos |
| Interpretación | Riesgo real | Riesgo sin cobertura |
