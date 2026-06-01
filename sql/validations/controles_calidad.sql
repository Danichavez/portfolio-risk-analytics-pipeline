-- =====================================================
-- Controles de Calidad — Pipeline de Métricas de Riesgo
-- Ejecutar después de cada corrida para validar resultados
-- =====================================================

-- 1. Suma de pesos portfolio = 1.0 por fondo
SELECT fund_type, ROUND(SUM(side_weight)::numeric, 6) AS suma_pesos
FROM analytics.te_pesos_consolidados
WHERE side = 'PORTFOLIO'
GROUP BY 1
HAVING ABS(SUM(side_weight) - 1.0) > 0.001;

-- 2. Suma de pesos benchmark = 1.0 por fondo
SELECT fund_type, ROUND(SUM(side_weight)::numeric, 6) AS suma_pesos
FROM analytics.te_pesos_consolidados
WHERE side = 'BENCHMARK'
GROUP BY 1
HAVING ABS(SUM(side_weight) - 1.0) > 0.001;

-- 3. Suma delta_peso ≈ 0 por fondo
SELECT fund_type, ROUND(SUM(delta_weight)::numeric, 8) AS suma_delta
FROM analytics.te_pesos_comparables
GROUP BY 1
HAVING ABS(SUM(delta_weight)) > 0.01;

-- 4. SWP con plazo cero debe tener retorno = 0
SELECT *
FROM analytics.te_retornos_base
WHERE asset_class = 'SWP'
  AND c2 = 'TASA'
  AND TRIM(c5) = '0'
  AND return_calc != 0;

-- 5. Retornos comparables dentro del cap ±20%
SELECT COUNT(*) AS violaciones_cap
FROM analytics.te_retornos_comparables
WHERE ABS(return_calc) > 0.20;

-- 6. Ventana de cálculo = 120 días
SELECT fund_type, COUNT(DISTINCT fecha) AS dias
FROM analytics.te_exante_diario
GROUP BY 1
HAVING COUNT(DISTINCT fecha) != 120;

-- 7. TE en rango razonable (0% - 5%)
SELECT fund_type, te_ex_ante
FROM analytics.te_exante_resumen
WHERE te_ex_ante < 0 OR te_ex_ante > 0.05;

-- 8. Beta en rango razonable (0 - 2)
SELECT fund_type, beta_ex_ante
FROM analytics.te_beta_exante_resumen
WHERE beta_ex_ante < 0 OR beta_ex_ante > 2.0;

-- 9. VaR positivo
SELECT fund_type, var_95_1d
FROM analytics.te_var_exante_resumen
WHERE var_95_1d <= 0;
