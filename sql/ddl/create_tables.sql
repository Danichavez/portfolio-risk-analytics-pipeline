-- =====================================================
-- DDL — Tablas de métricas de riesgo en Redshift
-- Schema: analytics
-- =====================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Tabla de parámetros para activos alternativos
CREATE TABLE IF NOT EXISTS analytics.parametros_retornos_alt (
    manager         VARCHAR(20),
    fund_type       VARCHAR(5),
    region          VARCHAR(50),
    asset_type      VARCHAR(100),
    return_param    NUMERIC(10,6)
)
DISTSTYLE AUTO;

-- Retornos base por activo/día/gestor/fondo
CREATE TABLE IF NOT EXISTS analytics.te_retornos_base (
    corte                       DATE        SORTKEY,
    fecha                       DATE,
    manager                     VARCHAR(50),
    fund_type                   VARCHAR(50),
    asset_key                   VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    c1                          VARCHAR(50),
    c2                          VARCHAR(50),
    c3                          VARCHAR(50),
    c4                          VARCHAR(50),
    c5                          VARCHAR(50),
    instrument_type             VARCHAR(50),
    return_calc                 DOUBLE PRECISION,
    weight_calc                 DOUBLE PRECISION,
    contribution_calc           DOUBLE PRECISION,
    currency_fallback_flag      INTEGER,
    source_record_count         BIGINT,
    observation_status          VARCHAR(20),
    imputation_method           VARCHAR(30)
)
DISTSTYLE AUTO;

-- Retornos comparables (con derivados)
CREATE TABLE IF NOT EXISTS analytics.te_retornos_comparables (
    corte                       DATE        SORTKEY,
    fecha                       DATE,
    fund_type                   VARCHAR(50),
    asset_key                   VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    return_calc                 DOUBLE PRECISION,
    observation_status          VARCHAR(20),
    imputation_method           VARCHAR(30),
    currency_fallback_flag      INTEGER
)
DISTSTYLE AUTO;

-- Retornos comparables (sin derivados)
CREATE TABLE IF NOT EXISTS analytics.te_retornos_comparables_sin_derivados (
    corte                       DATE        SORTKEY,
    fecha                       DATE,
    fund_type                   VARCHAR(50),
    asset_key                   VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    return_calc                 DOUBLE PRECISION,
    observation_status          VARCHAR(20),
    imputation_method           VARCHAR(30),
    currency_fallback_flag      INTEGER
)
DISTSTYLE AUTO;

-- Pesos base por activo al corte
CREATE TABLE IF NOT EXISTS analytics.te_pesos_base (
    corte                       DATE        SORTKEY,
    manager                     VARCHAR(50),
    fund_type                   VARCHAR(50),
    asset_key                   VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    comparable_instrument       VARCHAR(500),
    base_weight                 DOUBLE PRECISION,
    c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), c4 VARCHAR(50), c5 VARCHAR(50)
)
DISTSTYLE AUTO;

-- Pesos consolidados (portfolio vs benchmark, normalizados)
CREATE TABLE IF NOT EXISTS analytics.te_pesos_consolidados (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    asset_key                   VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    comparable_instrument       VARCHAR(500),
    side                        VARCHAR(50),
    side_weight                 DOUBLE PRECISION
)
DISTSTYLE AUTO;

-- Pesos comparables con delta (con derivados)
CREATE TABLE IF NOT EXISTS analytics.te_pesos_comparables (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    comparable_key              VARCHAR(300),
    key_type                    VARCHAR(4),
    asset_class                 VARCHAR(50),
    comparable_instrument       VARCHAR(500),
    portfolio_weight            DOUBLE PRECISION,
    benchmark_weight            DOUBLE PRECISION,
    delta_weight                DOUBLE PRECISION,
    exists_portfolio            INTEGER,
    exists_benchmark            INTEGER,
    zero_filled_portfolio       INTEGER,
    c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), c4 VARCHAR(50), c5 VARCHAR(50)
)
DISTSTYLE AUTO;

-- Pesos comparables (sin derivados)
CREATE TABLE IF NOT EXISTS analytics.te_pesos_comparables_sin_derivados (
    LIKE analytics.te_pesos_comparables
)
DISTSTYLE AUTO;

-- Serie diaria de retorno diferencial
CREATE TABLE IF NOT EXISTS analytics.te_exante_diario (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    fecha                       DATE,
    r_diff                      DOUBLE PRECISION,
    return_method               VARCHAR(30)
)
DISTSTYLE AUTO;

-- Resumen TE anualizado por fondo
CREATE TABLE IF NOT EXISTS analytics.te_exante_resumen (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    days                        INTEGER,
    avg_active_return           DOUBLE PRECISION,
    daily_tracking_error        DOUBLE PRECISION,
    te_ex_ante                  DOUBLE PRECISION
)
DISTSTYLE AUTO;

-- Beta ex-ante por fondo
CREATE TABLE IF NOT EXISTS analytics.te_beta_exante_resumen (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    days                        INTEGER,
    avg_r_portfolio             DOUBLE PRECISION,
    avg_r_benchmark             DOUBLE PRECISION,
    vol_portfolio_approx        DOUBLE PRECISION,
    vol_benchmark_approx        DOUBLE PRECISION,
    beta_ex_ante                DOUBLE PRECISION
)
DISTSTYLE AUTO;

-- VaR 95% (con derivados)
CREATE TABLE IF NOT EXISTS analytics.te_var_exante_resumen (
    corte                       DATE        SORTKEY,
    fund_type                   VARCHAR(50),
    portfolio_variance          DOUBLE PRECISION,
    sigma_p                     DOUBLE PRECISION,
    var_95_1d                   DOUBLE PRECISION,
    var_95_252d                 DOUBLE PRECISION
)
DISTSTYLE AUTO;

-- VaR 95% (sin derivados)
CREATE TABLE IF NOT EXISTS analytics.te_var_exante_resumen_sin_derivados (
    LIKE analytics.te_var_exante_resumen
)
DISTSTYLE AUTO;
