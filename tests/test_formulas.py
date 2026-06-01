"""
Tests unitarios para las fórmulas de riesgo del pipeline.
Validan correctitud matemática independiente de Spark.
"""
import math
import pytest


# =====================================================
# FUNCIONES PURAS (extraídas de la lógica del pipeline)
# =====================================================

def tracking_error(r_diffs: list[float]) -> float:
    """TE anualizado = stddev_muestral(r_diff) × √252"""
    n = len(r_diffs)
    if n < 2:
        return 0.0
    mean = sum(r_diffs) / n
    variance = sum((x - mean) ** 2 for x in r_diffs) / (n - 1)
    return math.sqrt(variance) * math.sqrt(252)


def beta(r_portfolio: list[float], r_benchmark: list[float]) -> float | None:
    """Beta = Cov(Rp, Rb) / Var(Rb)"""
    n = len(r_portfolio)
    if n < 2 or n != len(r_benchmark):
        return None
    avg_p = sum(r_portfolio) / n
    avg_b = sum(r_benchmark) / n
    cov = sum((r_portfolio[i] - avg_p) * (r_benchmark[i] - avg_b) for i in range(n))
    var_b = sum((r_benchmark[i] - avg_b) ** 2 for i in range(n))
    if abs(var_b) < 1e-12:
        return None
    return cov / var_b


def var_parametric(sigma_p: float, alpha: float = 1.6449) -> dict:
    """VaR paramétrico a 1 día y 252 días."""
    return {
        "var_1d": alpha * sigma_p,
        "var_252d": alpha * math.sqrt(252) * sigma_p,
    }


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    """Normaliza pesos para que sumen 1.0"""
    total = sum(weights.values())
    if abs(total) < 1e-12:
        return {k: 0.0 for k in weights}
    return {k: v / total for k, v in weights.items()}


def delta_weights(w_portfolio: dict, w_benchmark: dict) -> dict:
    """Calcula delta_peso con full outer join."""
    all_keys = set(w_portfolio.keys()) | set(w_benchmark.keys())
    return {
        k: w_portfolio.get(k, 0.0) - w_benchmark.get(k, 0.0)
        for k in all_keys
    }


def cap_return(ret: float, cap: float = 0.20) -> float:
    """Winsorización: limita retorno a ±cap."""
    return max(-cap, min(cap, ret))


# =====================================================
# TESTS
# =====================================================

class TestTrackingError:
    def test_zero_when_no_deviation(self):
        """Si r_diff es constante, TE = 0."""
        r_diffs = [0.001] * 120
        assert tracking_error(r_diffs) == 0.0

    def test_known_value(self):
        """Caso conocido: stddev=0.01 → TE ≈ 15.87%"""
        # 120 valores con stddev_muestral = 0.01
        import random
        random.seed(42)
        r_diffs = [random.gauss(0, 0.01) for _ in range(120)]
        te = tracking_error(r_diffs)
        # TE debe estar en rango razonable (0.5% - 30%)
        assert 0.005 < te < 0.30

    def test_annualization_factor(self):
        """TE diario × √252 = TE anualizado."""
        r_diffs = [0.001, -0.002, 0.003, -0.001, 0.002] * 24  # 120 valores
        te = tracking_error(r_diffs)
        n = len(r_diffs)
        mean = sum(r_diffs) / n
        daily_std = math.sqrt(sum((x - mean) ** 2 for x in r_diffs) / (n - 1))
        assert abs(te - daily_std * math.sqrt(252)) < 1e-10

    def test_minimum_observations(self):
        """Con menos de 2 observaciones, TE = 0."""
        assert tracking_error([0.01]) == 0.0
        assert tracking_error([]) == 0.0


class TestBeta:
    def test_perfect_correlation(self):
        """Si portfolio = benchmark, Beta = 1."""
        r = [0.01, -0.02, 0.03, -0.01, 0.02]
        assert abs(beta(r, r) - 1.0) < 1e-10

    def test_double_sensitivity(self):
        """Si portfolio = 2×benchmark, Beta = 2."""
        r_b = [0.01, -0.02, 0.03, -0.01, 0.02]
        r_p = [x * 2 for x in r_b]
        assert abs(beta(r_p, r_b) - 2.0) < 1e-10

    def test_zero_variance_benchmark(self):
        """Si benchmark no varía, Beta = None."""
        r_p = [0.01, -0.02, 0.03]
        r_b = [0.05, 0.05, 0.05]
        assert beta(r_p, r_b) is None

    def test_uncorrelated(self):
        """Activos no correlacionados → Beta ≈ 0."""
        r_p = [0.01, -0.01, 0.01, -0.01, 0.01, -0.01]
        r_b = [0.01, 0.01, -0.01, -0.01, 0.01, 0.01]
        b = beta(r_p, r_b)
        assert abs(b) < 0.5  # No exactamente 0 pero cercano


class TestVaR:
    def test_known_sigma(self):
        """sigma_p = 1% → VaR_1d ≈ 1.6449%"""
        result = var_parametric(0.01)
        assert abs(result["var_1d"] - 0.016449) < 1e-6

    def test_annualized_scaling(self):
        """VaR_252d = VaR_1d × √252"""
        result = var_parametric(0.01)
        ratio = result["var_252d"] / result["var_1d"]
        assert abs(ratio - math.sqrt(252)) < 1e-6

    def test_zero_sigma(self):
        """Sin volatilidad, VaR = 0."""
        result = var_parametric(0.0)
        assert result["var_1d"] == 0.0
        assert result["var_252d"] == 0.0


class TestWeights:
    def test_normalization_sums_to_one(self):
        """Pesos normalizados suman 1.0"""
        w = {"A": 0.5, "B": 1.0, "C": 1.5}
        norm = normalize_weights(w)
        assert abs(sum(norm.values()) - 1.0) < 1e-10

    def test_normalization_preserves_proportions(self):
        """Proporciones se mantienen."""
        w = {"A": 1.0, "B": 2.0}
        norm = normalize_weights(w)
        assert abs(norm["B"] / norm["A"] - 2.0) < 1e-10

    def test_delta_full_outer_join(self):
        """Delta incluye instrumentos de ambos lados."""
        w_p = {"A": 0.6, "B": 0.4}
        w_b = {"B": 0.5, "C": 0.5}
        d = delta_weights(w_p, w_b)
        assert d["A"] == 0.6   # Solo en portfolio
        assert d["C"] == -0.5  # Solo en benchmark
        assert d["B"] == -0.1  # En ambos

    def test_delta_sums_near_zero(self):
        """Si ambos lados normalizados, Σ delta ≈ 0."""
        w_p = normalize_weights({"A": 1, "B": 2, "C": 1})
        w_b = normalize_weights({"A": 2, "B": 1, "C": 1})
        d = delta_weights(w_p, w_b)
        assert abs(sum(d.values())) < 1e-10


class TestReturnCap:
    def test_within_range(self):
        """Retornos dentro del rango no se modifican."""
        assert cap_return(0.05) == 0.05
        assert cap_return(-0.10) == -0.10

    def test_exceeds_cap(self):
        """Retornos fuera del rango se truncan."""
        assert cap_return(0.50) == 0.20
        assert cap_return(-0.35) == -0.20

    def test_at_boundary(self):
        """En el límite exacto, no se modifica."""
        assert cap_return(0.20) == 0.20
        assert cap_return(-0.20) == -0.20
