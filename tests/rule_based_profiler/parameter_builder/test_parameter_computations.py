from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import scipy.stats as stats

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.util import (
    compute_bootstrap_quantiles,
    compute_bootstrap_quantiles_legacy,
)
from tests.conftest import skip_if_python_below_minimum_version


def _generate_distribution_samples(size: Optional[int] = 36) -> pd.DataFrame:
    data: Dict[str, np.ndarray] = {
        "normal": np.around(stats.norm.rvs(5000, 1000, size=size)),
        "uniform": np.around(stats.uniform.rvs(4000, 6000, size=size)),
        "bimodal": np.around(
            np.concatenate(
                [
                    stats.norm.rvs(4000, 500, size=size // 2),
                    stats.norm.rvs(6000, 500, size=size // 2),
                ]
            )
        ),
        "exponential": np.around(
            stats.gamma.rvs(a=1.5, loc=5000, scale=1000, size=size)
        ),
    }
    return pd.DataFrame(data)


def test_legacy_bootstrap_efficacy():
    df: pd.DataFrame = _generate_distribution_samples(size=1000)
    false_positive_rate: np.float64 = np.float64(0.01)
    columns: pd.Index = df.columns
    column: str
    lower_quantile: np.float64
    upper_quantile: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for column in columns:
        (lower_quantile, upper_quantile,) = compute_bootstrap_quantiles_legacy(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[column] = (
            1.0
            - np.sum(df[column].between(lower_quantile, upper_quantile)) / df.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[column]
            <= false_positive_rate + 0.01
        )


def test_bootstrap_efficacy():
    skip_if_python_below_minimum_version()

    df: pd.DataFrame = _generate_distribution_samples(size=1000)
    false_positive_rate: np.float64 = np.float64(0.01)
    columns: pd.Index = df.columns
    column: str
    lower_quantile: np.float64
    upper_quantile: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}

    for column in columns:
        (lower_quantile, upper_quantile,) = compute_bootstrap_quantiles(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[column] = (
            1.0
            - np.sum(df[column].between(lower_quantile, upper_quantile)) / df.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[column]
            <= false_positive_rate + 0.01
        )


def test_compare_legacy_and_new_bootstrap_results():
    skip_if_python_below_minimum_version

    df: pd.DataFrame = _generate_distribution_samples(size=1000)
    false_positive_rate: np.float64 = np.float64(0.01)
    columns: pd.Index = df.columns
    column: str
    lower_quantile_new: np.float64
    upper_quantile_new: np.float64
    actual_false_positive_rates_new: Dict[str, Union[float, np.float64]] = {}
    lower_quantile_legacy: np.float64
    upper_quantile_legacy: np.float64
    actual_false_positive_rates_legacy: Dict[str, Union[float, np.float64]] = {}

    for column in columns:
        (lower_quantile_new, upper_quantile_new,) = compute_bootstrap_quantiles(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates_new[column] = (
            1.0
            - np.sum(df[column].between(lower_quantile_new, upper_quantile_new))
            / df.shape[0]
        )
        (
            lower_quantile_legacy,
            upper_quantile_legacy,
        ) = compute_bootstrap_quantiles_legacy(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates_legacy[column] = (
            1.0
            - np.sum(df[column].between(lower_quantile_legacy, upper_quantile_legacy))
            / df.shape[0]
        )

        new_false_positive_error: float = abs(
            actual_false_positive_rates_new[column] - false_positive_rate
        )
        legacy_false_positive_error: float = abs(
            actual_false_positive_rates_legacy[column] - false_positive_rate
        )

        # Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
        # (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
        rtol: float = 1.0e-7
        atol: float = 5.0e-2

        try:
            np.testing.assert_allclose(
                new_false_positive_error,
                legacy_false_positive_error,
                rtol=rtol,
                atol=atol,
            )
        except AssertionError:
            assert new_false_positive_error < legacy_false_positive_error
