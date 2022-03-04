import math
from numbers import Number
from typing import Callable, Dict, List, Union

import numpy as np
import pandas as pd

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.util import (
    _compute_bootstrap_quantiles_point_estimate_bias_corrected,
    _compute_bootstrap_quantiles_point_estimate_scipy,
    compute_bootstrap_quantiles_point_estimate,
)
from great_expectations.util import probabilistic_test

# Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
# (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
RTOL: float = 1.0e-7
ATOL: float = 1.0e-2


def _compute_quantile_root_mean_squared_error_of_bootstrap(
    method: Callable,
    false_positive_rate: np.float64,
    distribution_parameters: Dict[str, Dict[str, Number]],
    distribution_samples: pd.DataFrame,
):
    distribution_types: pd.Index = distribution_samples.columns
    distribution: str

    distribution_lower_quantile_point_estimate: np.float64
    distribution_upper_quantile_point_estimate: np.float64
    lower_quantile_point_estimates: List[np.float64] = []
    upper_quantile_point_estimates: List[np.float64] = []
    lower_quantile_residuals: List[Number] = []
    upper_quantile_residuals: List[Number] = []

    for distribution in distribution_types:
        (
            distribution_lower_quantile_point_estimate,
            distribution_upper_quantile_point_estimate,
        ) = method(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )

        lower_quantile_point_estimates.append(
            distribution_lower_quantile_point_estimate
        )
        upper_quantile_point_estimates.append(
            distribution_upper_quantile_point_estimate
        )

        distribution_lower_quantile_residual_legacy: Number = abs(
            distribution_lower_quantile_point_estimate
            - distribution_parameters[distribution]["lower_quantile"]
        )
        distribution_upper_quantile_residual_legacy: Number = abs(
            distribution_upper_quantile_point_estimate
            - distribution_parameters[distribution]["upper_quantile"]
        )

        lower_quantile_residuals.append(distribution_lower_quantile_residual_legacy)
        upper_quantile_residuals.append(distribution_upper_quantile_residual_legacy)

    # RMSE
    lower_quantile_root_mean_squared_error: Number = math.sqrt(
        sum([r**2 for r in lower_quantile_residuals]) / len(lower_quantile_residuals)
    )
    upper_quantile_root_mean_squared_error: Number = math.sqrt(
        sum([r**2 for r in upper_quantile_residuals]) / len(upper_quantile_residuals)
    )

    return (
        lower_quantile_root_mean_squared_error,
        upper_quantile_root_mean_squared_error,
    )


def test_bootstrap_point_estimate_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    distribution_types: pd.Index = distribution_samples.columns
    distribution: str
    lower_quantile_point_estimate: np.float64
    upper_quantile_point_estimate: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for distribution in distribution_types:
        (
            lower_quantile_point_estimate,
            upper_quantile_point_estimate,
        ) = compute_bootstrap_quantiles_point_estimate(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[distribution] = (
            1.0
            - np.sum(
                distribution_samples[distribution].between(
                    lower_quantile_point_estimate, upper_quantile_point_estimate
                )
            )
            / distribution_samples.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + 0.01
        )


def test_bootstrap_point_estimate_bias_corrected_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    distribution_types: pd.Index = distribution_samples.columns
    distribution: str
    lower_quantile_point_estimate_bias_corrected: np.float64
    upper_quantile_point_estimate_bias_corrected: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for distribution in distribution_types:
        (
            lower_quantile_point_estimate_bias_corrected,
            upper_quantile_point_estimate_bias_corrected,
        ) = _compute_bootstrap_quantiles_point_estimate_bias_corrected(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[distribution] = (
            1.0
            - np.sum(
                distribution_samples[distribution].between(
                    lower_quantile_point_estimate_bias_corrected,
                    upper_quantile_point_estimate_bias_corrected,
                )
            )
            / distribution_samples.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + 0.01
        )


def test_bootstrap_point_estimate_scipy_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    distribution_types: pd.Index = distribution_samples.columns
    distribution: str
    lower_quantile_point_estimate_scipy: np.float64
    upper_quantile_point_estimate_scipy: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for distribution in distribution_types:
        (
            lower_quantile_point_estimate_scipy,
            upper_quantile_point_estimate_scipy,
        ) = _compute_bootstrap_quantiles_point_estimate_scipy(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[distribution] = (
            1.0
            - np.sum(
                distribution_samples[distribution].between(
                    lower_quantile_point_estimate_scipy,
                    upper_quantile_point_estimate_scipy,
                )
            )
            / distribution_samples.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + 0.01
        )


@probabilistic_test
def test_compare_bootstrap_large_sample_point_estimate_performance(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
        "distribution_parameters"
    ]
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    (
        lower_quantile_root_mean_squared_error,
        upper_quantile_root_mean_squared_error,
    ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
        method=compute_bootstrap_quantiles_point_estimate,
        false_positive_rate=false_positive_rate,
        distribution_parameters=distribution_parameters,
        distribution_samples=distribution_samples,
    )

    (
        lower_quantile_root_mean_squared_error_bias_corrected,
        upper_quantile_root_mean_squared_error_bias_corrected,
    ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
        method=_compute_bootstrap_quantiles_point_estimate_bias_corrected,
        false_positive_rate=false_positive_rate,
        distribution_parameters=distribution_parameters,
        distribution_samples=distribution_samples,
    )

    (
        lower_quantile_root_mean_squared_error_scipy,
        upper_quantile_root_mean_squared_error_scipy,
    ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
        method=_compute_bootstrap_quantiles_point_estimate_scipy,
        false_positive_rate=false_positive_rate,
        distribution_parameters=distribution_parameters,
        distribution_samples=distribution_samples,
    )

    # SciPy with "BCa" bias correction and "Mean of the Confidence Interval" point estimate consistently underperforms
    # custom implementation
    assert (
        lower_quantile_root_mean_squared_error_scipy
        > lower_quantile_root_mean_squared_error
    )
    assert (
        upper_quantile_root_mean_squared_error_scipy
        > upper_quantile_root_mean_squared_error
    )

    # Custom bias corrected point estimate consistently underperforms custom biased estimator implementation
    assert (
        lower_quantile_root_mean_squared_error_bias_corrected
        > lower_quantile_root_mean_squared_error
    )
    assert (
        upper_quantile_root_mean_squared_error_bias_corrected
        > upper_quantile_root_mean_squared_error
    )
