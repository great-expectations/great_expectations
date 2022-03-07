import math
from numbers import Number
from typing import Callable, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import scipy
from packaging import version

from great_expectations.rule_based_profiler.helpers.util import (
    _compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method,
    _compute_bootstrap_quantiles_point_estimate_custom_mean_method,
    _compute_bootstrap_quantiles_point_estimate_scipy_confidence_interval_midpoint_method,
)
from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.util import probabilistic_test
from tests.conftest import skip_if_python_below_minimum_version

# Allowable tolerance for how closely a bootstrap method approximates the sample
EFFICACY_TOLERANCE: float = 1.0e-2

# Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
# (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
RTOL: float = 1.0e-7
ATOL: float = 1.0e-2


def _compute_quantile_root_mean_squared_error_of_bootstrap(
    method: Callable,
    false_positive_rate: np.float64,
    distribution_parameters: Dict[str, Dict[str, Number]],
    distribution_samples: pd.DataFrame,
) -> Tuple[Number, Number]:
    """
    Computes the root mean squared error (RMSE) for how closely the lower and upper quantile point estimates from the
    sample, approximate the lower and upper quantile parameters in the population.

    Root mean squared error was selected as a performance metric, due to its abilities to evaluate prediction accuracy
    among different models, but there are other options.

    Performance metrics for continuous values:
        - Mean absolute error (MAE)
        - Mean squred error (MSE)
        - Root mean squared error (RMSE)
        - R squared
        - Adjusted R squared

    For a detailed comparison see:
    https://medium.com/analytics-vidhya/mae-mse-rmse-coefficient-of-determination-adjusted-r-squared-which-metric-is-better-cd0326a5697e

    sklearn and statsmodels both have built-in methods for computing RMSE, but neither are GE dependencies as of 3/4/22.
    """
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
    """
    Efficacy means the custom bootstrap mean method approximates the sample +/- efficacy tolerance
    """
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
        ) = _compute_bootstrap_quantiles_point_estimate_custom_mean_method(
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
        # Actual false-positives must be within the efficacy tolerance of desired (configured)
        # false_positive_rate parameter value.
        assert (
            false_positive_rate - EFFICACY_TOLERANCE
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + EFFICACY_TOLERANCE
        )


def test_bootstrap_point_estimate_bias_corrected_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    """
    Efficacy means the custom bootstrap bias corrected method approximates the sample +/- efficacy tolerance
    """
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
        ) = _compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method(
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
        # Actual false-positives must be within the efficacy tolerance of desired (configured)
        # false_positive_rate parameter value.
        assert (
            false_positive_rate - EFFICACY_TOLERANCE
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + EFFICACY_TOLERANCE
        )


def test_bootstrap_point_estimate_scipy_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    """
    Efficacy means the scipy.stats.bootstrap confidence interval midpoint method
    approximates the sample +/- efficacy tolerance
    """
    skip_if_python_below_minimum_version()

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
        ) = _compute_bootstrap_quantiles_point_estimate_scipy_confidence_interval_midpoint_method(
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
        # Actual false-positives must be within the efficacy tolerance of desired (configured)
        # false_positive_rate parameter value.
        assert (
            false_positive_rate - EFFICACY_TOLERANCE
            <= actual_false_positive_rates[distribution]
            <= false_positive_rate + EFFICACY_TOLERANCE
        )


@probabilistic_test
def test_compare_bootstrap_small_sample_point_estimate_performance(
    bootstrap_distribution_parameters_and_20_samples_with_01_false_positive,
):
    # We measure performance on a small metric value sample size. As metric value sample size gets large,
    # the relative performance of each method becomes chaotic and the decision to use the bootstrap method
    # in the first place becomes questionable.

    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_20_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = bootstrap_distribution_parameters_and_20_samples_with_01_false_positive[
        "distribution_parameters"
    ]
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_20_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    (
        lower_quantile_root_mean_squared_error_mean,
        upper_quantile_root_mean_squared_error_mean,
    ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
        method=_compute_bootstrap_quantiles_point_estimate_custom_mean_method,
        false_positive_rate=false_positive_rate,
        distribution_parameters=distribution_parameters,
        distribution_samples=distribution_samples,
    )

    (
        lower_quantile_root_mean_squared_error_bias_corrected,
        upper_quantile_root_mean_squared_error_bias_corrected,
    ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
        method=_compute_bootstrap_quantiles_point_estimate_custom_bias_corrected_method,
        false_positive_rate=false_positive_rate,
        distribution_parameters=distribution_parameters,
        distribution_samples=distribution_samples,
    )

    # Custom bias corrected point estimate consistently outperforms custom biased estimator implementation when
    # metric value sample size is large
    total_root_mean_squared_error_bias_corrected: Number = (
        lower_quantile_root_mean_squared_error_bias_corrected
        + upper_quantile_root_mean_squared_error_bias_corrected
    )
    total_root_mean_squared_error_mean: Number = (
        lower_quantile_root_mean_squared_error_mean
        + upper_quantile_root_mean_squared_error_mean
    )
    assert (
        total_root_mean_squared_error_bias_corrected
        < total_root_mean_squared_error_mean
    )

    # scipy.stats.bootstrap wasn't implemented until scipy 1.6
    if version.parse(scipy.__version__) >= version.parse("1.6"):
        (
            lower_quantile_root_mean_squared_error_scipy,
            upper_quantile_root_mean_squared_error_scipy,
        ) = _compute_quantile_root_mean_squared_error_of_bootstrap(
            method=_compute_bootstrap_quantiles_point_estimate_scipy_confidence_interval_midpoint_method,
            false_positive_rate=false_positive_rate,
            distribution_parameters=distribution_parameters,
            distribution_samples=distribution_samples,
        )

        # SciPy with "BCa" bias correction and "Mean of the Confidence Interval" point estimate consistently
        # underperforms both custom implementations
        total_root_mean_squared_error_scipy = (
            lower_quantile_root_mean_squared_error_scipy
            + upper_quantile_root_mean_squared_error_scipy
        )
        assert total_root_mean_squared_error_scipy > total_root_mean_squared_error_mean
        assert (
            total_root_mean_squared_error_scipy
            > total_root_mean_squared_error_bias_corrected
        )
