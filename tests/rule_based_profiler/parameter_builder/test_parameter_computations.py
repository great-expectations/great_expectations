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
from tests.conftest import skip_if_python_below_minimum_version

# Allowable tolerance for how closely a bootstrap method approximates the sample
EFFICACY_TOLERANCE: float = 1.0e-2

# Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
# (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
RTOL: float = 1.0e-7
ATOL: float = 1.0e-2


def compute_quantile_root_mean_squared_error_of_bootstrap(
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
        np.testing.assert_allclose(
            actual=actual_false_positive_rates[distribution],
            desired=false_positive_rate,
            rtol=RTOL,
            atol=EFFICACY_TOLERANCE,
            err_msg=f"Actual value of {actual_false_positive_rates[distribution]} differs from expected value of {false_positive_rate} by more than {ATOL + EFFICACY_TOLERANCE * abs(actual_false_positive_rates[distribution])} tolerance.",
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
        np.testing.assert_allclose(
            actual=actual_false_positive_rates[distribution],
            desired=false_positive_rate,
            rtol=RTOL,
            atol=EFFICACY_TOLERANCE,
            err_msg=f"Actual value of {actual_false_positive_rates[distribution]} differs from expected value of {false_positive_rate} by more than {ATOL + EFFICACY_TOLERANCE * abs(actual_false_positive_rates[distribution])} tolerance.",
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
        np.testing.assert_allclose(
            actual=actual_false_positive_rates[distribution],
            desired=false_positive_rate,
            rtol=RTOL,
            atol=EFFICACY_TOLERANCE,
            err_msg=f"Actual value of {actual_false_positive_rates[distribution]} differs from expected value of {false_positive_rate} by more than {ATOL + EFFICACY_TOLERANCE * abs(actual_false_positive_rates[distribution])} tolerance.",
        )
