from numbers import Number
from typing import Dict, List, Union

import numpy as np
import pandas as pd

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.util import (
    _compute_bootstrap_quantiles_point_estimate_legacy,
    compute_bootstrap_quantiles_bias_corrected_point_estimate,
)


def test_legacy_bootstrap_point_estimate_efficacy(
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
        ) = _compute_bootstrap_quantiles_point_estimate_legacy(
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


def test_bootstrap_bias_corrected_point_estimate_efficacy(
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
    lower_quantile_bias_corrected_point_estimate: np.float64
    upper_quantile_bias_corrected_point_estimate: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for distribution in distribution_types:
        (
            lower_quantile_bias_corrected_point_estimate,
            upper_quantile_bias_corrected_point_estimate,
        ) = compute_bootstrap_quantiles_bias_corrected_point_estimate(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[distribution] = (
            1.0
            - np.sum(
                distribution_samples[distribution].between(
                    lower_quantile_bias_corrected_point_estimate,
                    upper_quantile_bias_corrected_point_estimate,
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


def test_legacy_bootstrap_point_estimate_performance(
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
        ) = _compute_bootstrap_quantiles_point_estimate_legacy(
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

        distribution_lower_quantile_residual: Number = abs(
            distribution_lower_quantile_point_estimate
            - distribution_parameters["distribution"]["lower_quantile"]
        )
        distribution_upper_quantile_residual: Number = abs(
            distribution_upper_quantile_point_estimate
            - distribution_parameters["distribution"]["upper_quantile"]
        )

        lower_quantile_residuals.append(distribution_lower_quantile_residual)
        upper_quantile_residuals.append(distribution_upper_quantile_residual)

    assert False
