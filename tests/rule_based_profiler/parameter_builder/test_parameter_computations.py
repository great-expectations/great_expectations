from numbers import Number
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import scipy.stats as stats

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.util import (
    _compute_bootstrap_quantiles_point_estimate_legacy,
    compute_bootstrap_quantiles_bias_corrected_point_estimate,
)


def _generate_distribution_parameters(
    false_positive_rate: np.float64,
) -> Dict[str, Dict[str, Number]]:
    normal_mean: int = 5000
    normal_stdev: int = 1000
    normal_lower_quantile: Number = stats.norm.ppf(
        q=false_positive_rate / 2, loc=normal_mean, scale=normal_stdev
    )
    normal_upper_quantile: Number = stats.norm.ppf(
        q=1 - false_positive_rate / 2, loc=normal_mean, scale=normal_stdev
    )

    uniform_lower_bound: int = 4000
    uniform_scale: int = 6000
    uniform_lower_quantile: Number = stats.uniform.ppf(
        q=false_positive_rate / 2, loc=uniform_lower_bound, scale=uniform_scale
    )
    uniform_upper_quantile: Number = stats.uniform.ppf(
        q=1 - false_positive_rate / 2, loc=uniform_lower_bound, scale=uniform_scale
    )

    bimodal_mean_1: int = 4000
    bimodal_stdev_1: int = 500
    bimodal_lower_quantile_1: Number = stats.norm.ppf(
        q=false_positive_rate / 2, loc=bimodal_mean_1, scale=bimodal_stdev_1
    )
    bimodal_upper_quantile_1: Number = stats.norm.ppf(
        q=1 - false_positive_rate / 2, loc=bimodal_mean_1, scale=bimodal_stdev_1
    )

    bimodal_mean_2: int = 6000
    bimodal_stdev_2: int = 500
    bimodal_lower_quantile_2: Number = stats.norm.ppf(
        q=false_positive_rate / 2, loc=bimodal_mean_2, scale=bimodal_stdev_2
    )
    bimodal_upper_quantile_2: Number = stats.norm.ppf(
        q=1 - false_positive_rate / 2, loc=bimodal_mean_2, scale=bimodal_stdev_2
    )

    exponential_shape: int = 1.5
    exponential_lower_bound: int = 5000
    exponential_scale: int = 1000
    exponential_lower_quantile: Number = stats.gamma.ppf(
        q=false_positive_rate / 2,
        a=exponential_shape,
        loc=exponential_lower_bound,
        scale=exponential_scale,
    )
    exponential_upper_quantile: Number = stats.gamma.ppf(
        q=1 - false_positive_rate / 2,
        a=exponential_shape,
        loc=exponential_lower_bound,
        scale=exponential_scale,
    )

    return {
        "normal": {
            "mean": normal_mean,
            "stdev": normal_stdev,
            "lower_quantile": normal_lower_quantile,
            "upper_quantile": normal_upper_quantile,
        },
        "uniform": {
            "lower_bound": uniform_lower_bound,
            "scale": uniform_scale,
            "lower_quantile": uniform_lower_quantile,
            "upper_quantile": uniform_upper_quantile,
        },
        "bimodal": {
            "mean_1": bimodal_mean_1,
            "stdev_1": bimodal_stdev_1,
            "lower_quantile_1": bimodal_lower_quantile_1,
            "upper_quantile_1": bimodal_upper_quantile_1,
            "mean_2": bimodal_mean_2,
            "stdev_2": bimodal_stdev_2,
            "lower_quantile_2": bimodal_lower_quantile_2,
            "upper_quantile_2": bimodal_upper_quantile_2,
        },
        "exponential": {
            "shape": exponential_shape,
            "lower_bound": exponential_lower_bound,
            "scale": exponential_scale,
            "lower_quantile": exponential_lower_quantile,
            "upper_quantile": exponential_upper_quantile,
        },
    }


def _generate_distribution_samples(
    distribution_parameters: Dict[str, Dict[str, Number]], size: Optional[int] = 36
) -> pd.DataFrame:
    data: Dict[str, np.ndarray] = {
        "normal": np.around(
            stats.norm.rvs(
                loc=distribution_parameters["normal"]["mean"],
                scale=distribution_parameters["normal"]["stdev"],
                size=size,
            )
        ),
        "uniform": np.around(
            stats.uniform.rvs(
                loc=distribution_parameters["uniform"]["lower_bound"],
                scale=distribution_parameters["uniform"]["scale"],
                size=size,
            )
        ),
        "bimodal": np.around(
            np.concatenate(
                [
                    stats.norm.rvs(
                        loc=distribution_parameters["bimodal"]["mean_1"],
                        scale=distribution_parameters["bimodal"]["stdev_1"],
                        size=size // 2,
                    ),
                    stats.norm.rvs(
                        loc=distribution_parameters["bimodal"]["mean_2"],
                        scale=distribution_parameters["bimodal"]["stdev_2"],
                        size=size // 2,
                    ),
                ]
            )
        ),
        "exponential": np.around(
            stats.gamma.rvs(
                a=distribution_parameters["exponential"]["shape"],
                loc=distribution_parameters["exponential"]["lower_bound"],
                scale=distribution_parameters["exponential"]["scale"],
                size=size,
            )
        ),
    }
    return pd.DataFrame(data)


def test_legacy_bootstrap_point_estimate_efficacy():
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    df: pd.DataFrame = _generate_distribution_samples(
        distribution_parameters=distribution_parameters, size=1000
    )
    columns: pd.Index = df.columns
    column: str
    lower_quantile_point_estimate: np.float64
    upper_quantile_point_estimate: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for column in columns:
        (
            lower_quantile_point_estimate,
            upper_quantile_point_estimate,
        ) = _compute_bootstrap_quantiles_point_estimate_legacy(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[column] = (
            1.0
            - np.sum(
                df[column].between(
                    lower_quantile_point_estimate, upper_quantile_point_estimate
                )
            )
            / df.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[column]
            <= false_positive_rate + 0.01
        )


def test_bootstrap_bias_corrected_point_estimate_efficacy():
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    df: pd.DataFrame = _generate_distribution_samples(
        distribution_parameters=distribution_parameters, size=1000
    )
    columns: pd.Index = df.columns
    column: str
    lower_quantile_bias_corrected_point_estimate: np.float64
    upper_quantile_bias_corrected_point_estimate: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for column in columns:
        (
            lower_quantile_bias_corrected_point_estimate,
            upper_quantile_bias_corrected_point_estimate,
        ) = compute_bootstrap_quantiles_bias_corrected_point_estimate(
            metric_values=df[column],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_false_positive_rates[column] = (
            1.0
            - np.sum(
                df[column].between(
                    lower_quantile_bias_corrected_point_estimate,
                    upper_quantile_bias_corrected_point_estimate,
                )
            )
            / df.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            false_positive_rate - 0.01
            <= actual_false_positive_rates[column]
            <= false_positive_rate + 0.01
        )


def test_compare_bootstrap_point_estimate_efficacy_with_bias_corrected():
    experiment_repetitions = 20
    # This works for sample size 1000 (used in other efficacy tests), but is very slow.
    # Regardless, bootstrap is typically used for smaller sample sizes and should be tested as such.
    # sample_size = 1000
    sample_size = 200
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    lower_quantile_point_estimate: np.float64
    upper_quantile_point_estimate: np.float64
    lower_quantile_bias_corrected_point_estimate: np.float64
    upper_quantile_bias_corrected_point_estimate: np.float64
    actual_biased_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    actual_bias_corrected_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    improvement: list = []
    for experiment in range(experiment_repetitions):
        df: pd.DataFrame = _generate_distribution_samples(
            distribution_parameters=distribution_parameters, size=1000
        )
        columns: pd.Index = df.columns
        column: str
        for column in columns:
            (
                lower_quantile_point_estimate,
                upper_quantile_point_estimate,
            ) = _compute_bootstrap_quantiles_point_estimate_legacy(
                metric_values=df[column],
                false_positive_rate=false_positive_rate,
                n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
            )
            actual_biased_false_positive_rates[column] = (
                1.0
                - np.sum(
                    df[column].between(
                        lower_quantile_point_estimate,
                        upper_quantile_point_estimate,
                    )
                )
                / df.shape[0]
            )

            (
                lower_quantile_bias_corrected_point_estimate,
                upper_quantile_bias_corrected_point_estimate,
            ) = compute_bootstrap_quantiles_bias_corrected_point_estimate(
                metric_values=df[column],
                false_positive_rate=false_positive_rate,
                n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
            )
            actual_bias_corrected_false_positive_rates[column] = (
                1.0
                - np.sum(
                    df[column].between(
                        lower_quantile_bias_corrected_point_estimate,
                        upper_quantile_bias_corrected_point_estimate,
                    )
                )
                / df.shape[0]
            )

            improvement.append(
                actual_biased_false_positive_rates[column]
                >= actual_bias_corrected_false_positive_rates[column]
            )

    # bias correction should consistently improve performance at least 90% of the time
    assert sum(improvement) / len(improvement) >= 0.90
