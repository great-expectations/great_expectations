from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import scipy.stats as stats
from scipy import special

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
    NP_SQRT_2,
    ConfidenceInterval,
    NumericMetricRangeMultiBatchParameterBuilder,
)


# Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.
def test_standard_deviation_band_around_mean_rule():
    confidence_level: float
    stds_multiplier: float

    confidence_level = 0.0
    stds_multiplier = NP_SQRT_2 * special.erfinv(confidence_level)
    assert np.isclose(stds_multiplier, 0.0)

    confidence_level = 9.54499736104e-1
    stds_multiplier = NP_SQRT_2 * special.erfinv(confidence_level)
    assert np.isclose(stds_multiplier, 2.0)

    confidence_level = 9.97300203937e-1
    stds_multiplier = NP_SQRT_2 * special.erfinv(confidence_level)
    assert np.isclose(stds_multiplier, 3.0)


def test_custom_bootstrap_efficacy():
    df: pd.DataFrame = _generate_distribution_samples(size=1000)
    false_positive_rate: float = 1.0e-2
    confidence_level: np.float64 = np.float64(1.0 - false_positive_rate)
    columns: pd.Index = df.columns
    column: str
    confidence_interval: ConfidenceInterval
    actual_confidence_levels: Dict[str, Union[float, np.float64]] = {}
    for column in columns:
        confidence_interval = NumericMetricRangeMultiBatchParameterBuilder._compute_bootstrap_estimation_and_return_confidence_interval(
            metric_values=df[column],
            confidence_level=confidence_level,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
        )
        actual_confidence_levels[column] = (
            np.sum(
                df[column].between(confidence_interval.low, confidence_interval.high)
            )
            / df.shape[0]
        )
        # Actual false-positives must be within 1% of desired (configured) false_positive_rate parameter value.
        assert (
            9.9e-1 * confidence_level
            <= actual_confidence_levels[column]
            <= 1.01 * confidence_level
        )


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
