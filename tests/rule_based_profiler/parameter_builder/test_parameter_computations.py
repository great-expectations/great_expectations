from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import scipy.stats as stats

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.util import (
    compute_bootstrap_quantiles,
    compute_bootstrap_quantiles_mean,
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


def test_bootstrap_mean_efficacy():
    df: pd.DataFrame = _generate_distribution_samples(size=1000)
    false_positive_rate: np.float64 = np.float64(0.01)
    columns: pd.Index = df.columns
    column: str
    lower_quantile: np.float64
    upper_quantile: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for column in columns:
        (lower_quantile, upper_quantile,) = compute_bootstrap_quantiles_mean(
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
