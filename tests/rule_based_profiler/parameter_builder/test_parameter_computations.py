import numpy as np
from scipy import special

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    NP_SQRT_2,
)


# Please refer to "https://en.wikipedia.org/wiki/Normal_distribution" and references therein for background.
def test_standard_deviation_band_around_mean_rule():
    false_positive_rate: float
    true_negative_rate: float
    stds_multiplier: float

    false_positive_rate = 1.0
    true_negative_rate = 1.0 - false_positive_rate
    stds_multiplier = NP_SQRT_2 * special.erfinv(true_negative_rate)
    assert np.isclose(stds_multiplier, 0.0)

    false_positive_rate = 4.5500263896358e-2
    true_negative_rate = 1.0 - false_positive_rate
    stds_multiplier = NP_SQRT_2 * special.erfinv(true_negative_rate)
    assert np.isclose(stds_multiplier, 2.0)

    false_positive_rate = 2.699796063260e-3
    true_negative_rate = 1.0 - false_positive_rate
    stds_multiplier = NP_SQRT_2 * special.erfinv(true_negative_rate)
    assert np.isclose(stds_multiplier, 3.0)
