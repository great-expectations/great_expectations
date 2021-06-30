import numpy as np
from scipy import special

from great_expectations.rule_based_profiler.parameter_builder.numeric_metric_range_multi_batch_parameter_builder import (
    NP_SQRT_2,
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
