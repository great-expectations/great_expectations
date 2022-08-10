import logging
from typing import Dict, Optional

import numpy as np

from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.estimators.numeric_range_estimator import (
    NumericRangeEstimator,
)
from great_expectations.rule_based_profiler.helpers.util import (
    compute_quantiles,
    get_false_positive_rate_from_rule_state,
    get_quantile_statistic_interpolation_method_from_rule_state,
)
from great_expectations.rule_based_profiler.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class OneShotNumericRangeEstimator(NumericRangeEstimator):
    """
    Implements "oneshot" (one observation) computation.

    This parameteric estimator assumes a Normal distribution of data, and thus should be used for testing purposes only.
    """

    def __init__(
        self,
        configuration: Optional[Attributes] = None,
    ) -> None:
        super().__init__(
            name="oneshot",
            configuration=configuration,
        )

    def _get_numeric_range_estimate(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimationResult:
        false_positive_rate: np.float64 = get_false_positive_rate_from_rule_state(
            false_positive_rate=self.configuration.false_positive_rate,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        quantile_statistic_interpolation_method: str = get_quantile_statistic_interpolation_method_from_rule_state(
            quantile_statistic_interpolation_method=self.configuration.quantile_statistic_interpolation_method,
            round_decimals=self.configuration.round_decimals,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        return compute_quantiles(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        )
