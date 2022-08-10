import logging
from typing import Dict, Optional

import numpy as np

from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimator import (
    NumericRangeEstimator,
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_numeric_range_estimation_result,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExactNumericRangeEstimator(NumericRangeEstimator):
    """
    Implements deterministic, incorporating entire observed value range, computation.
    """

    def __init__(
        self,
    ) -> None:
        super().__init__(
            name="exact",
            configuration=None,
        )

    def _get_numeric_range_estimate(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimationResult:
        return build_numeric_range_estimation_result(
            metric_values=metric_values,
            min_value=np.amin(a=metric_values),
            max_value=np.amax(a=metric_values),
        )
