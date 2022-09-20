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
    convert_metric_values_to_float_dtype_best_effort,
)
from great_expectations.rule_based_profiler.metric_computation_result import MetricValue
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.util import convert_ndarray_float_to_datetime_tuple

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
        ndarray_is_datetime_type: bool
        metric_values_converted: np.ndarray
        (
            ndarray_is_datetime_type,
            metric_values_converted,
        ) = convert_metric_values_to_float_dtype_best_effort(
            metric_values=metric_values
        )

        min_value: MetricValue = np.amin(a=metric_values_converted)
        max_value: MetricValue = np.amax(a=metric_values_converted)

        if ndarray_is_datetime_type:
            min_value, max_value = convert_ndarray_float_to_datetime_tuple(
                data=[min_value, max_value]
            )

        return build_numeric_range_estimation_result(
            metric_values=metric_values,
            min_value=min_value,
            max_value=max_value,
        )
