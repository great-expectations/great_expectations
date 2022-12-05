import logging
from typing import Dict, Optional

import numpy as np

from great_expectations.core.domain import Domain
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimator import (
    NumericRangeEstimator,
)
from great_expectations.rule_based_profiler.helpers.util import (
    compute_quantiles,
    datetime_semantic_domain_type,
    get_false_positive_rate_from_rule_state,
    get_quantile_statistic_interpolation_method_from_rule_state,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import convert_ndarray_to_datetime_dtype_best_effort

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_QUANTILES_QUANTILE_STATISTIC_INTERPOLATION_METHOD = "nearest"


class QuantilesNumericRangeEstimator(NumericRangeEstimator):
    """
    Implements "quantiles" computation.

    This nonparameteric estimator calculates quantiles given a MetricValues vector of length N, the q-th quantile of
        the vector is the value q of the way from the minimum to the maximum in a sorted copy of the MetricValues.
    """

    def __init__(
        self,
        configuration: Optional[Attributes] = None,
    ) -> None:
        super().__init__(
            name="quantiles",
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
        if quantile_statistic_interpolation_method is None:
            quantile_statistic_interpolation_method = (
                DEFAULT_QUANTILES_QUANTILE_STATISTIC_INTERPOLATION_METHOD
            )

        datetime_detected: bool = datetime_semantic_domain_type(domain=domain)
        metric_values_converted: np.ndaarray
        (
            _,
            _,
            metric_values_converted,
        ) = convert_ndarray_to_datetime_dtype_best_effort(
            data=metric_values,
            datetime_detected=datetime_detected,
            parse_strings_as_datetimes=True,
            fuzzy=False,
        )
        return compute_quantiles(
            metric_values=metric_values_converted,
            false_positive_rate=false_positive_rate,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
        )
