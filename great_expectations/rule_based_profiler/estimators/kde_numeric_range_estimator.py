import logging
from typing import Callable, Dict, Optional, Union

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimator import (
    NumericRangeEstimator,
)
from great_expectations.rule_based_profiler.helpers.util import (
    compute_kde_quantiles_point_estimate,
    get_false_positive_rate_from_rule_state,
    get_parameter_value_and_validate_return_type,
    get_quantile_statistic_interpolation_method_from_rule_state,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import is_ndarray_datetime_dtype

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_KDE_NUM_RESAMPLES = 9999
DEFAULT_KDE_QUANTILE_STATISTIC_INTERPOLATION_METHOD = "nearest"
DEFAULT_KDE_BW_METHOD: Union[str, float, Callable] = "scott"


class KdeNumericRangeEstimator(NumericRangeEstimator):
    """
    Implements the "kde" (kernel density estimation) estimation of parameter values from data.

    (Please refer to "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.gaussian_kde.html" for details.)
    """

    def __init__(
        self,
        configuration: Optional[Attributes] = None,
    ) -> None:
        super().__init__(
            name="kde",
            configuration=configuration,
        )

    def _get_numeric_range_estimate(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimationResult:
        if is_ndarray_datetime_dtype(
            data=metric_values,
            parse_strings_as_datetimes=True,
            fuzzy=False,
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f'Estimator "{self.__class__.__name__}" does not support DateTime/TimeStamp data types.'
            )

        false_positive_rate: np.float64 = get_false_positive_rate_from_rule_state(
            false_positive_rate=self.configuration.false_positive_rate,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # Obtain n_resamples override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        n_resamples: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.configuration.n_resamples,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if n_resamples is None:
            n_resamples = DEFAULT_KDE_NUM_RESAMPLES

        # Obtain random_seed override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        random_seed: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.configuration.random_seed,
            expected_return_type=None,
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
                DEFAULT_KDE_QUANTILE_STATISTIC_INTERPOLATION_METHOD
            )

        # Obtain bw_method override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        bw_method: Optional[
            Union[str, float, Callable]
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.configuration.bw_method,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        # If "bw_method" (bandwidth method) is omitted (default), then "scott" is used.
        if bw_method is None:
            bw_method = DEFAULT_KDE_BW_METHOD

        return compute_kde_quantiles_point_estimate(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            n_resamples=n_resamples,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
            bw_method=bw_method,
            random_seed=random_seed,
        )
