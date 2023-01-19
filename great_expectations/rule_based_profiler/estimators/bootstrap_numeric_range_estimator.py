import logging
from typing import Dict, Optional

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
    compute_bootstrap_quantiles_point_estimate,
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


DEFAULT_BOOTSTRAP_NUM_RESAMPLES = 9999
DEFAULT_BOOTSTRAP_QUANTILE_STATISTIC_INTERPOLATION_METHOD = "linear"
DEFAULT_BOOTSTRAP_QUANTILE_BIAS_STD_ERROR_RATIO_THRESHOLD = 2.5e-1


class BootstrapNumericRangeEstimator(NumericRangeEstimator):
    """
    Implements the "bootstrapped" estimation of parameter values from data.

    (Please refer to "https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html" for details.)
    """

    def __init__(
        self,
        configuration: Optional[Attributes] = None,
    ) -> None:
        super().__init__(
            name="bootstrap",
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
            n_resamples = DEFAULT_BOOTSTRAP_NUM_RESAMPLES

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
                DEFAULT_BOOTSTRAP_QUANTILE_STATISTIC_INTERPOLATION_METHOD
            )

        # Obtain quantile_bias_correction override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        quantile_bias_correction: Optional[
            bool
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.configuration.quantile_bias_correction,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if quantile_bias_correction is None:
            quantile_bias_correction = False

        # Obtain quantile_bias_std_error_ratio_threshold override from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        quantile_bias_std_error_ratio_threshold: Optional[
            float
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.configuration.quantile_bias_std_error_ratio_threshold,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if quantile_bias_std_error_ratio_threshold is None:
            quantile_bias_std_error_ratio_threshold = (
                DEFAULT_BOOTSTRAP_QUANTILE_BIAS_STD_ERROR_RATIO_THRESHOLD
            )

        return compute_bootstrap_quantiles_point_estimate(
            metric_values=metric_values,
            false_positive_rate=false_positive_rate,
            n_resamples=n_resamples,
            random_seed=random_seed,
            quantile_statistic_interpolation_method=quantile_statistic_interpolation_method,
            quantile_bias_correction=quantile_bias_correction,
            quantile_bias_std_error_ratio_threshold=quantile_bias_std_error_ratio_threshold,
        )
