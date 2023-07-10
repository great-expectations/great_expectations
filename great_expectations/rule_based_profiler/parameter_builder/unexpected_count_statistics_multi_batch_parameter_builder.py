from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np
import scipy

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import numpy
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    get_false_positive_rate_from_rule_state,
    get_parameter_value_and_validate_return_type,
    get_quantile_statistic_interpolation_method_from_rule_state,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    RAW_PARAMETER_KEY,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


# TODO: <Alex>ALEX</Alex>
def _array_sigmoid(
    x: np.float64, a: np.ndarray, scale: np.float64 = np.float64(2.0e1)  # noqa B008
) -> np.ndarray:
    """
    Using vectorized logistic function (with large scale factor) as approximation to Heaviside step function.  Advantage
    of this approximation, compared to x < a vector, containing boolean-valued elements, is that it is differentiable,
    and differentiable objective functions behave better than non-differentiable functions in optimization problems.
    """
    return 1.0 / (1.0 + np.exp((-scale) * (a - x))) - 5.0e-1


# TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX</Alex>
def _cost_function(x: np.float64, a: np.ndarray) -> np.float64:
    """
    Mean (per-Batch) Hamming distance -- loss only when expectation validation fails; no change otherwise.
    Expectation validation fails when candidate unexpected_count_fraction x is less than observed array element value.
    Once optimal unexpected_count_fraction is computed, mostly becomes its complement (1.0 - unexpected_count_fraction).
    """

    # TODO: <Alex>ALEX</Alex>
    # return np.mean(x < a)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    def _penalty_function(x: np.float64, a: np.ndarray) -> np.ndarray:
        return _array_sigmoid(x=x, a=a, scale=np.float64(2.0e1))

    return np.mean(_penalty_function(x=x, a=a))
    # TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# def _compute_min_unexpected_count_fraction(a: np.ndarray):
#     # Sort the array in ascending order
#     sorted_a: np.ndarray = sorted(a)
#
#     # Set the search range
#     low: np.float64 = sorted_a[0] - 1  # Initialize the lower bound to a value lower than the smallest element in a
#     high: np.float64 = sorted_a[-1]  # Initialize the upper bound to the largest element in a
#
#     max_f: np.float64 = np.float64(0.0)
#     min_x: np.float64 = low
#
#     mid: np.float64
#     current_f: np.float64
#     while low <= high:
#         mid = np.float64((low + high) / 2.0)  # Calculate the middle value
#
#         current_f = _cost_function(x=mid, a=sorted_a)
#
#         if current_f > max_f:
#             max_f = current_f
#             min_x = mid
#
#         if current_f < 0.5:
#             low = np.float64(mid + 1.0)  # Update the lower bound if the current _cost_function(mid) is below 0.5
#         else:
#             high = np.float64(mid - 1.0)  # Update the upper bound if the current _cost_function(mid) is equal to or higher than 0.5
#
#     return min_x  # Return the minimum x that minimizes _cost_function(x)
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>

# def compute_min_x(a: np.ndarray) -> np.float64:
#     # Sort the array in ascending order
#     sorted_a: np.ndarray = np.sort(a, axis=None)
#
#     # Set the search range
#     low: np.float64 = sorted_a[0]  # Initialize the lower bound to the smallest element in a
#     high: np.float64 = sorted_a[-1] + 1.0  # Initialize the upper bound to a value higher than the largest element in a
#
#     mid: np.float64
#     while low < high:
#         mid = np.float64((low + high) / 2.0)  # Calculate the middle value
#
#         if f(mid, sorted_a) < 0.5:
#             low = np.float64(mid + 1.0)  # Update the lower bound if the current f(mid) is below 0.5
#         else:
#             high = mid  # Update the upper bound if the current f(mid) is equal to or higher than 0.5
#
#     return np.float64(high - 1.0)  # Return the minimum x that maximizes f(x)
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
# def _compute_min_unexpected_count_fraction(a: np.ndarray) -> np.float64:
#     """
#     Apply binary search algorithm to compute minimum x that minimizes _cost_function() of variable x given array a
#     """
#     # Sort the array in ascending order
#     sorted_a: np.ndarray = np.sort(a, axis=None)
#
#     # Set the search range
#     low: np.float64 = sorted_a[0]  # Initialize lower bound to smallest element in array a
#     high: np.float64 = sorted_a[-1] + 1.0  # Initialize upper bound to value higher than largest element in array a
#
#     mid: np.float64
#     while low < high:
#         mid = np.float64((low + high) / 2.0)  # Calculate middle value in array a
#
#         if _cost_function(x=mid, a=sorted_a) < 5.0e-1:  # 0.0 <= _cost_function() <= 1.0
#             low = np.float64(mid + 1.0)  # Update lower bound if current _cost_function() at x=mid is less than 0.5
#         else:
#             high = np.float64(mid - 1.0)  # Update upper bound if _cost_function() at x=mid is less than or equal to 0.5
#
#     # TODO: <Alex>ALEX</Alex>
#     return np.float64(high - 1.0)  # Return minimum x that minimizes _cost_function() of variable x given array a
#     # TODO: <Alex>ALEX</Alex>
#     # TODO: <Alex>ALEX</Alex>
#     # return low
#     # TODO: <Alex>ALEX</Alex>
#


# TODO: <Alex>ALEX</Alex>
def _compute_min_unexpected_count_fraction(
    a: np.ndarray, false_positive_rate: np.float64
) -> np.float64:
    """
    Use constrained optimization algorithm to compute minimum value of x ("unexpected_count_fraction") under constraint
    that _cost_function() of variable x given array a must be less than or equal to "false_positive_rate" constant.
    """

    # Define objective function to be minimized (minimum "unexpected_count_fraction" is desired to maximize "mostly").
    def _objective_function(x: np.floati64) -> np.float64:
        return x

    # Sort array in ascending order
    sorted_a: np.ndarray = np.sort(a, axis=None)

    # Define constraint function reflecting penalty incurred by lowering "unexpected_count_fraction" (raising "mostly").
    def _constraint_function(x: np.float64) -> np.float64:
        return np.float64(_cost_function(x=x, a=sorted_a) - false_positive_rate)

    # Perform optimization.
    # TODO: <Alex>ALEX</Alex>
    # result = scipy.optimize.minimize(_objective_function, x0=sorted_a[-1], bounds=[(np.min(sorted_a)-1.0, np.max(sorted_a))], constraints={'type': 'ineq', 'fun': _constraint_function})
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    result: scipy.optimize.OptimizeResult = scipy.optimize.minimize(
        _objective_function,
        x0=sorted_a[-1],
        bounds=[(np.float64(0.0), np.float64(1.0))],
        constraints={"type": "ineq", "fun": _constraint_function},
    )
    # TODO: <Alex>ALEX</Alex>

    # Return optimized variable (minimum "unexpected_count_fraction").
    return result.x


# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
# def _compute_min_unexpected_count_fraction(a: np.ndarray) -> np.float64:
#     sorted_a: np.ndarray = np.sort(a, axis=None)  # Sort array a in ascending order.
#
#     min_unexpected_count_fraction: np.float64  = np.float64(sorted_a[0] - 1.0)
#     max_gain: np.float64  = np.float64(0.0)
#
#     idx: int
#     element: np.float64
#     current_gain: np.float64
#     for idx, element in enumerate(sorted_a):
#         current_gain = _cost_function(x=element, a=sorted_a[:idx+1])
#
#         if current_gain > max_gain:
#             max_gain = current_gain
#             min_unexpected_count_fraction = element
#
#     return min_unexpected_count_fraction
# TODO: <Alex>ALEX</Alex>


class UnexpectedCountStatisticsMultiBatchParameterBuilder(ParameterBuilder):
    """
    Compute specified aggregate of unexpected count fraction (e.g., of a map metric) across every Batch of data given.
    """

    RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS: set = {
        "unexpected_count_fraction_values",
        "single_batch",
        "auto",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        unexpected_count_parameter_builder_name: str,
        total_count_parameter_builder_name: str,
        mode: str,
        false_positive_rate: Optional[Union[str, float]] = None,
        quantile_statistic_interpolation_method: str = None,
        round_decimals: Optional[Union[str, int]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            unexpected_count_parameter_builder_name: name of parameter that computes unexpected_count (of domain values in Batch).
            total_count_parameter_builder_name: name of parameter that computes total_count (of rows in Batch).
            mode: directive for aggregating/summarizing unexpected count fractions of domain over observed Batch samples.
            false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
                encountering unexpected values as judged by the upper quantile of the observed unexpected fraction.
            quantile_statistic_interpolation_method: Supplies value of (interpolation) "method" to "np.quantile()" statistic.
            round_decimals: user-configured non-negative integer indicating the number of decimals of the
                rounding precision of the computed quantile value prior to packaging it on output.  If omitted, then no
                rounding is performed.
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._total_count_parameter_builder_name = total_count_parameter_builder_name
        self._unexpected_count_parameter_builder_name = (
            unexpected_count_parameter_builder_name
        )
        self._mode = mode

        if false_positive_rate is None:
            false_positive_rate = 2.0e-2

        self._false_positive_rate = false_positive_rate

        if quantile_statistic_interpolation_method is None:
            quantile_statistic_interpolation_method = "auto"

        self._quantile_statistic_interpolation_method = (
            quantile_statistic_interpolation_method
        )

        self._round_decimals = round_decimals

    @property
    def unexpected_count_parameter_builder_name(self) -> str:
        return self._unexpected_count_parameter_builder_name

    @property
    def total_count_parameter_builder_name(self) -> str:
        return self._total_count_parameter_builder_name

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def false_positive_rate(self) -> Union[str, float]:
        return self._false_positive_rate

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        return self._quantile_statistic_interpolation_method

    @property
    def round_decimals(self) -> Optional[Union[str, int]]:
        return self._round_decimals

    def _build_parameters(  # noqa  PLR0915, PLR0915
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        # Obtain unexpected_count_parameter_builder_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        unexpected_count_parameter_builder_name: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.unexpected_count_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        fully_qualified_unexpected_count_parameter_builder_name: str = (
            f"{RAW_PARAMETER_KEY}{unexpected_count_parameter_builder_name}"
        )
        # Obtain unexpected_count from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        unexpected_count_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_unexpected_count_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        unexpected_count_values: MetricValues = unexpected_count_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]
        print(
            f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; UNEXPECTED_COUNT_VALUES:\n{unexpected_count_values} ; TYPE: {str(type(unexpected_count_values))} ; DOMAIN: {domain}"
        )

        # Obtain total_count_parameter_builder_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        total_count_parameter_builder_name: str = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.total_count_parameter_builder_name,
                expected_return_type=str,
                variables=variables,
                parameters=parameters,
            )
        )

        fully_qualified_total_count_parameter_builder_name: str = (
            f"{RAW_PARAMETER_KEY}{total_count_parameter_builder_name}"
        )
        # Obtain total_count from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        total_count_parameter_node: ParameterNode = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_total_count_parameter_builder_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
        )
        total_count_values: MetricValues = total_count_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]
        print(
            f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; TOTAL_COUNTS:\n{total_count_values} ; TYPE: {str(type(total_count_values))} ; DOMAIN: {domain}"
        )

        unexpected_count_fraction_values: np.ndarray = unexpected_count_values / (
            total_count_values + NP_EPSILON
        )
        print(
            f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; UNEXPECTED_COUNT_FRACTION_VALUES:\n{unexpected_count_fraction_values} ; TYPE: {str(type(unexpected_count_fraction_values))} ; DOMAIN: {domain}"
        )

        # Obtain mode from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        mode: str = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.mode,
            expected_return_type=str,
            variables=variables,
            parameters=parameters,
        )
        if mode and (
            mode
            not in UnexpectedCountStatisticsMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "mode" can only be one of \
{UnexpectedCountStatisticsMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS}, or must be omitted (or set to None); however, "{mode}" was detected.
"""
            )

        result: Union[np.float64, Dict[str, Union[np.float64, np.ndarray]]]

        if mode == "unexpected_count_fraction_values":
            result = unexpected_count_fraction_values
        else:
            num_batches: int = len(total_count_values)
            single_batch_mode: bool = num_batches == 1 or mode == "single_batch"
            print(
                f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-MODE/SINGLE_BATCH_MODE-{mode}/{single_batch_mode} ; SINGLE_BATCH_MODE_TYPE: {str(type(single_batch_mode))} ; DOMAIN: {domain}"
            )

            # TODO: <Alex>ALEX</Alex>
            result = {
                "single_batch_mode": single_batch_mode,
                "unexpected_count_fraction_active_batch_value": unexpected_count_fraction_values[
                    -1
                ],
            }

            # print(
            #     f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-COMPUTED-UNEXPECTED_COUNT_FRACTION_VALUES_FOR_ACTIVE_BATCH_VALUE_{mode}:\n{unexpected_count_fraction_values} ; TYPE: {str(type(unexpected_count_fraction_values))} ; DOMAIN: {domain}"
            # )
            mostly: np.float64
            # TODO: <Alex>ALEX</Alex>

            if single_batch_mode:
                # TODO: <Alex>ALEX</Alex>
                # result["unexpected_count_fraction_active_batch_value"] = unexpected_count_fraction_values[-1]
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Thu>Here is where we use Tal's flowchart to compute the value of "mostly" (I put a placeholder)</Thu>
                mostly: np.float64 = np.float64(6.5e-1)
                # TODO: <Thu></Thu>
                # TODO: <Alex>ALEX</Alex>
                result["mostly"] = mostly
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-MODE/SINGLE_BATCH_MODE-{mode}/{single_batch_mode} ; RESULT: {str(type(result))} ; DOMAIN: {domain}"
                )
                result["mean_error"] = np.float64(0.0)
            else:
                false_positive_rate: np.float64 = get_false_positive_rate_from_rule_state(  # type: ignore[assignment] # could be float
                    false_positive_rate=self.false_positive_rate,  # type: ignore[union-attr] # configuration could be None
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                )
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; FALSE_POSITIVE_RATE:\n{false_positive_rate} ; TYPE: {str(type(false_positive_rate))} ; DOMAIN: {domain}"
                )

                # Obtain round_decimals directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
                round_decimals: Optional[
                    int
                ] = get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=self.round_decimals,
                    expected_return_type=None,
                    variables=variables,
                    parameters=parameters,
                )
                if not (
                    round_decimals is None
                    or (isinstance(round_decimals, int) and (round_decimals >= 0))
                ):
                    raise gx_exceptions.ProfilerExecutionError(
                        message=f"""The directive "round_decimals" for {self.__class__.__name__} can be 0 or a positive \
integer, or must be omitted (or set to None).
"""
                    )

                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; ROUND_DECIMALS:\n{round_decimals} ; TYPE: {str(type(round_decimals))} ; DOMAIN: {domain}"
                )
                quantile_statistic_interpolation_method: str = get_quantile_statistic_interpolation_method_from_rule_state(
                    quantile_statistic_interpolation_method=self.quantile_statistic_interpolation_method,  # type: ignore[union-attr] # configuration could be None
                    # TODO: <Alex>ALEX</Alex>
                    # round_decimals=round_decimals or 3,
                    # TODO: <Alex>ALEX</Alex>
                    # TODO: <Alex>ALEX</Alex>
                    round_decimals=round_decimals,
                    # TODO: <Alex>ALEX</Alex>
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                )
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; QUANTILE_STATISTIC_INTERPOLATION_METHOD:\n{quantile_statistic_interpolation_method} ; TYPE: {str(type(quantile_statistic_interpolation_method))} ; DOMAIN: {domain}"
                )
                quantile: np.float64 = numpy.numpy_quantile(
                    a=unexpected_count_fraction_values,
                    # TODO: <Alex>ALEX</Alex>
                    # q=1.0 - false_positive_rate,
                    # TODO: <Alex>ALEX</Alex>
                    # TODO: <Alex>ALEX</Alex>
                    q=false_positive_rate,
                    # TODO: <Alex>ALEX</Alex>
                    axis=0,
                    method=quantile_statistic_interpolation_method,
                )
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; UNEXPECTED_COUNT_OUTPUT-QUANTILE:\n{quantile} ; TYPE: {str(type(quantile))} ; DOMAIN: {domain}"
                )

                if round_decimals is None:
                    # TODO: <Alex>ALEX</Alex>
                    mostly = np.float64(1.0 - quantile)
                    # TODO: <Alex>ALEX</Alex>
                    # TODO: <Alex>ALEX</Alex>
                    # mostly = np.float64(quantile)
                    # TODO: <Alex>ALEX</Alex>
                    print(
                        f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; UNEXPECTED_COUNT_OUTPUT-MOSTLY-NO_ROUNDING:\n{result} ; TYPE: {str(type(result))} ; DOMAIN: {domain}"
                    )
                else:
                    # TODO: <Alex>ALEX</Alex>
                    mostly = round(np.float64(1.0 - quantile), round_decimals)
                    # TODO: <Alex>ALEX</Alex>
                    # TODO: <Alex>ALEX</Alex>
                    # mostly = round(np.float64(quantile), round_decimals)
                    # TODO: <Alex>ALEX</Alex>

                # TODO: <Alex>ALEX</Alex>
                # mean_error: np.float64 = np.mean(unexpected_count_fraction_values > quantile)  # per-Batch Hamming expectation validation success/failure distance
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                # mostly = compute_max_x(a=1.0 - unexpected_count_fraction_values)
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                min_unexpected_count_fraction: np.float64 = (
                    _compute_min_unexpected_count_fraction(
                        a=unexpected_count_fraction_values,
                        false_positive_rate=false_positive_rate,
                    )
                )
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                # min_unexpected_count_fraction: np.float64 = _compute_min_unexpected_count_fraction(a=unexpected_count_fraction_values)
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                mostly = np.float64(1.0 - min_unexpected_count_fraction)
                # TODO: <Alex>ALEX</Alex>
                result["mostly"] = mostly
                # TODO: <Alex>ALEX</Alex>
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; UNEXPECTED_COUNT_OUTPUT_WITH-MOSTLY-OPTIMIZED:\n{result} ; TYPE: {str(type(result))} ; DOMAIN: {domain}"
                )
                # TODO: <Alex>ALEX</Alex>
                # mean_error: np.float64 = _cost_function(1.0 - unexpected_count_fraction_values, mostly)  # per-Batch Hamming expectation validation success/failure distance
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                mean_error: np.float64 = _cost_function(
                    x=min_unexpected_count_fraction, a=unexpected_count_fraction_values
                )  # per-Batch Hamming expectation validation success/failure distance
                # TODO: <Alex>ALEX</Alex>
                print(
                    f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER_NAME:\n{self.name} ; MEAN_ERROR:\n{mean_error} ; TYPE: {str(type(mean_error))} ; DOMAIN: {domain}"
                )
                result["mean_error"] = mean_error
                # TODO: <Alex>ALEX</Alex>

        details: dict = unexpected_count_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
        ]
        details["mode"] = mode
        print(
            f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-RETURNING-UNEXPECTED_COUNT_{mode}:\n{result} ; TYPE: {str(type(result))} ; DOMAIN: {domain}"
        )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: result,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
