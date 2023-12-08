from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np
import scipy

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    get_parameter_value_and_validate_return_type,
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


class UnexpectedCountStatisticsMultiBatchParameterBuilder(ParameterBuilder):
    """
    Compute specified aggregate of unexpected count fraction (e.g., of a map metric) across every Batch of data given.
    """

    RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS: set = {
        "unexpected_count_fraction_values",
        "single_batch",
        "multi_batch",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        unexpected_count_parameter_builder_name: str,
        total_count_parameter_builder_name: str,
        mode: str,
        max_error_rate: Optional[Union[str, float]] = None,
        expectation_type: Optional[str] = None,
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
            max_error_rate: user-configured fraction between 0 and 1 expressing maximum error rate for encountering
            unexpected values as judged by computing predicted validation errors based on observed unexpected fractions.
            expectation_type: name of Expectation (optional, for troubleshooting and/or single_batch mode purposes).
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

        self._expectation_type = expectation_type

        if max_error_rate is None:
            max_error_rate = 2.5e-2

        self._max_error_rate = max_error_rate

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
    def expectation_type(self) -> Optional[str]:
        return self._expectation_type

    @property
    def max_error_rate(self) -> Union[str, float]:
        return self._max_error_rate

    def _build_parameters(  # PLR0915, PLR0915
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

        if (
            domain.domain_type == MetricDomainTypes.COLUMN
            and "." in domain.domain_kwargs["column"]
        ):
            raise gx_exceptions.ProfilerExecutionError(
                "Column names cannot contain '.' when computing parameters for unexpected count statistics."
            )

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

        unexpected_count_fraction_values: np.ndarray = unexpected_count_values / (
            total_count_values + NP_EPSILON
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
            result = {
                "single_batch_mode": mode == "single_batch",
                "unexpected_count_fraction_active_batch_value": unexpected_count_fraction_values[
                    -1
                ],
            }

            mostly: np.float64

            if mode == "single_batch":
                unexpected_fraction: np.float64 = unexpected_count_fraction_values[-1]
                expected_fraction: np.float64 = np.float64(1.0 - unexpected_fraction)
                result["mostly"] = _standardize_mostly_for_single_batch(
                    self._expectation_type, expected_fraction
                )
                result["error_rate"] = np.float64(0.0)
            elif mode == "multi_batch":
                # Obtain max_error_rate directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
                max_error_rate: float = get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=self.max_error_rate,
                    expected_return_type=float,
                    variables=variables,
                    parameters=parameters,
                )

                min_unexpected_count_fraction: np.float64 = (
                    _compute_multi_batch_min_unexpected_count_fraction(
                        a=unexpected_count_fraction_values,
                        max_error_rate=np.float64(max_error_rate),
                    )
                )
                mostly = np.float64(1.0 - min_unexpected_count_fraction)
                result["mostly"] = mostly
                error_rate: np.float64 = _multi_batch_cost_function(
                    x=min_unexpected_count_fraction, a=unexpected_count_fraction_values
                )  # per-Batch Hamming expectation validation success/failure distance
                result["error_rate"] = error_rate

        details: dict = unexpected_count_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
        ]
        details["mode"] = mode

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: result,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )


def _standardize_mostly_for_single_batch(  # noqa: PLR0911
    expectation_type: str, mostly: np.float64
) -> np.float64:
    """
    Applies business logic to standardize "mostly" value for single-Batch case.
    """
    if expectation_type == "expect_column_values_to_be_null":
        if mostly >= 1.0:  # noqa: PLR2004
            return np.float64(1.0)

        if mostly >= 0.99:  # noqa: PLR2004
            return np.float64(0.99)

        if mostly >= 0.975:  # noqa: PLR2004
            return np.float64(0.975)

        return mostly

    if expectation_type == "expect_column_values_to_not_be_null":
        if mostly >= 1.0:  # noqa: PLR2004
            return np.float64(1.0)

        if mostly >= 0.99:  # noqa: PLR2004
            return np.float64(0.99)

        # round down to nearest 0.025
        return np.floor(mostly * 40) / 40


def _multi_batch_cost_function(x: np.float64, a: np.ndarray) -> np.float64:
    """
    Mean (per-Batch) Hamming distance -- loss only when expectation validation fails; no change otherwise.
    Expectation validation fails when candidate unexpected_count_fraction x is less than observed array element value.
    Once optimal unexpected_count_fraction is computed, mostly becomes its complement (1.0 - unexpected_count_fraction).
    """

    return np.mean(x < a)


def _compute_multi_batch_min_unexpected_count_fraction(
    a: np.ndarray, max_error_rate: np.float64
) -> np.float64:
    """
    Use constrained optimization algorithm to compute minimum value of x ("unexpected_count_fraction") under constraint
    that _cost_function() of variable x given array a must be less than or equal to "max_error_rate" constant.
    """

    # Define objective function to be minimized (minimum "unexpected_count_fraction" is desired to maximize "mostly").
    def _objective_function(x: np.float64) -> np.float64:
        return x[0]

    # Sort array in ascending order
    sorted_a: np.ndarray = np.sort(a, axis=None)

    # Define constraint function reflecting penalty incurred by lowering "unexpected_count_fraction" (raising "mostly").
    def _constraint_function(x: np.float64) -> np.float64:
        return np.float64(
            _multi_batch_cost_function(x=x[0], a=sorted_a) - max_error_rate
        )

    # Perform optimization.
    result: scipy.optimize.OptimizeResult = scipy.optimize.minimize(
        _objective_function,
        x0=sorted_a[-1],
        bounds=[(np.float64(0.0), np.float64(1.0))],
        constraints={"type": "ineq", "fun": _constraint_function},
    )

    # Return optimized variable (minimum "unexpected_count_fraction").
    return result.x[0]
