from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import numpy
from great_expectations.core.domain import Domain  # noqa: TCH001
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
        "all_values",
        "active_batch_value",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        unexpected_count_parameter_builder_name: str,
        total_count_parameter_builder_name: str,
        aggregation_method: Optional[str] = None,
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
            aggregation_method: directive for aggregating unexpected count fractions of domain over observed Batch samples.
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
        self._aggregation_method = aggregation_method

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
    def aggregation_method(self) -> Optional[str]:
        return self._aggregation_method

    @property
    def false_positive_rate(self) -> Union[str, float]:
        return self._false_positive_rate

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        return self._quantile_statistic_interpolation_method

    @property
    def round_decimals(self) -> Optional[Union[str, int]]:
        return self._round_decimals

    def _build_parameters(  # PLR0915
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
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_unexpected_count_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        unexpected_count_values: MetricValues = parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]
        unexpected_count_fraction_values: np.ndarray = unexpected_count_values / (
            total_count_values + NP_EPSILON
        )

        # Obtain aggregation_method from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        aggregation_method: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.aggregation_method,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        if (
            aggregation_method
            not in UnexpectedCountStatisticsMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "aggregation_method" can only be empty or one of \
{UnexpectedCountStatisticsMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS} ("{aggregation_method}" was detected).
"""
            )

        result: Union[np.ndarray, np.float64, Dict[str, np.float64]]

        if aggregation_method == "all_values":
            result = unexpected_count_fraction_values
        elif aggregation_method == "active_batch_value":
            print(
                f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-COMPUTED-UNEXPECTED_COUNT_FRACTION_VALUES_FOR_ACTIVE_BATCH_VALUE_{aggregation_method}:\n{unexpected_count_fraction_values} ; TYPE: {str(type(unexpected_count_fraction_values))} ; DOMAIN: {domain}"
            )
            active_batch_value: np.float64 = unexpected_count_fraction_values[-1]
            print(
                f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-COMPUTED-UNEXPECTED_COUNT_FRACTION_ACTIVE_BATCH_VALUE_{aggregation_method}:\n{active_batch_value} ; TYPE: {str(type(active_batch_value))} ; DOMAIN: {domain}"
            )
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Thu>Here is where we use Tal's flowchart to compute the value of "mostly" (I put a placeholder)</Thu>
            mostly: np.float64 = np.float64(6.5e-1)
            # TODO: <Thu></Thu>
            # TODO: <Alex>ALEX</Alex>
            result = {"active_batch_value": active_batch_value, "mostly": mostly}
        else:
            result = np.float64(0.0)  # This statement cannot be reached.

        details: dict = parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]
        details["aggregation_method"] = aggregation_method
        print(
            f"\n[ALEX_TEST] [UnexpectedCountStatisticsMultiBatchParameterBuilder._build_parameters()] PARAMETER_BUILDER-{self.name}-RETURNING-UNEXPECTED_COUNT_{aggregation_method}:\n{result} ; TYPE: {str(type(result))} ; DOMAIN: {domain}"
        )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: result,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
