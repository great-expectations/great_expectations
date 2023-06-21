from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Set, Union

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import numpy
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
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
    MetricMultiBatchParameterBuilder,
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


class AggregateUnexpectedMapMetricMultiBatchParameterBuilder(
    MetricMultiBatchParameterBuilder
):
    """
    Compute specified aggregate of unexpected count ratio of given map-style metric across every Batch of data given.
    """

    exclude_field_names: ClassVar[
        Set[str]
    ] = MetricMultiBatchParameterBuilder.exclude_field_names | {
        "metric_name",
        "single_batch_mode",
        "enforce_numeric_metric",
        "replace_nan_with_zero",
        "reduce_scalar_metric",
    }

    RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS: set = {
        "mean",
        "std",
        "median",
        "quantile",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        map_metric_name: str,
        total_count_parameter_builder_name: str,
        null_count_parameter_builder_name: Optional[str] = None,
        aggregation_method: Optional[str] = None,
        false_positive_rate: Optional[Union[str, float]] = None,
        quantile_statistic_interpolation_method: str = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
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
            map_metric_name: the name of a map metric (must be a supported and registered map metric); the suffix
            ".unexpected_count" will be appended to "map_metric_name" to be used in MetricConfiguration to get values.
            total_count_parameter_builder_name: name of parameter that computes total_count (of rows in Batch).
            null_count_parameter_builder_name: name of parameter that computes null_count (of domain values in Batch).
            aggregation_method: directive for aggregating unexpected count ratios of domain over observed Batch samples.
            false_positive_rate: user-configured fraction between 0 and 1 expressing desired false positive rate for
                encountering unexpected values as judged by the upper quantile of the observed unexpected ratio.
            quantile_statistic_interpolation_method: Supplies value of (interpolation) "method" to "np.quantile()" statistic.
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            metric_name=f"{map_metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=True,
            replace_nan_with_zero=True,
            reduce_scalar_metric=True,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._map_metric_name = map_metric_name
        self._total_count_parameter_builder_name = total_count_parameter_builder_name
        self._null_count_parameter_builder_name = null_count_parameter_builder_name
        self._aggregation_method = aggregation_method

        if false_positive_rate is None:
            false_positive_rate = 2.0e-2

        self._false_positive_rate = false_positive_rate

        if quantile_statistic_interpolation_method is None:
            quantile_statistic_interpolation_method = "auto"

        self._quantile_statistic_interpolation_method = (
            quantile_statistic_interpolation_method
        )

    @property
    def map_metric_name(self) -> str:
        return self._map_metric_name

    @property
    def total_count_parameter_builder_name(self) -> str:
        return self._total_count_parameter_builder_name

    @property
    def null_count_parameter_builder_name(self) -> Optional[str]:
        return self._null_count_parameter_builder_name

    @property
    def aggregation_method(self) -> Optional[str]:
        return self._aggregation_method

    @property
    def false_positive_rate(self) -> Union[str, float]:
        return self._false_positive_rate

    @property
    def quantile_statistic_interpolation_method(self) -> str:
        return self._quantile_statistic_interpolation_method

    def _build_parameters(
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

        # Obtain null_count_parameter_builder_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        null_count_parameter_builder_name: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.null_count_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        num_batch_ids: int = len(batch_ids)

        null_count_values: MetricValues
        if null_count_parameter_builder_name is None:
            null_count_values = np.zeros(shape=(num_batch_ids,))
        else:
            fully_qualified_null_count_parameter_builder_name: str = (
                f"{RAW_PARAMETER_KEY}{null_count_parameter_builder_name}"
            )
            # Obtain null_count from "rule state" (i.e., variables and parameters); from instance variable otherwise.
            null_count_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_null_count_parameter_builder_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            null_count_values = null_count_parameter_node[
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
            ]

        nonnull_count_values: np.ndarray = total_count_values - null_count_values

        # Compute "unexpected_count" corresponding to "map_metric_name" (given as argument to this "ParameterBuilder").
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            runtime_configuration=runtime_configuration,
        )

        # Retrieve "unexpected_count" corresponding to "map_metric_name" (given as argument to this "ParameterBuilder").
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.raw_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        unexpected_count_values: MetricValues = parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]

        unexpected_count_ratio_values: np.ndarray = unexpected_count_values / (
            nonnull_count_values + NP_EPSILON
        )

        # Obtain aggregation_method from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        aggregation_method: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.aggregation_method,
            expected_return_type=str,
            variables=variables,
            parameters=parameters,
        )
        if (
            aggregation_method
            not in AggregateUnexpectedMapMetricMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""The directive "aggregation_method" can be only one of \
{AggregateUnexpectedMapMetricMultiBatchParameterBuilder.RECOGNIZED_UNEXPECTED_RATIO_AGGREGATION_METHODS} ("{aggregation_method}" was detected).
"""
            )

        aggregated_unexpected_count_ratio: np.float64

        if aggregation_method == "mean":
            aggregated_unexpected_count_ratio = np.mean(unexpected_count_ratio_values)
        elif aggregation_method == "std":
            aggregated_unexpected_count_ratio = np.std(unexpected_count_ratio_values)
        elif aggregation_method == "median":
            aggregated_unexpected_count_ratio = np.median(unexpected_count_ratio_values)
        elif aggregation_method == "quantile":
            false_positive_rate: np.float64 = get_false_positive_rate_from_rule_state(  # type: ignore[assignment] # could be float
                false_positive_rate=self.false_positive_rate,  # type: ignore[union-attr] # configuration could be None
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            quantile_statistic_interpolation_method: str = get_quantile_statistic_interpolation_method_from_rule_state(
                quantile_statistic_interpolation_method=self.quantile_statistic_interpolation_method,  # type: ignore[union-attr] # configuration could be None
                round_decimals=2,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
            aggregated_unexpected_count_ratio = numpy.numpy_quantile(
                a=unexpected_count_ratio_values,
                q=1.0 - false_positive_rate,
                axis=0,
                method=quantile_statistic_interpolation_method,
            )
        else:
            aggregated_unexpected_count_ratio = np.float64(
                0.0
            )  # This location cannot be reached.

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: aggregated_unexpected_count_ratio,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ],
            }
        )
