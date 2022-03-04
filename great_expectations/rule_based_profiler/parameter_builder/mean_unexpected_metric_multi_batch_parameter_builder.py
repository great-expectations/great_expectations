from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    MetricValues,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    ParameterNode,
)


class MeanUnexpectedMapMetricMultiBatchParameterBuilder(
    MetricMultiBatchParameterBuilder
):
    """
    Compute mean unexpected count ratio (as a fraction) of a specified map-style metric across all specified batches.
    """

    def __init__(
        self,
        name: str,
        map_metric_name: str,
        total_count_parameter_builder_name: str,
        null_count_parameter_builder_name: Optional[str] = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            map_metric_name: the name of a map metric (must be a supported and registered map metric); the suffix
            ".unexpected_count" will be appended to "map_metric_name" to be used in MetricConfiguration to get values.
            total_count_parameter_builder_name: name of parameter that computes total_count (of rows in Batch).
            null_count_parameter_builder_name: name of parameter that computes null_count (of domain values in Batch).
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
        """
        super().__init__(
            name=name,
            metric_name=f"{map_metric_name}.unexpected_count",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._map_metric_name = map_metric_name
        self._total_count_parameter_builder_name = total_count_parameter_builder_name
        self._null_count_parameter_builder_name = null_count_parameter_builder_name

    @property
    def map_metric_name(self) -> str:
        return self._map_metric_name

    @property
    def total_count_parameter_builder_name(self) -> str:
        return self._total_count_parameter_builder_name

    @property
    def null_count_parameter_builder_name(self) -> Optional[str]:
        return self._null_count_parameter_builder_name

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Tuple[Any, dict]:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional
        details.

        return: Tuple containing computed_parameter_value and parameter_computation_details metadata.
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
            f"$parameters.{total_count_parameter_builder_name}.value"
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
        total_count_values: MetricValues = total_count_parameter_node.value

        # Obtain null_count_parameter_builder_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        null_count_parameter_builder_name: str = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.null_count_parameter_builder_name,
                expected_return_type=str,
                variables=variables,
                parameters=parameters,
            )
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
                f"$parameters.{null_count_parameter_builder_name}.value"
            )
            # Obtain null_count from "rule state" (i.e., variables and parameters); from instance variable otherwise.
            null_count_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_null_count_parameter_builder_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            null_count_values = null_count_parameter_node.value

        nonnull_count_values: np.ndarray = total_count_values - null_count_values

        # Compute "unexpected_count" corresponding to "map_metric_name" (given as argument to this "ParameterBuilder").
        super().build_parameters(
            parameter_container=parameter_container,
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
        )

        # Retrieve "unexpected_count" corresponding to "map_metric_name" (given as argument to this "ParameterBuilder").
        parameter_value_node: ParameterNode = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
        )
        unexpected_count_values: MetricValues = parameter_value_node.value

        unexpected_count_ratio_values: np.ndarray = (
            unexpected_count_values / nonnull_count_values
        )
        mean_unexpected_count_ratio: np.float64 = np.mean(unexpected_count_ratio_values)

        return (
            mean_unexpected_count_ratio,
            parameter_value_node.details,
        )
