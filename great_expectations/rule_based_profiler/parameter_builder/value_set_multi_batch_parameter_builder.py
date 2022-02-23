from typing import Any, Dict, Iterable, List, Optional, Set, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)
from great_expectations.rule_based_profiler.types.parameter_container import (
    ParameterNode,
)


class ValueSetMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    """Build a set of unique values.

    Compute unique values across a batch or a set of batches.
    """

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
    ):
        """

        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            data_context: DataContext
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """
        super().__init__(
            name=name,
            metric_name="column.distinct_values",
            data_context=data_context,
            batch_list=batch_list,
            batch_request=batch_request,
            reduce_scalar_metric=False,
            enforce_numeric_metric=False,
        )

        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    """
    Full getter/setter accessors for needed properties are for configuring MetricMultiBatchParameterBuilder dynamically.
    """

    @property
    def metric_domain_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[str, dict]]) -> None:
        self._metric_value_kwargs = value

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ParameterContainer:

        # Build the list of unique values for each batch
        super()._build_parameters(
            parameter_container=parameter_container,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # Retrieve and replace the list of unique values for each batch with
        # the set of unique values for all batches in the given domain.
        fully_qualified_parameter_name: str = f"$parameter.{self.name}"
        parameter_value_node: ParameterNode = (
            get_parameter_value_by_fully_qualified_parameter_name(
                fully_qualified_parameter_name=fully_qualified_parameter_name,
                domain=domain,
                parameters={domain.id: parameter_container},
            )
        )

        unique_parameter_values: Set[
            Any
        ] = _get_unique_values_from_iterable_of_iterables(parameter_value_node["value"])

        parameter_value_node.value = unique_parameter_values

        return parameter_container


def _get_unique_values_from_iterable_of_iterables(
    iterable: Iterable[Iterable[Any]],
) -> Set[Any]:
    """Get unique values from an iterable of iterables e.g. a list of sets.

    Args:
        iterable: List, Set containing iterables of values.

    Returns:
        Single flattened set containing unique values.
    """

    return set().union(*iterable)
