import itertools
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union

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
    """Build a set of unique values across all specified batches.

    This parameter builder can be used to build a unique value_set for each
    of the domains specified by the DomainBuilder from all of the batches
    specified. This value_set can be used to create Expectations.

    This unique value_set is the unique values from ALL batches accessible
    to the parameter builder. For example, if batch 1 has the unique values
    {1, 4, 8} and batch 2 {2, 8, 10} the unique values returned by this
    parameter builder are the set union, or {1, 2, 4, 8, 10}

    Note: The computation of the unique values across batches is done within
    this ParameterBuilder so please be aware that testing large columns with
    high cardinality could require a large amount of memory.
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
    ) -> Tuple[str, Any, dict]:
        # Build the list of unique values for each batch
        super().build_parameters(
            parameter_container=parameter_container,
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
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

        fully_qualified_parameter_name_details: str = f"$parameter.{self.name}.details"
        parameter_value_node_details: ParameterNode = (
            get_parameter_value_by_fully_qualified_parameter_name(
                fully_qualified_parameter_name=fully_qualified_parameter_name_details,
                domain=domain,
                parameters={domain.id: parameter_container},
            )
        )

        unique_parameter_values: Set[
            Any
        ] = _get_unique_values_from_nested_collection_of_sets(
            parameter_value_node["value"]
        )

        return (
            fully_qualified_parameter_name,
            unique_parameter_values,
            parameter_value_node_details,
        )


def _get_unique_values_from_nested_collection_of_sets(
    collection: Collection[Collection[Set[Any]]],
) -> Set[Any]:
    """Get unique values from a collection of sets e.g. a list of sets.

    Args:
        collection: Collection of Sets containing collections of values.
            can be nested Collections.

    Returns:
        Single flattened set containing unique values.
    """

    flattened: List[Set[Any]] = list(itertools.chain.from_iterable(collection))
    unique_values: Set[Any] = set().union(*flattened)
    return unique_values
