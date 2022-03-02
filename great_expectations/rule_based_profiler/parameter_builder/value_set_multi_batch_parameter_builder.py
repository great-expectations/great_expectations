import itertools
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.types.parameter_container import (
    ParameterNode,
)
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
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

    Notes:
        1. The computation of the unique values across batches is done within
           this ParameterBuilder so please be aware that testing large columns with
           high cardinality could require a large amount of memory.
        2. This ParameterBuilder filters null values out from the unique value_set.
    """

    def __init__(
        self,
        name: str,
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
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
        """
        super().__init__(
            name=name,
            metric_name="column.distinct_values",
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
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
    ) -> Tuple[Any, dict]:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional
        details.

        return: Tuple containing computed_parameter_value and parameter_computation_details metadata.
        """
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
        parameter_value_node: ParameterNode = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=self.fully_qualified_parameter_name,
                expected_return_type=None,
                parameters={
                    domain.id: parameter_container,
                },
            )
        )

        fully_qualified_parameter_name_details: str = (
            f"{self.fully_qualified_parameter_name}.details"
        )
        parameter_value_node_details: ParameterNode = (
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_parameter_name_details,
                expected_return_type=None,
                parameters={
                    domain.id: parameter_container,
                },
            )
        )

        unique_parameter_values: Set[
            Any
        ] = _get_unique_values_from_nested_collection_of_sets(
            parameter_value_node.value
        )

        return (
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

    """
    In multi-batch data analysis, values can be empty and missin, resulting in "None" added to set.  However, due to
    reliance on "np.ndarray", "None" gets converted to "numpy.Inf", whereas "numpy.Inf == numpy.Inf" returns False,
    resulting in numerous "None" elements in final set.  For this reason, all "None" elements must be filtered out.
    """
    unique_values: Set[Any] = set(
        filter(
            lambda element: element is not None,
            set().union(*flattened),
        )
    )

    return unique_values
