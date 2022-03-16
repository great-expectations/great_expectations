import itertools
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    AttributedResolvedMetrics,
    MetricMultiBatchParameterBuilder,
    MetricValues,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
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

    Notes:
        1. The computation of the unique values across batches is done within
           this ParameterBuilder so please be aware that testing large columns with
           high cardinality could require a large amount of memory.
        2. This ParameterBuilder filters null values out from the unique value_set.
    """

    exclude_field_names: Set[
        str
    ] = MetricMultiBatchParameterBuilder.exclude_field_names | {
        "metric_name",
        "enforce_numeric_metric",
        "replace_nan_with_zero",
        "reduce_scalar_metric",
    }

    def __init__(
        self,
        name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        json_serialize: Union[str, bool] = True,
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
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            data_context: DataContext
        """
        super().__init__(
            name=name,
            metric_name="column.distinct_values",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            batch_list=batch_list,
            batch_request=batch_request,
            json_serialize=json_serialize,
            data_context=data_context,
        )

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
        # Build the list of unique values for each Batch object.
        super().build_parameters(
            parameter_container=parameter_container,
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
        )

        # Retrieve and replace list of unique values for each Batch with set of unique values for all batches in domain.
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        if not (
            isinstance(parameter_node.value, list) and len(parameter_node.value) == 1
        ):
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Result of metric computations for {self.__class__.__name__} must be a list with exactly 1 element of type "AttributedResolvedMetrics" ({parameter_node.value} found).'
            )

        attributed_resolved_metrics: AttributedResolvedMetrics = parameter_node.value[0]
        metric_values: MetricValues = attributed_resolved_metrics.metric_values

        return (
            _get_unique_values_from_nested_collection_of_sets(collection=metric_values),
            parameter_node.details,
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
