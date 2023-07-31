from __future__ import annotations

import itertools
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

import numpy as np

from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.attributed_resolved_metrics import (
    AttributedResolvedMetrics,
)
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    datetime_semantic_domain_type,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import is_ndarray_datetime_dtype

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
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

    exclude_field_names: ClassVar[
        Set[str]
    ] = MetricMultiBatchParameterBuilder.exclude_field_names | {
        "metric_name",
        "single_batch_mode",
        "enforce_numeric_metric",
        "replace_nan_with_zero",
        "reduce_scalar_metric",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
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
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            metric_name="column.distinct_values",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

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
        # Build the list of unique values for each Batch object.
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            runtime_configuration=runtime_configuration,
        )

        # Retrieve and replace list of unique values for each Batch with set of unique values for all batches in domain.
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.raw_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        metric_values: MetricValues = AttributedResolvedMetrics.get_conditioned_metric_values_from_attributed_metric_values(
            attributed_metric_values=parameter_node[
                FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
            ]
        )
        details: dict = parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]

        unique_values: set = _get_unique_values_from_nested_collection_of_sets(
            collection=metric_values  # type: ignore[arg-type] # could be None
        )

        unique_values_as_array: np.ndarray = np.asarray(unique_values)
        if unique_values_as_array.ndim == 0:
            unique_values_as_array = np.asarray([unique_values])

        details["parse_strings_as_datetimes"] = datetime_semantic_domain_type(
            domain=domain
        ) or is_ndarray_datetime_dtype(
            data=unique_values_as_array,
            parse_strings_as_datetimes=False,
            fuzzy=False,
        )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: unique_values,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )


V = TypeVar("V")


def _get_unique_values_from_nested_collection_of_sets(
    collection: Collection[Collection[Set[V]]],
) -> Set[V]:
    """Get unique values from a collection of sets e.g. a list of sets.

    Args:
        collection: Collection of Sets containing collections of values.
            can be nested Collections.

    Returns:
        Single flattened set containing unique values.
    """

    flattened: Union[List[Set[V]], Set[V]] = list(
        itertools.chain.from_iterable(collection)
    )
    element: V
    if all(isinstance(element, set) for element in flattened):
        flattened = set().union(*flattened)

    """
    In multi-batch data analysis, values can be empty and missing, resulting in "None" added to set.  However, due to
    reliance on "np.ndarray", "None" gets converted to "numpy.Inf", whereas "numpy.Inf == numpy.Inf" returns False,
    resulting in numerous "None" elements in final set.  For this reason, all "None" elements must be filtered out.
    """
    unique_values: Set[V] = set(
        sorted(  # type: ignore[type-var,arg-type] # lambda destroys type info?
            filter(
                lambda element: not (
                    (element is None)
                    or (isinstance(element, float) and np.isnan(element))
                ),
                set(flattened),
            )
        )
    )

    return unique_values
