import itertools
from typing import Any, Collection, Dict, List, Optional, Set, Union

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    AttributedResolvedMetrics,
    MetricMultiBatchParameterBuilder,
    MetricValues,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    Domain,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes


class ValueSetMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    "Build a set of unique values across all specified batches.\n\n    This parameter builder can be used to build a unique value_set for each\n    of the domains specified by the DomainBuilder from all of the batches\n    specified. This value_set can be used to create Expectations.\n\n    This unique value_set is the unique values from ALL batches accessible\n    to the parameter builder. For example, if batch 1 has the unique values\n    {1, 4, 8} and batch 2 {2, 8, 10} the unique values returned by this\n    parameter builder are the set union, or {1, 2, 4, 8, 10}\n\n    Notes:\n        1. The computation of the unique values across batches is done within\n           this ParameterBuilder so please be aware that testing large columns with\n           high cardinality could require a large amount of memory.\n        2. This ParameterBuilder filters null values out from the unique value_set.\n"
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
        metric_domain_kwargs: Optional[Union[(str, dict)]] = None,
        metric_value_kwargs: Optional[Union[(str, dict)]] = None,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        json_serialize: Union[(str, bool)] = True,
        data_context: Optional["BaseDataContext"] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Args:\n            name: the name of this parameter -- this is user-specified parameter name (from configuration);\n            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."\n            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").\n            metric_domain_kwargs: used in MetricConfiguration\n            metric_value_kwargs: used in MetricConfiguration\n            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective\n            ParameterBuilder objects\' outputs available (as fully-qualified parameter names) is pre-requisite.\n            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".\n            json_serialize: If True (default), convert computed value to JSON prior to saving results.\n            data_context: BaseDataContext associated with this ParameterBuilder\n        '
        super().__init__(
            name=name,
            metric_name="column.distinct_values",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            data_context=data_context,
        )

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[(str, ParameterContainer)]] = None,
        recompute_existing_parameter_values: bool = False,
    ) -> Attributes:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.\n\n        Returns:\n            Attributes object, containing computed parameter values and parameter computation details metadata.\n        "
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            recompute_existing_parameter_values=recompute_existing_parameter_values,
        )
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        metric_values: MetricValues = (
            AttributedResolvedMetrics.get_metric_values_from_attributed_metric_values(
                attributed_metric_values=parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY
                ]
            )
        )
        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: _get_unique_values_from_nested_collection_of_sets(
                    collection=metric_values
                ),
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ],
            }
        )


def _get_unique_values_from_nested_collection_of_sets(
    collection: Collection[Collection[Set[Any]]],
) -> Set[Any]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get unique values from a collection of sets e.g. a list of sets.\n\n    Args:\n        collection: Collection of Sets containing collections of values.\n            can be nested Collections.\n\n    Returns:\n        Single flattened set containing unique values.\n    "
    flattened: List[Set[Any]] = list(itertools.chain.from_iterable(collection))
    '\n    In multi-batch data analysis, values can be empty and missin, resulting in "None" added to set.  However, due to\n    reliance on "np.ndarray", "None" gets converted to "numpy.Inf", whereas "numpy.Inf == numpy.Inf" returns False,\n    resulting in numerous "None" elements in final set.  For this reason, all "None" elements must be filtered out.\n    '
    unique_values: Set[Any] = set(
        sorted(filter((lambda element: (element is not None)), set().union(*flattened)))
    )
    return unique_values
