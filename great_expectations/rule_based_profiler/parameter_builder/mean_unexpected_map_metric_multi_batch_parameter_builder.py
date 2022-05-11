from typing import Dict, List, Optional, Set, Union

import numpy as np

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
    MetricValues,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    PARAMETER_KEY,
    Domain,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes


class MeanUnexpectedMapMetricMultiBatchParameterBuilder(
    MetricMultiBatchParameterBuilder
):
    "\n    Compute mean unexpected count ratio (as a fraction) of specified map-style metric across every Batch of data given.\n"
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
        map_metric_name: str,
        total_count_parameter_builder_name: str,
        null_count_parameter_builder_name: Optional[str] = None,
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
        '\n        Args:\n            name: the name of this parameter -- this is user-specified parameter name (from configuration);\n            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."\n            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").\n            map_metric_name: the name of a map metric (must be a supported and registered map metric); the suffix\n            ".unexpected_count" will be appended to "map_metric_name" to be used in MetricConfiguration to get values.\n            total_count_parameter_builder_name: name of parameter that computes total_count (of rows in Batch).\n            null_count_parameter_builder_name: name of parameter that computes null_count (of domain values in Batch).\n            metric_domain_kwargs: used in MetricConfiguration\n            metric_value_kwargs: used in MetricConfiguration\n            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective\n            ParameterBuilder objects\' outputs available (as fully-qualified parameter names) is pre-requisite.\n            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".\n            json_serialize: If True (default), convert computed value to JSON prior to saving results.\n            data_context: BaseDataContext associated with this ParameterBuilder\n        '
        super().__init__(
            name=name,
            metric_name=f"{map_metric_name}.unexpected_count",
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=True,
            replace_nan_with_zero=True,
            reduce_scalar_metric=True,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            data_context=data_context,
        )
        self._map_metric_name = map_metric_name
        self._total_count_parameter_builder_name = total_count_parameter_builder_name
        self._null_count_parameter_builder_name = null_count_parameter_builder_name

    @property
    def map_metric_name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._map_metric_name

    @property
    def total_count_parameter_builder_name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._total_count_parameter_builder_name

    @property
    def null_count_parameter_builder_name(self) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._null_count_parameter_builder_name

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
            f"{PARAMETER_KEY}{total_count_parameter_builder_name}"
        )
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
            domain=domain, variables=variables, parameters=parameters
        )
        num_batch_ids: int = len(batch_ids)
        null_count_values: MetricValues
        if null_count_parameter_builder_name is None:
            null_count_values = np.zeros(shape=(num_batch_ids,))
        else:
            fully_qualified_null_count_parameter_builder_name: str = (
                f"{PARAMETER_KEY}{null_count_parameter_builder_name}"
            )
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
        unexpected_count_values: MetricValues = parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]
        unexpected_count_ratio_values: np.ndarray = (
            unexpected_count_values / nonnull_count_values
        )
        mean_unexpected_count_ratio: np.float64 = np.mean(unexpected_count_ratio_values)
        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: mean_unexpected_count_ratio,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ],
            }
        )
