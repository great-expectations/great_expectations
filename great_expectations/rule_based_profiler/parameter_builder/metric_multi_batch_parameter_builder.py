
from typing import Dict, List, Optional, Union
import numpy as np
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import get_parameter_value_and_validate_return_type
from great_expectations.rule_based_profiler.parameter_builder import MetricComputationDetails, MetricComputationResult, ParameterBuilder
from great_expectations.rule_based_profiler.types import FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY, FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY, Domain, ParameterContainer
from great_expectations.types.attributes import Attributes

class MetricMultiBatchParameterBuilder(ParameterBuilder):
    '\n    A Single/Multi-Batch implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs,\n    and metric_name as arguments.\n    '

    def __init__(self, name: str, metric_name: Optional[str]=None, metric_domain_kwargs: Optional[Union[(str, dict)]]=None, metric_value_kwargs: Optional[Union[(str, dict)]]=None, enforce_numeric_metric: Union[(str, bool)]=False, replace_nan_with_zero: Union[(str, bool)]=False, reduce_scalar_metric: Union[(str, bool)]=True, evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]=None, json_serialize: Union[(str, bool)]=True, data_context: Optional['BaseDataContext']=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            name: the name of this parameter -- this is user-specified parameter name (from configuration);\n            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."\n            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").\n            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)\n            metric_domain_kwargs: used in MetricConfiguration\n            metric_value_kwargs: used in MetricConfiguration\n            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values\n            replace_nan_with_zero: if False (default), then if the computed metric gives NaN, then exception is raised;\n            otherwise, if True, then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.\n            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.\n            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective\n            ParameterBuilder objects\' outputs available (as fully-qualified parameter names) is pre-requisite.\n            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".\n            json_serialize: If True (default), convert computed value to JSON prior to saving results.\n            data_context: BaseDataContext associated with this ParameterBuilder\n        '
        super().__init__(name=name, evaluation_parameter_builder_configs=evaluation_parameter_builder_configs, json_serialize=json_serialize, data_context=data_context)
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs
        self._enforce_numeric_metric = enforce_numeric_metric
        self._replace_nan_with_zero = replace_nan_with_zero
        self._reduce_scalar_metric = reduce_scalar_metric

    @property
    def metric_name(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._metric_name

    @metric_name.setter
    def metric_name(self, value: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._metric_name = value

    @property
    def metric_domain_kwargs(self) -> Optional[Union[(str, dict)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[(str, dict)]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[(str, dict)]]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._metric_value_kwargs = value

    @property
    def enforce_numeric_metric(self) -> Union[(str, bool)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._enforce_numeric_metric

    @property
    def replace_nan_with_zero(self) -> Union[(str, bool)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._replace_nan_with_zero

    @property
    def reduce_scalar_metric(self) -> Union[(str, bool)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._reduce_scalar_metric

    def _build_parameters(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, recompute_existing_parameter_values: bool=False) -> Attributes:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.\n\n        Returns:\n            Attributes object, containing computed parameter values and parameter computation details metadata.\n        '
        metric_computation_result: MetricComputationResult = self.get_metrics(metric_name=self.metric_name, metric_domain_kwargs=self.metric_domain_kwargs, metric_value_kwargs=self.metric_value_kwargs, enforce_numeric_metric=self.enforce_numeric_metric, replace_nan_with_zero=self.replace_nan_with_zero, domain=domain, variables=variables, parameters=parameters)
        details: MetricComputationDetails = metric_computation_result.details
        reduce_scalar_metric: bool = get_parameter_value_and_validate_return_type(domain=domain, parameter_reference=self.reduce_scalar_metric, expected_return_type=bool, variables=variables, parameters=parameters)
        if (len(metric_computation_result.attributed_resolved_metrics) == 1):
            if (reduce_scalar_metric and isinstance(metric_computation_result.attributed_resolved_metrics[0].metric_values, np.ndarray) and (metric_computation_result.attributed_resolved_metrics[0].metric_values.shape[1] == 1)):
                return Attributes({FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[0].metric_values[:, 0], FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[0].attributed_metric_values, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details})
            return Attributes({FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[0].metric_values, FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[0].attributed_metric_values, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details})
        return Attributes({FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics, FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics, FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details})
