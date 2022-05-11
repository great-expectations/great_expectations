
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Union
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder, init_rule_parameter_builders
from great_expectations.rule_based_profiler.types import Builder, Domain, ParameterContainer
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ExpectationConfigurationBuilder(ABC, Builder):
    exclude_field_names: Set[str] = (Builder.exclude_field_names | {'validation_parameter_builders'})

    def __init__(self, expectation_type: str, validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]]=None, data_context: Optional['BaseDataContext']=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        The ExpectationConfigurationBuilder will build ExpectationConfiguration objects for a Domain from the Rule.\n\n        Args:\n            expectation_type: the "expectation_type" argument of "ExpectationConfiguration" object to be emitted.\n            validation_parameter_builder_configs: ParameterBuilder configurations, having whose outputs available (as\n            fully-qualified parameter names) is pre-requisite for present ExpectationConfigurationBuilder instance.\n            These "ParameterBuilder" configurations help build kwargs needed for this "ExpectationConfigurationBuilder"\n            data_context: BaseDataContext associated with this ExpectationConfigurationBuilder\n            kwargs: additional arguments\n        '
        super().__init__(data_context=data_context)
        self._expectation_type = expectation_type
        self._validation_parameter_builders = init_rule_parameter_builders(parameter_builder_configs=validation_parameter_builder_configs, data_context=self._data_context)
        '\n        Since ExpectationConfigurationBuilderConfigSchema allows arbitrary fields (as ExpectationConfiguration kwargs)\n        to be provided, they must be all converted to public property accessors and/or public fields in order for all\n        provisions by Builder, SerializableDictDot, and DictDot to operate properly in compliance with their interfaces.\n        '
        for (k, v) in kwargs.items():
            setattr(self, k, v)
            logger.debug('Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".', k, v, self.__class__.__name__)

    def build_expectation_configuration(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, batch_list: Optional[List[Batch]]=None, batch_request: Optional[Union[(BatchRequestBase, dict)]]=None) -> ExpectationConfiguration:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            domain: Domain object that is context for execution of this ParameterBuilder object.\n            variables: attribute name/value pairs\n            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.\n            batch_list: Explicit list of Batch objects to supply data at runtime.\n            batch_request: Explicit batch_request used to supply data at runtime.\n\n        Returns:\n            ExpectationConfiguration object.\n        '
        self.resolve_validation_dependencies(domain=domain, variables=variables, parameters=parameters, batch_list=batch_list, batch_request=batch_request)
        return self._build_expectation_configuration(domain=domain, variables=variables, parameters=parameters)

    def resolve_validation_dependencies(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None, batch_list: Optional[List[Batch]]=None, batch_request: Optional[Union[(BatchRequestBase, dict)]]=None, recompute_existing_parameter_values: bool=False) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        validation_parameter_builders: List[ParameterBuilder] = (self.validation_parameter_builders or [])
        validation_parameter_builder: ParameterBuilder
        for validation_parameter_builder in validation_parameter_builders:
            validation_parameter_builder.build_parameters(domain=domain, variables=variables, parameters=parameters, parameter_computation_impl=None, json_serialize=None, batch_list=batch_list, batch_request=batch_request, recompute_existing_parameter_values=recompute_existing_parameter_values)

    @abstractmethod
    def _build_expectation_configuration(self, domain: Domain, variables: Optional[ParameterContainer]=None, parameters: Optional[Dict[(str, ParameterContainer)]]=None) -> ExpectationConfiguration:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        pass

    @property
    def expectation_type(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._expectation_type

    @property
    def validation_parameter_builders(self) -> Optional[List[ParameterBuilder]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._validation_parameter_builders

def init_rule_expectation_configuration_builders(expectation_configuration_builder_configs: List[dict], data_context: Optional['BaseDataContext']=None) -> List['ExpectationConfigurationBuilder']:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    expectation_configuration_builder_config: dict
    return [init_expectation_configuration_builder(expectation_configuration_builder_config=expectation_configuration_builder_config, data_context=data_context) for expectation_configuration_builder_config in expectation_configuration_builder_configs]

def init_expectation_configuration_builder(expectation_configuration_builder_config: Union[('ExpectationConfigurationBuilder', dict)], data_context: Optional['BaseDataContext']=None) -> 'ExpectationConfigurationBuilder':
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (not isinstance(expectation_configuration_builder_config, dict)):
        expectation_configuration_builder_config = expectation_configuration_builder_config.to_dict()
    expectation_configuration_builder: 'ExpectationConfigurationBuilder' = instantiate_class_from_config(config=expectation_configuration_builder_config, runtime_environment={'data_context': data_context}, config_defaults={'class_name': 'DefaultExpectationConfigurationBuilder', 'module_name': 'great_expectations.rule_based_profiler.expectation_configuration_builder'})
    return expectation_configuration_builder
