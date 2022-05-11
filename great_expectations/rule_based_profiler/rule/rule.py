import copy
import json
from typing import Any, Dict, List, Optional, Union

from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.config.base import (
    domainBuilderConfigSchema,
    expectationConfigurationBuilderConfigSchema,
    parameterBuilderConfigSchema,
)
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
    ReconciliationDirectives,
    reconcile_rule_variables,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    RuleState,
    build_parameter_container_for_variables,
)
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


class Rule(SerializableDictDot):
    def __init__(
        self,
        name: str,
        variables: Optional[Union[(ParameterContainer, Dict[(str, Any)])]] = None,
        domain_builder: Optional[DomainBuilder] = None,
        parameter_builders: Optional[List[ParameterBuilder]] = None,
        expectation_configuration_builders: Optional[
            List[ExpectationConfigurationBuilder]
        ] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Sets Rule name, variables, domain builder, parameters builders, configuration builders, and other instance data.\n\n        Args:\n            name: A string representing the name of the ProfilerRule\n            variables: Any variables to be substituted within the rules\n            domain_builder: A Domain Builder object used to build rule data domain\n            parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters\n            expectation_configuration_builders: A list of Expectation Configuration Builders\n        "
        self._name = name
        if variables is None:
            variables = {}
        _variables: ParameterContainer
        if isinstance(variables, ParameterContainer):
            _variables = variables
        else:
            _variables: ParameterContainer = build_parameter_container_for_variables(
                variables_configs=variables
            )
        self.variables = _variables
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._expectation_configuration_builders = expectation_configuration_builders

    def run(
        self,
        variables: Optional[ParameterContainer] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[(BatchRequestBase, dict)]] = None,
        recompute_existing_parameter_values: bool = False,
        reconciliation_directives: ReconciliationDirectives = DEFAULT_RECONCILATION_DIRECTIVES,
    ) -> RuleState:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Builds a list of Expectation Configurations, returning a single Expectation Configuration entry for every\n        ConfigurationBuilder available based on the instantiation.\n\n        Args:\n            variables: Attribute name/value pairs, commonly-used in Builder objects\n            batch_list: Explicit list of Batch objects to supply data at runtime\n            batch_request: Explicit batch_request used to supply data at runtime\n            recompute_existing_parameter_values: If "True", recompute value if "fully_qualified_parameter_name" exists\n            reconciliation_directives: directives for how each rule component should be overwritten\n\n        Returns:\n            RuleState representing effect of executing Rule\n        '
        variables = build_parameter_container_for_variables(
            variables_configs=reconcile_rule_variables(
                variables=variables,
                variables_config=convert_variables_to_dict(variables=self.variables),
                reconciliation_strategy=reconciliation_directives.variables,
            )
        )
        domains: List[Domain] = (
            []
            if (self.domain_builder is None)
            else self.domain_builder.get_domains(
                rule_name=self.name,
                variables=variables,
                batch_list=batch_list,
                batch_request=batch_request,
            )
        )
        rule_state: RuleState = RuleState(
            rule=self, variables=variables, domains=domains
        )
        rule_state.reset_parameter_containers()
        domain: Domain
        for domain in domains:
            rule_state.initialize_parameter_container_for_domain(domain=domain)
            parameter_builders: List[ParameterBuilder] = self.parameter_builders or []
            parameter_builder: ParameterBuilder
            for parameter_builder in parameter_builders:
                parameter_builder.build_parameters(
                    domain=domain,
                    variables=variables,
                    parameters=rule_state.parameters,
                    parameter_computation_impl=None,
                    json_serialize=None,
                    batch_list=batch_list,
                    batch_request=batch_request,
                    recompute_existing_parameter_values=recompute_existing_parameter_values,
                )
            expectation_configuration_builders: List[
                ExpectationConfigurationBuilder
            ] = (self.expectation_configuration_builders or [])
            expectation_configuration_builder: ExpectationConfigurationBuilder
            for expectation_configuration_builder in expectation_configuration_builders:
                expectation_configuration_builder.resolve_validation_dependencies(
                    domain=domain,
                    variables=variables,
                    parameters=rule_state.parameters,
                    batch_list=batch_list,
                    batch_request=batch_request,
                    recompute_existing_parameter_values=recompute_existing_parameter_values,
                )
        return rule_state

    @property
    def name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._name = value

    @property
    def variables(self) -> Optional[ParameterContainer]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return copy.deepcopy(self._variables)

    @variables.setter
    def variables(self, value: Optional[ParameterContainer]) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._variables = value

    @property
    def domain_builder(self) -> Optional[DomainBuilder]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._domain_builder

    @property
    def parameter_builders(self) -> Optional[List[ParameterBuilder]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._parameter_builders

    @property
    def expectation_configuration_builders(
        self,
    ) -> Optional[List[ExpectationConfigurationBuilder]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._expectation_configuration_builders

    def to_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        parameter_builder_configs: Optional[List[dict]] = None
        parameter_builders: Optional[
            Dict[(str, ParameterBuilder)]
        ] = self._get_parameter_builders_as_dict()
        parameter_builder: ParameterBuilder
        if parameter_builders is not None:
            parameter_builder_configs = [
                parameterBuilderConfigSchema.load(parameter_builder.to_dict()).to_dict()
                for parameter_builder in parameter_builders.values()
            ]
        expectation_configuration_builder_configs: Optional[List[dict]] = None
        expectation_configuration_builders: Optional[
            Dict[(str, ExpectationConfigurationBuilder)]
        ] = self._get_expectation_configuration_builders_as_dict()
        expectation_configuration_builder: ExpectationConfigurationBuilder
        if expectation_configuration_builders is not None:
            expectation_configuration_builder_configs = [
                expectationConfigurationBuilderConfigSchema.load(
                    expectation_configuration_builder.to_dict()
                ).to_dict()
                for expectation_configuration_builder in expectation_configuration_builders.values()
            ]
        return {
            "domain_builder": domainBuilderConfigSchema.load(
                self.domain_builder.to_dict()
            ).to_dict(),
            "parameter_builders": parameter_builder_configs,
            "expectation_configuration_builders": expectation_configuration_builder_configs,
        }

    def to_json_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the\n        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,\n        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules\n        make this refactoring infeasible at the present time.\n        '
        dict_obj: dict = self.to_dict()
        variables_dict: Optional[Dict[(str, Any)]] = convert_variables_to_dict(
            variables=self.variables
        )
        dict_obj["variables"] = variables_dict
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(properties=json_dict, inplace=True)
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        # TODO: <Alex>2/4/2022</Alex>\n        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference\n        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the\n        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this\n        refactoring infeasible at the present time.\n        '
        return self.__repr__()

    def _get_parameter_builders_as_dict(self) -> Dict[(str, ParameterBuilder)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        parameter_builders: List[ParameterBuilder] = self.parameter_builders
        if parameter_builders is None:
            parameter_builders = []
        parameter_builder: ParameterBuilder
        return {
            parameter_builder.name: parameter_builder
            for parameter_builder in parameter_builders
        }

    def _get_expectation_configuration_builders_as_dict(
        self,
    ) -> Dict[(str, ExpectationConfigurationBuilder)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        expectation_configuration_builders: List[
            ExpectationConfigurationBuilder
        ] = self.expectation_configuration_builders
        if expectation_configuration_builders is None:
            expectation_configuration_builders = []
        expectation_configuration_builder: ExpectationConfigurationBuilder
        return {
            expectation_configuration_builder.expectation_type: expectation_configuration_builder
            for expectation_configuration_builder in expectation_configuration_builders
        }
