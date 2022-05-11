from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterNode,
    RuleState,
    get_fully_qualified_parameter_names,
    get_parameter_values_for_fully_qualified_parameter_names,
)


class RuleOutput:
    "\n    RuleOutput provides methods for extracting useful information from RuleState using directives and application logic.\n"

    def __init__(self, rule_state: RuleState) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Args:\n            rule_state: RuleState object represented by "Domain" objects and parameters,.computed for one Rule object.\n        '
        self._rule_state = rule_state

    @property
    def rule_state(self) -> RuleState:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._rule_state

    def get_expectation_configurations(self) -> List[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        expectation_configurations: List[ExpectationConfiguration] = []
        domains: List[Domain] = self.rule_state.domains
        domain: Domain
        expectation_configuration_builder: ExpectationConfigurationBuilder
        for domain in domains:
            for (
                expectation_configuration_builder
            ) in self.rule_state.rule.expectation_configuration_builders:
                expectation_configurations.append(
                    expectation_configuration_builder.build_expectation_configuration(
                        domain=domain,
                        variables=self.rule_state.variables,
                        parameters=self.rule_state.parameters,
                    )
                )
        return expectation_configurations

    def get_fully_qualified_parameter_names_by_domain(
        self,
    ) -> Dict[(Domain, List[str])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        domain: Domain
        return {
            domain: self.get_fully_qualified_parameter_names_for_domain(domain=domain)
            for domain in self.rule_state.domains
        }

    def get_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        domains_dict: Dict[(str, Domain)] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        fully_qualified_parameter_names: List[
            str
        ] = self.get_fully_qualified_parameter_names_for_domain(domain=domain)
        return fully_qualified_parameter_names

    def get_fully_qualified_parameter_names_for_domain(
        self, domain: Optional[Domain] = None
    ) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        fully_qualified_parameter_names: List[
            str
        ] = get_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_by_domain(
        self,
    ) -> Dict[(Domain, Dict[(str, ParameterNode)])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        domain: Domain
        return {
            domain: self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
                domain=domain
            )
            for domain in self.rule_state.domains
        }

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> Dict[(str, ParameterNode)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        domains_dict: Dict[(str, Domain)] = self.rule_state.get_domains_as_dict()
        domain: Domain = domains_dict[domain_id]
        parameter_values_for_fully_qualified_parameter_names: Dict[
            (str, ParameterNode)
        ] = self.get_parameter_values_for_fully_qualified_parameter_names_for_domain(
            domain=domain
        )
        return parameter_values_for_fully_qualified_parameter_names

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain(
        self, domain: Optional[Domain] = None
    ) -> Dict[(str, ParameterNode)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        parameter_values_for_fully_qualified_parameter_names: Dict[
            (str, ParameterNode)
        ] = get_parameter_values_for_fully_qualified_parameter_names(
            domain=domain,
            variables=self.rule_state.variables,
            parameters=self.rule_state.parameters,
        )
        return parameter_values_for_fully_qualified_parameter_names
