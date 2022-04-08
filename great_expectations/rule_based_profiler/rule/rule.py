import json
from typing import Dict, List, Optional, Union

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
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    RuleState,
)
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


class Rule(SerializableDictDot):
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilder,
        parameter_builders: Optional[List[ParameterBuilder]] = None,
        expectation_configuration_builders: Optional[
            List[ExpectationConfigurationBuilder]
        ] = None,
    ):
        """
        Sets Profiler rule name, domain builders, parameters builders, configuration builders,
        and other necessary instance data (variables)
        :param name: A string representing the name of the ProfilerRule
        :param domain_builder: A Domain Builder object used to build rule data domain
        :param parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters for
        every configuration
        :param expectation_configuration_builders: A list of Expectation Configuration Builders
        """
        self._name = name
        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._expectation_configuration_builders = expectation_configuration_builders

    def run(
        self,
        variables: Optional[ParameterContainer] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        force_batch_data: bool = False,
    ) -> RuleState:
        """
        Builds a list of Expectation Configurations, returning a single Expectation Configuration entry for every
        ConfigurationBuilder available based on the instantiation.
        Args:
            variables: attribute name/value pairs, commonly-used in Builder objects.
            batch_list: Explicit list of Batch objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            force_batch_data: Whether or not to overwrite any existing batch_request value in Builder components.

        Returns:
            RuleState representing effect of executing Rule
        """
        domains: List[Domain] = self.domain_builder.get_domains(
            variables=variables,
            batch_list=batch_list,
            batch_request=batch_request,
            force_batch_data=force_batch_data,
        )
        rule_state: RuleState = RuleState(
            rule=self,
            variables=variables,
            domains=domains,
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
                    force_batch_data=force_batch_data,
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
                    force_batch_data=force_batch_data,
                )

        return rule_state

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def domain_builder(self) -> DomainBuilder:
        return self._domain_builder

    @property
    def parameter_builders(self) -> Optional[List[ParameterBuilder]]:
        return self._parameter_builders

    @property
    def expectation_configuration_builders(
        self,
    ) -> Optional[List[ExpectationConfigurationBuilder]]:
        return self._expectation_configuration_builders

    @property
    def rule_state(self) -> RuleState:
        return self._rule_state

    def to_dict(self) -> dict:
        parameter_builder_configs: Optional[List[dict]] = None
        parameter_builders: Optional[
            Dict[str, ParameterBuilder]
        ] = self._get_parameter_builders_as_dict()
        parameter_builder: ParameterBuilder
        if parameter_builders is not None:
            # Roundtrip through schema validation to add/or restore any missing fields.
            parameter_builder_configs = [
                parameterBuilderConfigSchema.load(parameter_builder.to_dict()).to_dict()
                for parameter_builder in parameter_builders.values()
            ]

        expectation_configuration_builder_configs: Optional[List[dict]] = None
        expectation_configuration_builders: Optional[
            Dict[str, ExpectationConfigurationBuilder]
        ] = self._get_expectation_configuration_builders_as_dict()
        expectation_configuration_builder: ExpectationConfigurationBuilder
        if expectation_configuration_builders is not None:
            # Roundtrip through schema validation to add/or restore any missing fields.
            expectation_configuration_builder_configs = [
                expectationConfigurationBuilderConfigSchema.load(
                    expectation_configuration_builder.to_dict()
                ).to_dict()
                for expectation_configuration_builder in expectation_configuration_builders.values()
            ]

        return {
            # Roundtrip through schema validation to add/or restore any missing fields.
            "domain_builder": domainBuilderConfigSchema.load(
                self.domain_builder.to_dict()
            ).to_dict(),
            "parameter_builders": parameter_builder_configs,
            "expectation_configuration_builders": expectation_configuration_builder_configs,
        }

    def to_json_dict(self) -> dict:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        make this refactoring infeasible at the present time.
        """
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()

    def _get_parameter_builders_as_dict(self) -> Dict[str, ParameterBuilder]:
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
    ) -> Dict[str, ExpectationConfigurationBuilder]:
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
