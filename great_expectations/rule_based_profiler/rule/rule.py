import copy
import json
from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


class Rule(SerializableDictDot):
    def __init__(
        self,
        name: str,
        domain_builder: DomainBuilder,
        expectation_configuration_builders: List[ExpectationConfigurationBuilder],
        parameter_builders: Optional[List[ParameterBuilder]] = None,
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

        self._parameters = {}

    def generate(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[ExpectationConfiguration]:
        """
        Builds a list of Expectation Configurations, returning a single Expectation Configuration entry for every
        ConfigurationBuilder available based on the instantiation.

        :return: List of Corresponding Expectation Configurations representing every configured rule
        """
        expectation_configurations: List[ExpectationConfiguration] = []

        domains: List[Domain] = self._domain_builder.get_domains(variables=variables)

        domain: Domain
        for domain in domains:
            parameter_container: ParameterContainer = ParameterContainer(
                parameter_nodes=None
            )
            self._parameters[domain.id] = parameter_container
            parameter_builder: ParameterBuilder
            for parameter_builder in self._parameter_builders:
                parameter_builder.build_parameters(
                    parameter_container=parameter_container,
                    domain=domain,
                    variables=variables,
                    parameters=self.parameters,
                )

            expectation_configuration_builder: ExpectationConfigurationBuilder
            for (
                expectation_configuration_builder
            ) in self._expectation_configuration_builders:
                expectation_configurations.append(
                    expectation_configuration_builder.build_expectation_configuration(
                        domain=domain,
                        variables=variables,
                        parameters=self.parameters,
                    )
                )

        return expectation_configurations

    def to_dict(self) -> dict:
        parameter_builders_as_dicts: Optional[List[dict]] = None
        parameter_builders: Optional[
            Dict[str, ParameterBuilder]
        ] = self.parameter_builders
        parameter_builder: ParameterBuilder
        if parameter_builders is not None:
            parameter_builders_as_dicts = [
                parameter_builder.to_dict()
                for parameter_builder in list(parameter_builders.values())
            ]

        expectation_configuration_builders_as_dicts: Optional[List[dict]] = None
        expectation_configuration_builders: Optional[
            Dict[str, ExpectationConfigurationBuilder]
        ] = self.expectation_configuration_builders
        expectation_configuration_builder: ExpectationConfigurationBuilder
        if expectation_configuration_builders is not None:
            expectation_configuration_builders_as_dicts = [
                expectation_configuration_builder.to_dict()
                for expectation_configuration_builder in list(
                    expectation_configuration_builders.values()
                )
            ]

        return {
            "name": self.name,
            "domain_builder": self.domain_builder.to_dict(),
            "parameter_builders": parameter_builders_as_dicts,
            "expectation_configuration_builders": expectation_configuration_builders_as_dicts,
        }

    def to_json_dict(self) -> dict:
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def name(self) -> str:
        return self._name

    @property
    def domain_builder(self) -> DomainBuilder:
        return self._domain_builder

    @property
    def parameter_builders(self) -> Dict[str, ParameterBuilder]:
        if self._parameter_builders is None:
            return {}

        parameter_builder: ParameterBuilder
        return {
            parameter_builder.name: parameter_builder
            for parameter_builder in self._parameter_builders
        }

    @property
    def expectation_configuration_builders(
        self,
    ) -> Dict[str, ExpectationConfigurationBuilder]:
        expectation_configuration_builder: ExpectationConfigurationBuilder
        return {
            expectation_configuration_builder.expectation_type: expectation_configuration_builder
            for expectation_configuration_builder in self._expectation_configuration_builders
        }

    @property
    def parameters(self) -> Dict[str, ParameterContainer]:
        # Returning a copy of the "self._parameters" state variable in order to prevent write-before-read hazard.
        return copy.deepcopy(self._parameters)
