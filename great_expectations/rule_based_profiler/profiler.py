import uuid
from typing import Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)
from great_expectations.rule_based_profiler.rule.rule import Rule


class Profiler:
    """
    Profiler object serves to profile, or automatically evaluate a set of rules, upon a given
    batch / multiple batches of data.
    """

    def __init__(
        self,
        *,
        profiler_config: Optional[Dict[str, Dict[str, Dict]]] = None,
        data_context: Optional[DataContext] = None,
    ):
        """
        Create a new Profiler using configured rules.
        For a rule or an item in a rule configuration, instantiates the following if
        available: a domain builder, a parameter builder, and a configuration builder.
        These will be used to define profiler computation patterns.

        Args:
            variables_configs: Variables from a profiler configuration
            rules_configs: Rule configuration as a dictionary
            data_context: DataContext object that defines a full runtime environment (data access, etc.)
        """
        self._data_context = data_context
        self._rules = []

        rules_configs: Dict[str, Dict] = profiler_config.get("rules", {})
        rule_name: str
        rule_config: dict

        for rule_name, rule_config in rules_configs.items():
            domain_builder_config: dict = rule_config.get("domain_builder")

            if domain_builder_config is None:
                raise ge_exceptions.ProfilerConfigurationError(
                    message=f'Invalid rule "{rule_name}": no domain_builder found.'
                )

            domain_builder: DomainBuilder = instantiate_class_from_config(
                config=domain_builder_config,
                runtime_environment={"data_context": data_context},
                config_defaults={
                    "module_name": "great_expectations.rule_based_profiler.domain_builder"
                },
            )

            parameter_builders: List[ParameterBuilder] = []

            parameter_builder_configs: dict = rule_config.get("parameter_builders")

            if parameter_builder_configs:
                parameter_builder_config: dict
                for parameter_builder_config in parameter_builder_configs:
                    parameter_builders.append(
                        instantiate_class_from_config(
                            config=parameter_builder_config,
                            runtime_environment={"data_context": data_context},
                            config_defaults={
                                "module_name": "great_expectations.rule_based_profiler.parameter_builder"
                            },
                        )
                    )

            expectation_configuration_builders: List[
                ExpectationConfigurationBuilder
            ] = []

            expectation_configuration_builder_configs: dict = rule_config.get(
                "expectation_configuration_builders"
            )

            if expectation_configuration_builder_configs:
                expectation_configuration_builder_config: dict
                for (
                    expectation_configuration_builder_config
                ) in expectation_configuration_builder_configs:
                    expectation_configuration_builders.append(
                        instantiate_class_from_config(
                            config=expectation_configuration_builder_config,
                            runtime_environment={},
                            config_defaults={
                                "class_name": "DefaultExpectationConfigurationBuilder",
                                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                            },
                        )
                    )

            variables_configs: Dict[str, Dict] = profiler_config.get("variables", {})
            variables: Optional[ParameterContainer] = None

            if variables_configs:
                variables = build_parameter_container_for_variables(
                    variables_configs=variables_configs
                )

            self._rules.append(
                Rule(
                    name=rule_name,
                    domain_builder=domain_builder,
                    parameter_builders=parameter_builders,
                    expectation_configuration_builders=expectation_configuration_builders,
                    variables=variables,
                )
            )

    def profile(
        self,
        *,
        expectation_suite_name: Optional[str] = None,
    ) -> ExpectationSuite:
        """
        Args:
            :param expectation_suite_name: A name for returned Expectation suite.
        :return: Set of rule evaluation results in the form of an ExpectationSuite
        """
        if expectation_suite_name is None:
            expectation_suite_name = (
                f"tmp_suite_{self.__class__.__name__}_{str(uuid.uuid4())[:8]}"
            )

        expectation_suite: ExpectationSuite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name
        )

        rule: Rule
        for rule in self._rules:
            expectation_configurations: List[ExpectationConfiguration] = rule.generate()
            expectation_configuration: ExpectationConfiguration
            for expectation_configuration in expectation_configurations:
                expectation_suite.add_expectation(
                    expectation_configuration=expectation_configuration
                )

        return expectation_suite
