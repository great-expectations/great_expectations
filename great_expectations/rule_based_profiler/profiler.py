from typing import Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
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
from great_expectations.validator.validator import Validator


class Profiler:
    """Profiler object serves to profile, or automatically evaluate a set of rules, upon a given
    batch / multiple batches of data"""

    def __init__(
        self,
        *,
        variables_configs: Optional[Dict[str, Dict]] = None,
        rules_configs: Optional[Dict[str, Dict]] = None,
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
            data_context: An organizational DataContext object that defines a full runtime environment (data access, etc.)
        """
        self._data_context = data_context

        self._rules = []
        for rule_name, rule_config in rules_configs.items():
            domain_builder_config = rule_config.get("domain_builder")
            if domain_builder_config is None:
                raise ge_exceptions.ProfilerConfigurationError(
                    message=f'Invalid rule "{rule_name}": no domain_builder found.'
                )
            domain_builder: DomainBuilder = instantiate_class_from_config(
                domain_builder_config,
                runtime_environment={},
                config_defaults={
                    "module_name": "great_expectations.rule_based_profiler.domain_builder"
                },
            )

            parameter_builders: List[ParameterBuilder] = []
            parameter_builder_configs = rule_config.get("parameter_builders")
            if parameter_builder_configs:
                for parameter_builder_config in parameter_builder_configs:
                    parameter_builders.append(
                        instantiate_class_from_config(
                            parameter_builder_config,
                            runtime_environment={"data_context": data_context},
                            config_defaults={
                                "module_name": "great_expectations.rule_based_profiler.parameter_builder"
                            },
                        )
                    )

            expectation_configuration_builders: List[
                ExpectationConfigurationBuilder
            ] = []
            configuration_builder_configs = rule_config.get(
                "expectation_configuration_builders"
            )
            for configuration_builder_config in configuration_builder_configs:
                expectation_configuration_builders.append(
                    instantiate_class_from_config(
                        configuration_builder_config,
                        runtime_environment={},
                        config_defaults={
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        },
                    )
                )

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
        validator=None,
        batch=None,
        batches=None,
        batch_request=None,
        data_context=None,
        expectation_suite_name=None,
    ):
        """
        Utilizing one of validator, batch, batches, or batch_request (only one may be provided),
        evaluates Profiler object to evaluate rule set on the given data and returns results of rule
        evaluations as an Expectation Suite

        Args:
            :param validator: A Validator object to profile on
            :param batch: A Batch object to profile on
            :param batches: A list of Batch objects
            :param batch_request: A Batch request utilized to obtain a Validator Object
            :param data_context: A DataContext object used to define a great_expectations project environment
            :param expectation_suite_name: A name for returned Expectation suite.
        :return: Set of rule evaluation results in the form of an Expectation suite
        """
        if sum([bool(x) for x in (validator, batch, batches, batch_request)]) != 1:
            raise ge_exceptions.ProfilerError(
                "Exactly one of validator, batch, batches, or batch_request must be provided."
            )

        if data_context is not None:
            self._data_context = data_context

        if validator is None:
            if batch:
                # TODO: <Alex>ALEX -- Note, not specifying an "expectation_suite" explicitly causes: expectation_suite_name = "default" -- this seems problematic.</Alex>
                validator = Validator(
                    execution_engine=batch.data.execution_engine, batches=[batch]
                )
            elif batches:
                execution_engine = batches[0].data.execution_engine
                for batch in batches:
                    if batch.data.execution_engine != execution_engine:
                        raise ge_exceptions.ProfilerExecutionError(
                            f"batch {batch.id} does not share an execution engine with all other batches in the same batches list."
                        )
                # TODO: <Alex>ALEX -- Note, not specifying an "expectation_suite" explicitly causes: expectation_suite_name = "default" -- this seems problematic.</Alex>
                validator = Validator(
                    execution_engine=execution_engine, batches=batches
                )
            elif batch_request:
                if not self.data_context:
                    raise ge_exceptions.ProfilerExecutionError(
                        message="Unable to profile using a batch_request if no data_context is provided."
                    )
                # TODO: <Alex>ALEX -- Note, not specifying an "expectation_suite" explicitly causes: expectation_suite_name = "default" -- this seems problematic.</Alex>
                validator = self.data_context.get_validator(batch_request=batch_request)

        if expectation_suite_name is None:
            expectation_suite_name = (
                f"{self.__class__.__name__}_generated_expectation_suite"
            )
        suite = ExpectationSuite(expectation_suite_name=expectation_suite_name)
        for rule in self._rules:
            expectation_configurations = rule.generate(validator)
            for expectation_configuration in expectation_configurations:
                suite.add_expectation(expectation_configuration)

        return suite

    @property
    def data_context(self):
        return self._data_context
