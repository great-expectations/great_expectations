import copy
import json
from typing import Any, Callable, Dict, List, Optional, Union

from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.domain import Domain
from great_expectations.core.util import (
    convert_to_json_serializable,
    determine_progress_bar_method_by_environment,
)
from great_expectations.rule_based_profiler.config.base import (
    domainBuilderConfigSchema,
    expectationConfigurationBuilderConfigSchema,
    parameterBuilderConfigSchema,
)
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
)
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
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)
from great_expectations.rule_based_profiler.rule.rule_state import RuleState
from great_expectations.types import SerializableDictDot
from great_expectations.util import (
    deep_filter_properties_iterable,
    measure_execution_time,
)


class Rule(SerializableDictDot):
    def __init__(  # noqa: PLR0913
        self,
        name: str,
        variables: Optional[Union[ParameterContainer, Dict[str, Any]]] = None,
        domain_builder: Optional[DomainBuilder] = None,
        parameter_builders: Optional[List[ParameterBuilder]] = None,
        expectation_configuration_builders: Optional[
            List[ExpectationConfigurationBuilder]
        ] = None,
    ) -> None:
        """
        Sets Rule name, variables, domain builder, parameters builders, configuration builders, and other instance data.

        Args:
            name: A string representing the name of the ProfilerRule
            variables: Any variables to be substituted within the rules
            domain_builder: A Domain Builder object used to build rule data domain
            parameter_builders: A Parameter Builder list used to configure necessary rule evaluation parameters
            expectation_configuration_builders: A list of Expectation Configuration Builders
        """
        self._name = name

        if variables is None:
            variables = {}

        # Convert variables argument to ParameterContainer
        _variables: ParameterContainer
        if isinstance(variables, ParameterContainer):
            _variables = variables
        else:
            _variables = build_parameter_container_for_variables(
                variables_configs=variables
            )

        self.variables = _variables

        self._domain_builder = domain_builder
        self._parameter_builders = parameter_builders
        self._expectation_configuration_builders = expectation_configuration_builders

    @measure_execution_time(
        execution_time_holder_object_reference_name="rule_state",
        execution_time_property_name="rule_execution_time",
        pretty_print=False,
    )
    def run(  # noqa: PLR0913
        self,
        variables: Optional[ParameterContainer] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        runtime_configuration: Optional[dict] = None,
        reconciliation_directives: Optional[ReconciliationDirectives] = None,
        rule_state: Optional[RuleState] = None,
    ) -> RuleState:
        """
        Builds a list of Expectation Configurations, returning a single Expectation Configuration entry for every
        ConfigurationBuilder available based on the instantiation.

        Args:
            variables: Attribute name/value pairs, commonly-used in Builder objects
            batch_list: Explicit list of Batch objects to supply data at runtime
            batch_request: Explicit batch_request used to supply data at runtime
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            reconciliation_directives: directives for how each rule component should be overwritten
            rule_state: holds "Rule" execution state and responds to "execution_time_property_name" ("execution_time")

        Returns:
            RuleState representing effect of executing Rule
        """
        if not reconciliation_directives:
            reconciliation_directives = DEFAULT_RECONCILATION_DIRECTIVES

        variables = build_parameter_container_for_variables(
            variables_configs=reconcile_rule_variables(
                variables=self.variables,
                variables_config=convert_variables_to_dict(variables=variables),
                reconciliation_strategy=reconciliation_directives.variables,
            )
        )

        if rule_state is None:
            rule_state = RuleState()

        domains: List[Domain] = self._get_rule_domains(
            variables=variables,
            batch_list=batch_list,
            batch_request=batch_request,
            rule_state=rule_state,
            runtime_configuration=runtime_configuration,
        )

        rule_state.rule = self
        rule_state.variables = variables
        rule_state.domains = domains

        rule_state.reset_parameter_containers()

        pbar_method: Callable = determine_progress_bar_method_by_environment()

        domain: Domain
        for domain in pbar_method(
            domains,
            desc="Profiling Dataset:",
            position=1,
            leave=False,
            bar_format="{desc:25}{percentage:3.0f}%|{bar}{r_bar}",
        ):
            rule_state.initialize_parameter_container_for_domain(domain=domain)

            parameter_builders: List[ParameterBuilder] = self.parameter_builders or []
            parameter_builder: ParameterBuilder
            for parameter_builder in parameter_builders:
                parameter_builder.build_parameters(
                    domain=domain,
                    variables=variables,
                    parameters=rule_state.parameters,
                    parameter_computation_impl=None,
                    batch_list=batch_list,
                    batch_request=batch_request,
                    runtime_configuration=runtime_configuration,
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
                    runtime_configuration=runtime_configuration,
                )

        return rule_state

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def variables(self) -> ParameterContainer:
        # Returning a copy of the "self._variables" state variable in order to prevent write-before-read hazard.
        return copy.deepcopy(self._variables)

    @variables.setter
    def variables(self, value: ParameterContainer) -> None:
        self._variables = value

    @property
    def domain_builder(self) -> Optional[DomainBuilder]:
        return self._domain_builder

    @property
    def parameter_builders(self) -> Optional[List[ParameterBuilder]]:
        return self._parameter_builders

    @property
    def expectation_configuration_builders(
        self,
    ) -> Optional[List[ExpectationConfigurationBuilder]]:
        return self._expectation_configuration_builders

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

        domain_builder_configs: dict = (
            self.domain_builder.to_dict() if self.domain_builder else {}
        )

        return {
            # Roundtrip through schema validation to add/or restore any missing fields.
            "domain_builder": domainBuilderConfigSchema.load(
                domain_builder_configs
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
        variables_dict: Optional[Dict[str, Any]] = convert_variables_to_dict(
            variables=self.variables
        )
        dict_obj["variables"] = variables_dict
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
        parameter_builders: List[ParameterBuilder] = self.parameter_builders or []

        parameter_builder: ParameterBuilder
        return {
            parameter_builder.name: parameter_builder
            for parameter_builder in parameter_builders
        }

    def _get_expectation_configuration_builders_as_dict(
        self,
    ) -> Dict[str, ExpectationConfigurationBuilder]:
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = (
            self.expectation_configuration_builders or []
        )

        expectation_configuration_builder: ExpectationConfigurationBuilder
        return {
            expectation_configuration_builder.expectation_type: expectation_configuration_builder
            for expectation_configuration_builder in expectation_configuration_builders
        }

    # noinspection PyUnusedLocal
    @measure_execution_time(
        execution_time_holder_object_reference_name="rule_state",
        execution_time_property_name="rule_domain_builder_execution_time",
        pretty_print=False,
    )
    def _get_rule_domains(  # noqa: PLR0913
        self,
        variables: Optional[ParameterContainer] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        rule_state: Optional[RuleState] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        domains: List[Domain] = (
            []
            if self.domain_builder is None
            else self.domain_builder.get_domains(
                rule_name=self.name,
                variables=variables,
                batch_list=batch_list,
                batch_request=batch_request,
                runtime_configuration=runtime_configuration,
            )
        )
        return domains
