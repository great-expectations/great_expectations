from __future__ import annotations

import copy
import datetime
import json
import logging
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import (
    Batch,
    BatchRequestBase,
    batch_request_contains_batch_data,
)
from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    get_profiler_run_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.core.util import (
    convert_to_json_serializable,
    determine_progress_bar_method_by_environment,
    nested_update,
)
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.config.base import (
    DomainBuilderConfig,
    ExpectationConfigurationBuilderConfig,
    ParameterBuilderConfig,
    RuleBasedProfilerConfig,
    domainBuilderConfigSchema,
    expectationConfigurationBuilderConfigSchema,
    parameterBuilderConfigSchema,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
    init_rule_expectation_configuration_builders,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
    ReconciliationDirectives,
    ReconciliationStrategy,
    reconcile_rule_variables,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    init_rule_parameter_builders,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
    build_parameter_container_for_variables,
)
from great_expectations.rule_based_profiler.rule import Rule, RuleOutput
from great_expectations.rule_based_profiler.rule.rule_state import RuleState
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from great_expectations.core.domain import Domain
    from great_expectations.core.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.data_context.store.profiler_store import ProfilerStore
    from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
        DomainBuilder,
    )
    from great_expectations.rule_based_profiler.helpers.runtime_environment import (
        RuntimeEnvironmentDomainTypeDirectives,
        RuntimeEnvironmentVariablesDirectives,
    )


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseRuleBasedProfiler(ConfigPeer):
    """
    BaseRuleBasedProfiler class is initialized from RuleBasedProfilerConfig typed object and contains all functionality
    in the form of interface methods (which can be overwritten by subclasses) and their reference implementation.
    """

    EXPECTATION_SUCCESS_KEYS: Set[str] = {
        "auto",
        "profiler_config",
    }

    def __init__(
        self,
        profiler_config: RuleBasedProfilerConfig,
        data_context: Optional[AbstractDataContext] = None,
        usage_statistics_handler: Optional[UsageStatisticsHandler] = None,
    ) -> None:
        """
        Create a new RuleBasedProfilerBase using configured rules (as captured in the RuleBasedProfilerConfig object).

        For a Rule or an item in a Rule configuration, instantiates the following if
        available: a domain builder, a parameter builder, and a configuration builder.
        These will be used to define profiler computation patterns.

        Args:
            profiler_config: RuleBasedProfilerConfig -- formal typed object containing configuration
            data_context: AbstractDataContext object that defines full runtime environment (data access, etc.)
        """
        name: str = profiler_config.name
        id: Optional[str] = None
        if hasattr(profiler_config, "id"):
            id = profiler_config.id
        config_version: float = profiler_config.config_version
        variables: Optional[Dict[str, Any]] = profiler_config.variables
        rules: Optional[Dict[str, Dict[str, Any]]] = profiler_config.rules

        self._name = name
        self._id = id
        self._config_version = config_version

        self._profiler_config = profiler_config

        if variables is None:
            variables = {}

        # Convert variables argument to ParameterContainer
        _variables: ParameterContainer = build_parameter_container_for_variables(
            variables_configs=variables
        )
        self.variables = _variables

        self._usage_statistics_handler = usage_statistics_handler

        self._data_context = data_context

        self._rules = self._init_profiler_rules(rules=rules)

        self._rule_states = []

    @property
    def ge_cloud_id(self) -> Optional[str]:
        return self._id

    @ge_cloud_id.setter
    def ge_cloud_id(self, id: str) -> None:
        self._id = id

    def _init_profiler_rules(
        self,
        rules: Dict[str, Dict[str, Any]],
    ) -> List[Rule]:
        if rules is None:
            rules = {}

        rule_object_list: List[Rule] = []

        rule_name: str
        rule_config: Dict[str, Any]
        for rule_name, rule_config in rules.items():
            rule_object_list.append(
                self._init_rule(rule_name=rule_name, rule_config=rule_config)
            )

        return rule_object_list

    def _init_rule(
        self,
        rule_name: str,
        rule_config: Dict[str, Any],
    ) -> Rule:
        # Config is validated through schema but do a sanity check
        attr: str
        for attr in (
            "domain_builder",
            "expectation_configuration_builders",
        ):
            if attr not in rule_config:
                raise gx_exceptions.ProfilerConfigurationError(
                    message=f'Invalid rule "{rule_name}": missing mandatory {attr}.'
                )

        # Instantiate variables and builder attributes
        variables: Dict[str, Any] = rule_config.get("variables", {})
        domain_builder: DomainBuilder = RuleBasedProfiler._init_rule_domain_builder(
            domain_builder_config=rule_config.get("domain_builder"),
            data_context=self._data_context,
        )
        parameter_builders: Optional[
            List[ParameterBuilder]
        ] = init_rule_parameter_builders(
            parameter_builder_configs=rule_config.get("parameter_builders"),
            data_context=self._data_context,
        )
        expectation_configuration_builders: List[
            ExpectationConfigurationBuilder
        ] = init_rule_expectation_configuration_builders(
            expectation_configuration_builder_configs=rule_config.get(
                "expectation_configuration_builders"
            ),
            data_context=self._data_context,
        )

        # Compile previous steps and package into a Rule object
        return Rule(
            name=rule_name,
            variables=variables,
            domain_builder=domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

    @staticmethod
    def _init_rule_domain_builder(
        domain_builder_config: dict,
        data_context: Optional[AbstractDataContext] = None,
    ) -> DomainBuilder:
        domain_builder: DomainBuilder = instantiate_class_from_config(
            config=domain_builder_config,
            runtime_environment={"data_context": data_context},
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler.domain_builder"
            },
        )

        return domain_builder

    @public_api
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.RULE_BASED_PROFILER_RUN,
        args_payload_fn=get_profiler_run_usage_statistics,
    )
    def run(  # noqa: PLR0913
        self,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        runtime_configuration: Optional[dict] = None,
        reconciliation_directives: ReconciliationDirectives = DEFAULT_RECONCILATION_DIRECTIVES,
        variables_directives_list: Optional[
            List[RuntimeEnvironmentVariablesDirectives]
        ] = None,
        domain_type_directives_list: Optional[
            List[RuntimeEnvironmentDomainTypeDirectives]
        ] = None,
        comment: Optional[str] = None,
    ) -> RuleBasedProfilerResult:
        """Run the Rule-Based Profiler.

        Args:
            variables: Attribute name/value pairs (overrides), commonly-used in `Builder` objects.
            rules: A collection of rule configurations (overrides).
            batch_list: The batches of data supplied at runtime.
            batch_request: An explicit Batch Request used to supply data at runtime.
            runtime_configuration: Additional runtime settings (see `Validator.DEFAULT_RUNTIME_CONFIGURATION`).
            reconciliation_directives: Directives for how each rule component should be overwritten.
            variables_directives_list: Additional override runtime variables directives (modify `BaseRuleBasedProfiler`).
            domain_type_directives_list: Additional override runtime domain directives (modify `BaseRuleBasedProfiler`).
            comment: A citation for the Expectation Suite returned as part of the `RuleBasedProfilerResult`.

        Returns:
            A `RuleBasedProfilerResult` instance that contains the profiling output.
        """
        # Check to see if the user has disabled progress bars
        disable = False
        if self._data_context:
            progress_bars = self._data_context.progress_bars
            # If progress_bars are not present, assume we want them enabled
            if progress_bars is not None:
                if "globally" in progress_bars:
                    disable = not progress_bars["globally"]

                if "rule_based_profiler" in progress_bars:
                    disable = not progress_bars["rule_based_profiler"]

        effective_variables: Optional[
            ParameterContainer
        ] = self.reconcile_profiler_variables(
            variables=variables,
            reconciliation_strategy=reconciliation_directives.variables,
        )

        effective_rules: List[Rule] = self.reconcile_profiler_rules(
            rules=rules,
            reconciliation_directives=reconciliation_directives,
        )

        self._apply_runtime_environment(
            variables=effective_variables,
            rules=effective_rules,
            variables_directives_list=variables_directives_list,
            domain_type_directives_list=domain_type_directives_list,
        )

        rule: Rule
        effective_rules_configs: Optional[Dict[str, Dict[str, Any]]] = {
            rule.name: rule.to_json_dict() for rule in effective_rules
        }

        sys.stdout.write("\n")
        sys.stdout.flush()

        pbar_method: Callable = determine_progress_bar_method_by_environment()

        rule_state: RuleState
        rule: Rule
        for rule in pbar_method(
            effective_rules,
            desc="Generating Expectations:",
            disable=disable,
            position=0,
            leave=True,
            bar_format="{desc:25}{percentage:3.0f}%|{bar}{r_bar}",
        ):
            rule_state = rule.run(
                variables=effective_variables,
                batch_list=batch_list,
                batch_request=batch_request,
                runtime_configuration=runtime_configuration,
                reconciliation_directives=reconciliation_directives,
                rule_state=RuleState(),
            )
            self.rule_states.append(rule_state)

        return RuleBasedProfilerResult(
            fully_qualified_parameter_names_by_domain=self.get_fully_qualified_parameter_names_by_domain(),
            parameter_values_for_fully_qualified_parameter_names_by_domain=self.get_parameter_values_for_fully_qualified_parameter_names_by_domain(),
            expectation_configurations=list(
                filter(None, self.get_expectation_configurations())
            ),
            citation={
                "citation_date": convert_to_json_serializable(
                    data=datetime.datetime.now(datetime.timezone.utc)
                ),
                "comment": comment
                if comment
                else "Created by Rule-Based Profiler with the configuration included.",
                "profiler_config": {
                    "name": self.name,
                    "config_version": self.config_version,
                    "variables": convert_variables_to_dict(variables=self.variables),
                    "rules": effective_rules_configs,
                },
            },
            rule_domain_builder_execution_time={
                rule_state.rule.name: rule_state.rule_domain_builder_execution_time
                for rule_state in self.rule_states
            },
            rule_execution_time={
                rule_state.rule.name: rule_state.rule_execution_time
                for rule_state in self.rule_states
            },
            _usage_statistics_handler=self._usage_statistics_handler,
        )

    def get_expectation_configurations(self) -> List[ExpectationConfiguration]:
        """
        Returns:
            List of ExpectationConfiguration objects, accumulated from RuleState of every Rule executed.
        """
        expectation_configurations: List[ExpectationConfiguration] = []

        rule_state: RuleState
        rule_output: RuleOutput
        for rule_state in self.rule_states:
            rule_output = RuleOutput(rule_state=rule_state)
            expectation_configurations.extend(
                rule_output.get_expectation_configurations()
            )

        return expectation_configurations

    def get_fully_qualified_parameter_names_by_domain(self) -> Dict[Domain, List[str]]:
        """
        Returns:
            Dictionary of fully-qualified parameter names by Domain, accumulated from RuleState of every Rule executed.
        """
        fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]] = {}

        rule_state: RuleState
        rule_output: RuleOutput
        for rule_state in self.rule_states:
            rule_output = RuleOutput(rule_state=rule_state)
            fully_qualified_parameter_names_by_domain.update(
                rule_output.get_fully_qualified_parameter_names_by_domain()
            )

        return fully_qualified_parameter_names_by_domain

    def get_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> List[str]:
        """
        Args:
            domain_id: ID of desired Domain object.

        Returns:
            List of fully-qualified parameter names for Domain with domain_id as specified, accumulated from RuleState of corresponding Rule executed.
        """
        rule_state: RuleState
        for rule_state in self.rule_states:
            domain: Domain = rule_state.get_domains_as_dict().get(domain_id)
            if domain is not None:
                rule_output = RuleOutput(rule_state=rule_state)
                return rule_output.get_fully_qualified_parameter_names_for_domain_id(
                    domain_id=domain_id
                )

    def get_parameter_values_for_fully_qualified_parameter_names_by_domain(
        self,
    ) -> Dict[Domain, Dict[str, ParameterNode]]:
        """
        Returns:
            Dictionaries of values for fully-qualified parameter names by Domain, accumulated from RuleState of every Rule executed.
        """
        values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = {}

        rule_state: RuleState
        rule_output: RuleOutput
        for rule_state in self.rule_states:
            rule_output = RuleOutput(rule_state=rule_state)
            values_for_fully_qualified_parameter_names_by_domain.update(
                rule_output.get_parameter_values_for_fully_qualified_parameter_names_by_domain()
            )

        return values_for_fully_qualified_parameter_names_by_domain

    def get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        self, domain_id: str
    ) -> Dict[str, ParameterNode]:
        """
        Args:
            domain_id: ID of desired Domain object.

        Returns:
            Dictionary of values for fully-qualified parameter names for Domain with domain_id as specified, accumulated from RuleState of corresponding Rule executed.
        """
        rule_state: RuleState
        for rule_state in self.rule_states:
            domain: Domain = rule_state.get_domains_as_dict().get(domain_id)
            if domain is not None:
                rule_output = RuleOutput(rule_state=rule_state)
                return rule_output.get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
                    domain_id=domain_id
                )

    def add_rule(self, rule: Rule) -> None:
        """
        Add Rule object to existing profiler object by reconciling profiler rules and updating _profiler_config.
        """
        rules_dict: Dict[str, Dict[str, Any]] = {
            rule.name: rule.to_json_dict(),
        }
        effective_rules: List[Rule] = self.reconcile_profiler_rules(
            rules=rules_dict,
            reconciliation_directives=ReconciliationDirectives(
                domain_builder=ReconciliationStrategy.UPDATE,
                parameter_builder=ReconciliationStrategy.UPDATE,
                expectation_configuration_builder=ReconciliationStrategy.UPDATE,
            ),
        )
        self.rules = effective_rules
        updated_rules: Optional[Dict[str, Dict[str, Any]]] = {
            rule.name: rule.to_json_dict() for rule in effective_rules
        }
        self._profiler_config.rules = updated_rules

    def reconcile_profiler_variables(
        self,
        variables: Optional[Dict[str, Any]] = None,
        reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.variables,
    ) -> Optional[ParameterContainer]:
        """
        Profiler "variables" reconciliation involves combining the variables, instantiated from Profiler configuration
        (e.g., stored in a YAML file managed by the Profiler store), with the variables overrides, provided at run time.

        The reconciliation logic for "variables" is of the "replace" nature: An override value complements the original
        on key "miss", and replaces the original on key "hit" (or "collision"), because "variables" is a unique member.

        :param variables: variables overrides, supplied in dictionary (configuration) form
        :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites
        :return: reconciled variables in their canonical ParameterContainer object form
        """
        effective_variables: ParameterContainer
        if variables and isinstance(variables, dict):
            variables_configs: dict = self._reconcile_profiler_variables_as_dict(
                variables=variables, reconciliation_strategy=reconciliation_strategy
            )
            effective_variables = build_parameter_container_for_variables(
                variables_configs=variables_configs
            )
        else:
            effective_variables = self.variables

        return effective_variables

    def _reconcile_profiler_variables_as_dict(
        self,
        variables: Optional[Dict[str, Any]],
        reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.variables,
    ) -> dict:
        if variables is None:
            variables = {}

        variables_configs: Optional[Dict[str, Any]] = convert_variables_to_dict(
            variables=self.variables
        )

        if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
            variables_configs = nested_update(
                variables_configs,
                variables,
            )
        elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
            variables_configs = variables
        elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
            variables_configs.update(variables)

        return variables_configs

    def reconcile_profiler_rules(
        self,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        reconciliation_directives: ReconciliationDirectives = DEFAULT_RECONCILATION_DIRECTIVES,
    ) -> List[Rule]:
        """
        Profiler "rules" reconciliation involves combining the rules, instantiated from Profiler configuration (e.g.,
        stored in a YAML file managed by the Profiler store), with the rules overrides, provided at run time.

        The reconciliation logic for "rules" is of the "procedural" nature:
        (1) Combine every rule override configuration with any instantiated rule into a reconciled configuration
        (2) Re-instantiate Rule objects from the reconciled rule configurations

        :param rules: rules overrides, supplied in dictionary (configuration) form for each rule name as the key
        :param reconciliation_directives directives for how each rule component should be overwritten
        :return: reconciled rules in their canonical List[Rule] object form
        """
        effective_rules: Dict[str, Rule] = self._reconcile_profiler_rules_as_dict(
            rules=rules,
            reconciliation_directives=reconciliation_directives,
        )
        return list(effective_rules.values())

    def _reconcile_profiler_rules_as_dict(
        self,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        reconciliation_directives: ReconciliationDirectives = DEFAULT_RECONCILATION_DIRECTIVES,
    ) -> Dict[str, Rule]:
        if rules is None:
            rules = {}

        effective_rules: Dict[str, Rule] = self._get_rules_as_dict()

        rule_name: str
        rule_config: dict

        override_rule_configs: Dict[str, Dict[str, Any]] = {
            rule_name: RuleBasedProfiler._reconcile_rule_config(
                existing_rules=effective_rules,
                rule_name=rule_name,
                rule_config=rule_config,
                reconciliation_directives=reconciliation_directives,
            )
            for rule_name, rule_config in rules.items()
        }
        override_rules: Dict[str, Rule] = {
            rule_name: self._init_rule(rule_name=rule_name, rule_config=rule_config)
            for rule_name, rule_config in override_rule_configs.items()
        }
        effective_rules.update(override_rules)
        return effective_rules

    @classmethod
    def _reconcile_rule_config(
        cls,
        existing_rules: Dict[str, Rule],
        rule_name: str,
        rule_config: dict,
        reconciliation_directives: ReconciliationDirectives = DEFAULT_RECONCILATION_DIRECTIVES,
    ) -> Dict[str, Any]:
        """
        A "rule configuration" reconciliation is the process of combining the configuration of a single candidate
        override rule with at most one configuration corresponding to the list of rules instantiated from Profiler
        configuration (e.g., stored in a YAML file managed by the Profiler store).

        The reconciliation logic for "Rule configuration" employes the "by construction" principle:
        (1) Find a common configuration between the variables configuration, possibly supplied as part of the candiate
        override Rule configuration, and the variables configuration of an instantiated Rule
        (2) Find a common configuration between the domain builder configuration, possibly supplied as part of the
        candiate override Rule configuration, and the domain builder configuration of an instantiated Rule
        (3) Find common configurations between parameter builder configurations, possibly supplied as part of the
        candiate override Rule configuration, and the parameter builder configurations of an instantiated Rule
        (4) Find common configurations between expectation configuration builder configurations, possibly supplied as
        part of the candiate override Rule configuration, and the expectation configuration builder configurations of an
        instantiated Rule
        (5) Construct the reconciled Rule configuration dictionary using the formal Rule properties ("domain_builder",
        "parameter_builders", and "expectation_configuration_builders") as keys and their reconciled configuration
        dictionaries as values

        In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
        candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.

        :param existing_rules: all currently instantiated rules represented as a dictionary, keyed by rule name
        :param rule_name: name of the override rule candidate
        :param rule_config: configuration of an override rule candidate, supplied in dictionary (configuration) form
        :param reconciliation_directives directives for how each rule component should be overwritten
        :return: reconciled rule configuration, returned in dictionary (configuration) form
        """
        effective_rule_config: Dict[str, Any]
        if rule_name in existing_rules:
            rule: Rule = existing_rules[rule_name]

            variables_config: dict = rule_config.get("variables", {})
            effective_variables: dict = reconcile_rule_variables(
                variables=rule.variables,
                variables_config=variables_config,
                reconciliation_strategy=reconciliation_directives.variables,
            )

            domain_builder_config: dict = rule_config.get("domain_builder", {})
            effective_domain_builder_config: dict = (
                cls._reconcile_rule_domain_builder_config(
                    domain_builder=rule.domain_builder,
                    domain_builder_config=domain_builder_config,
                    reconciliation_strategy=reconciliation_directives.domain_builder,
                )
            )

            parameter_builder_configs: List[dict] = rule_config.get(
                "parameter_builders", []
            )
            effective_parameter_builder_configs: Optional[
                List[dict]
            ] = cls._reconcile_rule_parameter_builder_configs(
                rule=rule,
                parameter_builder_configs=parameter_builder_configs,
                reconciliation_strategy=reconciliation_directives.parameter_builder,
            )

            expectation_configuration_builder_configs: List[dict] = rule_config.get(
                "expectation_configuration_builders", []
            )
            effective_expectation_configuration_builder_configs: List[
                dict
            ] = cls._reconcile_rule_expectation_configuration_builder_configs(
                rule=rule,
                expectation_configuration_builder_configs=expectation_configuration_builder_configs,
                reconciliation_strategy=reconciliation_directives.expectation_configuration_builder,
            )

            effective_rule_config = {
                "variables": effective_variables,
                "domain_builder": effective_domain_builder_config,
                "parameter_builders": effective_parameter_builder_configs,
                "expectation_configuration_builders": effective_expectation_configuration_builder_configs,
            }
        else:
            effective_rule_config = rule_config

        return effective_rule_config

    @staticmethod
    def _reconcile_rule_domain_builder_config(
        domain_builder: DomainBuilder,
        domain_builder_config: dict,
        reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.domain_builder,
    ) -> dict:
        """
        Rule "domain builder" reconciliation involves combining the domain builder, instantiated from Rule configuration
        (e.g., stored in a YAML file managed by the Profiler store), with the domain builder override, possibly supplied
        as part of the candiate override rule configuration.

        The reconciliation logic for "domain builder" is of the "replace" nature: An override value complements the
        original on key "miss", and replaces the original on key "hit" (or "collision"), because "domain builder" is a
        unique member for a Rule.

        :param domain_builder: existing domain builder of a Rule
        :param domain_builder_config: domain builder configuration override, supplied in dictionary (configuration) form
        :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites
        :return: reconciled domain builder configuration, returned in dictionary (configuration) form
        """
        domain_builder_as_dict: dict = domain_builder.to_json_dict()
        domain_builder_as_dict["class_name"] = domain_builder.__class__.__name__
        domain_builder_as_dict["module_name"] = domain_builder.__class__.__module__

        # Roundtrip through schema validation to add/or restore any missing fields.
        deserialized_config: DomainBuilderConfig = domainBuilderConfigSchema.load(
            domain_builder_as_dict
        )
        serialized_config: dict = deserialized_config.to_dict()

        effective_domain_builder_config: dict = serialized_config
        if domain_builder_config:
            _validate_builder_override_config(builder_config=domain_builder_config)
            if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
                effective_domain_builder_config = nested_update(
                    effective_domain_builder_config,
                    domain_builder_config,
                )
            elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
                effective_domain_builder_config = domain_builder_config
            elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
                effective_domain_builder_config.update(domain_builder_config)

        return effective_domain_builder_config

    @staticmethod
    def _reconcile_rule_parameter_builder_configs(
        rule: Rule,
        parameter_builder_configs: List[dict],
        reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.parameter_builder,
    ) -> Optional[List[dict]]:
        """
        Rule "parameter builders" reconciliation involves combining the parameter builders, instantiated from Rule
        configuration (e.g., stored in a YAML file managed by the Profiler store), with the parameter builders
        overrides, possibly supplied as part of the candiate override rule configuration.

        The reconciliation logic for "parameter builders" is of the "upsert" nature: A candidate override parameter
        builder configuration contributes to the parameter builders list of the rule if the corresponding parameter
        builder name does not exist in the list of instantiated parameter builders of the rule; otherwise, once
        instnatiated, it replaces the configuration associated with the original parameter builder having the same name.

        :param rule: Profiler "rule", subject to parameter builder overrides
        :param parameter_builder_configs: parameter builder configuration overrides, supplied in dictionary (configuration) form
        :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites
        :return: reconciled parameter builder configuration, returned in dictionary (configuration) form
        """
        parameter_builder_config: dict
        for parameter_builder_config in parameter_builder_configs:
            _validate_builder_override_config(builder_config=parameter_builder_config)

        effective_parameter_builder_configs: Dict[str, dict] = {}

        current_parameter_builders: Dict[
            str, ParameterBuilder
        ] = rule._get_parameter_builders_as_dict()

        parameter_builder_name: str
        parameter_builder: ParameterBuilder
        parameter_builder_as_dict: dict
        for (
            parameter_builder_name,
            parameter_builder,
        ) in current_parameter_builders.items():
            parameter_builder_as_dict = parameter_builder.to_json_dict()
            parameter_builder_as_dict[
                "class_name"
            ] = parameter_builder.__class__.__name__
            parameter_builder_as_dict[
                "module_name"
            ] = parameter_builder.__class__.__module__

            # Roundtrip through schema validation to add/or restore any missing fields.
            deserialized_config: ParameterBuilderConfig = (
                parameterBuilderConfigSchema.load(parameter_builder_as_dict)
            )
            serialized_config: dict = deserialized_config.to_dict()

            effective_parameter_builder_configs[
                parameter_builder_name
            ] = serialized_config

        parameter_builder_configs_override: Dict[str, dict] = {
            parameter_builder_config["name"]: parameter_builder_config
            for parameter_builder_config in parameter_builder_configs
        }
        if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
            effective_parameter_builder_configs = nested_update(
                effective_parameter_builder_configs,
                parameter_builder_configs_override,
                dedup=True,
            )
        elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
            effective_parameter_builder_configs = parameter_builder_configs_override
        elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
            effective_parameter_builder_configs.update(
                parameter_builder_configs_override
            )

        if not effective_parameter_builder_configs:
            return None

        return list(effective_parameter_builder_configs.values())

    @staticmethod
    def _reconcile_rule_expectation_configuration_builder_configs(
        rule: Rule,
        expectation_configuration_builder_configs: List[dict],
        reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.expectation_configuration_builder,
    ) -> List[dict]:
        """
        Rule "expectation configuration builders" reconciliation involves combining the expectation configuration builders, instantiated from Rule
        configuration (e.g., stored in a YAML file managed by the Profiler store), with the expectation configuration builders
        overrides, possibly supplied as part of the candiate override rule configuration.

        The reconciliation logic for "expectation configuration builders" is of the "upsert" nature: A candidate override expectation configuration
        builder configuration contributes to the expectation configuration builders list of the rule if the corresponding expectation configuration
        builder name does not exist in the list of instantiated expectation configuration builders of the rule; otherwise, once
        instnatiated, it replaces the configuration associated with the original expectation configuration builder having the same name.

        :param rule: Profiler "rule", subject to expectations configuration builder overrides
        :param expectation_configuration_builder_configs: expectation configuration builder configuration overrides, supplied in dictionary (configuration) form
        :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites
        :return: reconciled expectation configuration builder configuration, returned in dictionary (configuration) form
        """
        expectation_configuration_builder_config: dict
        for (
            expectation_configuration_builder_config
        ) in expectation_configuration_builder_configs:
            _validate_builder_override_config(
                builder_config=expectation_configuration_builder_config
            )

        effective_expectation_configuration_builder_configs: Dict[str, dict] = {}

        current_expectation_configuration_builders: Dict[
            str, ExpectationConfigurationBuilder
        ] = rule._get_expectation_configuration_builders_as_dict()

        expectation_configuration_builder_name: str
        expectation_configuration_builder: ExpectationConfigurationBuilder
        expectation_configuration_builder_as_dict: dict
        for (
            expectation_configuration_builder_name,
            expectation_configuration_builder,
        ) in current_expectation_configuration_builders.items():
            expectation_configuration_builder_as_dict = (
                expectation_configuration_builder.to_json_dict()
            )
            expectation_configuration_builder_as_dict[
                "class_name"
            ] = expectation_configuration_builder.__class__.__name__
            expectation_configuration_builder_as_dict[
                "module_name"
            ] = expectation_configuration_builder.__class__.__module__

            # Roundtrip through schema validation to add/or restore any missing fields.
            deserialized_config: ExpectationConfigurationBuilderConfig = (
                expectationConfigurationBuilderConfigSchema.load(
                    expectation_configuration_builder_as_dict
                )
            )
            serialized_config: dict = deserialized_config.to_dict()

            effective_expectation_configuration_builder_configs[
                expectation_configuration_builder_name
            ] = serialized_config

        expectation_configuration_builder_configs_override: Dict[str, dict] = {
            expectation_configuration_builder_config[
                "expectation_type"
            ]: expectation_configuration_builder_config
            for expectation_configuration_builder_config in expectation_configuration_builder_configs
        }
        if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
            effective_expectation_configuration_builder_configs = nested_update(
                effective_expectation_configuration_builder_configs,
                expectation_configuration_builder_configs_override,
                dedup=True,
            )
        elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
            effective_expectation_configuration_builder_configs = (
                expectation_configuration_builder_configs_override
            )
        elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
            effective_expectation_configuration_builder_configs.update(
                expectation_configuration_builder_configs_override
            )

        if not effective_expectation_configuration_builder_configs:
            return []

        return list(effective_expectation_configuration_builder_configs.values())

    def _get_rules_as_dict(self) -> Dict[str, Rule]:
        rule: Rule
        return {rule.name: rule for rule in self.rules}

    # noinspection PyUnusedLocal
    def _apply_runtime_environment(
        self,
        variables: Optional[ParameterContainer] = None,
        rules: Optional[List[Rule]] = None,
        variables_directives_list: Optional[
            List[RuntimeEnvironmentVariablesDirectives]
        ] = None,
        domain_type_directives_list: Optional[
            List[RuntimeEnvironmentDomainTypeDirectives]
        ] = None,
    ) -> None:
        """
        variables: attribute name/value pairs, commonly-used in Builder objects, to modify using "runtime_environment"
        rules: name/(configuration-dictionary) to modify using "runtime_environment"
        variables_directives_list: additional/override runtime variables directives (modify "BaseRuleBasedProfiler")
        domain_type_directives_list: additional/override runtime domain directives (modify "BaseRuleBasedProfiler")
        """
        BaseRuleBasedProfiler._apply_variables_directives_runtime_environment(
            rules=rules,
            variables_directives_list=variables_directives_list,
        )
        BaseRuleBasedProfiler._apply_domain_type_directives_runtime_environment(
            rules=rules,
            domain_type_directives_list=domain_type_directives_list,
        )

        rule: Rule
        self._profiler_config.rules = {rule.name: rule.to_json_dict() for rule in rules}

    @staticmethod
    def _apply_variables_directives_runtime_environment(
        rules: Optional[List[Rule]] = None,
        variables_directives_list: Optional[
            List[RuntimeEnvironmentVariablesDirectives]
        ] = None,
    ) -> None:
        """
        rules: name/(configuration-dictionary) to modify using "runtime_environment"
        variables_directives_list: additional/override runtime "variables" directives (modify "BaseRuleBasedProfiler")
        """
        if rules is None:
            rules = []

        rule: Rule

        rule_names: List[str] = [rule.name for rule in rules]

        if variables_directives_list is None:
            variables_directives_list = []

        # 1. Ensure that "variables_directives_list" pertains to "Rule" objects (no spurrious "Rule" names).
        variables_directives_list = list(
            filter(
                lambda element: element.rule_name in rule_names,
                variables_directives_list,
            )
        )

        variables_directives: RuntimeEnvironmentVariablesDirectives

        # 2. Now obtain "Rule" names solely pertaining to additional/override runtime "variables" directives.
        rule_names = [
            variables_directives.rule_name
            for variables_directives in variables_directives_list
        ]

        # 3. Filter "Rule" objects to contain only those subject to additional/override runtime "variables" directives.
        rules = list(
            filter(
                lambda element: element.name in rule_names,
                rules,
            )
        )

        rules_as_dict: Dict[str, Rule] = {rule.name: rule for rule in rules}

        # 4. Update "variables" of pertinent "Rule" objects, according to corresponding additional/override directives.
        variables: Optional[Dict[str, Any]]
        rule_variables_configs: Optional[Dict[str, Any]]
        for variables_directives in variables_directives_list:
            variables = variables_directives.variables or {}
            rule = rules_as_dict[variables_directives.rule_name]
            rule_variables_configs = convert_variables_to_dict(variables=rule.variables)
            # Filter only those additional/override directives that correspond to keys in "Rule" "variables" settings.
            # noinspection PyTypeChecker
            variables = dict(
                filter(
                    lambda element: element[0] in rule_variables_configs,
                    variables.items(),
                )
            )
            # Update "Rule" "variables" settings with corresponding values specified by additional/override directives.
            rule_variables_configs.update(variables)
            # Restore "ParameterContainer" typed object satus of "Rule" "variables" field.
            rule.variables = build_parameter_container_for_variables(
                variables_configs=rule_variables_configs
            )

    @staticmethod
    def _apply_domain_type_directives_runtime_environment(
        rules: Optional[List[Rule]] = None,
        domain_type_directives_list: Optional[
            List[RuntimeEnvironmentDomainTypeDirectives]
        ] = None,
    ) -> None:
        """
        rules: name/(configuration-dictionary) to modify using "runtime_environment"
        domain_type_directives_list: additional/override runtime domain directives (modify "BaseRuleBasedProfiler")
        """
        if rules is None:
            rules = []

        if domain_type_directives_list is None:
            domain_type_directives_list = []

        domain_type_directives: RuntimeEnvironmentDomainTypeDirectives
        domain_rules: List[Rule]
        rule: Rule
        for domain_type_directives in domain_type_directives_list:
            # 1. Ensure that Domain directives pertain to "Rule" objects with "DomainBuilder" of correct "Domain" type.
            domain_rules = [
                rule
                for rule in rules
                if rule.domain_builder.domain_type == domain_type_directives.domain_type
            ]
            domain_property_key: str
            domain_property_value: Any
            existing_domain_property_value: Any
            # 2. Update Domain properties of pertinent "Rule" objects, according to corresponding Domain directives.
            for rule in domain_rules:
                for (
                    domain_property_key,
                    domain_property_value,
                ) in domain_type_directives.directives.items():
                    # Use property getter/setter methods on "DomainBuilder" of "Rule" to affect override directives.
                    try:
                        # Ensure that new directives augment (not eliminate) existing directives.
                        existing_domain_property_value = getattr(
                            rule.domain_builder, domain_property_key
                        )
                        domain_property_value = BaseRuleBasedProfiler._get_effective_domain_builder_property_value(  # noqa: PLW2901
                            dest_property_value=domain_property_value,
                            source_property_value=existing_domain_property_value,
                        )
                        setattr(
                            rule.domain_builder,
                            domain_property_key,
                            domain_property_value,
                        )
                    except AttributeError:
                        # Skip every directive that is not defined property of "DomainBuilder" object of "domain_type".
                        pass

    @staticmethod
    def _get_effective_domain_builder_property_value(
        dest_property_value: Optional[Any] = None,
        source_property_value: Optional[Any] = None,
    ) -> Optional[Any]:
        # Property values of collections types must be unique (use set for "list"/"tuple" and "update" for dictionary).
        if isinstance(dest_property_value, list) and isinstance(
            source_property_value, list
        ):
            return list(set(dest_property_value + source_property_value))

        if isinstance(dest_property_value, tuple) and isinstance(
            source_property_value, tuple
        ):
            return tuple(set(dest_property_value + source_property_value))

        if isinstance(dest_property_value, dict) and isinstance(
            source_property_value, dict
        ):
            return dict(dest_property_value, **source_property_value)

        return dest_property_value

    @classmethod
    def run_profiler(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        profiler_store: ProfilerStore,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        name: Optional[str] = None,
        id: Optional[str] = None,
        variables: Optional[dict] = None,
        rules: Optional[dict] = None,
    ) -> RuleBasedProfilerResult:
        profiler: RuleBasedProfiler = cls.get_profiler(
            data_context=data_context,
            profiler_store=profiler_store,
            name=name,
            id=id,
        )

        return profiler.run(
            variables=variables,
            rules=rules,
            batch_list=batch_list,
            batch_request=batch_request,
            runtime_configuration=None,
            reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
            variables_directives_list=None,
            domain_type_directives_list=None,
            comment=None,
        )

    @classmethod
    def run_profiler_on_data(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        profiler_store: ProfilerStore,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> RuleBasedProfilerResult:
        profiler: RuleBasedProfiler = cls.get_profiler(
            data_context=data_context,
            profiler_store=profiler_store,
            name=name,
            id=id,
        )

        rule: Rule
        rules: Dict[str, Dict[str, Any]] = {
            rule.name: rule.to_json_dict() for rule in profiler.rules
        }

        return profiler.run(
            variables=None,
            rules=rules,
            batch_list=batch_list,
            batch_request=batch_request,
            runtime_configuration=None,
            reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
            variables_directives_list=None,
            domain_type_directives_list=None,
            comment=None,
        )

    @classmethod
    def add_profiler(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        profiler_store: ProfilerStore,
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        return cls._persist_profiler(
            data_context=data_context,
            persistence_fn=profiler_store.add,
            name=name,
            id=id,
            config_version=config_version,
            rules=rules,
            variables=variables,
            profiler=profiler,
        )

    @classmethod
    def update_profiler(  # noqa: PLR0913
        cls,
        profiler_store: ProfilerStore,
        data_context: AbstractDataContext,
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        return cls._persist_profiler(
            data_context=data_context,
            persistence_fn=profiler_store.update,
            name=name,
            id=id,
            config_version=config_version,
            rules=rules,
            variables=variables,
            profiler=profiler,
        )

    @classmethod
    def add_or_update_profiler(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        profiler_store: ProfilerStore,
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        return cls._persist_profiler(
            data_context=data_context,
            persistence_fn=profiler_store.add_or_update,
            name=name,
            id=id,
            config_version=config_version,
            rules=rules,
            variables=variables,
            profiler=profiler,
        )

    @classmethod
    def _persist_profiler(  # noqa: PLR0913
        cls,
        data_context: AbstractDataContext,
        persistence_fn: Callable,
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfiler:
        config = RuleBasedProfiler._resolve_profiler_config_for_store(
            name=name,
            id=id,
            config_version=config_version,
            rules=rules,
            variables=variables,
            profiler=profiler,
        )

        if not RuleBasedProfiler._check_validity_of_batch_requests_in_config(
            config=config
        ):
            raise gx_exceptions.InvalidConfigError(
                "batch_data found in batch_request cannot be saved to ProfilerStore"
            )

        # Chetan - 20220204 - AbstractDataContext to be removed once it can be decoupled from RBP
        new_profiler: RuleBasedProfiler = instantiate_class_from_config(
            config=config.to_json_dict(),
            runtime_environment={
                "data_context": data_context,
            },
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler",
                "class_name": "RuleBasedProfiler",
            },
        )

        key: Union[GXCloudIdentifier, ConfigurationIdentifier]

        from great_expectations.data_context.data_context.cloud_data_context import (
            CloudDataContext,
        )

        if isinstance(data_context, CloudDataContext):
            key = GXCloudIdentifier(resource_type=GXCloudRESTResource.PROFILER)
        else:
            key = ConfigurationIdentifier(
                configuration_key=config.name,
            )

        try:
            response = persistence_fn(key=key, value=config)
        except gx_exceptions.StoreBackendError as e:
            raise gx_exceptions.ProfilerError(
                f"{e.message}; could not persist profiler"
            ) from e

        if isinstance(response, GXCloudResourceRef):
            new_profiler.ge_cloud_id = response.id

        return new_profiler

    @staticmethod
    def _resolve_profiler_config_for_store(  # noqa: PLR0913
        name: str | None = None,
        id: str | None = None,
        config_version: float | None = None,
        rules: dict[str, dict] | None = None,
        variables: dict | None = None,
        profiler: RuleBasedProfiler | None = None,
    ) -> RuleBasedProfilerConfig:
        if not (
            (profiler is None)
            ^ all(arg is None for arg in (name, config_version, rules))
        ):
            raise ValueError(
                "Must either pass in an existing profiler or individual constructor arguments (but not both)"
            )

        if profiler:
            config = profiler.config
            # Recognize any changes made to the runtime object
            config.config_version = profiler.config_version
            config.rules = {rule.name: rule.to_dict() for rule in profiler.rules}
            config.variables = convert_variables_to_dict(profiler.variables)
        else:
            config_data = {
                "name": name,
                "id": id,
                "config_version": config_version,
                "rules": rules,
                "variables": variables,
            }

            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            validated_config: dict = ruleBasedProfilerConfigSchema.load(config_data)
            profiler_config: dict = ruleBasedProfilerConfigSchema.dump(validated_config)
            profiler_config.pop("class_name")
            profiler_config.pop("module_name")

            config = RuleBasedProfilerConfig(**profiler_config)

        return config

    @staticmethod
    def _check_validity_of_batch_requests_in_config(
        config: RuleBasedProfilerConfig,
    ) -> bool:
        # Evaluate nested types in RuleConfig to parse out BatchRequests
        batch_requests: List[Union[BatchRequestBase, dict]] = []
        rule: dict
        for rule in config.rules.values():
            domain_builder: dict = rule["domain_builder"]
            if "batch_request" in domain_builder:
                batch_requests.append(domain_builder["batch_request"])

            parameter_builders: List[dict] = rule.get("parameter_builders", [])
            parameter_builder: dict
            for parameter_builder in parameter_builders:
                if "batch_request" in parameter_builder:
                    batch_requests.append(parameter_builder["batch_request"])

        # DataFrames shouldn't be saved to ProfilerStore
        batch_request: Union[BatchRequestBase, dict]
        for batch_request in batch_requests:
            if batch_request_contains_batch_data(batch_request=batch_request):
                return False

        return True

    @classmethod
    def get_profiler(
        cls,
        data_context: AbstractDataContext,
        profiler_store: ProfilerStore,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> RuleBasedProfiler:
        key = cls._construct_profiler_key(name=name, id=id)
        try:
            profiler_config: RuleBasedProfilerConfig = profiler_store.get(key=key)
        except gx_exceptions.InvalidKeyError as exc_ik:
            config_id: Union[GXCloudIdentifier, ConfigurationIdentifier] = (
                key.configuration_key
                if isinstance(key, ConfigurationIdentifier)
                else key
            )
            raise gx_exceptions.ProfilerNotFoundError(
                message=f'Non-existent Profiler configuration named "{config_id}".\n\nDetails: {exc_ik}'
            )

        config: dict = profiler_config.to_json_dict()
        if name:
            config.update({"name": name})

        config = filter_properties_dict(properties=config, clean_falsy=True)

        profiler: RuleBasedProfiler = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "data_context": data_context,
            },
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler",
                "class_name": "RuleBasedProfiler",
            },
        )

        return profiler

    @classmethod
    def delete_profiler(
        cls,
        profiler_store: ProfilerStore,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> None:
        key = cls._construct_profiler_key(name=name, id=id)

        try:
            profiler_store.remove_key(key=key)
        except (gx_exceptions.InvalidKeyError, KeyError) as exc_ik:
            config_id = (
                key.configuration_key
                if isinstance(key, ConfigurationIdentifier)
                else key
            )
            raise gx_exceptions.ProfilerNotFoundError(
                message=f'Non-existent Profiler configuration named "{config_id}".\n\nDetails: {exc_ik}'
            )

    @staticmethod
    def list_profilers(
        profiler_store: ProfilerStore,
        ge_cloud_mode: bool = False,
    ) -> List[str]:
        if ge_cloud_mode:
            return profiler_store.list_keys()
        return [x.configuration_key for x in profiler_store.list_keys()]

    @staticmethod
    def _construct_profiler_key(
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> ConfigurationIdentifier | GXCloudIdentifier:
        assert bool(name) ^ bool(id), "Must provide either name or id (but not both)"

        if id:
            return GXCloudIdentifier(resource_type=GXCloudRESTResource.PROFILER, id=id)
        return ConfigurationIdentifier(
            configuration_key=name,
        )

    def self_check(self, pretty_print: bool = True) -> dict:
        """
        Necessary to enable integration with `AbstractDataContext.test_yaml_config`
        Args:
            pretty_print: flag to turn on verbose output
        Returns:
            Dictionary that contains RuleBasedProfiler state
        """
        report_object: dict = {"config": self.config.to_json_dict()}

        if pretty_print:
            print(f"\nRuleBasedProfiler class name: {self.name}")

            if not self.variables:
                print(
                    'Your current RuleBasedProfiler configuration has an empty "variables" attribute. \
                    Please ensure you populate it if you\'d like to reference values in your "rules" attribute.'
                )

        return report_object

    @property
    def config(self) -> RuleBasedProfilerConfig:
        return self._profiler_config

    @property
    def name(self) -> str:
        return self._name

    @property
    def config_version(self) -> float:
        return self._config_version

    @property
    def variables(self) -> Optional[ParameterContainer]:
        # Returning a copy of the "self._variables" state variable in order to prevent write-before-read hazard.
        return copy.deepcopy(self._variables)

    @variables.setter
    def variables(self, value: Optional[ParameterContainer]) -> None:
        self._variables = value
        self.config.variables = convert_variables_to_dict(variables=value)

    @property
    def rules(self) -> List[Rule]:
        return self._rules

    @rules.setter
    def rules(self, value: List[Rule]) -> None:
        self._rules = value

    @property
    def rule_states(self) -> List[RuleState]:
        return self._rule_states

    @public_api
    def to_json_dict(self) -> dict:
        """Returns a JSON-serializable dict representation of this RuleBasedProfiler.

        Returns:
            A JSON-serializable dict representation of this RuleBasedProfiler.
        """
        variables_dict: Optional[Dict[str, Any]] = convert_variables_to_dict(
            variables=self.variables
        )

        rule: Rule
        serializeable_dict: dict = {
            "class_name": self.__class__.__name__,
            "module_name": self.__class__.__module__,
            "name": self.name,
            "config_version": self.config_version,
            "variables": variables_dict,
            "rules": [rule.to_json_dict() for rule in self.rules],
        }
        return serializeable_dict

    def __repr__(self) -> str:
        json_dict: dict = self.config.to_json_dict()
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        return self.__repr__()


@public_api
class RuleBasedProfiler(BaseRuleBasedProfiler):
    """Create a `RuleBasedProfiler` to profile one or more batches of data.

    For each rule in the `rules` configuration, instantiate the following if
    available: a domain builder, a parameter builder, and a configuration builder.
    These will be used to define profiler computation patterns.

    Args:
        name: Give the Profiler a name.
        config_version: Specify the version of the Profiler to use (currently only `1.0` is supported).
        variables: Variables to be substituted within the rules.
        rules: A collection of rule configurations, each having its own `domain_builder`, `parameter_builders`, and `expectation_configuration_builders`.
        data_context: Define the full runtime environment (data access, etc.).

    Returns:
        A `RuleBasedProfiler` instance.

    --ge-feature-maturity-info--

        id: rule_based_profiler_overall
        title: Rule-Based Profiler
        icon:
        short_description: Configuration Driven Profiler
        description: Use YAML to configure a flexible Profiler engine, which will then generate an ExpectationSuite for a data set
        how_to_guide_url:
        maturity: Experimental
        maturity_details:
            api_stability: Low (instantiation of Profiler and the signature of the run() method will change)
            implementation_completeness: Moderate (some augmentation and/or growth in capabilities is to be expected)
            unit_test_coverage: High (but not complete -- additional unit tests will be added, commensurate with the upcoming new functionality)
            integration_infrastructure_test_coverage: N/A -> TBD
            documentation_completeness: Moderate
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: domain_builders
        title: Domain Builders
        icon:
        short_description: Configurable Domain builders for generating lists of ExpectationConfiguration objects
        description: Use YAML to build domains for ExpectationConfiguration generator (table, column, semantic types, etc.)
        how_to_guide_url:
        maturity: Experimental
        maturity_details:
            api_stability: Moderate
            implementation_completeness: Moderate (additional DomainBuilder classes will be developed)
            unit_test_coverage: High (but not complete -- additional unit tests will be added, commensurate with the upcoming new functionality)
            integration_infrastructure_test_coverage: N/A -> TBD
            documentation_completeness: Moderate
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: parameter_builders
        title: Parameter Builders
        icon:
        short_description: Configurable Parameter builders for generating parameters to be used by ExpectationConfigurationBuilder classes for generating lists of ExpectationConfiguration objects (e.g., as kwargs and meta arguments), corresponding to the Domain built by a DomainBuilder class
        description: Use YAML to configure single and multi batch based parameter computation modules for the use by ExpectationConfigurationBuilder classes
        how_to_guide_url:
        maturity: Experimental
        maturity_details:
            api_stability: Moderate
            implementation_completeness: Moderate (additional ParameterBuilder classes will be developed)
            unit_test_coverage: High (but not complete -- additional unit tests will be added, commensurate with the upcoming new functionality)
            integration_infrastructure_test_coverage: N/A -> TBD
            documentation_completeness: Moderate
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: expectation_configuration_builders
        title: ExpectationConfiguration Builders
        icon:
        short_description: Configurable ExpectationConfigurationBuilder classes for generating lists of ExpectationConfiguration objects (e.g., as kwargs and meta arguments), corresponding to the Domain built by a DomainBuilder class and using parameters, computed by ParameterBuilder classes
        description: Use YAML to configure ExpectationConfigurationBuilder classes, which emit lists of ExpectationConfiguration objects (e.g., as kwargs and meta arguments)
        how_to_guide_url:
        maturity: Experimental
        maturity_details:
            api_stability: Moderate
            implementation_completeness: Moderate (additional ExpectationConfigurationBuilder classes might be developed)
            unit_test_coverage: High (but not complete -- additional unit tests will be added, commensurate with the upcoming new functionality)
            integration_infrastructure_test_coverage: N/A -> TBD
            documentation_completeness: Moderate
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

    --ge-feature-maturity-info--
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        config_version: float,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        data_context: Optional[AbstractDataContext] = None,
        id: Optional[str] = None,
    ) -> None:
        """Initialize a RuleBasedProfiler."""

        profiler_config = RuleBasedProfilerConfig(
            name=name,
            id=id,
            config_version=config_version,
            variables=variables,
            rules=rules,
        )

        usage_statistics_handler: Optional[UsageStatisticsHandler] = None
        if data_context:
            usage_statistics_handler = data_context.usage_statistics_handler

        super().__init__(
            profiler_config=profiler_config,
            data_context=data_context,
            usage_statistics_handler=usage_statistics_handler,
        )


def _validate_builder_override_config(builder_config: dict) -> None:
    """
    In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
    candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.

    :param builder_config: candidate builder override configuration
    :raises: ProfilerConfigurationError
    """
    if not all(
        (
            isinstance(builder_config, dict),
            "class_name" in builder_config,
            "module_name" in builder_config,
        )
    ):
        raise gx_exceptions.ProfilerConfigurationError(
            'Both "class_name" and "module_name" must be specified.'
        )
