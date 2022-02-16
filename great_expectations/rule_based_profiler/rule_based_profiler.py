import copy
import logging
import uuid
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.util import nested_update
from great_expectations.data_context.store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    domainBuilderConfigSchema,
    expectationConfigurationBuilderConfigSchema,
    parameterBuilderConfigSchema,
)
from great_expectations.rule_based_profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule.rule import Rule
from great_expectations.rule_based_profiler.types import (
    ParameterContainer,
    build_parameter_container_for_variables,
)
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


def _validate_builder_override_config(builder_config: dict):
    """
    In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
    candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.

    :param builder_config: candidate builder override configuration
    :raises: ProfilerConfigurationError
    """
    if not all(
        [
            isinstance(builder_config, dict),
            "class_name" in builder_config,
            "module_name" in builder_config,
        ]
    ):
        raise ge_exceptions.ProfilerConfigurationError(
            'Both "class_name" and "module_name" must be specified.'
        )


class BaseRuleBasedProfiler(ConfigPeer):
    """
    BaseRuleBasedProfiler class is initialized from RuleBasedProfilerConfig typed object and contains all functionality
    in the form of interface methods (which can be overwritten by subclasses) and their reference implementation.
    """

    def __init__(
        self,
        profiler_config: RuleBasedProfilerConfig,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Create a new RuleBasedProfilerBase using configured rules (as captured in the RuleBasedProfilerConfig object).

        For a rule or an item in a rule configuration, instantiates the following if
        available: a domain builder, a parameter builder, and a configuration builder.
        These will be used to define profiler computation patterns.

        Args:
            profiler_config: RuleBasedProfilerConfig -- formal typed object containing configuration
            data_context: DataContext object that defines a full runtime environment (data access, etc.)
        """
        name: str = profiler_config.name
        config_version: float = profiler_config.config_version
        variables: Optional[Dict[str, Any]] = profiler_config.variables
        rules: Optional[Dict[str, Dict[str, Any]]] = profiler_config.rules

        self._name = name
        self._config_version = config_version

        self._profiler_config = profiler_config

        if variables is None:
            variables = {}

        # Necessary to annotate ExpectationSuite during `run()`
        self._citation = {
            "name": name,
            "config_version": config_version,
            "variables": variables,
            "rules": rules,
        }

        # Convert variables argument to ParameterContainer
        _variables: ParameterContainer = build_parameter_container_for_variables(
            variables_configs=variables
        )
        self._variables = _variables

        self._data_context = data_context

        self._rules = self._init_profiler_rules(rules=rules)

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
                raise ge_exceptions.ProfilerConfigurationError(
                    message=f'Invalid rule "{rule_name}": missing mandatory {attr}.'
                )

        # Instantiate builder attributes
        domain_builder: DomainBuilder = RuleBasedProfiler._init_rule_domain_builder(
            domain_builder_config=rule_config["domain_builder"],
            data_context=self._data_context,
        )
        parameter_builders: Optional[
            List[ParameterBuilder]
        ] = RuleBasedProfiler._init_rule_parameter_builders(
            parameter_builder_configs=rule_config.get("parameter_builders"),
            data_context=self._data_context,
        )
        expectation_configuration_builders: List[
            ExpectationConfigurationBuilder
        ] = RuleBasedProfiler._init_rule_expectation_configuration_builders(
            expectation_configuration_builder_configs=rule_config[
                "expectation_configuration_builders"
            ]
        )

        # Compile previous steps and package into a Rule object
        return Rule(
            name=rule_name,
            domain_builder=domain_builder,
            parameter_builders=parameter_builders,
            expectation_configuration_builders=expectation_configuration_builders,
        )

    @staticmethod
    def _init_rule_domain_builder(
        domain_builder_config: dict,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ) -> DomainBuilder:
        domain_builder: DomainBuilder = instantiate_class_from_config(
            config=domain_builder_config,
            runtime_environment={"data_context": data_context},
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler.domain_builder"
            },
        )

        return domain_builder

    @staticmethod
    def _init_rule_parameter_builders(
        parameter_builder_configs: Optional[List[dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ) -> Optional[List[ParameterBuilder]]:
        if parameter_builder_configs is None:
            return None

        parameter_builders: List[ParameterBuilder] = []

        parameter_builder_config: dict
        for parameter_builder_config in parameter_builder_configs:
            parameter_builder: ParameterBuilder = (
                RuleBasedProfiler._init_parameter_builder(
                    parameter_builder_config=parameter_builder_config,
                    data_context=data_context,
                )
            )
            parameter_builders.append(parameter_builder)

        return parameter_builders

    @staticmethod
    def _init_parameter_builder(
        parameter_builder_config: dict,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ) -> ParameterBuilder:
        parameter_builder: ParameterBuilder = instantiate_class_from_config(
            config=parameter_builder_config,
            runtime_environment={"data_context": data_context},
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler.parameter_builder"
            },
        )
        return parameter_builder

    @staticmethod
    def _init_rule_expectation_configuration_builders(
        expectation_configuration_builder_configs: List[dict],
    ) -> List[ExpectationConfigurationBuilder]:
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = []

        expectation_configuration_builder_config: dict
        for (
            expectation_configuration_builder_config
        ) in expectation_configuration_builder_configs:
            expectation_configuration_builder: ExpectationConfigurationBuilder = RuleBasedProfiler._init_expectation_configuration_builder(
                expectation_configuration_builder_config=expectation_configuration_builder_config,
            )
            expectation_configuration_builders.append(expectation_configuration_builder)

        return expectation_configuration_builders

    @staticmethod
    def _init_expectation_configuration_builder(
        expectation_configuration_builder_config: dict,
    ) -> ExpectationConfigurationBuilder:
        expectation_configuration_builder: ExpectationConfigurationBuilder = instantiate_class_from_config(
            config=expectation_configuration_builder_config,
            runtime_environment={},
            config_defaults={
                "class_name": "DefaultExpectationConfigurationBuilder",
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
            },
        )
        return expectation_configuration_builder

    def run(
        self,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> ExpectationSuite:
        """
        Args:
            :param variables attribute name/value pairs (overrides)
            :param rules name/(configuration-dictionary) (overrides)
            :param expectation_suite_name: A name for returned Expectation suite.
            :param include_citation: Whether or not to include the Profiler config in the metadata for the ExpectationSuite produced by the Profiler
        :return: Set of rule evaluation results in the form of an ExpectationSuite
        """
        effective_variables: Optional[
            ParameterContainer
        ] = self.reconcile_profiler_variables(variables=variables)

        effective_rules: List[Rule] = self.reconcile_profiler_rules(rules=rules)

        if expectation_suite_name is None:
            expectation_suite_name = (
                f"tmp.profiler_{self.__class__.__name__}_suite_{str(uuid.uuid4())[:8]}"
            )

        expectation_suite: ExpectationSuite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name,
            data_context=self._data_context,
        )

        if include_citation:
            expectation_suite.add_citation(
                comment="Suite created by Rule-Based Profiler with the configuration included.",
                profiler_config=self._citation,
            )

        rule: Rule
        for rule in effective_rules:
            expectation_configurations: List[ExpectationConfiguration] = rule.generate(
                variables=effective_variables,
            )
            expectation_configuration: ExpectationConfiguration
            for expectation_configuration in expectation_configurations:
                expectation_suite._add_expectation(
                    expectation_configuration=expectation_configuration,
                    send_usage_event=False,
                )

        return expectation_suite

    def _reconcile_batch_requests_in_builders(
        self, batch_request: Union[dict, BatchRequest, RuntimeBatchRequest]
    ) -> None:
        """
        Profiler "batch_request" reconciliation involves combining existing Profiler state, instantiated from Profiler configuration
        (e.g., stored in a YAML file managed by the Profiler store), with the batch request overrides, provided at run time.

        The provided batch request is propagated to the following relevant Builders attributes (as applicable):
            - ParameterBuilders
            - ColumnDomainBuilder
              - We default to the latest value as a sensible default (using index: -1)

        The reconciliation logic for "batch_request" is of the "replace" nature: the provided data is consistently applied, regardless
        of existing Builder state.

        Args:
            batch_request: Data provided at runtime used to hydrate nested builder attributes
        """
        if not isinstance(batch_request, dict):
            batch_request = get_batch_request_as_dict(batch_request)

        for rule in self.rules:
            domain_builder = rule.domain_builder
            if isinstance(domain_builder, ColumnDomainBuilder):
                domain_builder.batch_request = copy.deepcopy(batch_request)
                domain_builder.batch_request.data_connector_query = {"index": -1}
                logger.info(
                    "Overwrote Rule %s's DomainBuilder batch_request attr", rule.name
                )

            parameter_builders = rule.parameter_builders
            if parameter_builders:
                for parameter_builder in parameter_builders:
                    parameter_builder.batch_request = copy.deepcopy(batch_request)
                    logger.info(
                        "Overwrote ParameterBuilder %s's batch_request attr",
                        parameter_builder.name,
                    )

    def reconcile_profiler_variables(
        self, variables: Optional[Dict[str, Any]] = None
    ) -> Optional[ParameterContainer]:
        """
        Profiler "variables" reconciliation involves combining the variables, instantiated from Profiler configuration
        (e.g., stored in a YAML file managed by the Profiler store), with the variables overrides, provided at run time.

        The reconciliation logic for "variables" is of the "replace" nature: An override value complements the original
        on key "miss", and replaces the original on key "hit" (or "collision"), because "variables" is a unique member.

        :param variables: variables overrides, supplied in dictionary (configuration) form
        :return: reconciled variables in their canonical ParameterContainer object form
        """
        effective_variables: ParameterContainer
        if variables is not None and isinstance(variables, dict):
            variables_configs: dict = self.variables.to_dict()["parameter_nodes"][
                "variables"
            ]["variables"]
            variables_configs.update(variables)
            effective_variables = build_parameter_container_for_variables(
                variables_configs=variables_configs
            )
        else:
            effective_variables = self.variables

        return effective_variables

    def reconcile_profiler_rules(
        self, rules: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> List[Rule]:
        """
        Profiler "rules" reconciliation involves combining the rules, instantiated from Profiler configuration (e.g.,
        stored in a YAML file managed by the Profiler store), with the rules overrides, provided at run time.

        The reconciliation logic for "rules" is of the "procedural" nature:
        (1) Combine every rule override configuration with any instantiated rule into a reconciled configuration
        (2) Re-instantiate Rule objects from the reconciled rule configurations

        :param rules: rules overrides, supplied in dictionary (configuration) form for each rule name as the key
        :return: reconciled rules in their canonical List[Rule] object form
        """
        if rules is None:
            rules = {}

        effective_rules: Dict[str, Rule] = self.get_rules_as_dict()

        rule_name: str
        rule_config: dict

        override_rule_configs: Dict[str, Dict[str, Any]] = {
            rule_name: RuleBasedProfiler._reconcile_rule_config(
                existing_rules=effective_rules,
                rule_name=rule_name,
                rule_config=rule_config,
            )
            for rule_name, rule_config in rules.items()
        }
        override_rules: Dict[str, Rule] = {
            rule_name: self._init_rule(rule_name=rule_name, rule_config=rule_config)
            for rule_name, rule_config in override_rule_configs.items()
        }
        effective_rules.update(override_rules)

        return list(effective_rules.values())

    @staticmethod
    def _reconcile_rule_config(
        existing_rules: Dict[str, Rule], rule_name: str, rule_config: dict
    ) -> Dict[str, Any]:
        """
        A "rule configuration" reconciliation is the process of combining the configuration of a single candidate
        override rule with at most one configuration corresponding to the list of rules instantiated from Profiler
        configuration (e.g., stored in a YAML file managed by the Profiler store).

        The reconciliation logic for "rule configuration" employes the "by construction" principle:
        (1) Find a common configuration between the domain builder configuration, possibly supplied as part of the
        candiate override rule configuration, and the comain builder configuration of an instantiated rule
        (2) Find common configurations between parameter builder configurations, possibly supplied as part of the
        candiate override rule configuration, and the parameter builder configurations of an instantiated rule
        (3) Find common configurations between expectation configuration builder configurations, possibly supplied as
        part of the candiate override rule configuration, and the expectation configuration builder configurations of an
        instantiated rule
        (4) Construct the reconciled rule configuration dictionary using the formal rule properties ("domain_builder",
        "parameter_builders", and "expectation_configuration_builders") as keys and their reconciled configuration
        dictionaries as values

        In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
        candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.

        :param existing_rules: all currently instantiated rules represented as a dictionary, keyed by rule name
        :param rule_name: name of the override rule candidate
        :param rule_config: configuration of an override rule candidate, supplied in dictionary (configuration) form
        :return: reconciled rule configuration, returned in dictionary (configuration) form
        """
        effective_rule_config: Dict[str, Any]
        if rule_name in existing_rules:
            rule: Rule = existing_rules[rule_name]

            domain_builder_config: dict = rule_config.get("domain_builder", {})
            effective_domain_builder_config: dict = (
                RuleBasedProfiler._reconcile_rule_domain_builder_config(
                    domain_builder=rule.domain_builder,
                    domain_builder_config=domain_builder_config,
                )
            )

            parameter_builder_configs: List[dict] = rule_config.get(
                "parameter_builders", []
            )
            effective_parameter_builder_configs: Optional[
                List[dict]
            ] = RuleBasedProfiler._reconcile_rule_parameter_builder_configs(
                rule=rule,
                parameter_builder_configs=parameter_builder_configs,
            )

            expectation_configuration_builder_configs: List[dict] = rule_config.get(
                "expectation_configuration_builders", []
            )
            effective_expectation_configuration_builder_configs: List[
                dict
            ] = RuleBasedProfiler._reconcile_rule_expectation_configuration_builder_configs(
                rule=rule,
                expectation_configuration_builder_configs=expectation_configuration_builder_configs,
            )

            effective_rule_config = {
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
    ) -> dict:
        """
        Rule "domain builder" reconciliation involves combining the domain builder, instantiated from Rule configuration
        (e.g., stored in a YAML file managed by the Profiler store), with the domain builder override, possibly supplied
        as part of the candiate override rule configuration.

        The reconciliation logic for "domain builder" is of the "replace" nature: An override value complements the
        original on key "miss", and replaces the original on key "hit" (or "collision"), because "domain builder" is a
        unique member for a rule.

        :param domain_builder: existing domain builder of a rule
        :param domain_builder_config: domain builder configuration override, supplied in dictionary (configuration) form
        :return: reconciled domain builder configuration, returned in dictionary (configuration) form
        """
        domain_builder_as_dict: dict = domain_builder.to_dict()
        domain_builder_as_dict["class_name"] = domain_builder.__class__.__name__
        domain_builder_as_dict["module_name"] = domain_builder.__class__.__module__

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        deserialized_config: dict = domainBuilderConfigSchema.load(
            domain_builder_as_dict
        )
        serialized_config: dict = domainBuilderConfigSchema.dump(deserialized_config)

        effective_domain_builder_config: dict = serialized_config
        if domain_builder_config:
            _validate_builder_override_config(builder_config=domain_builder_config)
            effective_domain_builder_config.update(domain_builder_config)

        return effective_domain_builder_config

    @staticmethod
    def _reconcile_rule_parameter_builder_configs(
        rule: Rule, parameter_builder_configs: List[dict]
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
            parameter_builder_as_dict = parameter_builder.to_dict()
            parameter_builder_as_dict[
                "class_name"
            ] = parameter_builder.__class__.__name__
            parameter_builder_as_dict[
                "module_name"
            ] = parameter_builder.__class__.__module__

            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            deserialized_config: dict = parameterBuilderConfigSchema.load(
                parameter_builder_as_dict
            )
            serialized_config: dict = parameterBuilderConfigSchema.dump(
                deserialized_config
            )

            effective_parameter_builder_configs[
                parameter_builder_name
            ] = serialized_config

        effective_parameter_builder_configs = nested_update(
            effective_parameter_builder_configs,
            {
                parameter_builder_config["name"]: parameter_builder_config
                for parameter_builder_config in parameter_builder_configs
            },
            dedup=True,
        )

        if not effective_parameter_builder_configs:
            return None

        return list(effective_parameter_builder_configs.values())

    @staticmethod
    def _reconcile_rule_expectation_configuration_builder_configs(
        rule: Rule, expectation_configuration_builder_configs: List[dict]
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
                expectation_configuration_builder.to_dict()
            )
            expectation_configuration_builder_as_dict[
                "class_name"
            ] = expectation_configuration_builder.__class__.__name__
            expectation_configuration_builder_as_dict[
                "module_name"
            ] = expectation_configuration_builder.__class__.__module__

            # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
            deserialized_config: dict = (
                expectationConfigurationBuilderConfigSchema.load(
                    expectation_configuration_builder_as_dict
                )
            )
            serialized_config: dict = expectationConfigurationBuilderConfigSchema.dump(
                deserialized_config
            )

            effective_expectation_configuration_builder_configs[
                expectation_configuration_builder_name
            ] = serialized_config

        effective_expectation_configuration_builder_configs = nested_update(
            effective_expectation_configuration_builder_configs,
            {
                expectation_configuration_builder_config[
                    "expectation_type"
                ]: expectation_configuration_builder_config
                for expectation_configuration_builder_config in expectation_configuration_builder_configs
            },
            dedup=True,
        )

        if not effective_expectation_configuration_builder_configs:
            return []

        return list(effective_expectation_configuration_builder_configs.values())

    def get_rules_as_dict(self) -> Dict[str, Rule]:
        rule: Rule
        return {rule.name: rule for rule in self._rules}

    def self_check(self, pretty_print=True) -> dict:
        """
        Necessary to enable integration with `DataContext.test_yaml_config`
        Args:
            pretty_print: flag to turn on verbose output
        Returns:
            Dictionary that contains RuleBasedProfiler state
        """
        # Provide visibility into parameters that RuleBasedProfiler was instantiated with.
        report_object: dict = {"config": self._citation}

        if pretty_print:
            print(f"\nRuleBasedProfiler class name: {self.name}")

            if not self._variables:
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
    def variables(self) -> Optional[ParameterContainer]:
        # Returning a copy of the "self._variables" state variable in order to prevent write-before-read hazard.
        return copy.deepcopy(self._variables)

    @variables.setter
    def variables(self, value: Optional[ParameterContainer]):
        self._variables = value

    @property
    def rules(self) -> List[Rule]:
        return self._rules

    @rules.setter
    def rules(self, value: List[Rule]):
        self._rules = value


class RuleBasedProfiler(BaseRuleBasedProfiler):
    """
    RuleBasedProfiler object serves to profile, or automatically evaluate a set of rules, upon a given
    batch / multiple batches of data.

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

    def __init__(
        self,
        name: str,
        config_version: float,
        variables: Optional[Dict[str, Any]] = None,
        rules: Optional[Dict[str, Dict[str, Any]]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Create a new Profiler using configured rules.
        For a rule or an item in a rule configuration, instantiates the following if
        available: a domain builder, a parameter builder, and a configuration builder.
        These will be used to define profiler computation patterns.

        Args:
            name: The name of the RBP instance
            config_version: The version of the RBP (currently only 1.0 is supported)
            rules: A set of dictionaries, each of which contains its own domain_builder, parameter_builders, and
            expectation_configuration_builders configuration components
            variables: Any variables to be substituted within the rules
            data_context: DataContext object that defines a full runtime environment (data access, etc.)
        """
        profiler_config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(
            name=name,
            config_version=config_version,
            variables=variables,
            rules=rules,
        )

        super().__init__(
            profiler_config=profiler_config,
            data_context=data_context,
        )

    @staticmethod
    def run_profiler(
        data_context: "DataContext",  # noqa: F821
        profiler_store: ProfilerStore,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
        variables: Optional[dict] = None,
        rules: Optional[dict] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> ExpectationSuite:

        profiler: RuleBasedProfiler = RuleBasedProfiler.get_profiler(
            data_context=data_context,
            profiler_store=profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )

        result: ExpectationSuite = profiler.run(
            variables=variables,
            rules=rules,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
        )

        return result

    @staticmethod
    def run_profiler_on_data(
        data_context: "DataContext",  # noqa: F821
        profiler_store: ProfilerStore,
        batch_request: Union[dict, BatchRequest, RuntimeBatchRequest],
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        profiler: RuleBasedProfiler = RuleBasedProfiler.get_profiler(
            data_context=data_context,
            profiler_store=profiler_store,
            name=name,
            ge_cloud_id=ge_cloud_id,
        )
        profiler._reconcile_batch_requests_in_builders(batch_request)

        result: ExpectationSuite = profiler.run()
        return result

    @staticmethod
    def add_profiler(
        config: RuleBasedProfilerConfig,
        data_context: "DataContext",  # noqa: F821
        profiler_store: ProfilerStore,
        ge_cloud_id: Optional[str] = None,
    ) -> "RuleBasedProfiler":
        if not RuleBasedProfiler._check_validity_of_batch_requests_in_config(
            config=config
        ):
            raise ge_exceptions.InvalidConfigError(
                f'batch_data found in batch_request cannot be saved to ProfilerStore "{profiler_store.store_name}"'
            )

        # Chetan - 20220204 - DataContext to be removed once it can be decoupled from RBP
        new_profiler: "RuleBasedProfiler" = instantiate_class_from_config(
            config=config.to_json_dict(),
            runtime_environment={
                "data_context": data_context,
            },
            config_defaults={
                "module_name": "great_expectations.rule_based_profiler",
                "class_name": "RuleBasedProfiler",
            },
        )

        key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if ge_cloud_id:
            key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
        else:
            key = ConfigurationIdentifier(
                configuration_key=config.name,
            )

        profiler_store.set(key=key, value=config)

        return new_profiler

    @staticmethod
    def _check_validity_of_batch_requests_in_config(
        config: RuleBasedProfilerConfig,
    ) -> bool:
        # Evaluate nested types in RuleConfig to parse out BatchRequests
        batch_requests: List[Union[BatchRequest, RuntimeBatchRequest, dict]] = []
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
        batch_request: Union[BatchRequest, RuntimeBatchRequest, dict]
        for batch_request in batch_requests:
            if batch_request_contains_batch_data(batch_request=batch_request):
                return False

        return True

    @staticmethod
    def get_profiler(
        data_context: "DataContext",  # noqa: F821
        profiler_store: ProfilerStore,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> "RuleBasedProfiler":
        assert bool(name) ^ bool(
            ge_cloud_id
        ), "Must provide either name or ge_cloud_id (but not both)"

        key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if ge_cloud_id:
            key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
        else:
            key = ConfigurationIdentifier(
                configuration_key=name,
            )
        try:
            profiler_config: RuleBasedProfilerConfig = profiler_store.get(key=key)
        except ge_exceptions.InvalidKeyError as exc_ik:
            id_ = (
                key.configuration_key
                if isinstance(key, ConfigurationIdentifier)
                else key
            )
            raise ge_exceptions.ProfilerNotFoundError(
                message=f'Non-existent Profiler configuration named "{id_}".\n\nDetails: {exc_ik}'
            )

        config = profiler_config.to_json_dict()
        if name:
            config.update({"name": name})
        config = filter_properties_dict(properties=config, clean_falsy=True)

        profiler = instantiate_class_from_config(
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

    @staticmethod
    def delete_profiler(
        profiler_store: ProfilerStore,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        assert bool(name) ^ bool(
            ge_cloud_id
        ), "Must provide either name or ge_cloud_id (but not both)"

        key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if ge_cloud_id:
            key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
        else:
            key = ConfigurationIdentifier(configuration_key=name)

        try:
            profiler_store.remove_key(key=key)
        except (ge_exceptions.InvalidKeyError, KeyError) as exc_ik:
            id_ = (
                key.configuration_key
                if isinstance(key, ConfigurationIdentifier)
                else key
            )
            raise ge_exceptions.ProfilerNotFoundError(
                message=f'Non-existent Profiler configuration named "{id_}".\n\nDetails: {exc_ik}'
            )

    @staticmethod
    def list_profilers(
        profiler_store: ProfilerStore,
        ge_cloud_mode: bool,
    ) -> List[str]:
        if ge_cloud_mode:
            return profiler_store.list_keys()
        return [x.configuration_key for x in profiler_store.list_keys()]
