import copy
import uuid
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
)
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
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


class RuleBasedProfiler:
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
        self._name = name
        self._config_version = config_version

        if variables is None:
            variables = {}

        # Convert variables argument to ParameterContainer
        _variables: ParameterContainer = build_parameter_container_for_variables(
            variables_configs=variables
        )
        self._variables = _variables

        self._data_context = data_context

        # Necessary to annotate ExpectationSuite during `run()`
        self._citation = {
            "name": name,
            "config_version": config_version,
            "variables": variables,
            "rules": rules,
        }

        self._rules = self._init_rules(rules=rules)

    def _init_rules(
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
                self._init_one_rule(rule_name=rule_name, rule_config=rule_config)
            )

        return rule_object_list

    def _init_one_rule(
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
        domain_builder: DomainBuilder = RuleBasedProfiler._init_domain_builder(
            domain_builder_config=rule_config["domain_builder"],
            data_context=self._data_context,
        )
        parameter_builders: Optional[
            List[ParameterBuilder]
        ] = RuleBasedProfiler._init_parameter_builders(
            parameter_builder_configs=rule_config.get("parameter_builders"),
            data_context=self._data_context,
        )
        expectation_configuration_builders: List[
            ExpectationConfigurationBuilder
        ] = RuleBasedProfiler._init_expectation_configuration_builders(
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
    def _init_domain_builder(
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
    def _init_parameter_builders(
        parameter_builder_configs: Optional[List[dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ) -> Optional[List[ParameterBuilder]]:
        if parameter_builder_configs is None:
            return None

        parameter_builders: List[ParameterBuilder] = []

        parameter_builder_config: dict
        for parameter_builder_config in parameter_builder_configs:
            parameter_builder: ParameterBuilder = (
                RuleBasedProfiler._init_one_parameter_builder(
                    parameter_builder_config=parameter_builder_config,
                    data_context=data_context,
                )
            )
            parameter_builders.append(parameter_builder)

        return parameter_builders

    @staticmethod
    def _init_one_parameter_builder(
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
    def _init_expectation_configuration_builders(
        expectation_configuration_builder_configs: List[dict],
    ) -> List[ExpectationConfigurationBuilder]:
        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = []

        expectation_configuration_builder_config: dict
        for (
            expectation_configuration_builder_config
        ) in expectation_configuration_builder_configs:
            expectation_configuration_builder: ExpectationConfigurationBuilder = RuleBasedProfiler._init_one_expectation_configuration_builder(
                expectation_configuration_builder_config=expectation_configuration_builder_config,
            )
            expectation_configuration_builders.append(expectation_configuration_builder)

        return expectation_configuration_builders

    @staticmethod
    def _init_one_expectation_configuration_builder(
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
        effective_variables: Optional[ParameterContainer] = self._reconcile_variables(
            variables=variables
        )

        # TODO: <Alex>ALEX -- Tests for Reconciliation are next immediate action items.</Alex>
        # TODO: <Alex>ALEX -- Replace "getattr/setattr" with "__dict__" (in a "to_dict()" method on Rule and below).</Alex>
        effective_rules: List[Rule] = self.reconcile_rules_for_profiler(rules=rules)

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

    def _reconcile_variables(
        self, variables: Optional[Dict[str, Any]] = None
    ) -> Optional[ParameterContainer]:
        effective_variables: ParameterContainer
        if variables is not None and isinstance(variables, dict):
            variables_dict: dict = self.variables.to_dict()
            variables_dict.update(variables)
            effective_variables = build_parameter_container_for_variables(
                variables_configs=variables
            )
        else:
            effective_variables = self.variables

        return effective_variables

    def reconcile_rules_for_profiler(
        self, rules: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> List[Rule]:
        if rules is None:
            rules = {}

        effective_rules: Dict[str, Rule] = self._get_rules_as_dict()

        rule_name: str
        rule_config: dict

        override_rule_configs: Dict[str, Dict[str, Any]] = {
            rule_name: RuleBasedProfiler._reconcile_rule(
                existing_rules=effective_rules,
                rule_name=rule_name,
                rule_config=rule_config,
            )
            for rule_name, rule_config in rules.items()
        }
        override_rules: Dict[str, Rule] = {
            rule_name: self._init_one_rule(rule_name=rule_name, rule_config=rule_config)
            for rule_name, rule_config in override_rule_configs.items()
        }
        effective_rules.update(override_rules)

        return list(effective_rules.values())

    @staticmethod
    def _reconcile_rule(
        existing_rules: Dict[str, Rule], rule_name: str, rule_config: dict
    ) -> Dict[str, Any]:
        effective_rule_config: Dict[str, Any]
        if rule_name in existing_rules:
            rule: Rule = existing_rules[rule_name]
            domain_builder_config: dict = rule_config.get("domain_builder", {})
            effective_domain_builder_config: dict = (
                RuleBasedProfiler._reconcile_domain_builder_config(
                    domain_builder=rule.domain_builder,
                    domain_builder_config=domain_builder_config,
                )
            )
            effective_parameter_builder_configs: Optional[
                List[dict]
            ] = RuleBasedProfiler._reconcile_parameter_builder_configs_for_rule(
                rule=rule,
                rule_config=rule_config,
            )

            effective_expectation_configuration_builder_configs: List[
                dict
            ] = RuleBasedProfiler._reconcile_expectation_configuration_builder_configs_for_rule(
                rule=rule,
                rule_config=rule_config,
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
    def _reconcile_domain_builder_config(
        domain_builder: DomainBuilder,
        domain_builder_config: dict,
    ) -> dict:
        effective_domain_builder_config: dict = {}
        batch_request: Optional[
            Union[BatchRequest, RuntimeBatchRequest, dict]
        ] = domain_builder_config.pop("batch_request", None)
        if batch_request is None:
            batch_request = get_batch_request_as_dict(
                batch_request=domain_builder.batch_request
            )

        effective_domain_builder_config["batch_request"] = batch_request

        key: str
        value: Any
        current_value: Any
        for key, value in domain_builder_config.items():
            if hasattr(domain_builder, f"{key}"):
                current_value = getattr(domain_builder, key)
                effective_domain_builder_config[key] = value or current_value
            else:
                effective_domain_builder_config[key] = value

        return effective_domain_builder_config

    @staticmethod
    def _reconcile_parameter_builder_configs_for_rule(
        rule: Rule, rule_config: dict
    ) -> Optional[List[dict]]:
        effective_parameter_builder_configs: List[dict] = []
        parameter_builder_configs: Optional[List[dict]] = rule_config.get(
            "parameter_builders", []
        )

        current_parameter_builders: Optional[
            Dict[str, ParameterBuilder]
        ] = rule.parameter_builders
        parameter_builder_config: dict
        for parameter_builder_config in parameter_builder_configs:
            parameter_builder_name: str = parameter_builder_config["name"]
            if parameter_builder_name in current_parameter_builders:
                parameter_builder: ParameterBuilder = current_parameter_builders[
                    parameter_builder_name
                ]
                effective_parameter_builder_configs.append(
                    RuleBasedProfiler._reconcile_parameter_builder_config(
                        parameter_builder=parameter_builder,
                        parameter_builder_config=parameter_builder_config,
                    )
                )
            else:
                effective_parameter_builder_configs.append(parameter_builder_config)

        return effective_parameter_builder_configs

    @staticmethod
    def _reconcile_parameter_builder_config(
        parameter_builder: ParameterBuilder,
        parameter_builder_config: dict,
    ) -> dict:
        effective_parameter_builder_config: dict = {}
        batch_request: Optional[
            Union[BatchRequest, RuntimeBatchRequest, dict]
        ] = parameter_builder_config.pop("batch_request", None)
        if batch_request is None:
            batch_request = get_batch_request_as_dict(
                batch_request=parameter_builder.batch_request
            )

        effective_parameter_builder_config["batch_request"] = batch_request

        key: str
        value: Any
        current_value: Any
        for key, value in parameter_builder_config.items():
            if hasattr(parameter_builder, f"{key}"):
                current_value = getattr(parameter_builder, key)
                effective_parameter_builder_config[key] = value or current_value
            else:
                effective_parameter_builder_config[key] = value

        return effective_parameter_builder_config

    @staticmethod
    def _reconcile_expectation_configuration_builder_configs_for_rule(
        rule: Rule, rule_config: dict
    ) -> List[dict]:
        effective_expectation_configuration_builder_configs: List[dict] = []
        expectation_configuration_builder_configs: List[dict] = rule_config.get(
            "expectation_configuration_builders", []
        )

        current_expectation_configuration_builders: Dict[
            str, ExpectationConfigurationBuilder
        ] = rule.expectation_configuration_builders
        expectation_configuration_builder_config: dict
        for (
            expectation_configuration_builder_config
        ) in expectation_configuration_builder_configs:
            expectation_configuration_builder_name: str = (
                expectation_configuration_builder_config["expectation_type"]
            )
            if (
                expectation_configuration_builder_name
                in current_expectation_configuration_builders
            ):
                expectation_configuration_builder: ExpectationConfigurationBuilder = (
                    current_expectation_configuration_builders[
                        expectation_configuration_builder_name
                    ]
                )
                effective_expectation_configuration_builder_configs.append(
                    RuleBasedProfiler._reconcile_expectation_configuration_builder(
                        expectation_configuration_builder=expectation_configuration_builder,
                        expectation_configuration_builder_config=expectation_configuration_builder_config,
                    )
                )
            else:
                effective_expectation_configuration_builder_configs.append(
                    expectation_configuration_builder_config
                )

        return effective_expectation_configuration_builder_configs

    @staticmethod
    def _reconcile_expectation_configuration_builder(
        expectation_configuration_builder: ExpectationConfigurationBuilder,
        expectation_configuration_builder_config: dict,
    ) -> dict:
        effective_expectation_configuration_builder_config: dict = {}

        key: str
        value: Any
        current_value: Any
        for key, value in expectation_configuration_builder_config.items():
            if hasattr(expectation_configuration_builder, f"{key}"):
                current_value = getattr(expectation_configuration_builder, key)
                effective_expectation_configuration_builder_config[key] = (
                    value or current_value
                )
            else:
                effective_expectation_configuration_builder_config[key] = value

        return effective_expectation_configuration_builder_config

    def _get_rules_as_dict(self) -> Dict[str, Rule]:
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
        return list(self._get_rules_as_dict().values())

    @rules.setter
    def rules(self, value: List[Rule]):
        self._rules = value
