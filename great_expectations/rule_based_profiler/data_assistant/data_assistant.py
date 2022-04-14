from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import altair as alt

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.batch import BatchRequestBase
from great_expectations.data_context import BaseDataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
    RuleBasedProfiler,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    DataAssistantResult,
    Domain,
)


class DataAssistant(ABC):
    """
    DataAssistant is an application built on top of the Rule-Based Profiler component.
    DataAssistant subclasses provide exploration and validation of particular aspects of specified data Batch objects.

    DataAssustant usage (e.g., in Jupyter notebook) adheres to the following pattern:

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="my_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    data_assistant.build()
    result: DataAssistantResult = data_assistant.run()

    Then:
        metrics: Dict[Domain, Dict[str, Any]] = result.metrics
        expectation_configurations: List[ExpectationConfiguration] = result.expectation_configurations
        expectation_suite: ExpectationSuite = result.expectation_suite
        expectation_suite_meta: Dict[str, Any] = expectation_suite.meta
        profiler_config: RuleBasedProfilerConfig = result.profiler_config
    """

    def __init__(
        self,
        name: str,
        batch_request: Union[BatchRequestBase, dict],
        data_context: BaseDataContext = None,
    ):
        """
        DataAssistant subclasses build "RuleBasedProfiler" to contain Rule configurations to embody profiling behaviors,
        corresponding to indended exploration and validation goals.  Then executing "RuleBasedProfiler.run()" yields
        "RuleBasedProfilerResult" object, containing metrics by "Domain", list of "ExpectationConfiguration" objects,
        and overall "ExpectationSuite" object, immediately available for validating underlying data "Batch" objects.

        Args:
            name: the name of this DataAssistant object.
            batch_request: specified specified for querying data Batch objects.
            data_context: DataContext
        """
        self._name = name

        self._data_context = data_context

        self._validator = get_validator_using_batch_list_or_batch_request(
            purpose=self.name,
            data_context=self.data_context,
            batch_list=None,
            batch_request=batch_request,
            domain=None,
            variables=None,
            parameters=None,
        )

        self._profiler = RuleBasedProfiler(
            name=self.name,
            config_version=1.0,
            variables=None,
            data_context=self.data_context,
        )

    def build(self) -> None:
        """
        Builds "RuleBasedProfiler", corresponding to present DataAssistant use case.

        Starts with empty "RuleBasedProfiler" (initialized in constructor) and adds Rule objects.

        Subclasses can add custom "Rule" objects as appropriate for their respective particular DataAssistant use cases.
        """
        variables: dict = {}

        profiler: Optional[BaseRuleBasedProfiler]
        rules: List[Rule]
        rule: Rule
        domain_builder: DomainBuilder
        parameter_builders: List[ParameterBuilder]
        expectation_configuration_builders: List[ExpectationConfigurationBuilder]

        """
        For each Self-Initializing Expectation as specified by "DataAssistant.expectation_kwargs_by_expectation_type()"
        interface method, retrieve its "RuleBasedProfiler" configuration, construct "Rule" object based on configuration
        therein and incorporating metrics "ParameterBuilder" objects for "MetricDomainTypes", emitted by "DomainBuilder"
        of comprised "Rule", specified by "DataAssistant.metrics_parameter_builders_by_domain_type()" interface method.
        Append this "Rule" object to overall DataAssistant "RuleBasedProfiler" object; incorporate "variables" as well.
        """
        expectation_kwargs: Dict[str, Any]
        for (
            expectation_type,
            expectation_kwargs,
        ) in self.expectation_kwargs_by_expectation_type.items():
            profiler = self._validator.build_rule_based_profiler_for_expectation(
                expectation_type=expectation_type
            )(**expectation_kwargs)
            # TODO: <Alex>Sharing same "variables" by all RuleBasedProfiler Rule objects is problematic.</Alex>
            variables.update(convert_variables_to_dict(variables=profiler.variables))
            rules = profiler.rules
            for rule in rules:
                domain_builder = rule.domain_builder
                parameter_builders = rule.parameter_builders or []
                parameter_builders.extend(
                    self.metrics_parameter_builders_by_domain_type[
                        domain_builder.domain_type
                    ]
                )
                expectation_configuration_builders = (
                    rule.expectation_configuration_builders or []
                )
                self.profiler.add_rule(
                    rule=Rule(
                        name=rule.name,
                        domain_builder=domain_builder,
                        parameter_builders=parameter_builders,
                        expectation_configuration_builders=expectation_configuration_builders,
                    )
                )

        self.profiler.variables = self.profiler.reconcile_profiler_variables(
            variables=variables,
            reconciliation_strategy=BaseRuleBasedProfiler.DEFAULT_RECONCILATION_DIRECTIVES.variables,
        )

    def run(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> DataAssistantResult:
        self.profiler.run(
            variables=None,
            rules=None,
            batch_list=list(self._validator.batches.values()),
            batch_request=None,
            force_batch_data=False,
            reconciliation_directives=BaseRuleBasedProfiler.DEFAULT_RECONCILATION_DIRECTIVES,
        )
        return DataAssistantResult(
            profiler_config=self.profiler.config,
            metrics=self.get_metrics(),
            expectation_configurations=self.get_expectation_configurations(),
            expectation_suite=self.get_expectation_suite(
                expectation_suite=expectation_suite,
                expectation_suite_name=expectation_suite_name,
                include_citation=include_citation,
            ),
        )

    def plot(self, charts: List[alt.Chart]):
        for c in charts:
            c.display()

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_context(self) -> BaseDataContext:
        return self._data_context

    @property
    def profiler(self) -> BaseRuleBasedProfiler:
        return self._profiler

    @property
    @abstractmethod
    def expectation_kwargs_by_expectation_type(self) -> Dict[str, Dict[str, Any]]:
        """
        DataAssistant subclasses implement this method to return relevant Self-Initializing Expectations with "kwargs".

        Returns:
            Dictionary of Expectation "kwargs", keyed by "expectation_type".
        """
        pass

    @property
    @abstractmethod
    def metrics_parameter_builders_by_domain_type(
        self,
    ) -> Dict[MetricDomainTypes, List[ParameterBuilder]]:
        """
        DataAssistant subclasses implement this method to return "ParameterBuilder" objects for "MetricDomainTypes", for
        every "Domain" type, for which generating metrics of interest is desired.  These metrics will be computed in
        addition to metrics already computed as part of "Rule" evaluation for every "Domain", emitted by "DomainBuilder"
        of comprised "Rule"; these auxiliary metrics are aimed entirely for data exploration / visualization purposes.

        Returns:
            Dictionary of "ParameterBuilder" objects, keyed by members of "MetricDomainTypes" Enum.
        """
        pass

    def get_metrics(self) -> Dict[Domain, Dict[str, Any]]:
        """
        Obtain subset of all parameter values for fully-qualified parameter names by Domain, available from entire
        "RuleBasedProfiler" state, where "Domain" types are among keys included in provisions as proscribed by return
        value of "DataAssistant.metrics_parameter_builders_by_domain_type()" interface method and actual fully-qualified
        parameter names match interface properties of "ParameterBuilder" objects, corresponding to these "Domain" types.

        Returns:
            Dictionaries of values for fully-qualified parameter names by Domain for metrics, computed by "RuleBasedProfiler" state.
        """
        # noinspection PyTypeChecker
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, Any]
        ] = dict(
            filter(
                lambda element: element[0].domain_type
                in list(self.metrics_parameter_builders_by_domain_type.keys()),
                self.profiler.get_parameter_values_for_fully_qualified_parameter_names_by_domain().items(),
            )
        )

        fully_qualified_metrics_parameter_names_by_domain_type: Dict[
            MetricDomainTypes : List[str]
        ] = {}

        domain_type: MetricDomainTypes
        parameter_builders: List[ParameterBuilder]
        parameter_builder: ParameterBuilder
        for (
            domain_type,
            parameter_builders,
        ) in self.metrics_parameter_builders_by_domain_type.items():
            fully_qualified_metrics_parameter_names_by_domain_type[domain_type] = []
            for parameter_builder in parameter_builders:
                fully_qualified_metrics_parameter_names_by_domain_type[
                    domain_type
                ].append(
                    f"{parameter_builder.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}"
                )
                fully_qualified_metrics_parameter_names_by_domain_type[
                    domain_type
                ].append(
                    f"{parameter_builder.fully_qualified_parameter_name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}"
                )

        doain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, Any]
        # noinspection PyTypeChecker
        parameter_values_for_fully_qualified_parameter_names_by_domain = {
            domain: dict(
                filter(
                    lambda element: element[0]
                    in fully_qualified_metrics_parameter_names_by_domain_type[
                        domain.domain_type
                    ],
                    parameter_values_for_fully_qualified_parameter_names.items(),
                )
            )
            for domain, parameter_values_for_fully_qualified_parameter_names in parameter_values_for_fully_qualified_parameter_names_by_domain.items()
        }

        return parameter_values_for_fully_qualified_parameter_names_by_domain

    def get_expectation_suite_meta(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> Dict[str, Any]:
        """
        Args:
            expectation_suite: An existing "ExpectationSuite" to update.
            expectation_suite_name: A name for returned "ExpectationSuite".
            include_citation: Whether or not to include the Profiler config in the metadata for "ExpectationSuite" produced by "RuleBasedProfiler"

        Returns:
            Dictionary corresponding to meta property of "ExpectationSuite" using "ExpectationConfiguration" objects, computed by "RuleBasedProfiler" state.
        """
        return self.profiler.get_expectation_suite_meta(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
        )

    def get_expectation_suite(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> ExpectationSuite:
        """
        Args:
            expectation_suite: An existing "ExpectationSuite" to update.
            expectation_suite_name: A name for returned "ExpectationSuite".
            include_citation: Whether or not to include the Profiler config in the metadata for "ExpectationSuite" produced by "RuleBasedProfiler"

        Returns:
            "ExpectationSuite" using "ExpectationConfiguration" objects, computed by "RuleBasedProfiler" state.
        """
        return self.profiler.get_expectation_suite(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
        )

    def get_expectation_configurations(self) -> List[ExpectationConfiguration]:
        """
        Returns:
            List of "ExpectationConfiguration" objects, computed by "RuleBasedProfiler" state.
        """
        return self.profiler.get_expectation_configurations()
