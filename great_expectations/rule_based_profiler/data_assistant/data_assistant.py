from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.data_context import BaseDataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
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
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.util import measure_execution_time


class DataAssistant(ABC):
    """
    DataAssistant is an application built on top of the Rule-Based Profiler component.
    DataAssistant subclasses provide exploration and validation of particular aspects of specified data Batch objects.

    DataAssistant usage (e.g., in Jupyter notebook) adheres to the following pattern:

    data_assistant: DataAssistant = VolumeDataAssistant(
        name="my_volume_data_assistant",
        batch_request=batch_request,
        data_context=context,
    )
    result: DataAssistantResult = data_assistant.run()

    Then:
        metrics: Dict[Domain, Dict[str, ParameterNode]] = result.metrics
        expectation_suite: ExpectationSuite = result.expectation_suite
        expectation_configurations: List[ExpectationConfiguration] = result.expectation_suite.expectations
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
        DataAssistant subclasses guide "RuleBasedProfiler" to contain Rule configurations to embody profiling behaviors,
        corresponding to indended exploration and validation goals.  Then executing "RuleBasedProfiler.run()" yields
        "RuleBasedProfilerResult" object, containing metrics by "Domain", list of "ExpectationConfiguration" objects,
        and overall "ExpectationSuite" object, immediately available for validating underlying data "Batch" objects.

        Args:
            name: the name of this DataAssistant object.
            batch_request: specified for querying data Batch objects.
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
        self._build_profiler()

    def _build_profiler(self) -> None:
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
        For each Self-Initializing "Expectation" as specified by "DataAssistant.expectation_kwargs_by_expectation_type"
        interface property, retrieve its "RuleBasedProfiler" configuration, construct "Rule" object based on it, while
        incorporating metrics "ParameterBuilder" objects for "MetricDomainTypes", emitted by "DomainBuilder"
        of comprised "Rule", specified by "DataAssistant.metrics_parameter_builders_by_domain_type" interface property.
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
                        variables=rule.variables,
                        domain_builder=domain_builder,
                        parameter_builders=parameter_builders,
                        expectation_configuration_builders=expectation_configuration_builders,
                    )
                )

        self.profiler.variables = self.profiler.reconcile_profiler_variables(
            variables=variables,
            reconciliation_strategy=DEFAULT_RECONCILATION_DIRECTIVES.variables,
        )

    def run(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> DataAssistantResult:
        """
        Run the DataAssistant as it is currently configured.

        Args:
            expectation_suite: An existing "ExpectationSuite" to update
            expectation_suite_name: A name for returned "ExpectationSuite"
            include_citation: Whether or not to include the Profiler config in the metadata for "ExpectationSuite" produced by "RuleBasedProfiler"

        Returns:
            DataAssistantResult: The result object for the DataAssistant
        """
        data_assistant_result: DataAssistantResult = DataAssistantResult(
            execution_time=0.0
        )
        run_profiler_on_data(
            data_assistant=self,
            data_assistant_result=data_assistant_result,
            profiler=self.profiler,
            variables=self.variables,
            rules=self.rules,
            batch_list=list(self._validator.batches.values()),
            batch_request=None,
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
        )
        return self._build_data_assistant_result(
            data_assistant_result=data_assistant_result
        )

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

    @property
    @abstractmethod
    def variables(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            Optional "variables" configuration attribute name/value pairs (overrides), commonly-used in Builder objects.
        """
        pass

    @property
    @abstractmethod
    def rules(self) -> Optional[List[Rule]]:
        """
        Returns:
            Optional custom list of "Rule" objects (overrides) can be added by subclasses (return "None" if not needed).
        """
        pass

    @abstractmethod
    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        """
        DataAssistant subclasses implement this method to return subclasses of DataAssistantResult object, which imbue
        base DataAssistantResult class with methods, pertaining to specifics of particular DataAssistantResult subclass.

        Args:
            data_assistant_result: Base DataAssistantResult result object of DataAssistant (contains only data fields)

        Returns:
            DataAssistantResult: The appropriate subclass of base DataAssistantResult result object of the DataAssistant
        """
        pass

    def get_metrics_by_domain(self) -> Dict[Domain, Dict[str, ParameterNode]]:
        """
        Obtain subset of all parameter values for fully-qualified parameter names by domain, available from entire
        "RuleBasedProfiler" state, where "Domain" types are among keys included in provisions as proscribed by return
        value of "DataAssistant.metrics_parameter_builders_by_domain_type" interface property and actual fully-qualified
        parameter names match interface properties of "ParameterBuilder" objects, corresponding to these "domain" types.

        returns:
            dictionaries of values for fully-qualified parameter names by domain for metrics, computed by "rulebasedprofiler" state.
        """
        # noinspection PyTypeChecker
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
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
            fully_qualified_metrics_parameter_names_by_domain_type[domain_type] = [
                parameter_builder.fully_qualified_parameter_name
                for parameter_builder in parameter_builders
            ]

        domain: Domain
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
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

    def get_expectation_suite(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        include_citation: bool = True,
    ) -> ExpectationSuite:
        """
        Args:
            expectation_suite: An existing "ExpectationSuite" to update
            expectation_suite_name: A name for returned "ExpectationSuite"
            include_citation: Whether or not to include the Profiler config in the metadata for "ExpectationSuite" produced by "RuleBasedProfiler"

        Returns:
            "ExpectationSuite" using "ExpectationConfiguration" objects, computed by "RuleBasedProfiler" state
        """
        return self.profiler.get_expectation_suite(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            include_citation=include_citation,
        )


@measure_execution_time(
    execution_time_holder_object_reference_name="data_assistant_result",
    execution_time_property_name="execution_time",
    pretty_print=False,
)
def run_profiler_on_data(
    data_assistant: DataAssistant,
    data_assistant_result: DataAssistantResult,
    profiler: BaseRuleBasedProfiler,
    variables: Optional[Dict[str, Any]] = None,
    rules: Optional[Dict[str, Dict[str, Any]]] = None,
    batch_list: Optional[List[Batch]] = None,
    batch_request: Optional[Union[BatchRequestBase, dict]] = None,
    expectation_suite: Optional[ExpectationSuite] = None,
    expectation_suite_name: Optional[str] = None,
    include_citation: bool = True,
) -> None:
    if rules is None:
        rules = []

    rule: Rule
    rules_configs: Optional[Dict[str, Dict[str, Any]]] = {
        rule.name: rule.to_json_dict() for rule in rules
    }
    profiler.run(
        variables=variables,
        rules=rules_configs,
        batch_list=batch_list,
        batch_request=batch_request,
        force_batch_data=False,
        recompute_existing_parameter_values=False,
        reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
    )
    result: DataAssistantResult = data_assistant_result
    result.profiler_config = profiler.config
    result.metrics_by_domain = data_assistant.get_metrics_by_domain()
    result.expectation_suite = data_assistant.get_expectation_suite(
        expectation_suite=expectation_suite,
        expectation_suite_name=expectation_suite_name,
        include_citation=include_citation,
    )
