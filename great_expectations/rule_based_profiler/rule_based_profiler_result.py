from dataclasses import dataclass, field
from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.domain import Domain
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
    get_expectation_suite_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.helpers.util import (
    get_or_create_expectation_suite,
)
from great_expectations.rule_based_profiler.parameter_container import ParameterNode
from great_expectations.types import SerializableDictDot


@dataclass(frozen=True)
class RuleBasedProfilerResult(SerializableDictDot):
    """
    "RuleBasedProfilerResult" is an immutable "dataclass" object, designed to hold results with auxiliary information of
    executing "RuleBasedProfiler.run()" method.  Principal properties are: "fully_qualified_parameter_names_by_domain",
    "parameter_values_for_fully_qualified_parameter_names_by_domain", "expectation_configurations", and "citation"
    (which represents configuration of effective Rule-Based Profiler, with all run-time overrides properly reconciled).
    """

    fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]]
    parameter_values_for_fully_qualified_parameter_names_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ]
    expectation_configurations: List[ExpectationConfiguration]
    citation: dict
    rule_domain_builder_execution_time: Dict[str, float]
    rule_execution_time: Dict[str, float]
    # Reference to  "UsageStatisticsHandler" object for this "RuleBasedProfilerResult" object (if configured).
    _usage_statistics_handler: Optional[UsageStatisticsHandler] = field(default=None)

    def to_dict(self) -> dict:
        """
        Returns: This RuleBasedProfilerResult as dictionary (JSON-serializable for RuleBasedProfilerResult objects).
        """
        domain: Domain
        fully_qualified_parameter_names: List[str]
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        expectation_configuration: ExpectationConfiguration
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = (self.parameter_values_for_fully_qualified_parameter_names_by_domain or {})
        return {
            "fully_qualified_parameter_names_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "fully_qualified_parameter_names": convert_to_json_serializable(
                        data=fully_qualified_parameter_names
                    ),
                }
                for domain, fully_qualified_parameter_names in self.fully_qualified_parameter_names_by_domain.items()
            ],
            "parameter_values_for_fully_qualified_parameter_names_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "parameter_values_for_fully_qualified_parameter_names": convert_to_json_serializable(
                        data=parameter_values_for_fully_qualified_parameter_names
                    ),
                }
                for domain, parameter_values_for_fully_qualified_parameter_names in parameter_values_for_fully_qualified_parameter_names_by_domain.items()
            ],
            "expectation_configurations": [
                expectation_configuration.to_json_dict()
                for expectation_configuration in self.expectation_configurations
            ],
            "citation": self.citation,
        }

    def to_json_dict(self) -> dict:
        """
        Returns: This RuleBasedProfilerResult as JSON-serializable dictionary.
        """
        return self.to_dict()

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.RULE_BASED_PROFILER_RESULT_GET_EXPECTATION_SUITE,
        args_payload_fn=get_expectation_suite_usage_statistics,
    )
    def get_expectation_suite(self, expectation_suite_name: str) -> ExpectationSuite:
        """
        Returns: "ExpectationSuite" object, built from properties, populated into this "RuleBasedProfilerResult" object.
        """
        expectation_suite: ExpectationSuite = get_or_create_expectation_suite(
            data_context=None,
            expectation_suite=None,
            expectation_suite_name=expectation_suite_name,
            component_name=self.__class__.__name__,
            persist=False,
        )
        expectation_suite.add_expectation_configurations(
            expectation_configurations=self.expectation_configurations,
            send_usage_event=False,
            match_type="domain",
            overwrite_existing=True,
        )
        expectation_suite.add_citation(
            **self.citation,
        )
        return expectation_suite
