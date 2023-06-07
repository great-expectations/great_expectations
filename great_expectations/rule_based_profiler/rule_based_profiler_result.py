from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

from great_expectations.core import (
    ExpectationConfiguration,  # noqa: TCH001
    ExpectationSuite,  # noqa: TCH001
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.domain import Domain  # noqa: TCH001
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
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterNode,  # noqa: TCH001
)
from great_expectations.types import SerializableDictDot

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues


@public_api
@dataclass(frozen=True)
class RuleBasedProfilerResult(SerializableDictDot):
    """
    ``RuleBasedProfilerResult`` is an immutable ``dataclass`` object which holds the results of executing the ``RuleBasedProfiler.run()`` method.

    Properties represents the configuration of the Rule-Based Profiler (effective configuration if run via a DataAssistant or auto-initializing expectation), with all run-time overrides properly reconciled.

    Args:
        fully_qualified_parameter_names_by_domain:
            `dict` of `Domain` keys and a list of their parameter names.
        parameter_values_for_fully_qualified_parameter_names_by_domain:
            `dict` of `Domain` and nested `ParameterNode` mappings.
        expectation_configurations:
            List of `ExpectationConfiguration` objects.
        citation:
            `dict` of citations.

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
        Returns:
            This `RuleBasedProfilerResult` as dictionary (JSON-serializable for `RuleBasedProfilerResult` objects).
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

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """
        Returns the `RuleBasedProfilerResult` as a JSON-serializable dictionary.

        Returns:
            Dictionary containing only JSON compatible python primitives.
        """
        return self.to_dict()

    @public_api
    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.RULE_BASED_PROFILER_RESULT_GET_EXPECTATION_SUITE,
        args_payload_fn=get_expectation_suite_usage_statistics,
    )
    def get_expectation_suite(self, expectation_suite_name: str) -> ExpectationSuite:
        """
        Retrieve the `ExpectationSuite` generated during the `RuleBasedProfiler` run.

        Args:
            expectation_suite_name: The name of the desired `ExpectationSuite`.

        Returns:
            `ExpectationSuite`
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
