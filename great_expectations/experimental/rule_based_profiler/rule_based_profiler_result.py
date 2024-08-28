from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import (
    ExpectationSuite,  # noqa: TCH001
)
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,  # noqa: TCH001
)
from great_expectations.experimental.rule_based_profiler.helpers.util import (
    get_or_create_expectation_suite,
)
from great_expectations.experimental.rule_based_profiler.parameter_container import (
    ParameterNode,  # noqa: TCH001
)
from great_expectations.types import SerializableDictDot
from great_expectations.util import convert_to_json_serializable  # noqa: TID251

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues


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
        catch_exceptions (boolean): \
            Defaults to False.
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).

    """  # noqa: E501

    fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]]
    parameter_values_for_fully_qualified_parameter_names_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ]
    expectation_configurations: List[ExpectationConfiguration]
    citation: dict
    rule_domain_builder_execution_time: Dict[str, float]
    rule_execution_time: Dict[str, float]
    rule_exception_tracebacks: Dict[str, Optional[str]]
    catch_exceptions: bool = field(default=False)

    @override
    def to_dict(self) -> dict:
        """
        Returns:
            This `RuleBasedProfilerResult` as dictionary (JSON-serializable for `RuleBasedProfilerResult` objects).
        """  # noqa: E501
        domain: Domain
        fully_qualified_parameter_names: List[str]
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        expectation_configuration: ExpectationConfiguration
        parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
            Domain, Dict[str, ParameterNode]
        ] = self.parameter_values_for_fully_qualified_parameter_names_by_domain or {}
        return {
            "fully_qualified_parameter_names_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "fully_qualified_parameter_names": convert_to_json_serializable(
                        data=fully_qualified_parameter_names
                    ),
                }
                for domain, fully_qualified_parameter_names in self.fully_qualified_parameter_names_by_domain.items()  # noqa: E501
            ],
            "parameter_values_for_fully_qualified_parameter_names_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "parameter_values_for_fully_qualified_parameter_names": convert_to_json_serializable(  # noqa: E501
                        data=parameter_values_for_fully_qualified_parameter_names
                    ),
                }
                for domain, parameter_values_for_fully_qualified_parameter_names in parameter_values_for_fully_qualified_parameter_names_by_domain.items()  # noqa: E501
            ],
            "expectation_configurations": [
                expectation_configuration.to_json_dict()
                for expectation_configuration in self.expectation_configurations
            ],
            "citation": self.citation,
        }

    @override
    def to_json_dict(self) -> dict[str, JSONValues]:
        """
        Returns the `RuleBasedProfilerResult` as a JSON-serializable dictionary.

        Returns:
            Dictionary containing only JSON compatible python primitives.
        """
        return self.to_dict()

    def get_expectation_suite(self, name: str) -> ExpectationSuite:
        """
        Retrieve the `ExpectationSuite` generated during the `RuleBasedProfiler` run.

        Args:
            name: The name of the desired `ExpectationSuite`.

        Returns:
            `ExpectationSuite`
        """
        expectation_suite: ExpectationSuite = get_or_create_expectation_suite(
            data_context=None,
            expectation_suite=None,
            expectation_suite_name=name,
            component_name=self.__class__.__name__,
            persist=False,
        )
        expectation_suite.add_expectation_configurations(
            expectation_configurations=self.expectation_configurations,
            match_type="domain",
            overwrite_existing=True,
        )
        return expectation_suite
