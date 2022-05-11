from dataclasses import dataclass
from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.types import SerializableDictDot


@dataclass(frozen=True)
class RuleBasedProfilerResult(SerializableDictDot):
    """
    RuleBasedProfilerResult is an immutable "dataclass" object, designed to hold results of executing
    "RuleBasedProfiler.run()" method.  Available properties are: "fully_qualified_parameter_names_by_domain",
    "parameter_values_for_fully_qualified_parameter_names_by_domain", "expectation_configurations", and "citation"
    (which represents configuration of effective Rule-Based Profiler, with all run-time overrides properly reconciled").
    """

    fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]]
    parameter_values_for_fully_qualified_parameter_names_by_domain: Optional[
        Dict[Domain, Dict[str, ParameterNode]]
    ]
    expectation_configurations: List[ExpectationConfiguration]
    citation: dict

    def to_dict(self) -> dict:
        """
        Returns: This RuleBasedProfilerResult as dictionary (JSON-serializable for RuleBasedProfilerResult objects).
        """
        domain: Domain
        fully_qualified_parameter_names: List[str]
        parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        expectation_configuration: ExpectationConfiguration
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
                for domain, parameter_values_for_fully_qualified_parameter_names in self.parameter_values_for_fully_qualified_parameter_names_by_domain.items()
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
