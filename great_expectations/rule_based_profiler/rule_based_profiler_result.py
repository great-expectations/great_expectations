from dataclasses import dataclass
from typing import Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import Domain, ParameterNode
from great_expectations.types import SerializableDictDot


@dataclass(frozen=True)
class RuleBasedProfilerResult(SerializableDictDot):
    '\n    RuleBasedProfilerResult is an immutable "dataclass" object, designed to hold results of executing\n    "RuleBasedProfiler.run()" method.  Available properties are: "fully_qualified_parameter_names_by_domain",\n    "parameter_values_for_fully_qualified_parameter_names_by_domain", "expectation_configurations", and "citation"\n    (which represents configuration of effective Rule-Based Profiler, with all run-time overrides properly reconciled").\n'
    fully_qualified_parameter_names_by_domain: Dict[(Domain, List[str])]
    parameter_values_for_fully_qualified_parameter_names_by_domain: Optional[
        Dict[(Domain, Dict[(str, ParameterNode)])]
    ]
    expectation_configurations: List[ExpectationConfiguration]
    citation: dict

    def to_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns: This RuleBasedProfilerResult as dictionary (JSON-serializable for RuleBasedProfilerResult objects).\n        "
        domain: Domain
        fully_qualified_parameter_names: List[str]
        parameter_values_for_fully_qualified_parameter_names: Dict[(str, ParameterNode)]
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
                for (
                    domain,
                    fully_qualified_parameter_names,
                ) in self.fully_qualified_parameter_names_by_domain.items()
            ],
            "parameter_values_for_fully_qualified_parameter_names_by_domain": [
                {
                    "domain_id": domain.id,
                    "domain": domain.to_json_dict(),
                    "parameter_values_for_fully_qualified_parameter_names": convert_to_json_serializable(
                        data=parameter_values_for_fully_qualified_parameter_names
                    ),
                }
                for (
                    domain,
                    parameter_values_for_fully_qualified_parameter_names,
                ) in self.parameter_values_for_fully_qualified_parameter_names_by_domain.items()
            ],
            "expectation_configurations": [
                expectation_configuration.to_json_dict()
                for expectation_configuration in self.expectation_configurations
            ],
            "citation": self.citation,
        }

    def to_json_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns: This RuleBasedProfilerResult as JSON-serializable dictionary.\n        "
        return self.to_dict()
