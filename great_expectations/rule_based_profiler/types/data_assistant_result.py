import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_DELIMITER_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    PARAMETER_NAME_ROOT_FOR_PARAMETERS,
    Domain,
    ParameterNode,
)
from great_expectations.types import SerializableDictDot


@dataclass
class DataAssistantResult(SerializableDictDot):
    """
    DataAssistantResult is an immutable "dataclass" object, designed to hold results of executing "data_assistant.run()"
    method.  Available properties ("metrics", "expectation_configurations", "expectation_suite", and configuration
    object (of type "RuleBasedProfilerConfig") of effective Rule-Based Profiler, which embodies given "DataAssistant".
    """

    data_assistant_cls: type
    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics: Optional[Dict[Domain, Dict[str, Any]]] = None
    # Obtain "expectation_configurations" using "expectation_configurations = expectation_suite.expectations".
    # Obtain "meta/details" using "meta = expectation_suite.meta".
    expectation_suite: Optional[ExpectationSuite] = None
    execution_time: Optional[float] = None  # Execution time (in seconds).

    def plot(self, prescriptive: bool = False):
        metrics: Optional[Dict[Domain, Dict[str, Any]]] = self.metrics
        metric_names: List[str] = []
        domain: Domain
        values_for_fully_qualified_parameter_names: Dict[str, ParameterNode]
        fully_qualified_parameter_name: str
        parameter_value: Any

        parameter_node_name_regex_list: list = [
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_DELIMITER_CHARACTER),
            PARAMETER_NAME_ROOT_FOR_PARAMETERS,
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER),
            "\\w+",
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER),
        ]

        for domain, values_for_fully_qualified_parameter_names in metrics.items():
            for (
                fully_qualified_parameter_name,
                parameter_value,
            ) in values_for_fully_qualified_parameter_names.items():
                details_key_regex_str: str = "".join(
                    parameter_node_name_regex_list
                    + [FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]
                )
                if (
                    re.match(details_key_regex_str, fully_qualified_parameter_name)
                    is not None
                ):
                    details_parameter_node: Any = parameter_value
                    metric_names.append(
                        details_parameter_node.metric_configuration.metric_name
                    )

                attributed_value_key_regex_str: str = "".join(
                    parameter_node_name_regex_list
                    + [FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY]
                )
                if (
                    re.match(
                        attributed_value_key_regex_str, fully_qualified_parameter_name
                    )
                    is not None
                ):
                    attributed_value: Any = parameter_value

        expectation_configurations: list[
            ExpectationConfiguration
        ] = self.expectation_suite.expectations

        self.data_assistant_cls._plot(
            self=self.data_assistant_cls,
            metric_names=metric_names,
            attributed_value=attributed_value,
            prescriptive=prescriptive,
            expectation_configurations=expectation_configurations,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
