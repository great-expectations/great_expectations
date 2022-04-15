import re
from abc import ABCMeta
from dataclasses import asdict, dataclass
from numbers import Number
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

    data_assistant_cls: ABCMeta
    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics: Optional[Dict[Domain, Dict[str, Any]]] = None
    expectation_configurations: Optional[List[ExpectationConfiguration]] = None
    expectation_suite: Optional[
        ExpectationSuite
    ] = None  # Obtain "meta/details" using "meta = expectation_suite.meta".
    execution_time: Optional[float] = None  # Execution time (in seconds).

    def plot(self, prescriptive: bool = False):
        metrics: Dict[Dict, ParameterNode] = self.metrics
        metric_names: List[str] = []
        metric_domain: Domain
        metric_nodes: Dict[str, ParameterNode]
        parameter_node_name: str
        parameter_node: ParameterNode

        parameter_node_name_regex_list: list = [
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_DELIMITER_CHARACTER),
            PARAMETER_NAME_ROOT_FOR_PARAMETERS,
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER),
            "\\w+",
            re.escape(FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER),
        ]

        for metric_domain, metric_nodes in metrics.items():
            for parameter_node_name, parameter_node in metric_nodes.items():
                details_key_regex_str: str = "".join(
                    parameter_node_name_regex_list
                    + [FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]
                )
                if re.match(details_key_regex_str, parameter_node_name) is not None:
                    details_parameter_node: ParameterNode = parameter_node
                    metric_names.append(
                        details_parameter_node.metric_configuration.metric_name
                    )

                attributed_value_key_regex_str: str = "".join(
                    parameter_node_name_regex_list
                    + [FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY]
                )
                if (
                    re.match(attributed_value_key_regex_str, parameter_node_name)
                    is not None
                ):
                    attributed_value_parameter_node: ParameterNode = parameter_node
                    data: list[Number] = sum(
                        attributed_value_parameter_node.values(), []
                    )

        expectation_configurations: list[
            ExpectationConfiguration
        ] = self.expectation_configurations

        self.data_assistant_cls._plot(
            self=self.data_assistant_cls,
            metric_names=metric_names,
            data=data,
            prescriptive=prescriptive,
            expectation_configurations=expectation_configurations,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
