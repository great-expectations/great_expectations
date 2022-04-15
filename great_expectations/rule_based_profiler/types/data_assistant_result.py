from abc import ABCMeta
from dataclasses import asdict, dataclass
from numbers import Number
from typing import Any, Dict, List, Optional

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import Domain
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
        attributed_value: str = self.metrics[Domain(domain_type="table")][
            "$parameter_table_row_count.attributed_value"
        ]

        data: list[Number] = sum(
            self.metrics[
                Domain(
                    domain_type="table",
                )
            ][attributed_value].values(),
            [],
        )

        self.data_assistant_cls._plot(data=data)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())

    def _plot(self):
        pass
