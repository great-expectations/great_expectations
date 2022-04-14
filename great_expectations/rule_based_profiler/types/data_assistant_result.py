from dataclasses import asdict, dataclass
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

    profiler_config: Optional["RuleBasedProfilerConfig"] = None  # noqa: F821
    metrics: Optional[Dict[Domain, Dict[str, Any]]] = None
    expectation_configurations: Optional[List[ExpectationConfiguration]] = None
    expectation_suite: Optional[
        ExpectationSuite
    ] = None  # Obtain "meta/details" using "meta = expectation_suite.meta".
    execution_time: Optional[float] = None  # Execution time (in seconds).

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
