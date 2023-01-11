from __future__ import annotations

from dataclasses import make_dataclass
from typing import Any, Dict, List, Set, Tuple, Union

from great_expectations.validator.computed_metric import MetricValue

MetricValues = Union[
    MetricValue, list[MetricValue], Set[MetricValue], Tuple[MetricValue, ...]
]
MetricComputationDetails = dict[str, Any]
MetricComputationResult = make_dataclass(
    "MetricComputationResult", ["attributed_resolved_metrics", "details"]
)
