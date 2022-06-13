from dataclasses import make_dataclass
from typing import Any, Dict, List, Set, Tuple, Union

import numpy as np
import pandas as pd

MetricValue = Union[
    Any, List[Any], Set[Any], Tuple[Any, ...], pd.DataFrame, pd.Series, np.ndarray
]
MetricValues = Union[
    MetricValue, List[MetricValue], Set[MetricValue], Tuple[MetricValue, ...]
]
MetricComputationDetails = Dict[str, Any]
MetricComputationResult = make_dataclass(
    "MetricComputationResult", ["attributed_resolved_metrics", "details"]
)
