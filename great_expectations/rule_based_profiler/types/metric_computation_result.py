from dataclasses import make_dataclass
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd

MetricValue = Union[Any, List[Any], pd.DataFrame, pd.Series, np.ndarray]
MetricValues = Union[MetricValue, pd.DataFrame, pd.Series, np.ndarray]
MetricComputationDetails = Dict[str, Any]
MetricComputationResult = make_dataclass(
    "MetricComputationResult", ["attributed_resolved_metrics", "details"]
)
