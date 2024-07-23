from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple, Union

import pandas as pd

import numpy as np

MetricValue = Union[
    Any,  # Encompasses deferred-query/execution plans ("SQLAlchemy" and "Spark") conditions and aggregation functions.  # noqa: E501
    List[Any],
    Set[Any],
    Tuple[Any, ...],
    pd.DataFrame,
    pd.Series,
    np.ndarray,
    int,
    str,
    float,
    bool,
    Dict[str, Any],
]
