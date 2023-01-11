from __future__ import annotations

from typing import Any, Set, Tuple, Union

import numpy as np
import pandas as pd

MetricValue = Union[
    Any,  # Encompasses deferred-query/execution plans ("SQLAlchemy" and "Spark") conditions and aggregation functions.
    list[Any],
    Set[Any],
    Tuple[Any, ...],
    pd.DataFrame,
    pd.Series,
    np.ndarray,
    int,
    str,
    float,
    bool,
    dict[str, Any],
]
