from typing import Any, Dict, List, Set, Tuple, Union

import numpy as np
import pandas as pd

MetricValue = Union[
    Any,  # Encompasses deferred-query/execution plans ("SQLAlchemy" and "Spark") conditions and aggregation functions.
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
