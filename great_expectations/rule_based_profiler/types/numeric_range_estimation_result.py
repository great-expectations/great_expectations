from dataclasses import asdict, dataclass

import numpy as np

from great_expectations.types import DictDot

NUM_HISTOGRAM_BINS: int = 10


@dataclass(frozen=True)
class NumericRangeEstimationResult(DictDot):
    '\n    NumericRangeEstimationResult is a "dataclass" object, designed to hold results of executing numeric range estimator\n    for multidimensional datasets, which consist of "estimation_histogram" and "value_range" for each numeric dimension.\n'
    estimation_histogram: np.ndarray
    value_range: np.ndarray

    def to_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Returns: this NumericRangeEstimationResult as a dictionary"
        return asdict(self)
