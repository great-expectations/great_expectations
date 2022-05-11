from dataclasses import asdict, dataclass

import numpy as np

from great_expectations.types import DictDot

NUM_HISTOGRAM_BINS: int = 10  # Equal to "numpy.histogram()" default (can be turned into configurable argument).


@dataclass(frozen=True)
class NumericRangeEstimationResult(DictDot):
    """
    NumericRangeEstimationResult is a "dataclass" object, designed to hold results of executing numeric range estimator
    for multidimensional datasets, which consist of "estimation_histogram" and "value_range" for each numeric dimension.
    """

    estimation_histogram: np.ndarray
    value_range: np.ndarray

    def to_dict(self) -> dict:
        """Returns: this NumericRangeEstimationResult as a dictionary"""
        return asdict(self)
