from dataclasses import asdict, dataclass
from typing import List, Union

import numpy as np

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import DictDot

NUM_HISTOGRAM_BINS: int = 10  # Equal to "numpy.histogram()" default (can be turned into configurable argument).


@dataclass(frozen=True)
class NumericRangeEstimationResult(DictDot):
    """
    NumericRangeEstimationResult is a "dataclass" object, designed to hold results of executing numeric range estimator
    for multidimensional datasets, which consist of "estimation_histogram" and "value_range" for each numeric dimension.

    In particular, "estimation_histogram" is "numpy.ndarray" of shape [2, NUM_HISTOGRAM_BINS + 1], containing
    [0] "histogram": (integer array of dimension [NUM_HISTOGRAM_BINS + 1] padded with 0 at right edge) histogram values;
    [1] "bin_edges": (float array of dimension [NUM_HISTOGRAM_BINS + 1]) binning edges.
    """

    estimation_histogram: np.ndarray
    value_range: Union[np.ndarray, List[np.float64]]

    def to_dict(self) -> dict:
        """Returns: this NumericRangeEstimationResult as a dictionary"""
        return asdict(self)

    def to_json_dict(self) -> dict:
        """Returns: this NumericRangeEstimationResult as a JSON dictionary"""
        return convert_to_json_serializable(data=self.to_dict())
