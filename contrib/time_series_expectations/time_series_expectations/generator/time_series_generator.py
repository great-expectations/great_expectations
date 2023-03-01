from abc import ABC, abstractmethod
from typing import TypedDict

import pandas as pd


class TrendParams(TypedDict):
    """Parameters for a trend segment of a time series.

    Each segment of a time series has a different alpha and beta value, corresponding to a different slope and intercept.

    Boundaries between segments are called "cutpoints."
    """

    alpha: float
    beta: float
    cutpoint: int


class TimeSeriesGenerator(ABC):
    """Base class for time series generators."""

    @abstractmethod
    def generate_df(
        self,
        *args,
        **kwargs,
    ) -> pd:
        """Generate a time series as a pandas DataFrame.

        Args:
            *args will differ depending on the specific generator class.

        Keyword Args:
            *kwargs will differ depending on the specific generator class.

        Returns:
            pd.DataFrame: A two-column pandas DataFrame with a datetime index and a column for the time series values

        """
        pass
