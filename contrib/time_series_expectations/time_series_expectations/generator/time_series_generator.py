import pandas as pd
import numpy as np

from abc import ABC

class TimeSeriesGenerator(ABC):
    """Base class for time series generators."""

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
        raise NotImplementedError()