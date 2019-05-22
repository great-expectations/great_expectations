import pandas as pd
import os

from .base_source import DataSource
from ..dataset.pandas_dataset import PandasDataset


class PandasCSVDataSource(DataSource):
    """
    A PandasCSVDataContext makes it easy to create, manage and validate expectations on
    a Pandas dataframe loaded from a CSV file.
    Its get_dataset method returns a new Pandas dataset with the provided name.

    """

    def __init__(self, *args, **kwargs):
        super(PandasCSVDataSource, self).__init__(*args, **kwargs)

    def get_data_asset(self, data_asset_name, file_path, *args, **kwargs):
        data_context = kwargs.pop("data_context", None)
        df = pd.read_csv(file_path, *args, **kwargs)
        return PandasDataset(df, data_context=data_context, data_asset_name=data_asset_name)
